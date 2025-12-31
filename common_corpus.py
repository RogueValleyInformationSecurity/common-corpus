#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "boto3",
#     "warcio",
# ]
# ///
"""
Common Corpus - Build coverage-minimized corpus data sets for fuzzing.

Downloads files from Common Crawl, runs them through a SanitizerCoverage-enabled
binary, and keeps only files that produce new code coverage.
"""

import argparse
import csv
import json
import os
import shlex
import signal
import sys
import threading
import time
from glob import glob
from pathlib import Path
from subprocess import DEVNULL, Popen

import boto3
import warcio

# Thread-safe shared state
index: list[list[str]] = []
index_lock = threading.Lock()
index_reader = None  # csv.reader instance

coverage: set[int] = set()
coverage_lock = threading.Lock()

corpus_id = 1
id_lock = threading.Lock()

stats_lock = threading.Lock()
tested_count = 0
start_time: float = 0

exiting = False


def signal_handler(sig, frame):
    """Handle Ctrl+C for graceful shutdown."""
    global exiting
    print("\nexiting gracefully...")
    exiting = True


def save_state(args: argparse.Namespace, index_file) -> None:
    """Save current state for resumption."""
    print("saving state")
    state = {
        "index_offset": index_file.tell(),
        "corpus_id": corpus_id,
        "tested_count": tested_count,
        "coverage": list(coverage),
    }
    with open(args.state_file, "w") as f:
        json.dump(state, f)


def load_state(args: argparse.Namespace, index_file) -> None:
    """Load state from previous run."""
    global corpus_id, tested_count, coverage
    print("loading state")
    with open(args.resume, "r") as f:
        state = json.load(f)

    index_file.seek(state["index_offset"])
    corpus_id = state["corpus_id"]
    tested_count = state["tested_count"]
    coverage = set(state["coverage"])


def refill_index() -> bool:
    """Refill the index buffer from CSV. Returns False if no more data."""
    global index_reader
    try:
        for _ in range(4096):
            row = next(index_reader)
            index.append(row)
    except StopIteration:
        if len(index) == 0:
            return False
    return True


def print_stats() -> None:
    """Print progress statistics."""
    elapsed = time.time() - start_time
    with stats_lock:
        count = tested_count
    with coverage_lock:
        edges = len(coverage)
    rate = count / elapsed if elapsed > 0 else 0
    print(f"\n[stats] tested={count} edges={edges} rate={rate:.1f}/s elapsed={elapsed:.0f}s")


def common_corpus(thread_id: int, args: argparse.Namespace) -> None:
    """Worker thread: fetch files from S3, run target, collect coverage."""
    global corpus_id, exiting, tested_count

    # Create S3 client (uses env vars if keys not provided)
    s3_kwargs = {}
    if args.aws_access_key:
        s3_kwargs["aws_access_key_id"] = args.aws_access_key
    if args.aws_secret_key:
        s3_kwargs["aws_secret_access_key"] = args.aws_secret_key
    s3 = boto3.Session().client("s3", **s3_kwargs)

    test_file = Path(f"test{thread_id}.{args.file_format}")
    output_dir = Path(args.output_dir)

    asan_env = os.environ.copy()
    asan_env["ASAN_OPTIONS"] = "coverage=1"

    last_stats_time = time.time()

    while not exiting:
        # Get next item from index (thread-safe)
        with index_lock:
            try:
                item = index.pop()
            except IndexError:
                if not refill_index():
                    print(f"\nthread {thread_id} finished")
                    break
                continue

        # Skip header row
        if len(item) < 4 or item[3] == "length":
            continue

        warc_path = item[1]

        try:
            warc_offset = int(item[2])
            warc_length = int(item[3])
        except (ValueError, IndexError):
            continue

        warc_range = f"bytes={warc_offset}-{warc_offset + warc_length - 1}"

        # Fetch from S3 with exponential backoff
        sleep_sec = 1
        while True:
            try:
                obj = s3.get_object(Bucket="commoncrawl", Key=warc_path, Range=warc_range)
                break
            except Exception:
                print(f"|{thread_id}-{sleep_sec}|", end="", flush=True)
                time.sleep(sleep_sec)
                if sleep_sec < 1024:
                    sleep_sec *= 2
                if exiting:
                    break

        if exiting:
            break

        stream = obj["Body"]
        record = next(warcio.ArchiveIterator(stream))

        try:
            file_data = record.content_stream().read()
        except Exception:
            print("error reading file data")
            exiting = True
            break

        # Write test file
        test_file.write_bytes(file_data)

        # Run target binary
        cmd_line = args.target_cmdline.replace("{}", str(test_file))
        cmd_args = shlex.split(cmd_line)

        p = Popen(cmd_args, env=asan_env, stdout=DEVNULL, stderr=DEVNULL)
        p.wait()

        sancov_file = Path(f"{args.target_binary}.{p.pid}.sancov")

        if exiting:
            if sancov_file.exists():
                sancov_file.unlink()
            break

        try:
            sancov_data = sancov_file.read_bytes()
        except OSError:
            if not exiting:
                print("error: sancov file missing")
                exiting = True
            break

        if len(sancov_data) % 8 != 0:
            print("error: malformed sancov file")
            sys.exit(-1)

        sancov_len = len(sancov_data) // 8

        # Check for new coverage
        unique = False
        with coverage_lock:
            for i in range(1, sancov_len):
                edge = int.from_bytes(sancov_data[8 * i : 8 * i + 8], "little")
                if edge not in coverage:
                    coverage.add(edge)
                    unique = True

        if unique:
            with id_lock:
                unique_id = corpus_id
                corpus_id += 1

            unique_path = output_dir / f"corpus{unique_id}.{args.file_format}"
            unique_sancov = output_dir / f"corpus{unique_id}.{args.file_format}.sancov"

            test_file.rename(unique_path)
            sancov_file.rename(unique_sancov)

            print("+", end="", flush=True)
        else:
            sancov_file.unlink()
            print(".", end="", flush=True)

        with stats_lock:
            tested_count += 1

        # Print stats periodically in verbose mode
        if args.verbose and time.time() - last_stats_time > 30:
            print_stats()
            last_stats_time = time.time()

    # Cleanup
    if test_file.exists():
        test_file.unlink()

    if args.cleanup_glob:
        for f in glob(args.cleanup_glob):
            Path(f).unlink()


def main() -> None:
    global index_reader, start_time, exiting

    parser = argparse.ArgumentParser(
        description="Build coverage-minimized corpus from Common Crawl data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s index.csv --target-cmdline './pdfium_test --ppm {}' \\
      --target-binary pdfium_test --file-format pdf

  %(prog)s index.csv --target-cmdline './pdfium_test --ppm {}' \\
      --target-binary pdfium_test --file-format pdf \\
      --cleanup-glob '*.ppm' --threads 8 --verbose
""",
    )

    parser.add_argument("index_csv", help="CSV file with Common Crawl index data")
    parser.add_argument(
        "--target-cmdline",
        required=True,
        help="Command line for sancov-enabled binary, use {} as testcase placeholder",
    )
    parser.add_argument(
        "--target-binary",
        required=True,
        help="Name of the sancov-enabled binary (for locating .sancov files)",
    )
    parser.add_argument(
        "--file-format",
        required=True,
        help="File extension for corpus files (e.g., pdf, png)",
    )
    parser.add_argument(
        "--aws-access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help="AWS access key (default: $AWS_ACCESS_KEY_ID)",
    )
    parser.add_argument(
        "--aws-secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help="AWS secret key (default: $AWS_SECRET_ACCESS_KEY)",
    )
    parser.add_argument(
        "--cleanup-glob",
        default="",
        help="Glob pattern for files to delete after each run",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=16,
        help="Number of worker threads (default: 16)",
    )
    parser.add_argument(
        "--output-dir",
        default="out",
        help="Output directory for corpus files (default: out)",
    )
    parser.add_argument(
        "--resume",
        metavar="STATE_FILE",
        help="Resume from a saved state file",
    )
    parser.add_argument(
        "--state-file",
        default="state.dat",
        help="Path to save state file (default: state.dat)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print periodic progress statistics",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and exit without processing",
    )

    args = parser.parse_args()

    # Validate target binary exists
    target_path = shlex.split(args.target_cmdline)[0]
    if not Path(target_path).exists():
        # Check if it's in PATH
        import shutil

        if not shutil.which(target_path):
            print(f"error: target binary not found: {target_path}")
            sys.exit(1)

    # Setup output directory
    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    elif not output_dir.is_dir():
        print(f"error: {args.output_dir} is not a directory")
        sys.exit(1)

    # Validate index CSV exists
    if not Path(args.index_csv).exists():
        print(f"error: index CSV not found: {args.index_csv}")
        sys.exit(1)

    if args.dry_run:
        print("Configuration validated successfully:")
        print(f"  Index CSV: {args.index_csv}")
        print(f"  Target: {args.target_cmdline}")
        print(f"  Binary: {args.target_binary}")
        print(f"  Format: {args.file_format}")
        print(f"  Threads: {args.threads}")
        print(f"  Output: {args.output_dir}")
        if args.cleanup_glob:
            print(f"  Cleanup: {args.cleanup_glob}")
        sys.exit(0)

    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    # Open index CSV
    index_file = open(args.index_csv, newline="")
    index_reader = csv.reader(index_file)

    # Skip header row
    next(index_reader, None)

    # Load saved state if resuming
    if args.resume:
        load_state(args, index_file)

    # Initial fill of index buffer
    if not refill_index():
        print("index csv is empty")
        sys.exit(1)

    start_time = time.time()

    # Start worker threads
    threads = []
    for i in range(1, args.threads + 1):
        t = threading.Thread(target=common_corpus, args=(i, args))
        t.start()
        threads.append(t)

    # Wait for all threads
    for t in threads:
        t.join()

    # Print final stats
    if args.verbose:
        print_stats()

    # Save state
    save_state(args, index_file)
    index_file.close()


if __name__ == "__main__":
    main()
