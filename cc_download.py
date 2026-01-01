#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
#     "duckdb",
# ]
# ///
"""
cc_download - Download files from Common Crawl for fuzzing corpus generation.

Downloads files matching a MIME type or extension from Common Crawl's web archive.
Supports three index sources:
- Local parquet files (fastest, requires ~250GB per crawl)
- AWS Athena (no setup, pay per query)
- Pre-generated CSV file
"""

import argparse
import csv
import gzip
import os
import sys
import threading
import time
from io import BytesIO
from pathlib import Path

import duckdb
import requests


# Thread-safe shared state
index: list[dict] = []
index_lock = threading.Lock()

downloaded_count = 0
skipped_count = 0
stats_lock = threading.Lock()

start_time: float = 0
exiting = False


def signal_handler(sig, frame):
    """Handle Ctrl+C for graceful shutdown."""
    global exiting
    print("\nexiting gracefully...")
    exiting = True


def format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


# --- Athena Backend ---


def run_athena_query(
    athena_client,
    query: str,
    database: str,
    output_location: str,
) -> str:
    """Run an Athena query and return the query execution ID."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    return response["QueryExecutionId"]


def wait_for_query(athena_client, query_id: str, timeout: int = 300) -> str:
    """Wait for Athena query to complete. Returns status."""
    start = time.time()
    while time.time() - start < timeout:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return state
        elif state in ("FAILED", "CANCELLED"):
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            raise RuntimeError(f"Query {state}: {reason}")

        time.sleep(2)

    raise RuntimeError(f"Query timed out after {timeout}s")


def get_query_results(athena_client, query_id: str) -> list[dict]:
    """Fetch all results from a completed Athena query."""
    results = []
    paginator = athena_client.get_paginator("get_query_results")
    headers = None

    for page in paginator.paginate(QueryExecutionId=query_id):
        rows = page["ResultSet"]["Rows"]
        if headers is None:
            # First page - extract headers
            headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
            rows = rows[1:]  # Skip header row

        for row in rows:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            results.append(dict(zip(headers, values)))

    return results


def build_athena_query(
    mime_type: str | None,
    extension: str | None,
    crawl: str,
    max_size: int,
    limit: int | None,
) -> str:
    """Build Athena SQL query for Common Crawl index."""
    conditions = [
        f"crawl = '{crawl}'",
        "subset = 'warc'",
        f"warc_record_length < {max_size}",
    ]

    if mime_type:
        conditions.append(f"content_mime_detected = '{mime_type}'")

    if extension:
        ext = extension.lstrip(".")
        conditions.append(
            f"(url LIKE '%.{ext}' OR url LIKE '%.{ext}?%' OR url LIKE '%.{ext}#%')"
        )

    query = f"""
SELECT
    url,
    warc_filename,
    warc_record_offset,
    warc_record_length
FROM ccindex.ccindex
WHERE {' AND '.join(conditions)}
"""
    if limit:
        query += f"LIMIT {limit}"
    return query


def query_athena(args) -> list[dict]:
    """Query Common Crawl index via AWS Athena."""
    print(f"Querying Athena for files in {args.crawl}...")
    session = boto3.Session()
    athena = session.client("athena")

    query = build_athena_query(
        args.mime, args.extension, args.crawl, args.max_file_size, args.limit
    )
    print(f"Query:\n{query}\n")

    query_id = run_athena_query(
        athena, query, args.athena_database, args.athena_output
    )
    print(f"Query ID: {query_id}")
    print("Waiting for query to complete...", end="", flush=True)

    wait_for_query(athena, query_id)
    print(" done.")

    print("Fetching results...")
    return get_query_results(athena, query_id)


# --- Local DuckDB Backend ---


def build_local_query(
    mime_type: str | None,
    extension: str | None,
    max_size: int,
    limit: int | None,
) -> str:
    """Build DuckDB SQL query for local parquet files."""
    conditions = [f"warc_record_length < {max_size}"]

    if mime_type:
        conditions.append(f"content_mime_detected = '{mime_type}'")

    if extension:
        ext = extension.lstrip(".")
        conditions.append(
            f"(url LIKE '%.{ext}' OR url LIKE '%.{ext}?%' OR url LIKE '%.{ext}#%')"
        )

    query = f"""
SELECT
    url,
    warc_filename,
    warc_record_offset,
    warc_record_length
FROM parquet_scan
WHERE {' AND '.join(conditions)}
"""
    if limit:
        query += f"LIMIT {limit}"
    return query


def query_local_index(args) -> list[dict]:
    """Query Common Crawl index from local parquet files using DuckDB."""
    index_path = Path(args.local_index)

    if not index_path.exists():
        print(f"error: local index not found: {index_path}")
        sys.exit(1)

    # Find parquet files
    parquet_files = list(index_path.glob("*.parquet"))
    if not parquet_files:
        # Try subdirectories
        parquet_files = list(index_path.glob("**/*.parquet"))

    if not parquet_files:
        print(f"error: no parquet files found in {index_path}")
        print(f"\nTo download the index for a crawl:")
        print(f"  curl -s 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/cc-index-table.paths.gz' | \\")
        print(f"    gunzip | grep 'subset=warc' | \\")
        print(f"    xargs -I{{}} -P4 curl -sO 'https://data.commoncrawl.org/{{}}'")
        sys.exit(1)

    print(f"Found {len(parquet_files)} parquet files in {index_path}")
    print("Querying with DuckDB...")

    # Build query
    query_template = build_local_query(
        args.mime, args.extension, args.max_file_size, args.limit
    )

    # Create DuckDB connection and query
    con = duckdb.connect(":memory:")

    # Use glob pattern for parquet_scan
    glob_pattern = str(index_path / "**/*.parquet")
    query = query_template.replace("parquet_scan", f"read_parquet('{glob_pattern}')")

    print(f"Query:\n{query}\n")

    start = time.time()
    result = con.execute(query).fetchall()
    elapsed = time.time() - start

    print(f"Query completed in {elapsed:.1f}s")

    # Convert to list of dicts
    columns = ["url", "warc_filename", "warc_record_offset", "warc_record_length"]
    return [dict(zip(columns, row)) for row in result]


# --- Preprocessed DuckDB Backend ---


def build_duckdb_query(
    mime_type: str | None,
    extension: str | None,
    max_size: int,
    limit: int | None,
) -> str:
    """Build SQL query for preprocessed DuckDB index."""
    conditions = [f"warc_record_length < {max_size}"]

    if mime_type:
        conditions.append(f"content_mime_detected = '{mime_type}'")

    if extension:
        ext = extension.lstrip(".")
        conditions.append(f"extension = '{ext}'")

    query = f"""
SELECT
    url,
    warc_filename,
    warc_record_offset,
    warc_record_length
FROM cc_index
WHERE {' AND '.join(conditions)}
"""
    if limit:
        query += f"LIMIT {limit}"
    return query


def query_duckdb_index(args) -> list[dict]:
    """Query preprocessed DuckDB index file."""
    db_path = Path(args.duckdb)

    if not db_path.exists():
        print(f"error: DuckDB file not found: {db_path}")
        sys.exit(1)

    print(f"Querying {db_path}...")

    con = duckdb.connect(str(db_path), read_only=True)

    query = build_duckdb_query(
        args.mime, args.extension, args.max_file_size, args.limit
    )

    start = time.time()
    result = con.execute(query).fetchall()
    elapsed = time.time() - start

    print(f"Query completed in {elapsed:.2f}s")

    con.close()

    columns = ["url", "warc_filename", "warc_record_offset", "warc_record_length"]
    return [dict(zip(columns, row)) for row in result]


# --- CSV Backend ---


def load_csv_index(csv_path: str) -> list[dict]:
    """Load index from CSV file."""
    results = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


# --- Download Logic ---


def estimate_download(index_data: list[dict]) -> tuple[int, int]:
    """Estimate total download count and size."""
    count = len(index_data)
    total_size = 0

    for item in index_data:
        try:
            total_size += int(item.get("warc_record_length", 0))
        except (ValueError, TypeError):
            pass

    return count, total_size


def download_worker(
    thread_id: int,
    args: argparse.Namespace,
    session: requests.Session,
) -> None:
    """Worker thread: fetch files via HTTPS and save them."""
    global downloaded_count, skipped_count, exiting

    output_dir = Path(args.output_dir)

    while not exiting:
        # Get next item from index (thread-safe)
        with index_lock:
            if not index:
                break
            item = index.pop()

        warc_path = item.get("warc_filename", "")
        try:
            warc_offset = int(item.get("warc_record_offset", 0))
            warc_length = int(item.get("warc_record_length", 0))
        except (ValueError, TypeError):
            continue

        if not warc_path or warc_length == 0:
            continue

        url = f"https://data.commoncrawl.org/{warc_path}"
        headers = {"Range": f"bytes={warc_offset}-{warc_offset + warc_length - 1}"}

        # Fetch via HTTPS with exponential backoff
        sleep_sec = 1
        response = None
        while not exiting:
            try:
                response = session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                break
            except Exception:
                print(f"|{thread_id}-{sleep_sec}|", end="", flush=True)
                time.sleep(sleep_sec)
                if sleep_sec < 1024:
                    sleep_sec *= 2

        if exiting or not response:
            break

        # Extract file from WARC record
        # WARC format: gzip(WARC-headers \r\n\r\n HTTP-headers \r\n\r\n body)
        try:
            decompressed = gzip.decompress(response.content)
            # Split into WARC headers, HTTP headers, and body
            parts = decompressed.split(b'\r\n\r\n', 2)
            if len(parts) < 3:
                raise ValueError("Invalid WARC record structure")
            file_data = parts[2]
        except Exception:
            with stats_lock:
                skipped_count += 1
            print("x", end="", flush=True)
            continue

        # Generate output filename
        with stats_lock:
            file_id = downloaded_count + 1
            downloaded_count += 1

        output_path = output_dir / f"file{file_id:06d}.{args.file_format}"
        output_path.write_bytes(file_data)

        print(".", end="", flush=True)

    print(f"\nthread {thread_id} finished")


def main() -> None:
    global index, start_time, exiting

    import signal

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="Download files from Common Crawl for fuzzing corpus generation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query preprocessed DuckDB index (fastest)
  %(prog)s --duckdb cc-2024-51.duckdb --extension qoi \\
      --file-format qoi --output-dir corpus/

  # Query local parquet index for QOI files by extension
  %(prog)s --local-index ./cc-index/CC-MAIN-2024-51/ --extension qoi \\
      --file-format qoi --output-dir corpus/

  # Query Athena for PNG files by MIME type
  %(prog)s --mime image/png --file-format png \\
      --athena-output s3://my-bucket/athena/ --output-dir corpus/

  # Use existing CSV index
  %(prog)s --csv index.csv --file-format pdf --output-dir corpus/

  # Estimate only (don't download)
  %(prog)s --duckdb cc-2024-51.duckdb --extension qoi \\
      --file-format qoi --estimate-only
""",
    )

    # Input source (mutually exclusive)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--csv",
        metavar="FILE",
        help="CSV file with Common Crawl index data",
    )
    source.add_argument(
        "--local-index",
        metavar="DIR",
        help="Directory containing parquet files from Common Crawl index",
    )
    source.add_argument(
        "--athena",
        action="store_true",
        help="Query AWS Athena (requires --athena-output)",
    )
    source.add_argument(
        "--duckdb",
        metavar="FILE",
        help="Preprocessed DuckDB index file (from cc_preprocess.py)",
    )

    # Query filters
    filter_group = parser.add_argument_group("Query filters")
    filter_group.add_argument(
        "--mime",
        metavar="TYPE",
        help="Filter by MIME type (e.g., image/png, application/pdf)",
    )
    filter_group.add_argument(
        "--extension",
        metavar="EXT",
        help="Filter by file extension (e.g., qoi, png, pdf)",
    )
    filter_group.add_argument(
        "--limit",
        type=int,
        help="Maximum number of files to query/download",
    )
    filter_group.add_argument(
        "--max-file-size",
        type=int,
        default=1048576,
        help="Maximum file size in bytes (default: 1MB)",
    )

    # Athena options
    athena_group = parser.add_argument_group("Athena options")
    athena_group.add_argument(
        "--crawl",
        default="CC-MAIN-2024-51",
        help="Common Crawl crawl ID (default: CC-MAIN-2024-51)",
    )
    athena_group.add_argument(
        "--athena-database",
        default="ccindex",
        help="Athena database name (default: ccindex)",
    )
    athena_group.add_argument(
        "--athena-output",
        metavar="S3_URI",
        help="S3 location for Athena query results (required with --athena)",
    )

    # Output options
    output_group = parser.add_argument_group("Output options")
    output_group.add_argument(
        "--file-format",
        required=True,
        help="File extension for downloaded files (e.g., pdf, png, qoi)",
    )
    output_group.add_argument(
        "--output-dir",
        default="corpus",
        help="Output directory for downloaded files (default: corpus)",
    )

    # Control options
    control_group = parser.add_argument_group("Control options")
    control_group.add_argument(
        "--threads",
        type=int,
        default=16,
        help="Number of download threads (default: 16)",
    )
    control_group.add_argument(
        "--estimate-only",
        action="store_true",
        help="Show estimated download count and size, then exit",
    )
    control_group.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.athena and not args.athena_output:
        parser.error("--athena-output is required when using --athena")

    if not args.csv and not args.duckdb and not args.mime and not args.extension:
        parser.error("at least one of --mime or --extension is required")

    # Load or query index
    if args.csv:
        print(f"Loading index from {args.csv}...")
        index_data = load_csv_index(args.csv)
    elif args.duckdb:
        index_data = query_duckdb_index(args)
    elif args.local_index:
        index_data = query_local_index(args)
    else:  # args.athena
        index_data = query_athena(args)

    # Estimate download
    count, total_size = estimate_download(index_data)
    print(f"\nEstimate:")
    print(f"  Files: {count:,}")
    print(f"  Total size: {format_bytes(total_size)}")

    if args.estimate_only:
        return

    if count == 0:
        print("No files to download.")
        return

    # Confirm download
    if not args.yes:
        response = input(f"\nDownload {count:,} files to {args.output_dir}/? [y/N] ")
        if response.lower() != "y":
            print("Aborted.")
            return

    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Populate shared index
    index = index_data.copy()

    # Create requests session for connection pooling
    session = requests.Session()

    start_time = time.time()
    print(f"\nDownloading with {args.threads} threads...")

    # Start worker threads
    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=download_worker, args=(i + 1, args, session))
        t.start()
        threads.append(t)

    # Wait for all threads
    for t in threads:
        t.join()

    # Print summary
    elapsed = time.time() - start_time
    print(f"\nDone!")
    print(f"  Downloaded: {downloaded_count:,} files")
    print(f"  Skipped: {skipped_count:,} files")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Output: {args.output_dir}/")


if __name__ == "__main__":
    main()
