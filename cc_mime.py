#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "duckdb",
# ]
# ///
"""
cc_mime - Find common MIME types for a file extension in Common Crawl index.

Queries the local parquet index to show what MIME types are most commonly
associated with a given file extension, helping you choose the right --mime
filter for cc_download.py.
"""

import argparse
import sys
from pathlib import Path

import duckdb


def query_mime_from_parquet(index_path: Path, extension: str, limit: int) -> list[tuple]:
    """Query raw parquet index for MIME types associated with an extension."""
    ext = extension.lstrip(".")

    con = duckdb.connect(":memory:")

    query = f"""
    WITH matching_urls AS (
        SELECT content_mime_detected
        FROM read_parquet('{index_path}/**/*.parquet')
        WHERE (url LIKE '%.{ext}' OR url LIKE '%.{ext}?%' OR url LIKE '%.{ext}#%')
          AND content_mime_detected IS NOT NULL
          AND content_mime_detected != ''
    )
    SELECT
        content_mime_detected as mime,
        COUNT(*) as count
    FROM matching_urls
    GROUP BY content_mime_detected
    ORDER BY count DESC
    LIMIT {limit}
    """

    return con.execute(query).fetchall()


def query_mime_from_duckdb(db_path: Path, extension: str, limit: int) -> list[tuple]:
    """Query preprocessed DuckDB index for MIME types associated with an extension."""
    ext = extension.lstrip(".")

    con = duckdb.connect(str(db_path), read_only=True)

    query = f"""
    SELECT
        content_mime_detected as mime,
        COUNT(*) as count
    FROM cc_index
    WHERE extension = '{ext}'
      AND content_mime_detected IS NOT NULL
      AND content_mime_detected != ''
    GROUP BY content_mime_detected
    ORDER BY count DESC
    LIMIT {limit}
    """

    return con.execute(query).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find common MIME types for a file extension in Common Crawl index.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --duckdb cc-2024-51.duckdb pdf     # Query preprocessed index (fast)
  %(prog)s --index ./cc-index/CC-MAIN-2024-51/ png  # Query raw parquet (slow)
  %(prog)s --duckdb cc.duckdb qoi --limit 20  # Show top 20 MIME types
""",
    )

    parser.add_argument(
        "extension",
        help="File extension to look up (e.g., pdf, png, qoi)",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--duckdb",
        metavar="FILE",
        help="Preprocessed DuckDB index file (fastest)",
    )
    source.add_argument(
        "--index", "-i",
        metavar="DIR",
        help="Path to raw Common Crawl parquet index",
    )

    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=10,
        help="Number of MIME types to show (default: 10)",
    )

    args = parser.parse_args()
    ext = args.extension.lstrip(".")

    if args.duckdb:
        db_path = Path(args.duckdb)
        if not db_path.exists():
            print(f"error: DuckDB file not found: {db_path}", file=sys.stderr)
            sys.exit(1)

        print(f"Querying {db_path} for .{ext} extension...\n")

        try:
            results = query_mime_from_duckdb(db_path, ext, args.limit)
        except Exception as e:
            print(f"error: query failed: {e}", file=sys.stderr)
            sys.exit(1)

        index_arg = f"--duckdb {args.duckdb}"
    else:
        index_path = Path(args.index)
        if not index_path.exists():
            print(f"error: index not found: {index_path}", file=sys.stderr)
            print(f"\nDownload the index first:", file=sys.stderr)
            print(f"  aws s3 sync --no-sign-request \\", file=sys.stderr)
            print(f"    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-51/subset=warc/ \\", file=sys.stderr)
            print(f"    ./cc-index/CC-MAIN-2024-51/", file=sys.stderr)
            sys.exit(1)

        parquet_files = list(index_path.glob("**/*.parquet"))
        if not parquet_files:
            print(f"error: no parquet files found in {index_path}", file=sys.stderr)
            sys.exit(1)

        print(f"Searching {len(parquet_files)} parquet files for .{ext} URLs...\n")

        try:
            results = query_mime_from_parquet(index_path, ext, args.limit)
        except Exception as e:
            print(f"error: query failed: {e}", file=sys.stderr)
            sys.exit(1)

        index_arg = f"--local-index {args.index}"

    if not results:
        print(f"No files found with .{ext} extension.")
        sys.exit(0)

    total = sum(count for _, count in results)

    print(f"MIME types for .{ext} files:\n")
    print(f"{'MIME Type':<50} {'Count':>12} {'%':>7}")
    print("-" * 71)

    for mime, count in results:
        pct = (count / total) * 100
        print(f"{mime:<50} {count:>12,} {pct:>6.1f}%")

    print("-" * 71)
    print(f"{'Total':<50} {total:>12,}")

    # Suggest the best MIME type if it's not text/html
    if results:
        top_mime, top_count = results[0]
        top_pct = (top_count / total) * 100

        print(f"\nSuggestion:")
        if top_mime in ("text/html", "application/xhtml+xml"):
            # Find first non-HTML type
            for mime, count in results:
                if mime not in ("text/html", "application/xhtml+xml"):
                    pct = (count / total) * 100
                    print(f"  Most .{ext} URLs serve HTML (likely error pages).")
                    print(f"  Best actual content type: {mime} ({pct:.1f}%)")
                    print(f"\n  ./cc_download.py {index_arg} --mime '{mime}' \\")
                    print(f"      --output-extension {ext} --output-dir corpus/")
                    break
            else:
                print(f"  Warning: .{ext} URLs mostly serve HTML - may not contain actual {ext} files.")
        else:
            print(f"  ./cc_download.py {index_arg} --mime '{top_mime}' \\")
            print(f"      --output-extension {ext} --output-dir corpus/")


if __name__ == "__main__":
    main()
