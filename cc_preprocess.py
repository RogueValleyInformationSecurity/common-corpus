#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "duckdb",
# ]
# ///
"""
cc_preprocess - Build a compact DuckDB index from Common Crawl parquet files.

Filters out HTML content and extracts only the columns needed for downloading,
reducing 196GB of parquet files to a ~2-4GB DuckDB database.
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb


def extract_extension_sql() -> str:
    """SQL expression to extract file extension from URL."""
    # Extract the path part, get the last component, find extension
    # Handles: .ext, .ext?query, .ext#fragment
    return """
        CASE
            WHEN regexp_extract(url, '\\.([a-zA-Z0-9]{1,10})(?:[?#]|$)', 1) != ''
            THEN lower(regexp_extract(url, '\\.([a-zA-Z0-9]{1,10})(?:[?#]|$)', 1))
            ELSE NULL
        END
    """


def build_index(
    input_dir: Path,
    output_file: Path,
    exclude_html: bool = True,
    verbose: bool = False,
) -> int:
    """Build DuckDB index from parquet files. Returns row count."""

    # Find parquet files
    parquet_files = list(input_dir.glob("*.parquet"))
    if not parquet_files:
        parquet_files = list(input_dir.glob("**/*.parquet"))

    if not parquet_files:
        print(f"error: no parquet files found in {input_dir}")
        sys.exit(1)

    print(f"Found {len(parquet_files)} parquet files in {input_dir}")

    # Build filter conditions
    conditions = []
    if exclude_html:
        conditions.append(
            "content_mime_detected NOT IN ('text/html', 'application/xhtml+xml')"
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Build query
    glob_pattern = str(input_dir / "**/*.parquet")
    query = f"""
        CREATE TABLE cc_index AS
        SELECT
            url,
            {extract_extension_sql()} AS extension,
            content_mime_detected,
            warc_filename,
            warc_record_offset,
            warc_record_length
        FROM read_parquet('{glob_pattern}')
        {where_clause}
    """

    if verbose:
        print(f"Query:\n{query}\n")

    # Remove existing output file
    if output_file.exists():
        output_file.unlink()

    # Create database and run query
    print(f"Building index (this may take a while)...")
    start = time.time()

    con = duckdb.connect(str(output_file))

    # Enable progress bar for long operations
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_progress_bar_print = true")

    con.execute(query)

    # Get row count
    row_count = con.execute("SELECT COUNT(*) FROM cc_index").fetchone()[0]

    elapsed = time.time() - start
    print(f"Created table with {row_count:,} rows in {elapsed:.1f}s")

    # Add metadata
    con.execute("""
        CREATE TABLE metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        )
    """)

    metadata = [
        ("source_dir", str(input_dir)),
        ("created_at", datetime.now().isoformat()),
        ("exclude_html", str(exclude_html)),
        ("row_count", str(row_count)),
        ("parquet_files", str(len(parquet_files))),
    ]

    con.executemany(
        "INSERT INTO metadata (key, value) VALUES (?, ?)",
        metadata
    )

    # Create indexes for common query patterns
    print("Creating indexes...")
    con.execute("CREATE INDEX idx_mime ON cc_index(content_mime_detected)")
    con.execute("CREATE INDEX idx_extension ON cc_index(extension)")

    con.close()

    # Report file size
    file_size = output_file.stat().st_size
    print(f"Output: {output_file} ({file_size / 1024 / 1024 / 1024:.2f} GB)")

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a compact DuckDB index from Common Crawl parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build index from local parquet files
  %(prog)s ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

  # Include HTML content (not recommended - much larger)
  %(prog)s ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb --include-html
""",
    )

    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing Common Crawl parquet index files",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output DuckDB file path",
    )
    parser.add_argument(
        "--include-html",
        action="store_true",
        help="Include text/html and application/xhtml+xml (default: exclude)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed query information",
    )

    args = parser.parse_args()

    if not args.input_dir.exists():
        print(f"error: input directory not found: {args.input_dir}")
        sys.exit(1)

    build_index(
        args.input_dir,
        args.output,
        exclude_html=not args.include_html,
        verbose=args.verbose,
    )

    print("\nDone! Use with: cc_download.py --duckdb", args.output)


if __name__ == "__main__":
    main()
