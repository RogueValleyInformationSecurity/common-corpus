# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tools for downloading files from Common Crawl's web archive for fuzzing corpus generation:

- **cc_preprocess.py** - Build a compact DuckDB index from raw parquet files (one-time, ~2-4GB output)
- **cc_download.py** - Download files matching MIME type or extension from the index

The tools focus solely on downloading—corpus minimization is left to specialized tools like `afl-cmin`.

## Running the Tools

Both scripts use `uv run --script` with inline dependencies (PEP 723):

```bash
# 1. Preprocess parquet index into DuckDB (one-time, ~30 min)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

# 2. Query and download (sub-second queries)
./cc_download.py --duckdb cc-2024-51.duckdb --search-extension qoi --output-dir corpus/

# Estimate only (no download)
./cc_download.py --duckdb cc-2024-51.duckdb --search-extension qoi --estimate-only
```

## CLI Arguments

### Input Source (one required)

| Argument | Description |
|----------|-------------|
| `--duckdb FILE` | Preprocessed DuckDB index (fastest, recommended) |
| `--local-index DIR` | Raw parquet files (~250GB per crawl, slow) |
| `--csv FILE` | CSV file with Common Crawl index data |
| `--athena` | Query AWS Athena (requires `--athena-output`) |

### Query Filters

| Argument | Description |
|----------|-------------|
| `--mime TYPE` | Filter by MIME type (e.g., `image/png`) |
| `--search-extension EXT` | Filter by URL extension (e.g., `qoi`) |
| `--limit N` | Max files to query |
| `--max-file-size` | Max file size filter (default: 1MB) |

### Athena Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--crawl` | `CC-MAIN-2024-51` | Common Crawl crawl ID |
| `--athena-database` | `ccindex` | Athena database name |
| `--athena-output` | (required with --athena) | S3 location for query results |

### Output Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-extension` | `--search-extension` | File extension for downloads |
| `--output-dir` | `corpus` | Output directory |
| `--threads` | 16 | Download thread count |
| `--estimate-only` | False | Show estimate, don't download |
| `--yes` | False | Skip confirmation prompt |

## Architecture

Two Python scripts using `uv run --script` with inline dependencies:

**cc_preprocess.py:**
- Reads raw parquet index files (~196GB, 2.6B rows)
- Filters out HTML content (keeps ~2% = ~52M rows)
- Extracts needed columns + derives file extension
- Outputs compact DuckDB file (~2-4GB)

**cc_download.py:**
- **Four index backends**: Preprocessed DuckDB (fastest), raw parquet, Athena, CSV
- **Multi-threaded downloads**: Parallel fetches with exponential backoff
- **WARC extraction**: Extracts files from Common Crawl gzip-compressed archives
- **Estimation**: Shows file count and total size before downloading

## Typical Workflow

```bash
# 1. Download raw parquet index (one-time, ~250GB)
aws s3 sync --no-sign-request \
    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-51/subset=warc/ \
    ./cc-index/CC-MAIN-2024-51/

# 2. Preprocess into DuckDB (one-time, ~30 min)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

# 3. Download corpus (sub-second queries, repeat for different formats)
./cc_download.py --duckdb cc-2024-51.duckdb \
    --search-extension qoi --output-dir raw/

# 4. Minimize with AFL
afl-cmin -Q -i raw/ -o minimized/ -- ./harness @@

# 5. Fuzz
afl-fuzz -Q -i minimized/ -o findings/ -- ./harness @@
```

## Preprocessing Options

```bash
# Default: exclude HTML (recommended, smallest output)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

# Include HTML content (much larger output, rarely needed)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb --include-html
```

The preprocessed DuckDB file contains:
- `url`, `extension`, `content_mime_detected`, `warc_filename`, `warc_record_offset`, `warc_record_length`
- Indexes on `extension` and `content_mime_detected` for fast queries

## AWS Setup (for Athena)

Requires:
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- S3 bucket for Athena query results
- Athena database with Common Crawl index table

See README.md for Athena table setup SQL.
