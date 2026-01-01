# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

cc_download is a tool for downloading files from Common Crawl's web archive for fuzzing corpus generation. It can query AWS Athena directly or use a pre-generated CSV index, then downloads matching files from Common Crawl's S3-hosted WARC archives.

The tool focuses solely on downloading—corpus minimization is left to specialized tools like `afl-cmin`.

## Running the Tool

The script uses `uv run --script` with inline dependencies (PEP 723):

```bash
# Query Athena directly
./cc_download.py --mime image/qoi --file-format qoi \
    --athena-output s3://my-bucket/athena/ \
    --output-dir corpus/

# Use existing CSV
./cc_download.py --csv index.csv --file-format pdf --output-dir corpus/

# Estimate only (no download)
./cc_download.py --mime image/png --file-format png \
    --athena-output s3://my-bucket/athena/ \
    --estimate-only
```

## CLI Arguments

### Input Source (one required)

| Argument | Description |
|----------|-------------|
| `--csv FILE` | CSV file with Common Crawl index data |
| `--local-index DIR` | Local parquet files (fastest, ~250GB per crawl) |
| `--athena` | Query AWS Athena (requires `--athena-output`) |

### Query Filters

| Argument | Description |
|----------|-------------|
| `--mime TYPE` | Filter by MIME type (e.g., `image/png`) |
| `--extension EXT` | Filter by URL extension (e.g., `qoi`) - more reliable for obscure formats |
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
| `--file-format` | (required) | File extension for downloads |
| `--output-dir` | `corpus` | Output directory |
| `--threads` | 16 | Download thread count |
| `--estimate-only` | False | Show estimate, don't download |
| `--yes` | False | Skip confirmation prompt |

## Architecture

Single-file Python script (~350 lines) with:

- **Three index backends**: Local DuckDB (fastest), AWS Athena, CSV
- **Multi-threaded downloads**: Parallel S3 fetches with exponential backoff
- **WARC extraction**: Uses `warcio` to extract files from Common Crawl archives
- **Estimation**: Shows file count and total size before downloading

## Typical Workflow

```bash
# 1. Download raw corpus
./cc_download.py --local-index ./cc-index/CC-MAIN-2024-51/ \
    --extension qoi --file-format qoi --output-dir raw/

# 2. Minimize with AFL
afl-cmin -Q -i raw/ -o minimized/ -- ./harness @@

# 3. Fuzz
afl-fuzz -Q -i minimized/ -o findings/ -- ./harness @@
```

## Local Index Setup

Download a crawl's index once (~250GB), then query instantly with DuckDB:

```bash
aws s3 sync --no-sign-request \
    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-51/subset=warc/ \
    ./cc-index/CC-MAIN-2024-51/
```

## AWS Setup (for Athena)

Requires:
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- S3 bucket for Athena query results
- Athena database with Common Crawl index table

See README.md for Athena table setup SQL.
