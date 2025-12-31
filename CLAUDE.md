# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Common Corpus is a tool for building coverage-minimized corpus data sets for fuzzing. It downloads files from Common Crawl (via S3), runs them through a SanitizerCoverage-enabled binary, and keeps only files that produce new code coverage. The goal is to produce the smallest set of files that cover the largest amount of branch coverage.

## Running the Tool

```bash
# Install dependencies
pip3 install warcio boto3

# Run with an index CSV file
python3 common_corpus.py <index_csv>

# Resume from saved state
python3 common_corpus.py <index_csv> state.dat
```

## Configuration

Before running, configure these variables at the top of `common_corpus.py`:

- `ACCESS_KEY` / `SECRET_KEY`: AWS credentials for S3 access to Common Crawl
- `TARGET_CMDLINE`: Command line for the sancov-enabled binary (use `%s` as placeholder for test file)
- `TARGET_BINARY`: Name of the sancov-enabled binary (for locating .sancov files)
- `FILE_FORMAT`: File extension for corpus files
- `CLEANUP_GLOB`: Glob pattern for files to clean up after each run (empty string to skip)
- `NTHREADS`: Number of worker threads (default: 16)

## Prerequisites

### 1. Generate Index CSV via AWS Athena

Create a database and table in AWS Athena pointing to Common Crawl's index:
```sql
CREATE DATABASE ccindex;

CREATE EXTERNAL TABLE ccindex (
  -- includes content_mime_detected, warc_filename, warc_record_offset, warc_record_length
) STORED AS parquet LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';
```

Query for your target file type:
```sql
SELECT url, warc_filename, warc_record_offset, warc_record_length
FROM ccindex
WHERE crawl = 'CC-MAIN-2023-14'
  AND subset = 'warc'
  AND content_mime_detected = 'application/pdf'
  AND warc_record_length < 1048576;  -- 1MB limit (Common Crawl truncates larger files)
```

### 2. Compile Target with SanitizerCoverage

```bash
clang -fsanitize=address -fsanitize-coverage=trace-pc-guard -o target target.c
```

## Architecture

Single-file Python script with multi-threaded design:

- **Main thread**: Loads index CSV, spawns worker threads, handles graceful shutdown on Ctrl+C
- **Worker threads**: Each thread independently fetches WARC records from S3 (using `warcio`), runs the target binary with `ASAN_OPTIONS=coverage=1`, analyzes resulting `.sancov` files
- **Shared state**: Global `coverage` set tracks all seen edges; `id_lock` mutex protects corpus ID assignment

The tool uses SanitizerCoverage's trace-pc-guard mode. Each test file produces a `.sancov` file containing 8-byte edge addresses. Files triggering new edges are saved to `out/`.

## Output

- Corpus files: `out/corpus<N>.<format>`
- Coverage data: `out/corpus<N>.<format>.sancov`
- State file: `state.dat` (JSON with index offset, corpus ID, tested count, coverage set)

Progress indicators: `+` = new coverage added, `.` = no new coverage, `|N-S|` = S3 retry (thread N, sleep S seconds)

## References

See the [Isosceles blog post](https://blog.isosceles.com/how-to-build-a-corpus-for-fuzzing/) for full setup instructions including AWS IAM configuration and Athena query editor setup.
