# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Common Corpus is a tool for building coverage-minimized corpus data sets for fuzzing. It downloads files from Common Crawl (via S3), runs them through a SanitizerCoverage-enabled binary, and keeps only files that produce new code coverage. The goal is to produce the smallest set of files that cover the largest amount of branch coverage.

## Running the Tool

The script uses `uv run --script` with inline dependencies (PEP 723), so no separate install step is needed:

```bash
# Basic usage
./common_corpus.py index.csv \
    --target-cmdline './pdfium_test --ppm {}' \
    --target-binary pdfium_test \
    --file-format pdf

# With all options
./common_corpus.py index.csv \
    --target-cmdline './pdfium_test --ppm {}' \
    --target-binary pdfium_test \
    --file-format pdf \
    --cleanup-glob '*.ppm' \
    --threads 8 \
    --output-dir corpus \
    --verbose \
    --resume state.dat

# Validate configuration without processing
./common_corpus.py index.csv \
    --target-cmdline './pdfium_test --ppm {}' \
    --target-binary pdfium_test \
    --file-format pdf \
    --dry-run
```

## CLI Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `index_csv` | Yes | - | CSV file from AWS Athena query |
| `--target-cmdline` | Yes | - | Command line with `{}` as testcase placeholder |
| `--target-binary` | Yes | - | Binary name for locating .sancov files |
| `--file-format` | Yes | - | File extension (pdf, png, etc.) |
| `--aws-access-key` | No | `$AWS_ACCESS_KEY_ID` | AWS credentials |
| `--aws-secret-key` | No | `$AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `--cleanup-glob` | No | None | Glob pattern for cleanup |
| `--threads` | No | 16 | Worker thread count |
| `--output-dir` | No | `out` | Corpus output directory |
| `--resume` | No | None | Resume from state file |
| `--state-file` | No | `state.dat` | State file path |
| `--verbose` | No | False | Show progress stats |
| `--dry-run` | No | False | Validate config only |

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

- **Main thread**: Parses CLI args, loads index CSV, spawns worker threads, handles graceful shutdown via SIGINT
- **Worker threads**: Each thread independently fetches WARC records from S3 (using `warcio`), runs the target binary with `ASAN_OPTIONS=coverage=1`, analyzes resulting `.sancov` files
- **Thread safety**: Explicit locks protect shared state (`index`, `coverage`, `tested_count`, `corpus_id`)

The tool uses SanitizerCoverage's trace-pc-guard mode. Each test file produces a `.sancov` file containing 8-byte edge addresses. Files triggering new edges are saved to the output directory.

## Output

- Corpus files: `<output-dir>/corpus<N>.<format>`
- Coverage data: `<output-dir>/corpus<N>.<format>.sancov`
- State file: `state.dat` (JSON with index offset, corpus ID, tested count, coverage set)

Progress indicators: `+` = new coverage added, `.` = no new coverage, `|N-S|` = S3 retry (thread N, sleep S seconds)

## References

See the [Isosceles blog post](https://blog.isosceles.com/how-to-build-a-corpus-for-fuzzing/) for full setup instructions including AWS IAM configuration and Athena query editor setup.
