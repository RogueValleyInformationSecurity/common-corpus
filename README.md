# Common Corpus

Build coverage-minimized corpus data sets for fuzzing by leveraging Common Crawl's petabyte-scale web archive. Produces the smallest set of files that cover the largest amount of branch coverage in your target binary.

## How It Works

1. Query Common Crawl's index via AWS Athena to find files of your target type (PDF, PNG, etc.)
2. Download candidates from Common Crawl's S3-hosted WARC archives
3. Run each file through a SanitizerCoverage-instrumented binary
4. Keep only files that trigger new code paths

## Quick Start

The script uses `uv run --script` with inline dependencies—no install step needed:

```bash
# Set AWS credentials (or use --aws-access-key/--aws-secret-key)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Run corpus generation
./common_corpus.py index.csv \
    --target-cmdline './pdfium_test --ppm {}' \
    --target-binary pdfium_test \
    --file-format pdf
```

### Prerequisites

1. Follow the [Isosceles blog guide](https://blog.isosceles.com/how-to-build-a-corpus-for-fuzzing/) to set up AWS Athena and generate an index CSV
2. Compile your target with SanitizerCoverage: `-fsanitize=address -fsanitize-coverage=trace-pc-guard`

## Usage

```
usage: common_corpus.py [-h] --target-cmdline TARGET_CMDLINE
                        --target-binary TARGET_BINARY --file-format FILE_FORMAT
                        [--aws-access-key AWS_ACCESS_KEY]
                        [--aws-secret-key AWS_SECRET_KEY]
                        [--cleanup-glob CLEANUP_GLOB] [--threads THREADS]
                        [--output-dir OUTPUT_DIR] [--resume STATE_FILE]
                        [--state-file STATE_FILE] [--verbose] [--dry-run]
                        index_csv
```

### Required Arguments

| Argument | Description |
|----------|-------------|
| `index_csv` | CSV file from AWS Athena query |
| `--target-cmdline` | Command line with `{}` as testcase placeholder |
| `--target-binary` | Binary name for locating .sancov files |
| `--file-format` | File extension (pdf, png, etc.) |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--aws-access-key` | `$AWS_ACCESS_KEY_ID` | AWS access key |
| `--aws-secret-key` | `$AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `--cleanup-glob` | None | Glob pattern for files to delete after each run |
| `--threads` | 16 | Number of worker threads |
| `--output-dir` | `out` | Output directory for corpus files |
| `--resume` | None | Resume from a saved state file |
| `--state-file` | `state.dat` | Path to save state file |
| `--verbose` | False | Print periodic progress statistics |
| `--dry-run` | False | Validate configuration and exit |

## Output

- Corpus files: `out/corpus<N>.<format>`
- Progress: `+` = new coverage, `.` = no new coverage
- State is auto-saved to `state.dat` for resumption

## Notes

- Common Crawl truncates files >1MB, so filter your Athena query accordingly
- Use multiple crawl snapshots for rarer file formats
- Supports graceful shutdown via Ctrl+C
