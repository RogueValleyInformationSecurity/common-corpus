# Common Corpus (common-corpus)

Common Corpus builds coverage-minimized corpus data sets for fuzzing by leveraging Common Crawl's petabyte-scale web archive. It produces the smallest set of files that cover the largest amount of branch coverage in your target binary.

## How It Works

1. Query Common Crawl's index via AWS Athena to find files of your target type (PDF, PNG, etc.)
2. Download candidates from Common Crawl's S3-hosted WARC archives
3. Run each file through a SanitizerCoverage-instrumented binary
4. Keep only files that trigger new code paths

## Quick Start

```bash
pip3 install warcio boto3
```

1. Follow the [Isosceles blog guide](https://blog.isosceles.com/how-to-build-a-corpus-for-fuzzing/) to set up AWS Athena and generate an index CSV
2. Compile your target with SanitizerCoverage: `-fsanitize=address -fsanitize-coverage=trace-pc-guard`
3. Configure variables in `common_corpus.py`: AWS keys, target command line, file format
4. Run: `python3 common_corpus.py <index_csv>`

## Output

- Corpus files: `out/corpus<N>.<format>`
- Progress: `+` = new coverage, `.` = no new coverage
- State is auto-saved to `state.dat` for resumption

## Notes

- Common Crawl truncates files >1MB, so filter your Athena query accordingly
- Use multiple crawl snapshots for rarer file formats
- The tool supports graceful shutdown via Ctrl+C
