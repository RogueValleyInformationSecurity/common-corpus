# cc_download

Download files from Common Crawl for fuzzing corpus generation. Supports three index sources: local parquet files (fastest), AWS Athena, or pre-generated CSV.

## Quick Start

The script uses `uv run --script` with inline dependencies—no install step needed.

### Option 1: Local Index (Recommended)

Download a crawl's index once (~250GB), then query instantly:

```bash
# Download index (one-time, ~250GB)
aws s3 sync --no-sign-request \
    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-51/subset=warc/ \
    ./cc-index/CC-MAIN-2024-51/

# Query by extension (fast!)
./cc_download.py --local-index ./cc-index/CC-MAIN-2024-51/ \
    --extension qoi --file-format qoi --output-dir corpus/
```

### Option 2: AWS Athena

No local storage needed, pay per query (~$0.25-1.00):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

./cc_download.py --athena --mime image/png --file-format png \
    --athena-output s3://my-bucket/athena/ --output-dir corpus/
```

### Option 3: Pre-generated CSV

```bash
./cc_download.py --csv index.csv --file-format pdf --output-dir corpus/
```

## Usage

```
cc_download.py (--local-index DIR | --athena | --csv FILE)
               (--mime TYPE | --extension EXT) --file-format EXT [options]
```

### Index Source (one required)

| Argument | Description |
|----------|-------------|
| `--local-index DIR` | Directory with parquet files (~250GB per crawl) |
| `--athena` | Query AWS Athena (requires `--athena-output`) |
| `--csv FILE` | Pre-generated CSV file |

### Query Filters

| Argument | Description |
|----------|-------------|
| `--mime TYPE` | Filter by MIME type (e.g., `image/png`) |
| `--extension EXT` | Filter by URL extension (e.g., `qoi`, `png`) |
| `--limit N` | Maximum files to return |
| `--max-file-size N` | Maximum file size in bytes (default: 1MB) |

### Output Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--file-format` | (required) | Extension for downloaded files |
| `--output-dir` | `corpus` | Output directory |
| `--threads` | 16 | Download thread count |
| `--estimate-only` | | Show count/size, don't download |
| `--yes` | | Skip confirmation prompt |

## Index Backends

| Backend | Setup | Query Speed | Cost |
|---------|-------|-------------|------|
| Local (DuckDB) | ~250GB download | 1-10 seconds | Free |
| AWS Athena | S3 bucket | 10-60 seconds | ~$0.25-1/query |
| CSV | Generate manually | Instant | Free |

## Workflow Example

```bash
# 1. Download files from Common Crawl
./cc_download.py --local-index ./cc-index/CC-MAIN-2024-51/ \
    --extension qoi --file-format qoi --output-dir raw_corpus/

# 2. Minimize corpus with AFL
afl-cmin -Q -i raw_corpus/ -o minimized/ -- ./my_harness @@

# 3. Fuzz!
afl-fuzz -Q -i minimized/ -o findings/ -- ./my_harness @@
```

## Downloading the Index

Each crawl has a manifest file listing all parquet files. Download ~300 files (~400GB):

```bash
# Create directory
mkdir -p ./cc-index/CC-MAIN-2024-51
cd ./cc-index/CC-MAIN-2024-51

# Download using manifest (parallel with 4 connections)
curl -s 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/cc-index-table.paths.gz' | \
    gunzip | grep 'subset=warc' | \
    xargs -I{} -P4 curl -sO 'https://data.commoncrawl.org/{}'
```

Alternative with wget:
```bash
curl -s 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/cc-index-table.paths.gz' | \
    gunzip | grep 'subset=warc' | \
    sed 's|^|https://data.commoncrawl.org/|' > urls.txt
wget -P ./cc-index/CC-MAIN-2024-51 -i urls.txt
```

## Finding MIME Types

Use `cc_mime.py` to discover which MIME types are associated with a file extension:

```bash
# Find MIME types for PDF files
./cc_mime.py pdf

# Output:
# MIME types for .pdf files:
# application/pdf    18,720,479   96.1%
# text/html             686,057    3.5%
# ...
# Suggestion:
#   ./cc_download.py --mime 'application/pdf' --file-format pdf ...
```

This helps when you know the extension but not the MIME type. The script warns if most URLs with that extension serve HTML (common for error pages or dynamic content).

```bash
./cc_mime.py png
# Warning: Most .png URLs serve HTML (likely error pages).
# Best actual content type: image/png (0.3%)
```

## Notes

- Common Crawl truncates files >1MB
- MIME queries (`--mime`) are more reliable than extension for getting actual file content
- Extension queries (`--extension`) match URL patterns but may return HTML error pages
- Use `cc_mime.py` to find the right MIME type for an extension
- Local index queries use DuckDB for fast columnar scans
- Supports graceful shutdown via Ctrl+C
