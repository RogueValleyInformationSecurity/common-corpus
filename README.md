# cc_download

Download files from Common Crawl for fuzzing corpus generation.

## Quick Start

Both scripts use `uv run --script` with inline dependencies—no install step needed.

### Option 1: Preprocessed DuckDB Index (Recommended)

Download the raw index once, preprocess it into a compact DuckDB file, then query instantly:

```bash
# 1. Download raw parquet index (one-time, ~250GB)
aws s3 sync --no-sign-request \
    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-51/subset=warc/ \
    ./cc-index/CC-MAIN-2024-51/

# 2. Preprocess into DuckDB (one-time, ~10 min, outputs ~4GB file)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

# 3. Query (sub-second!)
./cc_download.py --duckdb cc-2024-51.duckdb \
    --search-extension pdf --output-dir corpus/
```

### Option 2: Raw Parquet Index

Query the raw parquet files directly (slower, but no preprocessing):

```bash
./cc_download.py --local-index ./cc-index/CC-MAIN-2024-51/ \
    --search-extension qoi --output-dir corpus/
```

### Option 3: AWS Athena

No local storage needed, pay per query (~$0.25-1.00):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

./cc_download.py --athena --mime image/png --output-extension png \
    --athena-output s3://my-bucket/athena/ --output-dir corpus/
```

### Option 4: Pre-generated CSV

```bash
./cc_download.py --csv index.csv --output-extension pdf --output-dir corpus/
```

## Usage

```
cc_download.py (--duckdb FILE | --local-index DIR | --athena | --csv FILE)
               (--mime TYPE | --search-extension EXT) [options]
```

### Index Source (one required)

| Argument | Description |
|----------|-------------|
| `--duckdb FILE` | Preprocessed DuckDB index (fastest, recommended) |
| `--local-index DIR` | Raw parquet files (~250GB per crawl) |
| `--athena` | Query AWS Athena (requires `--athena-output`) |
| `--csv FILE` | Pre-generated CSV file |

### Query Filters

| Argument | Description |
|----------|-------------|
| `--mime TYPE` | Filter by MIME type (e.g., `image/png`) |
| `--search-extension EXT` | Filter by URL extension (e.g., `qoi`, `png`) |
| `--limit N` | Maximum files to return |
| `--max-file-size N` | Maximum file size in bytes (default: 1MB) |

### Output Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-extension` | `--search-extension` | Extension for downloaded files |
| `--output-dir` | `corpus` | Output directory |
| `--threads` | 16 | Download thread count |
| `--estimate-only` | | Show count/size, don't download |
| `--yes` | | Skip confirmation prompt |

## Index Backends

| Backend | Setup | Query Speed | Cost |
|---------|-------|-------------|------|
| DuckDB (preprocessed) | ~250GB download + 10min preprocess | <1 second | Free |
| Local parquet | ~250GB download | 1-5 minutes | Free |
| AWS Athena | S3 bucket | 10-60 seconds | ~$0.25-1/query |
| CSV | Generate manually | Instant | Free |

## Workflow Example

```bash
# 1. Download and preprocess index (one-time)
./cc_preprocess.py ./cc-index/CC-MAIN-2024-51/ -o cc-2024-51.duckdb

# 2. Download files from Common Crawl
./cc_download.py --duckdb cc-2024-51.duckdb \
    --mime application/pdf --output-extension pdf --output-dir raw_corpus/

# 3. Minimize corpus with AFL
afl-cmin -Q -i raw_corpus/ -o minimized/ -- ./my_harness @@

# 4. Fuzz!
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
- Preprocessed DuckDB index excludes HTML content by default (~4GB vs ~250GB raw)
- Supports graceful shutdown via Ctrl+C
