#!/bin/bash
# Download and preprocess a single Common Crawl index
set -e

CRAWL="$1"
if [ -z "$CRAWL" ]; then
    echo "Usage: $0 CC-MAIN-YYYY-NN"
    exit 1
fi

INDEX_DIR="./cc-index/$CRAWL"
DUCKDB_FILE="./$CRAWL.duckdb"

echo "=== Processing $CRAWL ==="

# Skip if already done
if [ -f "$DUCKDB_FILE" ]; then
    echo "Already exists: $DUCKDB_FILE"
    exit 0
fi

# Download parquet index via HTTPS (S3 listing is restricted)
echo "Downloading index to $INDEX_DIR..."
mkdir -p "$INDEX_DIR"
cd "$INDEX_DIR"
curl -s "https://data.commoncrawl.org/crawl-data/$CRAWL/cc-index-table.paths.gz" | \
    gunzip | grep 'subset=warc' | \
    xargs -I{} -P4 curl -sO "https://data.commoncrawl.org/{}"
cd - > /dev/null

# Preprocess into DuckDB
echo "Preprocessing into $DUCKDB_FILE..."
./cc_preprocess.py "$INDEX_DIR" -o "$DUCKDB_FILE"

echo "=== Done: $CRAWL ==="
