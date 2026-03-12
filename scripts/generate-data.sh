#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-target/parquet-events}"
TOTAL_RECORDS="${2:-50000}"
NUM_DAYS="${3:-7}"

usage() {
  cat <<'EOF'
Usage: scripts/generate-data.sh [output_dir] [total_records] [num_days]

Generates skewed security-event Parquet test data using the existing Java generator.

Defaults:
  output_dir    target/parquet-events
  total_records 50000
  num_days      7

Examples:
  scripts/generate-data.sh
  scripts/generate-data.sh target/my-events 50000 7
  scripts/generate-data.sh target/my-events 100000 14
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

cd "${ROOT_DIR}"

echo "Generating ${TOTAL_RECORDS} events into ${OUTPUT_DIR} over ${NUM_DAYS} day(s)..."
mvn -q exec:java \
  -Dexec.mainClass="com.example.replay.datalake.SecurityEventParquetGenerator" \
  -Dexec.args="${OUTPUT_DIR} ${TOTAL_RECORDS} ${NUM_DAYS}"

echo "Done. Output written under: ${ROOT_DIR}/${OUTPUT_DIR}"
