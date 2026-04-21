#!/usr/bin/env bash
set -euo pipefail

# -------- Config / Defaults --------
SA_FDP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_DIR="${YCSB_DIR:-$SA_FDP_ROOT/workloads/rocksDB/YCSB}"
WORKLOAD_FILE="${WORKLOAD_FILE:-workloads/workloada}"
ROCKSDB_DIR="${ROCKSDB_DIR:-/mnt/sa_fdp/rocksdb_data}"
ROCKSDB_OPTION_FILE="${ROCKSDB_OPTION_FILE:-$SCRIPT_DIR/rocksdb.ini}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Load
RECORDCOUNT="${RECORDCOUNT:-500000000}"

# Run (split to avoid 32-bit int overflow in YCSB)
OPERATIONCOUNT="${OPERATIONCOUNT:-2000000000}"
RUNS="${RUNS:-10}"
THREADS="${THREADS:-16}"

# Access distribution
REQUEST_DISTRIBUTION="${REQUEST_DISTRIBUTION:-hotspot}"
HOTSPOT_DATA_FRACTION="${HOTSPOT_DATA_FRACTION:-0.2}"
HOTSPOT_OPN_FRACTION="${HOTSPOT_OPN_FRACTION:-0.8}"
# ------------------------

usage() {
  cat <<'EOF'
Usage:
  ./ycsb_rocksdb_workloada.sh load
  ./ycsb_rocksdb_workloada.sh run

Env overrides:
  YCSB_DIR, WORKLOAD_FILE, ROCKSDB_DIR, ROCKSDB_OPTION_FILE, PYTHON_BIN
  RECORDCOUNT, OPERATIONCOUNT, RUNS, THREADS
  REQUEST_DISTRIBUTION, HOTSPOT_DATA_FRACTION, HOTSPOT_OPN_FRACTION
EOF
}

MODE="${1:-}"
if [[ -z "$MODE" ]]; then
  usage
  exit 2
fi

cd "$YCSB_DIR"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN" >&2
  exit 127
fi

ROCKSDB_OPTION_ARGS=()
if [[ -n "$ROCKSDB_OPTION_FILE" ]]; then
  ROCKSDB_OPTION_ARGS=(-p "rocksdb.optionsfile=$ROCKSDB_OPTION_FILE")
fi

ACCESS_DISTRIBUTION_ARGS=(
  -p "requestdistribution=$REQUEST_DISTRIBUTION"
  -p "hotspotdatafraction=$HOTSPOT_DATA_FRACTION"
  -p "hotspotopnfraction=$HOTSPOT_OPN_FRACTION"
)

case "$MODE" in
  load)
    echo "[+] YCSB load rocksdb (recordcount=$RECORDCOUNT, requestdistribution=$REQUEST_DISTRIBUTION, hotspotdatafraction=$HOTSPOT_DATA_FRACTION, hotspotopnfraction=$HOTSPOT_OPN_FRACTION, threads=$THREADS, rocksdb.dir=$ROCKSDB_DIR, optionsfile=${ROCKSDB_OPTION_FILE:-<none>})"
    "$PYTHON_BIN" ./bin/ycsb load rocksdb -s -P "$WORKLOAD_FILE" \
      -p "rocksdb.dir=$ROCKSDB_DIR" \
      "${ROCKSDB_OPTION_ARGS[@]}" \
      "${ACCESS_DISTRIBUTION_ARGS[@]}" \
      -p "recordcount=$RECORDCOUNT" \
      -threads "$THREADS"
    ;;
  run)
    echo "[+] YCSB run rocksdb only (operationcount=$OPERATIONCOUNT, requestdistribution=$REQUEST_DISTRIBUTION, hotspotdatafraction=$HOTSPOT_DATA_FRACTION, hotspotopnfraction=$HOTSPOT_OPN_FRACTION, runs=$RUNS, threads=$THREADS, rocksdb.dir=$ROCKSDB_DIR, optionsfile=${ROCKSDB_OPTION_FILE:-<none>})"
    for i in $(seq 1 "$RUNS"); do
      echo "[+] YCSB run rocksdb (run=$i/$RUNS, operationcount=$OPERATIONCOUNT)"
      "$PYTHON_BIN" ./bin/ycsb run rocksdb -s -P "$WORKLOAD_FILE" \
        -p "rocksdb.dir=$ROCKSDB_DIR" \
        "${ROCKSDB_OPTION_ARGS[@]}" \
        "${ACCESS_DISTRIBUTION_ARGS[@]}" \
        -p "recordcount=$RECORDCOUNT" \
        -p "operationcount=$OPERATIONCOUNT" \
        -threads "$THREADS"
    done
    ;;
  *)
    echo "Invalid mode: $MODE (expected load|run)" >&2
    usage
    exit 2
    ;;
esac