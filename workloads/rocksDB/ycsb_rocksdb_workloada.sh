#!/usr/bin/env bash
set -euo pipefail

# -------- Config / Defaults --------
SA_FDP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_DIR="${YCSB_DIR:-$SA_FDP_ROOT/workloads/rocksDB/YCSB}"
WORKLOAD_FILE="${WORKLOAD_FILE:-workloads/workloada}"
ROCKSDB_DIR="${ROCKSDB_DIR:-/mnt/sa_fdp/rocksdb_data}"
ROCKSDB_OPTION_FILE="${ROCKSDB_OPTION_FILE:-$SCRIPT_DIR/rocksdb.ini}"

# Load
RECORDCOUNT="${RECORDCOUNT:-300000000}"

# Run (split to avoid 32-bit int overflow in YCSB)
OPERATIONCOUNT="${OPERATIONCOUNT:-2000000000}"
RUNS="${RUNS:-4}"
THREADS="${THREADS:-16}"
# ------------------------

usage() {
  cat <<'EOF'
Usage:
  ./ycsb_rocksdb_workloada.sh load
  ./ycsb_rocksdb_workloada.sh run

Env overrides:
  YCSB_DIR, WORKLOAD_FILE, ROCKSDB_DIR, ROCKSDB_OPTION_FILE
  RECORDCOUNT, OPERATIONCOUNT, RUNS, THREADS
EOF
}

MODE="${1:-}"
if [[ -z "$MODE" ]]; then
  usage
  exit 2
fi

cd "$YCSB_DIR"

ROCKSDB_OPTION_ARGS=()
if [[ -n "$ROCKSDB_OPTION_FILE" ]]; then
  ROCKSDB_OPTION_ARGS=(-p "rocksdb.optionsfile=$ROCKSDB_OPTION_FILE")
fi

case "$MODE" in
  load)
    echo "[+] YCSB load rocksdb (recordcount=$RECORDCOUNT, threads=$THREADS, rocksdb.dir=$ROCKSDB_DIR, optionsfile=${ROCKSDB_OPTION_FILE:-<none>})"
    ./bin/ycsb load rocksdb -s -P "$WORKLOAD_FILE" \
      -p "rocksdb.dir=$ROCKSDB_DIR" \
      "${ROCKSDB_OPTION_ARGS[@]}" \
      -p "recordcount=$RECORDCOUNT" \
      -threads "$THREADS"
    ;;
  run)
    echo "[+] YCSB run rocksdb only (operationcount=$OPERATIONCOUNT, runs=$RUNS, threads=$THREADS, rocksdb.dir=$ROCKSDB_DIR, optionsfile=${ROCKSDB_OPTION_FILE:-<none>})"
    for i in $(seq 1 "$RUNS"); do
      echo "[+] YCSB run rocksdb (run=$i/$RUNS, operationcount=$OPERATIONCOUNT)"
      ./bin/ycsb run rocksdb -s -P "$WORKLOAD_FILE" \
        -p "rocksdb.dir=$ROCKSDB_DIR" \
        "${ROCKSDB_OPTION_ARGS[@]}" \
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