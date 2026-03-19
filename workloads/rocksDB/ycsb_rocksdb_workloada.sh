#!/usr/bin/env bash
set -euo pipefail

# -------- Config / Defaults --------
SA_FDP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YCSB_DIR="${YCSB_DIR:-$SA_FDP_ROOT/workloads/rocksDB/YCSB}"
WORKLOAD_FILE="${WORKLOAD_FILE:-workloads/workloada}"
ROCKSDB_DIR="${ROCKSDB_DIR:-/mnt/sa_fdp/rocksdb_data}"

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
  YCSB_DIR, WORKLOAD_FILE, ROCKSDB_DIR
  RECORDCOUNT, OPERATIONCOUNT, RUNS, THREADS
EOF
}

MODE="${1:-}"
if [[ -z "$MODE" ]]; then
  usage
  exit 2
fi

cd "$YCSB_DIR"

case "$MODE" in
  load)
    echo "[+] YCSB load rocksdb (recordcount=$RECORDCOUNT, rocksdb.dir=$ROCKSDB_DIR)"
    ./bin/ycsb load rocksdb -s -P "$WORKLOAD_FILE" \
      -p "rocksdb.dir=$ROCKSDB_DIR" \
      -p "recordcount=$RECORDCOUNT"
    ;;
  run)
    echo "[+] YCSB run rocksdb only (operationcount=$OPERATIONCOUNT, runs=$RUNS, threads=$THREADS, rocksdb.dir=$ROCKSDB_DIR)"
    for i in $(seq 1 "$RUNS"); do
      echo "[+] YCSB run rocksdb (run=$i/$RUNS, operationcount=$OPERATIONCOUNT)"
      ./bin/ycsb run rocksdb -s -P "$WORKLOAD_FILE" \
        -p "rocksdb.dir=$ROCKSDB_DIR" \
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