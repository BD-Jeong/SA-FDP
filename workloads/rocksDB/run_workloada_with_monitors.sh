#!/usr/bin/env bash
set -euo pipefail

# Runs YCSB workloada while collecting:
# - scripts/ssd_metrics_logger.py
# - training/training.py
# Then stops monitors 10 minutes after YCSB finishes.

# -------- Config --------
POST_YCSB_WAIT_SEC="${POST_YCSB_WAIT_SEC:-600}"  # 10 minutes

SA_FDP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

YCSB_SCRIPT="${YCSB_SCRIPT:-$SA_FDP_ROOT/workloads/rocksDB/ycsb_rocksdb_workloada.sh}"
SSD_LOGGER="${SSD_LOGGER:-$SA_FDP_ROOT/scripts/ssd_metrics_logger.py}"
TRAINING="${TRAINING:-$SA_FDP_ROOT/training/training.py}"

OUT_DIR="${OUT_DIR:-$SA_FDP_ROOT/out}"
TRAIN_DIR="${TRAIN_DIR:-$OUT_DIR/train}"
# ------------------------

usage() {
  cat <<'EOF'
Usage:
  run_workloada_with_monitors.sh --dev /dev/nvme0 --ns 1

Env:
  OUT_DIR             (default: <repo>/out)
  TRAIN_DIR           (default: <repo>/out/train)
  POST_YCSB_WAIT_SEC  (default: 600)
  YCSB_SCRIPT         (default: workloads/rocksDB/ycsb_rocksdb_workloada.sh)
  SSD_LOGGER          (default: scripts/ssd_metrics_logger.py)
  TRAINING            (default: training/training.py)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E -- "$0" "$@"
fi

dev=""
ns=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev) dev="${2:-}"; shift 2 ;;
    --ns) ns="${2:-}"; shift 2 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$dev" || -z "$ns" ]]; then
  echo "Missing --dev/--ns" >&2
  usage
  exit 2
fi

if [[ ! "$dev" =~ ^/dev/nvme[0-9]+$ ]]; then
  echo "Expected --dev like /dev/nvme0, got: $dev" >&2
  exit 2
fi
if ! [[ "$ns" =~ ^[0-9]+$ ]]; then
  echo "Expected --ns integer, got: $ns" >&2
  exit 2
fi

ns_dev="${dev}n${ns}"

mkdir -p "$OUT_DIR"
mkdir -p "$TRAIN_DIR"
ssd_out="$OUT_DIR/ssd_metrics.csv"
training_out="$TRAIN_DIR/training.csv"
ycsb_out="$OUT_DIR/ycsb.log"

ssd_pid=""
training_pid=""

cleanup() {
  set +e
  if [[ -n "${ssd_pid:-}" ]]; then
    echo "[+] Stopping SSD logger (pid=$ssd_pid)"
    kill -INT "$ssd_pid" 2>/dev/null || true
  fi
  if [[ -n "${training_pid:-}" ]]; then
    echo "[+] Stopping training monitor (pid=$training_pid)"
    kill -INT "$training_pid" 2>/dev/null || true
  fi
  sleep 2
  if [[ -n "${ssd_pid:-}" ]]; then kill -TERM "$ssd_pid" 2>/dev/null || true; fi
  if [[ -n "${training_pid:-}" ]]; then kill -TERM "$training_pid" 2>/dev/null || true; fi
  wait 2>/dev/null || true
}

trap cleanup EXIT INT TERM

echo "[+] Starting SSD metrics logger: $ns_dev -> $ssd_out"
python3 "$SSD_LOGGER" --device "$ns_dev" --output "$ssd_out" &
ssd_pid="$!"

echo "[+] Starting training monitor: --dev $dev --ns $ns -> $training_out (window files: *_1.csv, *_2.csv, ...)"
python3 "$TRAINING" --dev "$dev" --ns "$ns" --output "$training_out" &
training_pid="$!"

echo "[+] Running YCSB script: $YCSB_SCRIPT"
set +e
"$YCSB_SCRIPT" run 2>&1 | tee "$ycsb_out"
ycsb_rc="${PIPESTATUS[0]}"
set -e

echo "[+] YCSB finished (exit=$ycsb_rc). Waiting ${POST_YCSB_WAIT_SEC}s before stopping monitors."
sleep "$POST_YCSB_WAIT_SEC"

exit "$ycsb_rc"

