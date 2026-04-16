#!/usr/bin/env bash
set -euo pipefail

# Update these if your environment changes.
TRAINING_SCRIPT="/home/bd/SA-FDP/training/training.py"
TRAINING_DEV="/dev/nvme0"
TRAINING_NS="1"
TRAINING_OUT="/home/bd/SA-FDP/training/fio/out/training.csv"

FIO_SCRIPT="/home/bd/SA-FDP/training/fio/fio_matrix_runner.py"
FIO_TARGET="/dev/nvme0n1"
# Repeat/add values to run more total-size combinations.
FIO_TOTAL_SIZES=("2T" "4T" "8T")
FIO_WORKERS="16"
FIO_OUTPUT_DIR="/home/bd/SA-FDP/training/fio/out/fio-matrix"

TRAINING_PID=""

cleanup() {
  if [[ -n "${TRAINING_PID}" ]] && kill -0 "${TRAINING_PID}" 2>/dev/null; then
    echo "[cleanup] stopping training.py (pid=${TRAINING_PID})"
    kill "${TRAINING_PID}" || true
    wait "${TRAINING_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

echo "[run] starting training.py"
python3 "${TRAINING_SCRIPT}" \
  --dev "${TRAINING_DEV}" \
  --ns "${TRAINING_NS}" \
  --output "${TRAINING_OUT}" &
TRAINING_PID=$!
echo "[run] training.py pid=${TRAINING_PID}"

echo "[run] starting fio matrix"
FIO_SIZE_ARGS=()
for size in "${FIO_TOTAL_SIZES[@]}"; do
  FIO_SIZE_ARGS+=(--total-size "${size}")
done

python3 "${FIO_SCRIPT}" \
  --filename "${FIO_TARGET}" \
  "${FIO_SIZE_ARGS[@]}" \
  --workers "${FIO_WORKERS}" \
  --output-dir "${FIO_OUTPUT_DIR}"

echo "[done] fio finished; training.py will be stopped by cleanup"
