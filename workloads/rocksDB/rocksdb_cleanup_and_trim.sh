#!/usr/bin/env bash
set -euo pipefail

ROCKSDB_DIR="${ROCKSDB_DIR:-/mnt/sa_fdp/rocksdb_data}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/sa_fdp}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  rocksdb_cleanup_and_trim.sh

Env:
  ROCKSDB_DIR   (default: /mnt/sa_fdp/rocksdb_data)
  MOUNT_POINT   (default: /mnt/sa_fdp)

Does:
  - rm -rf ${ROCKSDB_DIR}/*
  - fstrim -v ${MOUNT_POINT}
EOF
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E -- "$0" "$@"
fi

if [[ "$MOUNT_POINT" != /mnt/* ]]; then
  echo "Refusing: MOUNT_POINT must be under /mnt (got: $MOUNT_POINT)" >&2
  exit 2
fi

if ! mountpoint -q "$MOUNT_POINT"; then
  echo "Mount point is not mounted: $MOUNT_POINT" >&2
  exit 2
fi

mkdir -p "$ROCKSDB_DIR"

case "$ROCKSDB_DIR" in
  "$MOUNT_POINT"/*) ;;
  *)
    echo "Refusing: ROCKSDB_DIR must be under MOUNT_POINT ($MOUNT_POINT). Got: $ROCKSDB_DIR" >&2
    exit 2
    ;;
esac

echo "[+] Cleaning RocksDB dir: $ROCKSDB_DIR"
rm -rf -- "$ROCKSDB_DIR"/* || true

sync
echo "[+] Running fstrim: $MOUNT_POINT"
fstrim -v "$MOUNT_POINT"

