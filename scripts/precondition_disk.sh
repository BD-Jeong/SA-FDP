#!/usr/bin/env bash
set -euo pipefail

# -------- Global config --------
PREFILL_PERCENT=90
MOUNT_POINT="/mnt/sa_fdp"
FS_TYPE="ext4"
# nvme format (erase) settings
NVME_SECURE_ERASE=1   # -s 1 (user data erase). See nvme-cli docs.
NVME_FORCE=1          # add -f when 1
# dummy file prefill
PREFILL_FILENAME=".prefill.bin"
# ------------------------------

usage() {
  cat <<'EOF'
Usage:
  precondition_disk.sh --dev /dev/nvme0 --ns 1
  precondition_disk.sh --device /dev/nvme0n1

Does:
  - Unmounts the target device if mounted
  - Erases the namespace via nvme format
  - Formats as ext4
  - Mounts to /mnt/sa_fdp
  - Creates a dummy file sized to PREFILL_PERCENT% of the raw device
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E -- "$0" "$@"
fi

base_disk_of() {
  # Print base disk device name (e.g. nvme0n1) for a given /dev node.
  # Returns empty on failure.
  local node="$1"
  local pk
  pk="$(lsblk -no PKNAME "$node" 2>/dev/null | head -n1 || true)"
  if [[ -n "$pk" ]]; then
    echo "$pk"
    return 0
  fi
  # If it has no parent, it might already be a whole-disk device.
  lsblk -no NAME "$node" 2>/dev/null | head -n1 || true
}

is_os_disk() {
  # Best-effort safety check: if target base disk matches rootfs or swap base disk, treat as OS disk.
  local target_dev="$1"
  local target_base root_src root_base swap_src swap_base

  if ! command -v findmnt >/dev/null 2>&1; then
    return 1
  fi

  target_base="$(base_disk_of "$target_dev")"
  [[ -z "$target_base" ]] && return 1

  root_src="$(findmnt -n -o SOURCE / 2>/dev/null || true)"
  if [[ "$root_src" == /dev/* ]]; then
    root_base="$(base_disk_of "$root_src")"
    if [[ -n "$root_base" && "$root_base" == "$target_base" ]]; then
      return 0
    fi
  fi

  if command -v swapon >/dev/null 2>&1; then
    while IFS= read -r swap_src; do
      [[ "$swap_src" == /dev/* ]] || continue
      swap_base="$(base_disk_of "$swap_src")"
      if [[ -n "$swap_base" && "$swap_base" == "$target_base" ]]; then
        return 0
      fi
    done < <(swapon --noheadings --raw --output=NAME 2>/dev/null || true)
  fi

  return 1
}

dev=""
ns=""
device=""
controller=""
nsid=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev) dev="${2:-}"; shift 2 ;;
    --ns) ns="${2:-}"; shift 2 ;;
    --device) device="${2:-}"; shift 2 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$device" ]]; then
  if [[ -z "$dev" || -z "$ns" ]]; then
    echo "Need either --device or (--dev and --ns)." >&2
    usage
    exit 2
  fi
  if [[ ! "$dev" =~ ^/dev/nvme[0-9]+$ ]]; then
    echo "Expected NVMe controller path like /dev/nvme0, got: $dev" >&2
    exit 2
  fi
  if ! [[ "$ns" =~ ^[0-9]+$ ]]; then
    echo "NS must be an integer, got: $ns" >&2
    exit 2
  fi
  controller="$dev"
  nsid="$ns"
  device="${dev}n${ns}"
else
  # Try to infer controller + nsid from /dev/nvme0n1
  if [[ "$device" =~ ^/dev/(nvme[0-9]+)n([0-9]+)$ ]]; then
    controller="/dev/${BASH_REMATCH[1]}"
    nsid="${BASH_REMATCH[2]}"
  fi
fi

if [[ ! -b "$device" ]]; then
  echo "Not a block device: $device" >&2
  exit 2
fi

echo "[+] Target device: $device"

if is_os_disk "$device"; then
  echo "Refusing to operate: $device appears to be the OS/root disk." >&2
  exit 3
fi

if ! command -v nvme >/dev/null 2>&1; then
  echo "nvme not found. Install it (e.g. sudo apt install -y nvme-cli) and retry." >&2
  exit 2
fi

if ! command -v fio >/dev/null 2>&1; then
  echo "fio not found. Install it (e.g. sudo apt install -y fio) and retry." >&2
  exit 2
fi

mounted_at="$(lsblk -no MOUNTPOINT "$device" | head -n1 || true)"
if [[ -n "$mounted_at" ]]; then
  echo "[+] Unmounting $device from $mounted_at"
  umount "$device" || umount "$mounted_at"
fi

if [[ -z "$controller" || -z "$nsid" ]]; then
  echo "Could not infer NVMe controller/nsid from --device=$device." >&2
  echo "Use: --dev /dev/nvmeX --ns N (or pass --device like /dev/nvme0n1)." >&2
  exit 2
fi

nvme_force_flag=()
if [[ "$NVME_FORCE" -eq 1 ]]; then
  nvme_force_flag=(-f)
fi
echo "[+] Erasing namespace: nvme format -n $nsid -s $NVME_SECURE_ERASE ${nvme_force_flag[*]} $controller"
nvme format "$controller" -n "$nsid" -s "$NVME_SECURE_ERASE" "${nvme_force_flag[@]}"

echo "[+] Formatting $device as $FS_TYPE"
mkfs.ext4 -F "$device" >/dev/null

echo "[+] Mounting to $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"
mount -t "$FS_TYPE" "$device" "$MOUNT_POINT"

device_bytes="$(blockdev --getsize64 "$device")"
fs_bytes=$(df -B1 --output=size "$MOUNT_POINT" | tail -1)
prefill_bytes=$(( fs_bytes * PREFILL_PERCENT / 100 ))
prefill_path="$MOUNT_POINT/$PREFILL_FILENAME"
echo "[debug] device_bytes=$device_bytes fs_bytes=$fs_bytes"

sync
echo 3 > /proc/sys/vm/drop_caches || true

echo "[+] Creating dummy file: ${PREFILL_PERCENT}% of raw device"
echo "    file=$prefill_path bytes=$prefill_bytes"

# Create by actually writing data (not sparse).
fio --name=create_dummy \
  --filename="$prefill_path" \
  --ioengine=libaio \
  --rw=write \
  --bs=1M \
  --filesize="$prefill_bytes" \
  --size="$prefill_bytes" \
  --iodepth=256 \
  --numjobs=1 \
  --direct=1 \
  --end_fsync=1

sync

echo "[+] Done. Mounted at $MOUNT_POINT"

fstrim -v "$MOUNT_POINT"
echo "[+] fstrim done"