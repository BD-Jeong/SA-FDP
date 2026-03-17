#!/bin/bash
# Apply 0003: bpf_probe_write_kernel_bio_write_stream (helper 212) to kernel source.
# Usage: ./apply_patches.sh /path/to/linux
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -z "$1" ]; then
	echo "Usage: $0 <kernel_source_root>" >&2
	echo "" >&2
	echo "  Path must contain kernel/trace/bpf_trace.c and include/uapi/linux/bpf.h." >&2
	echo "  Example: $0 /usr/src/linux-$(uname -r)" >&2
	echo "  Example: $0 \$HOME/linux" >&2
	echo "" >&2
	exit 1
fi

LINUX="$1"
if [ ! -f "$LINUX/kernel/trace/bpf_trace.c" ]; then
	echo "Error: $LINUX/kernel/trace/bpf_trace.c not found." >&2
	echo "  Not a full kernel source tree." >&2
	echo "  Use a full tree: linux-source package or kernel.org tarball." >&2
	exit 1
fi

cd "$LINUX"
patch -p1 --forward --dry-run < "$SCRIPT_DIR/0003-bpf-add-probe_write_kernel-helper.patch" && \
patch -p1 --forward < "$SCRIPT_DIR/0003-bpf-add-probe_write_kernel-helper.patch" && \
echo "Patch applied successfully." || echo "Done. If patch failed, see README.md."
