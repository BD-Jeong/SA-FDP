#!/usr/bin/env python3
import argparse
import ctypes
import math
import re
import subprocess
import time
import os
import threading
from pathlib import Path
from bcc import BPF

# Global constants
DEFAULT_INTERVAL = 60  # Default window interval in seconds
CHUNK_SIZE = 1024 * 1024  # Chunk size: 1MB (1048576 bytes)
TRACE_PIPE = "/sys/kernel/debug/tracing/trace_pipe"
# BPF __SV_EVENT_AT_UPDATE__: this → set sv_t2e_ms / indicator.
SV_EVENT_AT_UPDATE = 10

class WindowTimes(ctypes.Structure):
    """Must match struct window_times in training.bpf.c (monotonic ms, u64)."""

    _fields_ = [
        ("window_start_ms", ctypes.c_uint64),
        ("window_end_ms", ctypes.c_uint64),
    ]


def namespace_block_path(controller_path, nsid):
    """Build /dev/nvme0n1 from /dev/nvme0 and NSID."""
    m = re.match(r"^/dev/(nvme\d+)$", controller_path)
    if not m:
        raise ValueError(
            f"device must be NVMe controller path (e.g. /dev/nvme0), got: {controller_path!r}"
        )
    return f"/dev/{m.group(1)}n{int(nsid)}"


def get_namespace_size_bytes(controller_path, nsid):
    """
    Namespace size in bytes via blockdev on /dev/nvme{c}n{nsid}.

    Args:
        controller_path: e.g. /dev/nvme0
        nsid: Namespace ID (1, 2, ...)

    Returns:
        int: Size of that namespace in bytes
    """
    ns_path = namespace_block_path(controller_path, nsid)
    try:
        result = subprocess.run(
            ["sudo", "blockdev", "--getsize64", ns_path],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError, OSError) as e:
        raise RuntimeError(
            f"Failed to get namespace size for {ns_path} (controller={controller_path}, ns={nsid}): {e}"
        ) from e


def get_logical_block_size_bytes(ns_path):
    """Logical sector size in bytes (512 or 4096 typical) via blockdev --getss."""
    try:
        result = subprocess.run(
            ["sudo", "blockdev", "--getss", ns_path],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError, OSError) as e:
        raise RuntimeError(f"Failed to get logical block size for {ns_path}: {e}") from e


def lba_shift_and_lbas_per_chunk(logical_b):
    """Match kernel: slba from sector, chunk index from 1MB-aligned logical extents."""
    if logical_b == 512:
        return 9, CHUNK_SIZE // 512
    if logical_b == 4096:
        return 12, CHUNK_SIZE // 4096
    raise ValueError(
        f"Unsupported logical block size {logical_b} (expected 512 or 4096)"
    )


# --- Load eBPF C code from file ---
def load_bpf_code(max_chunks, ns_path, lba_shift, lbas_per_chunk):
    """
    Load eBPF C code from file and inject max_chunks, ns_path, and bd major/minor.

    ns_path is passed as a C string macro; the program matches I/O to that namespace
    via stat(ns_path).st_rdev (injected as __TRACK_BD_MAJOR__ / __TRACK_BD_MINOR__).

    Args:
        max_chunks: Maximum number of chunks (array size and chunk index bound).
        ns_path: Block device path (e.g. /dev/nvme0n1).
        lba_shift: 9 for 512B LBA, 12 for 4K LBA.
        lbas_per_chunk: LBAs per 1MB chunk (2048 or 256).

    Returns:
        str: eBPF C code with parameters injected
    """
    script_dir = Path(__file__).parent
    bpf_file = script_dir / "training.bpf.c"
    with open(bpf_file, "r") as f:
        bpf_text = f.read()
    st = os.stat(ns_path)
    bd_major = os.major(st.st_rdev)
    bd_minor = os.minor(st.st_rdev)
    c_path = ns_path.replace("\\", "\\\\").replace('"', '\\"')
    # Inject max chunks as compile-time constant (verifier rejects global variable read)
    bpf_text = bpf_text.replace("#define __MAX_CHUNKS__ 0", f"#define __MAX_CHUNKS__ {max_chunks}")
    bpf_text = bpf_text.replace("#define __TRACKED_NS_PATH__ \"\"", f'#define __TRACKED_NS_PATH__ "{c_path}"')
    bpf_text = bpf_text.replace("#define __TRACK_BD_MAJOR__ 0", f"#define __TRACK_BD_MAJOR__ {bd_major}")
    bpf_text = bpf_text.replace("#define __TRACK_BD_MINOR__ 0", f"#define __TRACK_BD_MINOR__ {bd_minor}")
    bpf_text = bpf_text.replace("#define __LBA_SHIFT__ 0", f"#define __LBA_SHIFT__ {lba_shift}")
    bpf_text = bpf_text.replace(
        "#define __LBAS_PER_CHUNK__ 0", f"#define __LBAS_PER_CHUNK__ {lbas_per_chunk}"
    )
    bpf_text = bpf_text.replace(
        "#define __SV_EVENT_AT_UPDATE__ 1",
        f"#define __SV_EVENT_AT_UPDATE__ {SV_EVENT_AT_UPDATE}",
    )
    # Inject max chunks for BPF_ARRAY size (dynamically set based on device size)
    bpf_text = bpf_text.replace(
        "BPF_ARRAY(chunk_array, struct chunk_info, 1);",
        f"BPF_ARRAY(chunk_array, struct chunk_info, {max_chunks});",
    )
    return bpf_text

def trace_pipe_reader(stop_event):
    """Read bpf_trace_printk() output from trace_pipe and print to stdout."""
    try:
        with open(TRACE_PIPE, "r") as f:
            while not stop_event.is_set():
                line = f.readline()
                if line:
                    print(f"[trace] {line.rstrip()}")
    except FileNotFoundError:
        pass  # tracefs not mounted
    except Exception:
        pass

# --- User space (control plane) ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", required=True, help="NVMe controller character device (e.g. /dev/nvme0)")
    parser.add_argument("--ns", type=int, required=True, metavar="NSID", help="Namespace ID (e.g. 1 for nvme0n1)")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    args = parser.parse_args()

    ns_path = namespace_block_path(args.dev, args.ns)
    device_size = get_namespace_size_bytes(args.dev, args.ns)
    logical_b = get_logical_block_size_bytes(ns_path)
    lba_shift, lbas_per_chunk = lba_shift_and_lbas_per_chunk(logical_b)
    max_chunks = device_size // CHUNK_SIZE
    print(
        f"Tracking {ns_path} (controller {args.dev}, NS {args.ns}) every {DEFAULT_INTERVAL}s"
    )
    print(f"Device size: {device_size / (1024**3):.2f} GB")
    print(
        f"Logical block: {logical_b} B → lba_shift={lba_shift}, LBAs/chunk={lbas_per_chunk}"
    )
    print(f"Max chunks: {max_chunks:,} (BPF_ARRAY size will be set to {max_chunks:,})")
    print(f"SV event at update (BPF __SV_EVENT_AT_UPDATE__): {SV_EVENT_AT_UPDATE}")
    print(f"Saving to {args.output}\n")

    bpf_code = load_bpf_code(max_chunks, ns_path, lba_shift, lbas_per_chunk)
    b = BPF(text=bpf_code, cflags=["-Wno-duplicate-decl-specifier"])
    chunk_array = b.get_table("chunk_array")
    window_cfg = b.get_table("window_cfg")
    wkey = ctypes.c_int(0)
    interval_ms = DEFAULT_INTERVAL * 1000

    # kretprobe__nvme_setup_cmd is auto-attached by BCC

    # Start trace_pipe reader so bpf_trace_printk() shows in this terminal
    stop_trace = threading.Event()
    trace_thread = threading.Thread(target=trace_pipe_reader, args=(stop_trace,), daemon=True)
    trace_thread.start()
    print("BPF trace_pipe reader started (bpf_trace_printk will appear as [trace] ...)\n")

    out_path = Path(args.output)
    out_dir = out_path.parent if str(out_path.parent) != "" else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_path.stem
    suffix = out_path.suffix or ".csv"
    header = "cidx,sv_t2e_s,sv_ev_ind,upd_cnt_log1p,last_int_ms_log1p\n"
    window_idx = 0

    try:
        while True:
            window_idx += 1
            window_start_ms = time.monotonic_ns() // 1_000_000
            window_end_ms = window_start_ms + interval_ms
            window_cfg[wkey] = WindowTimes(window_start_ms, window_end_ms)
            time.sleep(DEFAULT_INTERVAL)

            # 1) Write CSV: BPF already stores window-relative values.
            window_out = out_dir / f"{stem}_{window_idx}{suffix}"
            with open(window_out, "w") as f:
                f.write(header)
                for chunk_id in range(max_chunks):
                    v = chunk_array[chunk_id]
                    uc = v.update_count
                    ev = v.sv_event_indicator
                    if uc > 0:
                        if ev:
                            sv_t2e_s = min(int(v.sv_time2event_ms), interval_ms) / 1000.0
                            if (sv_t2e_s >= interval_ms / 1000.0):
                                print(f"1. sv_t2e_s >= interval_ms / 1000.0: {sv_t2e_s}")
                                sv_t2e_s = interval_ms / 1000.0
                        else:
                            sv_t2e_s = interval_ms / 1000.0

                        last_int_ms = int(v.last_update_interval_ms)
                        if last_int_ms >= interval_ms:
                            print(f"2. last_int_ms >= interval_ms: {last_int_ms}")
                            last_int_ms = interval_ms
                        
                        uc_log1p = math.log1p(float(uc))
                        last_int_ms_log1p = math.log1p(float(last_int_ms))
                        
                        f.write(
                            f"{chunk_id},{sv_t2e_s:.3f},{ev},{uc_log1p:.4f},{last_int_ms_log1p:.4f}\n"
                        )

            # 2) Clear chunk_array after writing (next window starts here)
            for chunk_id in range(max_chunks):
                v = chunk_array[chunk_id]
                v.update_count = 0
                v.last_update_interval_ms = 0
                v.last_update_ts_ms = 0
                v.sv_time2event_ms = 0
                v.sv_event_indicator = 0
                v.last_accessed_lba = 0xFFFFFFFFFFFFFFFF  # unset for next window
                chunk_array[chunk_id] = v

            print(f"[window_end_ms={window_end_ms}] Wrote {window_out} and cleared map.")

    except KeyboardInterrupt:
        print("\nTracing stopped.")
    finally:
        stop_trace.set()

if __name__ == "__main__":
    main()