#!/usr/bin/env python3
import argparse
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

def get_device_size(device_path):
    """
    Get total controller size in bytes using nvme id-ctrl command
    Reads tnvmcap (Total NVM Capacity) from controller
    
    Args:
        device_path: Path to controller device (e.g., /dev/nvme0)
        
    Returns:
        int: Total controller size in bytes (tnvmcap value)
    """
    try:
        # Run nvme id-ctrl command to get tnvmcap
        result = subprocess.run(
            ["sudo", "nvme", "id-ctrl", device_path],
            capture_output=True,
            text=True,
            check=True
        )
        # Parse tnvmcap from output (case-insensitive)
        # Output is always in decimal format
        for line in result.stdout.split('\n'):
            if 'tnvmcap' in line.lower():
                # Extract the value (format: "tnvmcap : 1234567890")
                parts = line.split(':')
                if len(parts) >= 2:
                    # Remove whitespace and parse as decimal integer
                    value_str = parts[1].strip()
                    total_size = int(value_str)
                    return total_size
        raise RuntimeError(f"tnvmcap not found in nvme id-ctrl output")
    except (subprocess.CalledProcessError, ValueError, IOError) as e:
        raise RuntimeError(f"Failed to get device size: {e}")

# --- Load eBPF C code from file ---
def load_bpf_code(max_chunks):
    """
    Load eBPF C code from file and inject max chunks
    
    Args:
        max_chunks: Maximum number of chunks to inject
        
    Returns:
        str: eBPF C code with parameters injected
    """
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    bpf_file = script_dir / "training.bpf.c"
    with open(bpf_file, "r") as f:
        bpf_text = f.read()
    # Inject max chunks as compile-time constant (verifier rejects global variable read)
    bpf_text = bpf_text.replace("#define __MAX_CHUNKS__ 0", f"#define __MAX_CHUNKS__ {max_chunks}")
    # Inject max chunks for BPF_ARRAY size (dynamically set based on device size)
    bpf_text = bpf_text.replace("BPF_ARRAY(chunk_array, struct chunk_info, 1);",
                                f"BPF_ARRAY(chunk_array, struct chunk_info, {max_chunks});")
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
    parser.add_argument("--device", required=True, help="Target NVMe controller device (e.g., /dev/nvme0)")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    args = parser.parse_args()

    # Get device size and calculate max chunks
    # Note: max_chunks will be injected into eBPF code as the BPF_ARRAY size
    device_size = get_device_size(args.device)
    max_chunks = device_size // CHUNK_SIZE
    print(f"Tracking {args.device} (all namespaces) every {DEFAULT_INTERVAL}s")
    print(f"Device size: {device_size / (1024**3):.2f} GB")
    print(f"Max chunks: {max_chunks:,} (BPF_ARRAY size will be set to {max_chunks:,})")
    print(f"Saving to {args.output}\n")

    # Load eBPF code from file and inject max chunks
    bpf_code = load_bpf_code(max_chunks)
    b = BPF(text=bpf_code)
    chunk_array = b.get_table("chunk_array")

    # kretprobe__nvme_setup_cmd is auto-attached by BCC

    # Start trace_pipe reader so bpf_trace_printk() shows in this terminal
    stop_trace = threading.Event()
    trace_thread = threading.Thread(target=trace_pipe_reader, args=(stop_trace,), daemon=True)
    trace_thread.start()
    print("BPF trace_pipe reader started (bpf_trace_printk will appear as [trace] ...)\n")

    # CSV header: cidx, sv_t2e, sv_ev_ind, upd_cnt, last_int (monotonic time)
    with open(args.output, "w") as f:
        f.write("cidx,sv_t2e,sv_ev_ind,upd_cnt,last_int\n")

    snapshot_buffer = [None] * max_chunks  # Snapshot buffer
    try:
        while True:
            window_start = time.monotonic()
            time.sleep(DEFAULT_INTERVAL)
            window_end = time.monotonic()

            # 1) Copy chunk_array to snapshot buffer
            for chunk_id in range(max_chunks):
                v = chunk_array[chunk_id]
                snapshot_buffer[chunk_id] = (
                    v.sv_time2event_ts,
                    v.last_update_ts,
                    v.update_count,
                    v.sv_event_indicator,
                )

            # 2) Initialize chunk_array immediately (next window starts here)
            for chunk_id in range(max_chunks):
                v = chunk_array[chunk_id]
                v.update_count = 0
                v.last_update_ts = 0
                v.sv_time2event_ts = 0
                v.sv_event_indicator = 0
                v.last_accessed_lba = 0xFFFFFFFFFFFFFFFF  # unset for next window

            # 3) Write snapshot buffer to file (monotonic time operations)
            with open(args.output, "a") as f:
                for chunk_id in range(max_chunks):
                    t2e_ts, luts, uc, ev = snapshot_buffer[chunk_id]
                    if uc > 0:
                        sv_t2e = max(0.0, t2e_ts - window_start) # time to event from window start
                        last_int = max(0.0, window_end - luts) # latest update interval 
                        f.write(f"{chunk_id},{sv_t2e},{ev},{uc},{last_int}\n")

            print(f"[{window_end:.1f}] Dumped window data and cleared map.")

    except KeyboardInterrupt:
        print("\nTracing stopped.")
    finally:
        stop_trace.set()

if __name__ == "__main__":
    main()