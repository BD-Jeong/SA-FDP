#!/usr/bin/env python3
import argparse
import itertools
import json
import os
import shlex
import stat
import subprocess
import time
from pathlib import Path

# Global test matrix/constants
IODEPTHS = [1, 8, 16]
SKEWS = [0.0, 0.6, 1.2]
BS = "4k"
RUNTIME_SEC = 300


def parse_size_to_bytes(size_str: str) -> int:
    units = {
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }
    s = size_str.strip().lower()
    if s[-1] in units:
        return int(float(s[:-1]) * units[s[-1]])
    return int(s)


def uniform_read_mix_percent(worker_idx: int, total_workers: int) -> float:
    if total_workers <= 1:
        return 0.0
    # Spread uniformly in [0, 100) so no worker becomes 100% read.
    return 100.0 * worker_idx / float(total_workers)


def region_layout(total_size: int, workers: int, bs_bytes: int):
    chunk = total_size // workers
    chunk = (chunk // bs_bytes) * bs_bytes
    if chunk <= 0:
        raise ValueError("Per-worker region became 0. Increase --total-size or reduce --workers.")

    regions = []
    for i in range(workers):
        offset = i * chunk
        if i == workers - 1:
            size = total_size - offset
            size = (size // bs_bytes) * bs_bytes
        else:
            size = chunk
        if size <= 0:
            raise ValueError("Invalid region size (<= 0) after alignment.")
        regions.append((offset, size))
    return regions


def build_fio_cmd(args, combo_tag: str, worker_idx: int, iodepth: int, skew: float, offset: int, size: int):
    rw_mix = uniform_read_mix_percent(worker_idx, args.workers)
    output_path = args.output_dir / f"{combo_tag}_w{worker_idx:02d}.json"

    cmd = [
        "fio",
        f"--name={combo_tag}_w{worker_idx:02d}",
        f"--filename={args.filename}",
        "--ioengine=libaio",
        "--direct=1",
        "--time_based=1",
        f"--runtime={RUNTIME_SEC}",
        "--group_reporting=1",
        "--rw=randrw",
        f"--bs={BS}",
        f"--iodepth={iodepth}",
        f"--offset={offset}",
        f"--size={size}",
        f"--rwmixread={rw_mix:.2f}",
        f"--random_distribution=zipf:{skew}",
        "--output-format=json",
        f"--output={output_path}",
    ]
    return cmd, rw_mix, output_path


def get_lsblk_parent_map():
    out = subprocess.check_output(
        ["lsblk", "-rno", "NAME,PKNAME"], text=True
    ).strip().splitlines()
    parent_map = {}
    for line in out:
        parts = line.split()
        if not parts:
            continue
        name = parts[0]
        pkname = parts[1] if len(parts) > 1 else ""
        parent_map[name] = pkname
    return parent_map


def top_level_disk_name(dev_path: str) -> str:
    real = os.path.realpath(dev_path)
    name = os.path.basename(real)
    parent_map = get_lsblk_parent_map()
    cur = name
    seen = set()
    while cur in parent_map and parent_map[cur]:
        if cur in seen:
            break
        seen.add(cur)
        cur = parent_map[cur]
    return cur


def assert_not_os_disk(target_dev: str):
    st = os.stat(target_dev)
    if not stat.S_ISBLK(st.st_mode):
        raise RuntimeError(f"Target is not a block device: {target_dev}")

    root_source = subprocess.check_output(["findmnt", "-nro", "SOURCE", "/"], text=True).strip()
    target_top = top_level_disk_name(target_dev)
    root_top = top_level_disk_name(root_source)
    if target_top == root_top:
        raise RuntimeError(
            "Refusing to run fio on OS disk.\n"
            f"target={target_dev} (top={target_top})\n"
            f"rootfs_source={root_source} (top={root_top})"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Run fio matrix: iodepth x skew with N independent workers (numjobs=1 each)."
    )
    parser.add_argument("--filename", required=True, help="Target block device/file (e.g. /dev/nvme0n1).")
    parser.add_argument(
        "--total-size",
        action="append",
        required=True,
        help="Total range size for partitioning (e.g. 256G). Repeat this option for multiple sizes.",
    )
    parser.add_argument("--workers", type=int, default=16, help="Number of fio processes. Default: 16")
    parser.add_argument("--output-dir", default="out/fio-matrix", help="Directory for fio json outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only.")
    args = parser.parse_args()

    if args.workers <= 0:
        raise ValueError("--workers must be > 0")
    assert_not_os_disk(args.filename)

    iodepths = IODEPTHS
    skews = SKEWS
    bs_bytes = 4096

    args.output_dir = Path(args.output_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    matrix = list(itertools.product(iodepths, skews))
    total_sizes = args.total_size
    total_combos = len(total_sizes) * len(matrix)
    print(f"[info] workers={args.workers}, matrix={len(matrix)} combos/size, runtime={RUNTIME_SEC}s per combo")
    print(f"[info] target={args.filename}, total_sizes={total_sizes}, output_dir={args.output_dir}")

    combo_idx = 0
    for total_size_str in total_sizes:
        total_size = parse_size_to_bytes(total_size_str)
        regions = region_layout(total_size, args.workers, bs_bytes)
        size_tag = total_size_str.lower().replace(".", "p")
        print(f"\n[size] {total_size_str} ({total_size} bytes)")

        for iodepth, skew in matrix:
            combo_idx += 1
            combo_tag = f"s{size_tag}_c{combo_idx:02d}_qd{iodepth}_sk{str(skew).replace('.', 'p')}"
            print(f"\n[combo {combo_idx}/{total_combos}] size={total_size_str}, iodepth={iodepth}, skew={skew}")
            procs = []
            combo_meta = []
            t0 = time.time()

            for worker_idx, (offset, size) in enumerate(regions):
                cmd, rw_mix, out_json = build_fio_cmd(
                    args=args,
                    combo_tag=combo_tag,
                    worker_idx=worker_idx,
                    iodepth=iodepth,
                    skew=skew,
                    offset=offset,
                    size=size,
                )
                combo_meta.append(
                    {
                        "worker": worker_idx,
                        "offset": offset,
                        "size": size,
                        "rwmixread_percent": rw_mix,
                        "command": cmd,
                        "output_json": str(out_json),
                    }
                )
                if args.dry_run:
                    print(shlex.join(cmd))
                    continue
                procs.append((worker_idx, subprocess.Popen(cmd)))

            if args.dry_run:
                continue

            failed = False
            for worker_idx, proc in procs:
                rc = proc.wait()
                if rc != 0:
                    failed = True
                    print(f"[error] worker {worker_idx} exited with rc={rc}")

            elapsed = time.time() - t0
            meta_path = args.output_dir / f"{combo_tag}_meta.json"
            with meta_path.open("w") as f:
                json.dump(
                    {
                        "combo_tag": combo_tag,
                        "total_size_input": total_size_str,
                        "total_size_bytes": total_size,
                        "iodepth": iodepth,
                        "skew": skew,
                        "workers": args.workers,
                        "runtime_sec": RUNTIME_SEC,
                        "elapsed_sec": elapsed,
                        "failed": failed,
                        "jobs": combo_meta,
                    },
                    f,
                    indent=2,
                )
            print(f"[done] {combo_tag} elapsed={elapsed:.1f}s meta={meta_path}")

            if failed:
                raise RuntimeError(f"Combination failed: {combo_tag}")


if __name__ == "__main__":
    main()
