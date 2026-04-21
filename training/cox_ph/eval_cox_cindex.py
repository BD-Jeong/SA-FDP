#!/usr/bin/env python3
"""
학습된 Cox PH 아티팩트(.pkl)와 정답지 CSV( train_cox_model.py 와 동일 컬럼 )로 C-index 평가.

정답지 형식: training_k.csv — 컬럼 cidx, sv_t2e_s, sv_ev_ind, upd_cnt_log1p, last_int_ms_log1p
(실제 값이 log1p가 아니어도 컬럼 이름만 맞으면 됨)

예:
  python3 eval_cox_cindex.py \
    --model fio_model/cox_model_fio_1_55.pkl \
    --data-dir ../../out/train \
    --eval-range 180 204

  python3 eval_cox_cindex.py --model model.pkl --eval-csvs a.csv b.csv
"""
from __future__ import annotations

import argparse
import gc
import os
import pickle
import sys
from typing import Iterable, List, Sequence

import pandas as pd
from lifelines.utils import concordance_index


def parse_args():
    p = argparse.ArgumentParser(
        description="Evaluate concordance index (C-index) with a trained Cox PH artifact and label CSVs."
    )
    p.add_argument("--model", required=True, help="Path to pickle from train_cox_model.py")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--data-dir",
        help="Directory containing training_{k}.csv",
    )
    g.add_argument(
        "--eval-csvs",
        nargs="+",
        help="Explicit list of evaluation CSV paths",
    )
    p.add_argument(
        "--eval-range",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        help="With --data-dir: use training_START.csv ... training_END.csv",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per read_csv chunk (default: 500000)",
    )
    p.add_argument(
        "--per-file",
        action="store_true",
        help="Print C-index for each file in addition to pooled.",
    )
    return p.parse_args()


def _build_paths_from_range(data_dir: str, start: int, end: int) -> List[str]:
    paths = []
    for i in range(start, end + 1):
        path = os.path.join(data_dir, f"training_{i}.csv")
        if os.path.isfile(path):
            paths.append(path)
        else:
            print(f"[WARN] missing: {path}", file=sys.stderr)
    return paths


def load_artifact(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


def prepare_eval_df(
    df: pd.DataFrame,
    *,
    duration_col: str,
    event_col: str,
    feature_cols: Sequence[str],
    id_col: str,
) -> pd.DataFrame:
    required = [id_col, duration_col, event_col, *feature_cols]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    df = df[required].copy()
    for col in [duration_col, event_col, *feature_cols]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=[duration_col, event_col, *feature_cols])
    dropped = before - len(df)
    if dropped:
        print(f"  dropped NA rows: {dropped:,}")
    df[event_col] = df[event_col].astype(int)
    df = df[df[duration_col] > 0].copy()
    return df


def eval_one_dataframe(
    cph,
    scaler,
    df: pd.DataFrame,
    *,
    duration_col: str,
    event_col: str,
    feature_cols: Sequence[str],
) -> float:
    x = df[list(feature_cols)].astype("float64", copy=False)
    x_scaled = pd.DataFrame(
        scaler.transform(x),
        columns=list(feature_cols),
        index=df.index,
    )
    risk = cph.predict_partial_hazard(x_scaled)
    return float(
        concordance_index(
            df[duration_col].values,
            -risk.values.ravel(),
            df[event_col].values,
        )
    )


def load_csvs_concat(
    paths: Iterable[str],
    *,
    chunksize: int,
    duration_col: str,
    event_col: str,
    feature_cols: Sequence[str],
    id_col: str,
) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    usecols = [id_col, duration_col, event_col, *feature_cols]
    for path in paths:
        for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
            parts.append(chunk)
        gc.collect()
    if not parts:
        raise ValueError("No rows loaded from CSV paths.")
    return pd.concat(parts, ignore_index=True)


def main():
    args = parse_args()
    artifact_path = os.path.abspath(os.path.expanduser(args.model))
    art = load_artifact(artifact_path)
    cph = art["cph"]
    scaler = art["scaler"]
    duration_col = art["duration_col"]
    event_col = art["event_col"]
    id_col = art["id_col"]
    feature_cols = list(art["feature_cols"])

    if args.eval_csvs:
        paths = [os.path.abspath(os.path.expanduser(p)) for p in args.eval_csvs]
    else:
        if args.eval_range is None:
            print("With --data-dir you must pass --eval-range START END", file=sys.stderr)
            sys.exit(2)
        start, end = args.eval_range
        if start > end:
            print("eval-range: START must be <= END", file=sys.stderr)
            sys.exit(2)
        data_dir = os.path.abspath(os.path.expanduser(args.data_dir))
        paths = _build_paths_from_range(data_dir, start, end)
        if not paths:
            print("No evaluation CSV files found.", file=sys.stderr)
            sys.exit(1)

    print(f"Model: {artifact_path}")
    print(f"Features: {feature_cols}")
    print(f"Eval files: {len(paths)}")

    if args.per_file:
        for path in paths:
            df = load_csvs_concat(
                [path],
                chunksize=args.chunksize,
                duration_col=duration_col,
                event_col=event_col,
                feature_cols=feature_cols,
                id_col=id_col,
            )
            df = prepare_eval_df(
                df,
                duration_col=duration_col,
                event_col=event_col,
                feature_cols=feature_cols,
                id_col=id_col,
            )
            ci = eval_one_dataframe(
                cph,
                scaler,
                df,
                duration_col=duration_col,
                event_col=event_col,
                feature_cols=feature_cols,
            )
            print(f"  C-index [{os.path.basename(path)}]: {ci:.6f}  (n={len(df):,}, events={int(df[event_col].sum()):,})")

    df_all = load_csvs_concat(
        paths,
        chunksize=args.chunksize,
        duration_col=duration_col,
        event_col=event_col,
        feature_cols=feature_cols,
        id_col=id_col,
    )
    df_all = prepare_eval_df(
        df_all,
        duration_col=duration_col,
        event_col=event_col,
        feature_cols=feature_cols,
        id_col=id_col,
    )
    ci_pooled = eval_one_dataframe(
        cph,
        scaler,
        df_all,
        duration_col=duration_col,
        event_col=event_col,
        feature_cols=feature_cols,
    )
    print(
        f"Pooled C-index: {ci_pooled:.6f}  (n={len(df_all):,}, events={int(df_all[event_col].sum()):,})"
    )


if __name__ == "__main__":
    main()
