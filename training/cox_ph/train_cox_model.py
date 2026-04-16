#!/usr/bin/env python3
"""
Train Cox PH model and save artifacts (model + scaler + metadata).
python3 train_cox_model.py \
  --data-dir ../fio/out/ \
  --train-range 1 30 \
  --model-out cox_model_fio_1_30.pkl \
  --penalizer 0.1 \
  --sample-frac 0.8
"""
import argparse
import gc
import os
import pickle

import numpy as np
import pandas as pd
from lifelines import CoxPHFitter
from lifelines.utils import concordance_index
from sklearn.preprocessing import StandardScaler

DURATION_COL = "sv_t2e_s"
EVENT_COL = "sv_ev_ind"
ID_COL = "cidx"
FEATURE_COLS = ["upd_cnt_log1p","last_int_ms_log1p"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train Cox PH from training_{start}..training_{end} and save model artifact."
    )
    parser.add_argument("--data-dir", required=True, help="Directory containing training_*.csv")
    parser.add_argument(
        "--train-range",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        required=True,
        help="Train files range: training_START.csv ... training_END.csv",
    )
    parser.add_argument(
        "--model-out",
        required=True,
        help="Output path for model artifact (.pkl)",
    )
    parser.add_argument(
        "--penalizer",
        type=float,
        default=0.1,
        help="L2 penalizer for CoxPHFitter (default: 0.1)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per read_csv chunk (default: 500000).",
    )
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=None,
        help="If set (0,1], keep this fraction of rows per chunk.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="RNG seed for sampling (default: 0).",
    )
    return parser.parse_args()


def build_file_list(data_dir, start_idx, end_idx):
    files = []
    for i in range(start_idx, end_idx + 1):
        path = os.path.join(data_dir, f"training_{i}.csv")
        if os.path.exists(path):
            files.append(path)
        else:
            print(f"[WARN] File not found: {path}")
    return files


def _read_cols():
    return [ID_COL, DURATION_COL, EVENT_COL] + FEATURE_COLS


def load_csvs(
    file_list,
    *,
    chunksize,
    sample_frac,
    seed,
):
    if sample_frac is not None and not (0 < sample_frac <= 1):
        raise ValueError("--sample-frac must be in (0, 1]")

    rng = np.random.default_rng(seed)
    usecols = _read_cols()
    dfs = []

    for f in file_list:
        base = os.path.basename(f)
        for chunk in pd.read_csv(f, usecols=usecols, chunksize=chunksize):
            if sample_frac is not None:
                sample_n = max(1, int(round(len(chunk) * sample_frac)))
                if sample_n < len(chunk):
                    chunk = chunk.sample(
                        n=sample_n,
                        random_state=int(rng.integers(0, 2**31 - 1)),
                    )

            chunk = chunk.copy()
            chunk["source_file"] = base
            dfs.append(chunk)

        gc.collect()

    if not dfs:
        raise ValueError("No CSV files were loaded.")
    return pd.concat(dfs, ignore_index=True)


def prepare_df(df):
    required_cols = [ID_COL, DURATION_COL, EVENT_COL] + FEATURE_COLS
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required_cols + ["source_file"]].copy()
    for col in [DURATION_COL, EVENT_COL] + FEATURE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=[DURATION_COL, EVENT_COL] + FEATURE_COLS)
    print(f"Dropped rows with NA: {before - len(df):,}")

    df[EVENT_COL] = df[EVENT_COL].astype(int)
    df = df[df[DURATION_COL] > 0].copy()
    return df


def main():
    args = parse_args()
    data_dir = os.path.abspath(os.path.expanduser(args.data_dir))
    train_start, train_end = args.train_range
    model_out = os.path.abspath(os.path.expanduser(args.model_out))

    if train_start > train_end:
        raise ValueError("train-range START must be <= END")

    files = build_file_list(data_dir, train_start, train_end)
    if not files:
        raise SystemExit("No train files found.")

    print(f"Train files: {len(files)} ({train_start}..{train_end})")
    if args.sample_frac is None:
        print(
            f"Loading train CSVs (chunksize={args.chunksize}, full rows) ... "
            "If OOM, use --sample-frac."
        )
    else:
        print(
            f"Loading train CSVs (chunksize={args.chunksize}, "
            f"sample_frac={args.sample_frac}, seed={args.seed}) ..."
        )
    train_df = load_csvs(
        files,
        chunksize=args.chunksize,
        sample_frac=args.sample_frac,
        seed=args.seed,
    )
    print(f"Train rows (raw): {len(train_df):,}")

    train_df = prepare_df(train_df)
    print(f"Train rows (processed): {len(train_df):,}")
    print(f"Train event rate: {train_df[EVENT_COL].mean():.4f}")
    print(f"Train unique chunks: {train_df[ID_COL].nunique():,}")

    scaler = StandardScaler()
    train_df = train_df.astype({c: "float64" for c in FEATURE_COLS}, copy=False)
    train_df[FEATURE_COLS] = pd.DataFrame(
        scaler.fit_transform(train_df[FEATURE_COLS]),
        columns=FEATURE_COLS,
        index=train_df.index,
    )

    cph = CoxPHFitter(penalizer=args.penalizer)
    cph.fit(
        train_df[[DURATION_COL, EVENT_COL] + FEATURE_COLS],
        duration_col=DURATION_COL,
        event_col=EVENT_COL,
    )

    train_risk = cph.predict_partial_hazard(train_df[FEATURE_COLS])
    train_cindex = concordance_index(
        train_df[DURATION_COL], -train_risk.values.ravel(), train_df[EVENT_COL]
    )
    print(f"Train C-index: {train_cindex:.4f}")
    print("\n=== Cox summary ===")
    print(cph.summary[["coef", "exp(coef)", "se(coef)", "p"]])

    artifact = {
        "cph": cph,
        "scaler": scaler,
        "duration_col": DURATION_COL,
        "event_col": EVENT_COL,
        "id_col": ID_COL,
        "feature_cols": FEATURE_COLS,
        "penalizer": args.penalizer,
        "train_range": [train_start, train_end],
    }
    os.makedirs(os.path.dirname(model_out) or ".", exist_ok=True)
    with open(model_out, "wb") as f:
        pickle.dump(artifact, f)
    print(f"\nSaved model artifact: {model_out}")


if __name__ == "__main__":
    main()
