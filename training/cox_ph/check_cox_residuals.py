#!/usr/bin/env python3
"""
Check Cox PH residual diagnostics using a trained artifact and dataset.

Example:
  python3 check_cox_residuals.py \
    --model cox_model_fio_1_30.pkl \
    --data-dir ../fio/out \
    --train-range 1 30 \
    --sample-frac 0.01 \
    --out-prefix cox_residuals_fio \
    --cache-out cox_residuals_fio_residual_cache.pkl \
    --max-plot-points 100000

  python3 check_cox_residuals.py \
    --cache-in cox_residuals_fio_residual_cache.pkl \
    --out-prefix cox_residuals_fio \
    --max-plot-points 100000
"""
import argparse
import os
import pickle

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from lifelines import CoxPHFitter
from lifelines.statistics import proportional_hazard_test
from statsmodels.nonparametric.smoothers_lowess import lowess


def parse_args():
    p = argparse.ArgumentParser(description="Residual diagnostics for trained Cox PH model.")
    p.add_argument("--model", required=False, help="Path to model artifact .pkl")
    p.add_argument("--data-dir", required=False, help="Directory containing training_*.csv")
    p.add_argument(
        "--train-range",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        required=False,
        help="CSV range: training_START.csv ... training_END.csv",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per read_csv chunk (default: 500000).",
    )
    p.add_argument(
        "--sample-frac",
        type=float,
        default=None,
        help="Optional sampling fraction per chunk in (0,1].",
    )
    p.add_argument("--seed", type=int, default=0, help="RNG seed for sampling")
    p.add_argument(
        "--max-plot-points",
        type=int,
        default=30000,
        help="Max points per covariate to draw in scatter/LOWESS (default: 30000). Use <=0 for all.",
    )
    p.add_argument(
        "--out-prefix",
        default="cox_residuals",
        help="Output prefix for csv/pdf (default: cox_residuals)",
    )
    p.add_argument(
        "--cache-out",
        default=None,
        help="Optional path to save residual cache (.pkl). Default: <out-prefix>_residual_cache.pkl",
    )
    p.add_argument(
        "--cache-in",
        default=None,
        help="If set, skip model/data compute and draw plot from cached residual .pkl.",
    )
    args = p.parse_args()
    if not args.cache_in:
        missing = []
        if not args.model:
            missing.append("--model")
        if not args.data_dir:
            missing.append("--data-dir")
        if args.train_range is None:
            missing.append("--train-range")
        if missing:
            p.error(
                "the following arguments are required when --cache-in is not set: "
                + ", ".join(missing)
            )
    return args


def build_file_list(data_dir, start_idx, end_idx):
    files = []
    for i in range(start_idx, end_idx + 1):
        path = os.path.join(data_dir, f"training_{i}.csv")
        if os.path.exists(path):
            files.append(path)
        else:
            print(f"[WARN] missing file: {path}")
    return files


def load_csvs(file_list, usecols, chunksize, sample_frac, seed):
    if sample_frac is not None and not (0 < sample_frac <= 1):
        raise ValueError("--sample-frac must be in (0,1]")

    rng = np.random.default_rng(seed)
    dfs = []
    for f in file_list:
        for chunk in pd.read_csv(f, usecols=usecols, chunksize=chunksize):
            if sample_frac is not None:
                sample_n = max(1, int(round(len(chunk) * sample_frac)))
                if sample_n < len(chunk):
                    chunk = chunk.sample(
                        n=sample_n,
                        random_state=int(rng.integers(0, 2**31 - 1)),
                    )
            dfs.append(chunk.copy())
    if not dfs:
        raise ValueError("No rows loaded from CSV files.")
    return pd.concat(dfs, ignore_index=True)


def draw_schoenfeld_plot(feature_cols, scho, event_times, ph_summary, pdf_out, max_plot_points, seed):
    n = len(feature_cols)
    fig, axes = plt.subplots(n, 1, figsize=(10, 3.6 * n), constrained_layout=True)
    if n == 1:
        axes = [axes]

    sns.set_style("whitegrid")
    rng = np.random.default_rng(seed)

    for i, cov in enumerate(feature_cols):
        ax = axes[i]
        y = scho[cov].values
        x = event_times

        if max_plot_points and max_plot_points > 0 and len(x) > max_plot_points:
            idx = rng.choice(len(x), size=max_plot_points, replace=False)
            x = x[idx]
            y = y[idx]

        order = np.argsort(x)
        x_sorted = x[order]
        y_sorted = y[order]
        smooth = lowess(y_sorted, x_sorted, frac=0.3, return_sorted=True)

        ax.scatter(x, y, s=14, color="gray", alpha=0.65, linewidths=0)

        yhat_interp = np.interp(x_sorted, smooth[:, 0], smooth[:, 1])
        resid_std = float(np.std(y_sorted - yhat_interp))
        ax.fill_between(
            smooth[:, 0],
            smooth[:, 1] - resid_std,
            smooth[:, 1] + resid_std,
            color="gray",
            alpha=0.22,
            label="~1 sigma band",
        )

        ax.plot(smooth[:, 0], smooth[:, 1], color="#2A6FDF", linewidth=2.2, label="LOWESS")
        ax.axhline(0.0, linestyle="--", color="black", linewidth=1)
        ax.set_ylim(-5, 5)

        pval = np.nan
        if ph_summary is not None and cov in ph_summary.index and "p" in ph_summary.columns:
            pval = ph_summary.loc[cov, "p"]
        ax.set_title(f"Schoenfeld test p={pval:.3g} ({cov})", fontsize=11)
        ax.set_xlabel("Time", fontsize=10)
        ax.set_ylabel("Residual", fontsize=10)
        ax.legend(loc="best", fontsize=8)

    plt.savefig(pdf_out, dpi=150)
    plt.close(fig)
    print(f"[done] Schoenfeld residual plot: {pdf_out}")


def main():
    args = parse_args()
    pdf_out = f"{args.out_prefix}_schoenfeld.pdf"

    # Fast path: plot-only from cached residual payload.
    if args.cache_in:
        with open(args.cache_in, "rb") as f:
            payload = pickle.load(f)
        feature_cols = payload["feature_cols"]
        scho = payload["scho"]
        event_times = np.asarray(payload["event_times"])
        ph_summary = payload.get("ph_summary", None)
        draw_schoenfeld_plot(
            feature_cols, scho, event_times, ph_summary, pdf_out, args.max_plot_points, args.seed
        )
        return

    with open(args.model, "rb") as f:
        artifact = pickle.load(f)

    cph = artifact["cph"]
    scaler = artifact["scaler"]
    duration_col = artifact["duration_col"]
    event_col = artifact["event_col"]
    feature_cols = artifact["feature_cols"]

    start_idx, end_idx = args.train_range
    files = build_file_list(args.data_dir, start_idx, end_idx)
    if not files:
        raise SystemExit("No input CSV files found in the requested range.")

    usecols = [duration_col, event_col] + feature_cols
    df = load_csvs(
        file_list=files,
        usecols=usecols,
        chunksize=args.chunksize,
        sample_frac=args.sample_frac,
        seed=args.seed,
    )

    for col in usecols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=usecols).copy()
    df[event_col] = df[event_col].astype(int)
    df = df[df[duration_col] > 0].copy()
    df[feature_cols] = scaler.transform(df[feature_cols].astype("float64"))

    # If sampled rows differ from original training size, fit a diagnostic model.
    diag_cph = cph
    fitted_n = getattr(cph, "_n_examples", None)
    if fitted_n is not None and int(fitted_n) != len(df):
        penalizer = float(artifact.get("penalizer", 0.1))
        print(
            "[info] row count differs from trained model "
            f"(trained={int(fitted_n):,}, current={len(df):,}). "
            "Refitting diagnostic Cox model on current data."
        )
        diag_cph = CoxPHFitter(penalizer=penalizer)
        diag_cph.fit(
            df[[duration_col, event_col] + feature_cols],
            duration_col=duration_col,
            event_col=event_col,
        )

    ph_test = proportional_hazard_test(diag_cph, df, time_transform="rank")
    ph_summary = ph_test.summary.copy()
    ph_csv = f"{args.out_prefix}_ph_test.csv"
    ph_summary.to_csv(ph_csv)
    print(f"[done] PH test summary: {ph_csv}")

    scho = diag_cph.compute_residuals(df, kind="schoenfeld")
    event_times = df.loc[scho.index, duration_col].values
    draw_schoenfeld_plot(
        feature_cols, scho, event_times, ph_summary, pdf_out, args.max_plot_points, args.seed
    )

    cache_out = args.cache_out or f"{args.out_prefix}_residual_cache.pkl"
    payload = {
        "feature_cols": feature_cols,
        "event_times": event_times,
        "scho": scho,
        "ph_summary": ph_summary,
    }
    with open(cache_out, "wb") as f:
        pickle.dump(payload, f)
    print(f"[done] residual cache saved: {cache_out}")


if __name__ == "__main__":
    main()
