#!/usr/bin/env python3
"""
Inspect a trained Cox PH artifact (.pkl) and export:
1) feature statistics (coef, HR, p-value, -log2(p))
2) coefficient/hazard-ratio plot image

Example:
  python3 inspect_cox_model.py \
    --model cox_model_fio_1_30.pkl \
    --stats-out cox_feature_stats.csv \
    --plot-out cox_ph_coefficient_plot.pdf
"""
import argparse
import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def parse_args():
    p = argparse.ArgumentParser(description="Inspect trained Cox PH model artifact.")
    p.add_argument("--model", required=True, help="Path to model artifact .pkl")
    p.add_argument(
        "--stats-out",
        default="cox_feature_stats.csv",
        help="Output CSV for feature statistics",
    )
    p.add_argument(
        "--plot-out",
        default="cox_ph_coefficient_plot.pdf",
        help="Output image path for coefficient/hazard-ratio plot",
    )
    return p.parse_args()


def main():
    args = parse_args()
    model_path = Path(args.model)
    stats_out = Path(args.stats_out)
    plot_out = Path(args.plot_out)

    with model_path.open("rb") as f:
        artifact = pickle.load(f)

    cph = artifact["cph"]
    summary = cph.summary.copy()

    cols = [c for c in ["coef", "exp(coef)", "se(coef)", "p"] if c in summary.columns]
    stats = summary[cols].copy()

    if "p" in stats.columns:
        stats["-log2(p)"] = -np.log2(stats["p"].clip(lower=1e-300))

    stats_out.parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(stats_out)

    fig, ax = plt.subplots(figsize=(10, 6))
    cph.plot(ax=ax)
    plt.title("Hazard Ratios (log scale) with 95% Confidence Intervals")
    plt.tight_layout()
    plot_out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(plot_out, dpi=150)
    plt.close(fig)

    print(f"[done] stats: {stats_out}")
    print(f"[done] plot : {plot_out}")


if __name__ == "__main__":
    main()
