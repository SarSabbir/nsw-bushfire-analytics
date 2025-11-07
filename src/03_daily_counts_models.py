#!/usr/bin/env python3
import os, numpy as np, pandas as pd, statsmodels.api as sm
from scipy.stats import kendalltau
import matplotlib.pyplot as plt

IN_PATH = "data/processed/daily_counts_nsw.csv"
OUT_DIR = "data/processed"
FIG_DIR = "reports/figures"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

def fit_poisson_glm(d):
    # simple time index
    d = d.copy()
    d["t"] = np.arange(len(d))
    X = sm.add_constant(d["t"].values)
    y = d["count"].values
    model = sm.GLM(y, X, family=sm.families.Poisson())
    res = model.fit()
    return res, d

def main():
    if not os.path.exists(IN_PATH):
        raise SystemExit("❌ Run 02_join_regions.py first.")
    daily = pd.read_csv(IN_PATH, parse_dates=["date"]).sort_values("date")

    # Basic stats
    tau, p = kendalltau(np.arange(len(daily)), daily["count"].values)
    res, d2 = fit_poisson_glm(daily)

    # Save model summary
    summary_path = os.path.join(FIG_DIR, "glm_summary.txt")
    with open(summary_path, "w") as f:
        f.write(res.summary().as_text())
        f.write(f"\n\nKendall tau = {tau:.4f}, p = {p:.4f}\n")
    print(f"✅ Saved GLM summary → {summary_path}")

    # Save fitted values for plotting
    daily["t"] = np.arange(len(daily))
    daily["pred"] = res.predict(sm.add_constant(daily["t"]))
    daily.to_csv(os.path.join(OUT_DIR, "daily_with_pred.csv"), index=False)

if __name__ == "__main__":
    main()
