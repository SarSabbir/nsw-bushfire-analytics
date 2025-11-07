#!/usr/bin/env python3
import os, pandas as pd, numpy as np, matplotlib.pyplot as plt

PROC_DIR = "data/processed"
FIG_DIR  = "reports/figures"
os.makedirs(FIG_DIR, exist_ok=True)

def save_ts_with_trend():
    path = os.path.join(PROC_DIR, "daily_with_pred.csv")
    if not os.path.exists(path):
        raise SystemExit("❌ Run 03_daily_counts_models.py first.")
    d = pd.read_csv(path, parse_dates=["date"])
    plt.figure()
    plt.plot(d["date"], d["count"], label="Daily count")
    plt.plot(d["date"], d["pred"], label="Poisson GLM fit")
    plt.title("NSW daily fire detections (FIRMS/MODIS/VIIRS)")
    plt.xlabel("Date"); plt.ylabel("Count"); plt.legend()
    out = os.path.join(FIG_DIR, "nsw_daily_with_trend.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Saved {out}")

def save_hist():
    path = os.path.join(PROC_DIR, "daily_counts_nsw.csv")
    d = pd.read_csv(path, parse_dates=["date"])
    plt.figure()
    plt.hist(d["count"].values, bins=20)
    plt.title("Distribution of daily counts")
    plt.xlabel("Count"); plt.ylabel("Frequency")
    out = os.path.join(FIG_DIR, "nsw_daily_hist.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Saved {out}")

def save_weather_scatter():
    path = os.path.join(PROC_DIR, "counts_weather_join.csv")
    if not os.path.exists(path):
        print("ℹ️ No weather join file found; skipping weather scatter.")
        return
    j = pd.read_csv(path, parse_dates=["date"])
    # find a numeric weather column
    numeric_cols = j.select_dtypes(include=[np.number]).columns.tolist()
    # remove 't' if present, retain 'count'
    candidates = [c for c in numeric_cols if c not in ("t","pred","count")]
    if not candidates:
        print("ℹ️ No numeric weather columns to plot.")
        return
    col = candidates[0]
    plt.figure()
    plt.scatter(j[col], j["count"])
    plt.xlabel(col); plt.ylabel("Daily fire count")
    plt.title(f"Daily fire count vs {col}")
    out = os.path.join(FIG_DIR, "count_vs_weather.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Saved {out}")

def main():
    save_ts_with_trend()
    save_hist()
    save_weather_scatter()

if __name__ == "__main__":
    main()
