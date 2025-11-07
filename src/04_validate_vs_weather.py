#!/usr/bin/env python3
import os, pandas as pd, numpy as np

COUNTS_PATH = "data/processed/daily_counts_nsw.csv"
# Flexible: we copy whatever weather file existed to a processed aux in step 01
# Try to find any aux_*Latest Weather* csv
AUX_DIR = "data/processed"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def pick_weather_file():
    for name in os.listdir(AUX_DIR):
        if name.startswith("aux_Latest Weather Observations for New South Wales"):
            return os.path.join(AUX_DIR, name)
    return None

def coerce_date(series):
    out = pd.to_datetime(series, errors="coerce", dayfirst=True)
    return out

def main():
    if not os.path.exists(COUNTS_PATH):
        raise SystemExit("❌ Run 02_join_regions.py first.")
    counts = pd.read_csv(COUNTS_PATH, parse_dates=["date"])

    wfile = pick_weather_file()
    if wfile is None:
        print("⚠️ No weather file found (aux_*Latest Weather*). Skipping validation join.")
        counts.to_csv(os.path.join(OUT_DIR, "counts_weather_join.csv"), index=False)
        return

    w = pd.read_csv(wfile)
    # Try to locate a date column
    date_col = None
    for c in w.columns:
        lc = c.lower()
        if "date" in lc or "time" in lc:
            date_col = c; break
    if date_col is None:
        # fallback: no date → cannot join sensibly
        print("⚠️ Weather file has no obvious date column. Saving counts only.")
        counts.to_csv(os.path.join(OUT_DIR, "counts_weather_join.csv"), index=False)
        return

    w["date"] = coerce_date(w[date_col]).dt.date
    # Select numeric columns only for correlation
    num_cols = [c for c in w.columns if np.issubdtype(type(w[c].dropna().iloc[0]) if w[c].dropna().shape[0] else np.float64, np.number)]
    keep = ["date"] + num_cols
    w2 = w[keep].copy()
    # Aggregate by date (mean of numeric cols)
    wagg = w2.groupby("date").mean(numeric_only=True).reset_index()

    # Join
    counts["date_only"] = counts["date"].dt.date
    joined = counts.merge(wagg, left_on="date_only", right_on="date", how="left", suffixes=("","_w"))
    joined.drop(columns=["date_w"], errors="ignore", inplace=True)
    outp = os.path.join(OUT_DIR, "counts_weather_join.csv")
    joined.to_csv(outp, index=False)
    print(f"✅ Wrote {outp} ({len(joined)} rows)")

if __name__ == "__main__":
    main()
