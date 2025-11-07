#!/usr/bin/env python3
import os, pandas as pd

IN_PATH = "data/processed/events_standardized.csv"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def main():
    if not os.path.exists(IN_PATH):
        raise SystemExit("❌ Run 01_ingest_standardize.py first.")

    df = pd.read_csv(IN_PATH, parse_dates=["date"])
    # Minimal daily NSW counts (no SA2 join to keep it fast)
    daily = (
        df.groupby(df["date"].dt.date)
          .size()
          .reset_index(name="count")
          .rename(columns={"date":"date"})
    )
    out = os.path.join(OUT_DIR, "daily_counts_nsw.csv")
    daily.to_csv(out, index=False)
    print(f"✅ Wrote {out} ({len(daily)} days)")

if __name__ == "__main__":
    main()
