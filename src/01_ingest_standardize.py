#!/usr/bin/env python3
import os, sys, pandas as pd
from datetime import datetime

RAW_DIR = "data/external"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def read_firms_like(path):
    """Read a FIRMS/MODIS/VIIRS CSV and standardize columns."""
    df = pd.read_csv(path)
    # Try common column names
    lat_cols = [c for c in df.columns if c.lower() in ("latitude","lat")]
    lon_cols = [c for c in df.columns if c.lower() in ("longitude","lon","long")]
    date_cols= [c for c in df.columns if c.lower() in ("acq_date","date","acqdate")]
    inst_cols= [c for c in df.columns if c.lower() in ("instrument","satellite","sensor")]
    conf_cols= [c for c in df.columns if "conf" in c.lower()]
    frp_cols = [c for c in df.columns if c.lower()=="frp"]

    def pick(cols, default=None):
        return cols[0] if cols else default

    df = df.rename(columns={
        pick(lat_cols,"lat"): "latitude",
        pick(lon_cols,"lon"): "longitude",
        pick(date_cols,"date"): "date"
    })
    if pick(inst_cols): df = df.rename(columns={pick(inst_cols):"instrument"})
    if pick(conf_cols): df = df.rename(columns={pick(conf_cols):"confidence"})
    if pick(frp_cols):  df = df.rename(columns={pick(frp_cols):"frp"})

    # parse date
    def parse_date(x):
        for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d","%d-%m-%Y"):
            try: return datetime.strptime(str(x), fmt).date()
            except: pass
        # try pandas
        try: return pd.to_datetime(x).date()
        except: return pd.NaT

    df["date"] = df["date"].apply(parse_date)
    df = df.dropna(subset=["latitude","longitude","date"])
    keep = [c for c in ["latitude","longitude","date","instrument","confidence","frp"] if c in df.columns]
    return df[keep].assign(source=os.path.basename(path))

def main():
    if not os.path.isdir(RAW_DIR):
        print(f"❌ Missing folder: {RAW_DIR}", file=sys.stderr); sys.exit(1)

    # Candidate files you said you uploaded
    candidates = [
        "modis_2024_Australia.csv",
        "fire_archive_M6_101673.csv",
        "fire_archive_V1_101674.csv",
        "fire_nrt_M6_101673.csv",
        "fire_nrt_V1_101674.csv",
        "Fire_For16-21_Attributes.csv",  # will try, may or may not be FIRMS-like
    ]

    frames = []
    for name in candidates:
        path = os.path.join(RAW_DIR, name)
        if os.path.exists(path):
            try:
                df = read_firms_like(path)
                if len(df):
                    frames.append(df)
                    print(f"✅ Loaded {name}: {len(df)} rows")
                else:
                    print(f"ℹ️ {name} had no usable rows after parsing.")
            except Exception as e:
                print(f"⚠️ Skipped {name} due to error: {e}")

    if not frames:
        print("❌ No FIRMS/MODIS/VIIRS-like files found. Put CSVs in data/external/", file=sys.stderr)
        sys.exit(1)

    all_events = pd.concat(frames, ignore_index=True)
    all_events = all_events.sort_values("date")
    out_path = os.path.join(OUT_DIR, "events_standardized.csv")
    all_events.to_csv(out_path, index=False)
    print(f"✅ Wrote {out_path} ({len(all_events)} rows)")

    # Optional small climate & exposure inputs (kept flexible)
    # Save as-is for later joins if present
    aux_candidates = [
        "Latest Weather Observations for New South Wales.csv",
        "australian_annual_bushfire_area_(19902020).csv",
        "air-quality-monitoring-sites-summary.csv",
    ]
    for name in aux_candidates:
        p = os.path.join(RAW_DIR, name)
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                df.to_csv(os.path.join(OUT_DIR, f"aux_{os.path.splitext(name)[0]}.csv"), index=False)
                print(f"✅ Copied aux: {name}")
            except Exception as e:
                print(f"⚠️ Could not parse aux {name}: {e}")

if __name__ == "__main__":
    main()
