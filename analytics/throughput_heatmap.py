#!/usr/bin/env python3
"""
throughput_heatmap.py — Scrape-rate analytics via file CreationTime.

Scans data_v2/REPORTER/YEAR/*.json, buckets by os.path.getctime,
and produces:
  • Hour-by-hour scrape rate for a given day (default: today)
  • 7-day × 24-hour heatmap (seaborn) -> analytics/output/throughput_heatmap.png
  • Peak hour, average rate, total-per-day stats on console
"""

import os, sys, glob, time, io
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Fix Windows console encoding for emoji/unicode
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# -- paths ------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / "data_v2"
OUTPUT_DIR  = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]


# -- scan files -------------------------------------------------------
def scan_files():
    """Return list of (filepath, creation_datetime) for every JSON."""
    records = []
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        for json_path in reporter_dir.rglob("*.json"):
            try:
                ctime = os.path.getctime(json_path)
                dt = datetime.fromtimestamp(ctime)
                records.append((str(json_path), dt, reporter))
            except OSError:
                continue
    return records


def build_dataframe(records):
    df = pd.DataFrame(records, columns=["path", "created", "reporter"])
    df["date"]  = df["created"].dt.date
    df["hour"]  = df["created"].dt.hour
    df["day"]   = df["created"].dt.strftime("%a %m-%d")
    return df


# -- console stats ----------------------------------------------------
def print_day_stats(df, target_date):
    day_df = df[df["date"] == target_date]
    if day_df.empty:
        print(f"\n  No files scraped on {target_date}.\n")
        return

    hourly = day_df.groupby("hour").size()
    total  = len(day_df)
    peak_h = hourly.idxmax()
    peak_v = hourly.max()
    avg    = hourly.mean()

    print(f"\n{'='*52}")
    print(f"  📅  Throughput for {target_date}")
    print(f"{'='*52}")
    print(f"  Total scraped : {total:,}")
    print(f"  Peak hour     : {peak_h:02d}:00  ({peak_v:,} files)")
    print(f"  Average / hour: {avg:,.1f}")
    print(f"{'-'*52}")
    print(f"  {'Hour':>4}  {'Count':>7}  Bar")
    print(f"  {'----':>4}  {'-----':>7}  {'---'}")
    for h in range(24):
        v = hourly.get(h, 0)
        bar = "#" * int(v / max(peak_v, 1) * 30)
        print(f"  {h:4d}  {v:7,}  {bar}")
    print()


def print_weekly_summary(df, end_date):
    start = end_date - timedelta(days=6)
    week  = df[(df["date"] >= start) & (df["date"] <= end_date)]
    daily = week.groupby("date").size().sort_index()

    print(f"{'='*52}")
    print(f"  📊  7-Day Summary  ({start} -> {end_date})")
    print(f"{'='*52}")
    grand = 0
    for d, cnt in daily.items():
        grand += cnt
        print(f"  {d}  {cnt:>7,}")
    print(f"  {'-'*30}")
    print(f"  {'Total':>10}  {grand:>7,}")
    print()


# -- heatmap ----------------------------------------------------------
def make_heatmap(df, end_date):
    start = end_date - timedelta(days=6)
    week  = df[(df["date"] >= start) & (df["date"] <= end_date)].copy()

    if week.empty:
        print("  [!]  No data in the last 7 days for heatmap.")
        return

    # pivot: rows = day label, cols = hour 0-23
    week["day_label"] = week["created"].dt.strftime("%a %m-%d")
    pivot = week.groupby(["date", "day_label", "hour"]).size().reset_index(name="count")

    # ensure all 24 hours present
    all_days = sorted(pivot["date"].unique())
    day_labels = []
    matrix = []
    for d in all_days:
        sub = pivot[pivot["date"] == d]
        label = sub["day_label"].iloc[0]
        day_labels.append(label)
        row = [0] * 24
        for _, r in sub.iterrows():
            row[int(r["hour"])] = int(r["count"])
        matrix.append(row)

    arr = np.array(matrix)

    fig, ax = plt.subplots(figsize=(14, max(4, len(all_days) * 0.9)))
    sns.heatmap(
        arr, ax=ax,
        xticklabels=[f"{h:02d}" for h in range(24)],
        yticklabels=day_labels,
        cmap="YlOrRd", annot=True, fmt="d",
        linewidths=0.5, linecolor="white",
        cbar_kws={"label": "Files scraped"},
    )
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("")
    ax.set_title("Scrape Throughput — 7-Day × 24-Hour Heatmap", fontsize=14, weight="bold")
    plt.tight_layout()

    out = OUTPUT_DIR / "throughput_heatmap.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  ✅  Heatmap saved -> {out}\n")


# -- main -------------------------------------------------------------
def main():
    # Optional CLI arg: date in YYYY-MM-DD format
    if len(sys.argv) > 1:
        try:
            target = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print(f"  [!]  Invalid date '{sys.argv[1]}', using today.")
            target = datetime.now().date()
    else:
        target = datetime.now().date()

    print("\n  🔍  Scanning data_v2 for JSON files …")
    records = scan_files()
    if not records:
        print("  ❌  No JSON files found in data_v2/.")
        return

    print(f"  📂  Found {len(records):,} files across {len(REPORTERS)} reporters.\n")
    df = build_dataframe(records)

    print_day_stats(df, target)
    print_weekly_summary(df, target)
    make_heatmap(df, target)


if __name__ == "__main__":
    main()
