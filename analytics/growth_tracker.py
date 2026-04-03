#!/usr/bin/env python3
"""
growth_tracker.py — Cumulative case-growth analytics.

Reads file CreationTime from data_v2/REPORTER/YEAR/*.json and produces:
  • Cumulative growth chart with milestone markers -> analytics/output/growth_chart.png
  • Daily totals bar chart                        -> analytics/output/daily_totals.png
  • Records board (best hour, best day, etc.)     -> console
  • Per-reporter breakdown for today              -> console
"""

import os, sys, io
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
import matplotlib.dates as mdates

# -- paths ------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / "data_v2"
OUTPUT_DIR  = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
MILESTONES = [1_000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000]


# -- scan -------------------------------------------------------------
def scan_files():
    records = []
    for reporter in REPORTERS:
        rdir = DATA_DIR / reporter
        if not rdir.exists():
            continue
        for jp in rdir.rglob("*.json"):
            try:
                ctime = os.path.getctime(jp)
                dt = datetime.fromtimestamp(ctime)
                records.append({"path": str(jp), "created": dt, "reporter": reporter})
            except OSError:
                continue
    return pd.DataFrame(records)


# -- records board ----------------------------------------------------
def records_board(df):
    df = df.copy()
    df["date"] = df["created"].dt.date
    df["hour_bucket"] = df["created"].dt.floor("h")

    # best hour
    hourly = df.groupby("hour_bucket").size()
    best_hour_ts = hourly.idxmax()
    best_hour_val = hourly.max()

    # best day
    daily = df.groupby("date").size()
    best_day = daily.idxmax()
    best_day_val = daily.max()

    # best single-reporter day
    rep_daily = df.groupby(["reporter", "date"]).size().reset_index(name="count")
    best_rd = rep_daily.loc[rep_daily["count"].idxmax()]

    grand_total = len(df)

    print(f"\n{'='*58}")
    print(f"  🏆  RECORDS BOARD")
    print(f"{'='*58}")
    print(f"  Grand total files       : {grand_total:>10,}")
    print(f"  Best hour               : {best_hour_ts.strftime('%Y-%m-%d %H:%M')}  ({best_hour_val:,})")
    print(f"  Best day                : {best_day}  ({best_day_val:,})")
    print(f"  Best single-reporter day: {best_rd['reporter']} on {best_rd['date']}  ({int(best_rd['count']):,})")
    print(f"{'='*58}\n")


def today_breakdown(df):
    today = datetime.now().date()
    day_df = df[df["created"].dt.date == today]
    print(f"  📋  Per-reporter breakdown for {today}:")
    if day_df.empty:
        print("       (no files scraped today)\n")
        return
    counts = day_df.groupby("reporter").size().sort_values(ascending=False)
    for rep, cnt in counts.items():
        print(f"       {rep:<8} {cnt:>6,}")
    print(f"       {'-'*18}")
    print(f"       {'TOTAL':<8} {len(day_df):>6,}\n")


# -- cumulative growth chart ------------------------------------------
def growth_chart(df):
    df = df.sort_values("created").reset_index(drop=True)
    df["cumulative"] = range(1, len(df) + 1)

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(df["created"], df["cumulative"], color="#2563eb", linewidth=1.2)
    ax.fill_between(df["created"], df["cumulative"], alpha=0.10, color="#2563eb")

    # milestone markers
    for ms in MILESTONES:
        hits = df[df["cumulative"] >= ms]
        if hits.empty:
            continue
        row = hits.iloc[0]
        ax.axhline(ms, color="grey", linewidth=0.5, linestyle="--", alpha=0.5)
        ax.plot(row["created"], ms, "o", color="#dc2626", markersize=7, zorder=5)
        ax.annotate(
            f"  {ms // 1000}K — {row['created'].strftime('%b %d %H:%M')}",
            xy=(row["created"], ms),
            fontsize=8, color="#dc2626", va="bottom",
        )

    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Cases Scraped")
    ax.set_title("Case Scraping — Cumulative Growth", fontsize=14, weight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    out = OUTPUT_DIR / "growth_chart.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  ✅  Growth chart saved -> {out}")


# -- daily totals bar chart -------------------------------------------
def daily_totals_chart(df):
    daily = df.groupby(df["created"].dt.date).size().reset_index(name="count")
    daily.columns = ["date", "count"]
    daily = daily.sort_values("date")

    if len(daily) == 0:
        return

    fig, ax = plt.subplots(figsize=(14, 6))

    dates = pd.to_datetime(daily["date"])
    colors = ["#2563eb" if i < len(daily) - 1 else "#dc2626" for i in range(len(daily))]
    ax.bar(dates, daily["count"], width=0.8, color=colors, edgecolor="white", linewidth=0.3)

    ax.set_xlabel("Date")
    ax.set_ylabel("Files Scraped")
    ax.set_title("Daily Scrape Totals", fontsize=14, weight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    out = OUTPUT_DIR / "daily_totals.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  ✅  Daily totals saved -> {out}")


# -- main -------------------------------------------------------------
def main():
    print("\n  🔍  Scanning data_v2 for JSON files …")
    df = scan_files()
    if df.empty:
        print("  ❌  No JSON files found.")
        return
    print(f"  📂  Found {len(df):,} files.\n")

    records_board(df)
    today_breakdown(df)
    growth_chart(df)
    daily_totals_chart(df)
    print()


if __name__ == "__main__":
    main()
