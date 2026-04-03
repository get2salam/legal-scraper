#!/usr/bin/env python3
"""
eta_projector.py — Completion ETA projections.

Uses file CreationTime to calculate scraping velocity and project
when milestones (75K, 100K, 107K) will be reached.

Output:
  • Rolling 24h / 7d / 30d average velocities
  • ETA table for milestones
  • Per-reporter progress bars (text-based)
  • Projection chart -> analytics/output/eta_projection.png
"""

import os, sys, io
from datetime import datetime, timedelta
from pathlib import Path

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

# -- config -----------------------------------------------------------
SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / "data_v2"
OUTPUT_DIR  = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
PLS_TOTAL = 107_655
MILESTONES = [75_000, 100_000, 107_655]

# Rough estimated totals per reporter (from PLS search counts, approximate)
# These are ballpark; adjust when better numbers are known.
REPORTER_TARGETS = {
    "SCMR":  15_000,
    "PLD":   12_000,
    "MLD":   14_000,
    "CLC":   12_000,
    "PCrLJ": 14_000,
    "PTD":    8_000,
    "PLC":    4_000,
    "YLR":   18_000,
    "CLD":    6_000,
    "GBLR":   4_655,
}


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
                records.append({"created": dt, "reporter": reporter})
            except OSError:
                continue
    return pd.DataFrame(records)


# -- velocity ---------------------------------------------------------
def compute_velocities(df):
    now = datetime.now()
    windows = {"24h": 1, "7d": 7, "30d": 30}
    velocities = {}
    for label, days in windows.items():
        cutoff = now - timedelta(days=days)
        count = len(df[df["created"] >= cutoff])
        # files per day (avoid div-by-zero for very fresh projects)
        elapsed = min(days, (now - df["created"].min()).total_seconds() / 86400)
        elapsed = max(elapsed, 0.01)
        vel = count / elapsed  # files/day over the window
        velocities[label] = {"count": count, "days": elapsed, "vel": vel}
    return velocities


def eta_for_milestone(current, target, vel_per_day):
    """Return (eta_datetime, days_remaining) or (None, None)."""
    remaining = target - current
    if remaining <= 0:
        return datetime.now(), 0.0
    if vel_per_day <= 0:
        return None, None
    days = remaining / vel_per_day
    return datetime.now() + timedelta(days=days), days


# -- console output ---------------------------------------------------
def print_velocities(vels, total):
    print(f"\n{'='*60}")
    print(f"  🚀  VELOCITY DASHBOARD")
    print(f"{'='*60}")
    print(f"  Current total : {total:>10,} / {PLS_TOTAL:,}  ({total/PLS_TOTAL*100:.1f}%)")
    print(f"  Remaining     : {PLS_TOTAL - total:>10,}\n")
    print(f"  {'Window':<8} {'Scraped':>9} {'Files/day':>11} {'Files/hr':>10}")
    print(f"  {'------':<8} {'-------':>9} {'---------':>11} {'--------':>10}")
    for label in ["24h", "7d", "30d"]:
        v = vels[label]
        fph = v["vel"] / 24
        print(f"  {label:<8} {v['count']:>9,} {v['vel']:>11,.1f} {fph:>10,.1f}")
    print()


def print_etas(total, vels):
    print(f"  {'Milestone':>10}  {'@ 24h rate':>16}  {'@ 7d rate':>16}  {'@ 30d rate':>16}")
    print(f"  {'-'*10}  {'-'*16}  {'-'*16}  {'-'*16}")
    for ms in MILESTONES:
        label = f"{ms // 1000}K" if ms < PLS_TOTAL else f"{ms // 1000}K (all)"
        parts = []
        for window in ["24h", "7d", "30d"]:
            eta, days = eta_for_milestone(total, ms, vels[window]["vel"])
            if eta is None:
                parts.append(f"{'∞':>16}")
            elif days <= 0:
                parts.append(f"{'✅ done':>16}")
            else:
                parts.append(f"{eta.strftime('%b %d'):>8} ({days:.0f}d)")
        print(f"  {label:>10}  {'  '.join(parts)}")
    print()


def print_reporter_progress(df):
    print(f"  📊  Per-Reporter Progress")
    print(f"  {'Reporter':<8} {'Have':>7} {'Target':>7} {'%':>6}  Bar")
    print(f"  {'--------':<8} {'----':>7} {'------':>7} {'-':>6}  {'---'}")
    for rep in REPORTERS:
        have = len(df[df["reporter"] == rep])
        target = REPORTER_TARGETS.get(rep, 0)
        pct = (have / target * 100) if target > 0 else 0
        filled = int(min(pct, 100) / 100 * 30)
        bar = "#" * filled + "░" * (30 - filled)
        print(f"  {rep:<8} {have:>7,} {target:>7,} {pct:>5.1f}%  [{bar}]")
    total_have = len(df)
    total_target = sum(REPORTER_TARGETS.values())
    pct = total_have / total_target * 100 if total_target else 0
    print(f"  {'-'*70}")
    filled = int(min(pct, 100) / 100 * 30)
    bar = "#" * filled + "░" * (30 - filled)
    print(f"  {'ALL':<8} {total_have:>7,} {total_target:>7,} {pct:>5.1f}%  [{bar}]")
    print()


# -- projection chart -------------------------------------------------
def projection_chart(df, vels):
    now = datetime.now()
    total = len(df)

    # Historical cumulative (daily)
    daily = df.groupby(df["created"].dt.date).size().sort_index().cumsum()
    hist_dates = pd.to_datetime(daily.index)
    hist_vals  = daily.values

    fig, ax = plt.subplots(figsize=(14, 7))

    # Historical line
    ax.plot(hist_dates, hist_vals, color="#2563eb", linewidth=1.5, label="Actual")

    # Projection lines for each velocity
    styles = {"24h": ("#dc2626", "--"), "7d": ("#f59e0b", "-."), "30d": ("#10b981", ":")}
    for window, (color, ls) in styles.items():
        vel = vels[window]["vel"]
        if vel <= 0:
            continue
        days_to_end = max(1, (PLS_TOTAL - total) / vel)
        proj_days = int(min(days_to_end + 5, 365))  # cap at 1yr
        proj_dates = [now + timedelta(days=d) for d in range(proj_days + 1)]
        proj_vals  = [min(total + vel * d, PLS_TOTAL) for d in range(proj_days + 1)]
        ax.plot(proj_dates, proj_vals, color=color, linewidth=1.2, linestyle=ls,
                label=f"Proj @ {window} rate ({vel:,.0f}/d)")

    # Milestone lines
    for ms in MILESTONES:
        ax.axhline(ms, color="grey", linewidth=0.5, linestyle="--", alpha=0.5)
        lbl = f"{ms//1000}K" if ms < PLS_TOTAL else "107K (all)"
        ax.text(hist_dates[0], ms + PLS_TOTAL * 0.005, lbl, fontsize=8, color="grey")

    ax.set_xlabel("Date")
    ax.set_ylabel("Total Cases")
    ax.set_title("ETA Projection — When Will We Finish?", fontsize=14, weight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    out = OUTPUT_DIR / "eta_projection.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  ✅  Projection chart saved -> {out}\n")


# -- main -------------------------------------------------------------
def main():
    print("\n  🔍  Scanning data_v2 for JSON files …")
    df = scan_files()
    if df.empty:
        print("  ❌  No JSON files found.")
        return
    total = len(df)
    print(f"  📂  Found {total:,} files.\n")

    vels = compute_velocities(df)
    print_velocities(vels, total)
    print_etas(total, vels)
    print_reporter_progress(df)
    projection_chart(df, vels)


if __name__ == "__main__":
    main()
