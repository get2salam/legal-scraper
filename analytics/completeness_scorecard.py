#!/usr/bin/env python3
"""
Completeness Scorecard — scores each case 0-100 on data completeness.
Scans all JSON files in data_v2/REPORTER/YEAR/*.json

Scoring weights:
  - citation present: 10 pts
  - judgment_clean present and > 500 chars: 30 pts (20 if present but short)
  - judges list non-empty: 15 pts
  - court present: 10 pts
  - date present: 10 pts
  - statutes_cited non-empty: 15 pts
  - headnotes present and > 50 chars: 10 pts
"""

import json
import os
import re
import sys
import math
from pathlib import Path
from collections import defaultdict

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Fix Windows console encoding
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# -- Configuration ----------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data_v2"
OUTPUT_DIR = SCRIPT_DIR / "output"

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def score_case(case: dict) -> dict:
    """Score a single case on data completeness (0-100)."""
    score = 0
    breakdown = {}

    # citation present: 10 pts
    citation = (case.get("citation") or "").strip()
    if citation:
        score += 10
        breakdown["citation"] = 10
    else:
        breakdown["citation"] = 0

    # judgment_clean present and > 500 chars: 30 pts (20 if present but short)
    jc = (case.get("judgment_clean") or "").strip()
    if len(jc) > 500:
        score += 30
        breakdown["judgment_clean"] = 30
    elif len(jc) > 0:
        score += 20
        breakdown["judgment_clean"] = 20
    else:
        breakdown["judgment_clean"] = 0

    # judges list non-empty: 15 pts
    judges = case.get("judges") or []
    if isinstance(judges, list) and len(judges) > 0:
        score += 15
        breakdown["judges"] = 15
    else:
        breakdown["judges"] = 0

    # court present: 10 pts
    court = (case.get("court") or "").strip()
    if court:
        score += 10
        breakdown["court"] = 10
    else:
        breakdown["court"] = 0

    # date present: 10 pts
    date = (case.get("date") or "").strip()
    if date:
        score += 10
        breakdown["date"] = 10
    else:
        breakdown["date"] = 0

    # statutes_cited non-empty: 15 pts
    statutes = case.get("statutes_cited") or []
    if isinstance(statutes, list) and len(statutes) > 0:
        score += 15
        breakdown["statutes_cited"] = 15
    else:
        breakdown["statutes_cited"] = 0

    # headnotes present and > 50 chars: 10 pts
    headnotes = (case.get("headnotes") or "").strip()
    if len(headnotes) > 50:
        score += 10
        breakdown["headnotes"] = 10
    else:
        breakdown["headnotes"] = 0

    return {"score": score, "breakdown": breakdown}


def grade(score: float) -> str:
    if score >= 90:
        return "A"
    elif score >= 70:
        return "B"
    elif score >= 50:
        return "C"
    elif score >= 30:
        return "D"
    else:
        return "F"


def load_all_cases():
    """Load all JSON case files from data_v2/REPORTER/YEAR/*.json"""
    records = []
    errors = 0

    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.is_dir():
            print(f"  [!] Reporter directory not found: {reporter}")
            continue

        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            year_str = year_dir.name
            try:
                year = int(year_str)
            except ValueError:
                continue

            for json_file in year_dir.glob("*.json"):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        case = json.load(f)
                    result = score_case(case)
                    records.append({
                        "file": str(json_file),
                        "reporter": reporter,
                        "year": year,
                        "citation": case.get("citation", ""),
                        "case_name": case.get("case_name", ""),
                        "court": case.get("court", ""),
                        "score": result["score"],
                        "grade": grade(result["score"]),
                        **{f"pts_{k}": v for k, v in result["breakdown"].items()},
                    })
                except (json.JSONDecodeError, OSError) as e:
                    errors += 1

    print(f"  Loaded {len(records)} cases ({errors} errors/corrupt files skipped)")
    return pd.DataFrame(records)


def plot_quality_by_reporter(df: pd.DataFrame):
    """Bar chart: average quality score per reporter."""
    reporter_avg = df.groupby("reporter")["score"].mean().sort_values(ascending=False)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = sns.color_palette("viridis", len(reporter_avg))
    bars = ax.bar(reporter_avg.index, reporter_avg.values, color=colors, edgecolor="white", linewidth=0.5)

    for bar, val in zip(bars, reporter_avg.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.1f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.set_xlabel("Reporter", fontsize=12)
    ax.set_ylabel("Average Quality Score", fontsize=12)
    ax.set_title("Data Completeness by Reporter", fontsize=14, fontweight="bold")
    ax.set_ylim(0, 105)
    ax.grid(axis="y", alpha=0.3)
    sns.despine()
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "quality_by_reporter.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved quality_by_reporter.png")


def plot_quality_by_year(df: pd.DataFrame):
    """Line chart: average quality score per year."""
    year_avg = df.groupby("year")["score"].mean().sort_index()

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(year_avg.index, year_avg.values, marker="o", linewidth=2, markersize=4,
            color="#2196F3", markerfacecolor="#FF5722")
    ax.fill_between(year_avg.index, year_avg.values, alpha=0.15, color="#2196F3")

    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Average Quality Score", fontsize=12)
    ax.set_title("Data Completeness Over Time", fontsize=14, fontweight="bold")
    ax.set_ylim(0, 105)
    ax.grid(alpha=0.3)
    sns.despine()
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "quality_by_year.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved quality_by_year.png")


def plot_grade_distribution(df: pd.DataFrame):
    """Pie chart: grade distribution (A/B/C/D/F)."""
    grade_counts = df["grade"].value_counts().reindex(["A", "B", "C", "D", "F"], fill_value=0)
    grade_colors = {"A": "#4CAF50", "B": "#8BC34A", "C": "#FFC107", "D": "#FF9800", "F": "#F44336"}
    colors = [grade_colors[g] for g in grade_counts.index]

    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(
        grade_counts.values,
        labels=[f"{g}\n({c:,})" for g, c in zip(grade_counts.index, grade_counts.values)],
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.75,
        textprops={"fontsize": 12},
    )
    for at in autotexts:
        at.set_fontweight("bold")

    ax.set_title("Quality Grade Distribution", fontsize=14, fontweight="bold", pad=20)
    centre_circle = plt.Circle((0, 0), 0.50, fc="white")
    ax.add_artist(centre_circle)

    # Add legend
    legend_labels = ["A: 90-100", "B: 70-89", "C: 50-69", "D: 30-49", "F: 0-29"]
    ax.legend(legend_labels, loc="lower right", fontsize=10)

    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "grade_distribution.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved grade_distribution.png")


def print_summary(df: pd.DataFrame):
    """Print console summary."""
    total = len(df)
    avg_score = df["score"].mean()

    print("\n" + "=" * 70)
    print("  COMPLETENESS SCORECARD — SUMMARY")
    print("=" * 70)
    print(f"\n  Total cases analyzed: {total:,}")
    print(f"  Overall average quality score: {avg_score:.1f} / 100  ({grade(avg_score)})")

    # Per-reporter
    print(f"\n  {'Reporter':<10} {'Count':>8} {'Avg Score':>10} {'Grade':>6}")
    print("  " + "-" * 36)
    reporter_stats = df.groupby("reporter").agg(
        count=("score", "size"),
        avg=("score", "mean"),
    ).sort_values("avg", ascending=False)
    for rpt, row in reporter_stats.iterrows():
        print(f"  {rpt:<10} {int(row['count']):>8,} {row['avg']:>10.1f} {grade(row['avg']):>6}")

    # Grade distribution
    grade_counts = df["grade"].value_counts().reindex(["A", "B", "C", "D", "F"], fill_value=0)
    print(f"\n  Grade Distribution:")
    for g in ["A", "B", "C", "D", "F"]:
        cnt = grade_counts[g]
        pct = 100.0 * cnt / total if total > 0 else 0
        bar = "#" * int(pct / 2)
        print(f"    {g}: {cnt:>7,} ({pct:5.1f}%) {bar}")

    # Field completeness
    print(f"\n  Field Completeness:")
    fields = ["citation", "judgment_clean", "judges", "court", "date", "statutes_cited", "headnotes"]
    for field in fields:
        col = f"pts_{field}"
        if col in df.columns:
            nonzero = (df[col] > 0).sum()
            pct = 100.0 * nonzero / total if total > 0 else 0
            print(f"    {field:<18} {nonzero:>7,} / {total:>7,} ({pct:5.1f}%)")

    # Top 10 lowest quality cases
    print(f"\n  Top 10 Lowest Quality Cases (for re-scraping):")
    worst = df.nsmallest(10, "score")
    for i, (_, row) in enumerate(worst.iterrows(), 1):
        cit = row["citation"] or os.path.basename(row["file"])
        print(f"    {i:>2}. [{row['score']:>3}] {cit}")

    print("\n" + "=" * 70)


def main():
    print("╔======================================================╗")
    print("║        COMPLETENESS SCORECARD ANALYZER              ║")
    print("╚======================================================╝\n")

    ensure_output_dir()

    print("Loading cases from data_v2/...")
    df = load_all_cases()

    if df.empty:
        print("  ❌ No cases found. Check data_v2 directory structure.")
        sys.exit(1)

    print("\nGenerating charts...")
    plot_quality_by_reporter(df)
    plot_quality_by_year(df)
    plot_grade_distribution(df)

    print_summary(df)


if __name__ == "__main__":
    main()
