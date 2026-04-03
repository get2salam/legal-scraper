#!/usr/bin/env python3
"""
Judgment Complexity Analyzer — measures judgment complexity for each case.

Metrics (pure Python, no external NLP libs):
  - Word count of judgment_clean
  - Average sentence length (split on '. ')
  - Number of statutes cited
  - Number of cases cited
  - Headnote length
  - Composite score = log(word_count)*30 + avg_sentence_len*20 +
                      log(statutes+1)*25 + log(cases_cited+1)*25
    (normalized 0-100)
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


def compute_complexity(case: dict) -> dict | None:
    """Compute complexity metrics for a single case."""
    judgment = (case.get("judgment_clean") or "").strip()

    # Skip cases with no judgment text
    if not judgment or len(judgment) < 50:
        return None

    # Word count
    words = judgment.split()
    word_count = len(words)
    if word_count == 0:
        return None

    # Average sentence length (split on '. ')
    sentences = [s.strip() for s in judgment.split('. ') if s.strip()]
    num_sentences = max(len(sentences), 1)
    avg_sentence_len = word_count / num_sentences

    # Number of statutes cited
    statutes = case.get("statutes_cited") or []
    if not isinstance(statutes, list):
        statutes = []
    num_statutes = len(statutes)

    # Number of cases cited
    cases_cited = case.get("cases_cited") or []
    if not isinstance(cases_cited, list):
        cases_cited = []
    num_cases_cited = len(cases_cited)

    # Headnote length
    headnotes = (case.get("headnotes") or "").strip()
    headnote_len = len(headnotes)

    # Composite raw score
    raw_score = (
        math.log(max(word_count, 1)) * 30 +
        avg_sentence_len * 20 +
        math.log(num_statutes + 1) * 25 +
        math.log(num_cases_cited + 1) * 25
    )

    return {
        "word_count": word_count,
        "num_sentences": num_sentences,
        "avg_sentence_len": round(avg_sentence_len, 1),
        "num_statutes": num_statutes,
        "num_cases_cited": num_cases_cited,
        "headnote_len": headnote_len,
        "raw_score": raw_score,
    }


def extract_year_from_citation(citation: str) -> int | None:
    """Extract year from citation like '2024 SCMR 847'."""
    match = re.match(r'(\d{4})', citation)
    if match:
        return int(match.group(1))
    return None


def load_all_cases():
    """Load all JSON case files and compute complexity metrics."""
    records = []
    skipped = 0
    errors = 0

    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.is_dir():
            print(f"  [!] Reporter directory not found: {reporter}")
            continue

        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue

            for json_file in year_dir.glob("*.json"):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        case = json.load(f)
                except (json.JSONDecodeError, OSError):
                    errors += 1
                    continue

                metrics = compute_complexity(case)
                if metrics is None:
                    skipped += 1
                    continue

                citation = case.get("citation", "")
                judges = case.get("judges") or []
                if isinstance(judges, list):
                    judges_str = ", ".join(judges)
                else:
                    judges_str = str(judges)

                records.append({
                    "file": str(json_file),
                    "reporter": reporter,
                    "year": year,
                    "citation": citation,
                    "case_name": case.get("case_name", ""),
                    "court": (case.get("court") or "").strip(),
                    "judges": judges_str,
                    **metrics,
                })

    print(f"  Loaded {len(records):,} cases with judgment text")
    print(f"  Skipped {skipped:,} cases (no/short judgment), {errors} read errors")
    return pd.DataFrame(records)


def normalize_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw_score to 0-100 scale."""
    if df.empty:
        return df

    min_score = df["raw_score"].min()
    max_score = df["raw_score"].max()

    if max_score == min_score:
        df["complexity_score"] = 50.0
    else:
        df["complexity_score"] = ((df["raw_score"] - min_score) / (max_score - min_score) * 100).round(1)

    return df


def plot_complexity_by_reporter(df: pd.DataFrame):
    """Box plot: complexity by reporter."""
    # Only plot reporters that have data
    reporters_with_data = [r for r in REPORTERS if r in df["reporter"].values]
    if not reporters_with_data:
        return

    fig, ax = plt.subplots(figsize=(14, 7))
    order = df.groupby("reporter")["complexity_score"].median().sort_values(ascending=False).index
    sns.boxplot(
        data=df[df["reporter"].isin(reporters_with_data)],
        x="reporter",
        y="complexity_score",
        order=order,
        palette="viridis",
        fliersize=1.5,
        linewidth=0.8,
        ax=ax,
    )
    ax.set_xlabel("Reporter", fontsize=12)
    ax.set_ylabel("Complexity Score (0-100)", fontsize=12)
    ax.set_title("Judgment Complexity by Reporter", fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    sns.despine()
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "complexity_by_reporter.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved complexity_by_reporter.png")


def plot_complexity_by_court(df: pd.DataFrame):
    """Bar chart: average complexity by court."""
    court_avg = df.groupby("court")["complexity_score"].agg(["mean", "count"])
    # Filter out courts with very few cases (noise)
    court_avg = court_avg[court_avg["count"] >= 5]
    court_avg = court_avg.sort_values("mean", ascending=True)

    # Show top 20 courts
    court_avg = court_avg.tail(20)

    fig, ax = plt.subplots(figsize=(12, max(8, len(court_avg) * 0.4)))
    colors = sns.color_palette("viridis", len(court_avg))
    bars = ax.barh(court_avg.index, court_avg["mean"], color=colors, edgecolor="white", linewidth=0.5)

    for bar, val, cnt in zip(bars, court_avg["mean"], court_avg["count"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f} (n={int(cnt)})", va="center", fontsize=8)

    ax.set_xlabel("Average Complexity Score", fontsize=12)
    ax.set_ylabel("")
    ax.set_title("Average Judgment Complexity by Court (top 20, min 5 cases)", fontsize=13, fontweight="bold")
    ax.set_xlim(0, 110)
    ax.grid(axis="x", alpha=0.3)
    sns.despine()
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "complexity_by_court.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved complexity_by_court.png")


def plot_complexity_trend(df: pd.DataFrame):
    """Line chart: complexity trend over years."""
    year_stats = df.groupby("year")["complexity_score"].agg(["mean", "median", "std", "count"]).sort_index()
    # Only years with enough data
    year_stats = year_stats[year_stats["count"] >= 3]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(year_stats.index, year_stats["mean"], marker="o", linewidth=2, markersize=4,
            color="#2196F3", label="Mean", markerfacecolor="#FF5722")
    ax.plot(year_stats.index, year_stats["median"], marker="s", linewidth=1.5, markersize=3,
            color="#4CAF50", alpha=0.7, label="Median", linestyle="--")

    # Confidence band
    if "std" in year_stats.columns:
        ax.fill_between(
            year_stats.index,
            year_stats["mean"] - year_stats["std"],
            year_stats["mean"] + year_stats["std"],
            alpha=0.1,
            color="#2196F3",
            label="±1 std",
        )

    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Complexity Score (0-100)", fontsize=12)
    ax.set_title("Judgment Complexity Trend Over Time", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    sns.despine()
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "complexity_trend.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved complexity_trend.png")


def print_summary(df: pd.DataFrame):
    """Print console summary."""
    total = len(df)
    avg_score = df["complexity_score"].mean()
    median_score = df["complexity_score"].median()

    print("\n" + "=" * 80)
    print("  JUDGMENT COMPLEXITY ANALYSIS — SUMMARY")
    print("=" * 80)
    print(f"\n  Total cases analyzed: {total:,}")
    print(f"  Average complexity score: {avg_score:.1f}")
    print(f"  Median complexity score:  {median_score:.1f}")

    # Metric averages
    print(f"\n  Average Metrics:")
    print(f"    Word count:         {df['word_count'].mean():>10,.0f}")
    print(f"    Sentences:          {df['num_sentences'].mean():>10,.0f}")
    print(f"    Avg sentence len:   {df['avg_sentence_len'].mean():>10.1f} words")
    print(f"    Statutes cited:     {df['num_statutes'].mean():>10.1f}")
    print(f"    Cases cited:        {df['num_cases_cited'].mean():>10.1f}")
    print(f"    Headnote length:    {df['headnote_len'].mean():>10,.0f} chars")

    # Per-reporter summary
    print(f"\n  {'Reporter':<10} {'Count':>7} {'Mean':>7} {'Median':>8} {'Max':>6}")
    print("  " + "-" * 40)
    for rpt in REPORTERS:
        sub = df[df["reporter"] == rpt]
        if sub.empty:
            print(f"  {rpt:<10} {'—':>7}")
            continue
        print(f"  {rpt:<10} {len(sub):>7,} {sub['complexity_score'].mean():>7.1f} "
              f"{sub['complexity_score'].median():>8.1f} {sub['complexity_score'].max():>6.1f}")

    # Top 50 most complex
    top50 = df.nlargest(50, "complexity_score")
    print(f"\n  Top 50 Most Complex Judgments:")
    print(f"  {'#':>3} {'Score':>6} {'Words':>8} {'Stat':>5} {'Cited':>6} {'Citation':<40} {'Court':<30}")
    print("  " + "-" * 100)
    for i, (_, row) in enumerate(top50.iterrows(), 1):
        cit = (row["citation"] or "")[:38]
        court = (row["court"] or "")[:28]
        print(f"  {i:>3} {row['complexity_score']:>6.1f} {row['word_count']:>8,} "
              f"{row['num_statutes']:>5} {row['num_cases_cited']:>6} {cit:<40} {court:<30}")

    print("\n" + "=" * 80)


def main():
    print("╔======================================================╗")
    print("║        JUDGMENT COMPLEXITY ANALYZER                 ║")
    print("╚======================================================╝\n")

    ensure_output_dir()

    print("Loading cases from data_v2/...")
    df = load_all_cases()

    if df.empty:
        print("  ❌ No cases found with judgment text.")
        sys.exit(1)

    print("\nNormalizing scores...")
    df = normalize_scores(df)

    print("\nGenerating charts...")
    plot_complexity_by_reporter(df)
    plot_complexity_by_court(df)
    plot_complexity_trend(df)

    print_summary(df)


if __name__ == "__main__":
    main()
