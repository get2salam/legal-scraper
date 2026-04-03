#!/usr/bin/env python3
"""
Gap Analysis — analyzes coverage gaps using citation number sequences.

For each reporter/year, extracts the citation number (e.g., "2024 SCMR 847" -> 847).
Compares actual vs expected (1 to max_number). Identifies missing citation numbers.
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


def extract_citation_number(citation: str, reporter: str) -> int | None:
    """Extract the numeric part from a citation like '2024 SCMR 847' -> 847."""
    if not citation:
        return None
    # Try pattern: YEAR REPORTER NUMBER
    # Handle PCrLJ / PCRLJ case insensitively
    pattern = rf'\d{{4}}\s+{re.escape(reporter)}\s+(\d+)'
    match = re.search(pattern, citation, re.IGNORECASE)
    if match:
        return int(match.group(1))
    # Also try extracting from filename-style: YEAR_REPORTER_NUMBER
    pattern2 = rf'\d{{4}}_{re.escape(reporter)}_(\d+)'
    match2 = re.search(pattern2, citation, re.IGNORECASE)
    if match2:
        return int(match2.group(1))
    return None


def extract_number_from_filename(filename: str) -> int | None:
    """Extract citation number from filename like '2024_SCMR_847.json' -> 847."""
    match = re.search(r'_(\d+)\.json$', filename)
    if match:
        return int(match.group(1))
    return None


def load_citations():
    """Load all cases and extract citation numbers per reporter/year."""
    # Structure: {(reporter, year): set of citation numbers}
    citations = defaultdict(set)
    total_files = 0
    parse_errors = 0

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
                total_files += 1
                num = None
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        case = json.load(f)
                    citation = case.get("citation", "")
                    num = extract_citation_number(citation, reporter)
                except (json.JSONDecodeError, OSError):
                    parse_errors += 1

                # Fallback: extract from filename
                if num is None:
                    num = extract_number_from_filename(json_file.name)

                if num is not None:
                    citations[(reporter, year)].add(num)

    print(f"  Loaded {total_files:,} files ({parse_errors} errors)")
    return citations


def analyze_gaps(citations: dict):
    """Analyze gaps for each reporter/year combo."""
    gap_records = []
    total_missing = 0

    for (reporter, year), numbers in sorted(citations.items()):
        if not numbers:
            continue

        max_num = max(numbers)
        min_num = min(numbers)
        count = len(numbers)

        # Expected: all numbers from 1 to max_num
        expected = set(range(1, max_num + 1))
        actual = numbers
        missing = sorted(expected - actual)
        missing_count = len(missing)
        total_missing += missing_count

        coverage_pct = (count / max_num * 100) if max_num > 0 else 100.0

        gap_records.append({
            "reporter": reporter,
            "year": year,
            "actual_count": count,
            "max_number": max_num,
            "min_number": min_num,
            "expected_count": max_num,
            "missing_count": missing_count,
            "coverage_pct": coverage_pct,
            "missing_numbers": missing,
        })

    return gap_records, total_missing


def plot_coverage_heatmap(gap_records: list):
    """Reporter × Year coverage heatmap (% complete)."""
    df = pd.DataFrame(gap_records)
    if df.empty:
        print("  [!] No data for heatmap")
        return

    pivot = df.pivot_table(index="reporter", columns="year", values="coverage_pct", aggfunc="first")
    pivot = pivot.reindex(REPORTERS)  # Ensure consistent reporter order
    pivot = pivot.sort_index(axis=1)  # Sort years

    fig, ax = plt.subplots(figsize=(max(20, len(pivot.columns) * 0.45), 8))

    mask = pivot.isna()
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".0f",
        cmap="RdYlGn",
        vmin=0,
        vmax=100,
        mask=mask,
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "Coverage %", "shrink": 0.8},
        annot_kws={"fontsize": 6},
        ax=ax,
    )

    ax.set_title("Case Coverage by Reporter × Year (% of expected range)", fontsize=14, fontweight="bold", pad=15)
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Reporter", fontsize=11)
    ax.tick_params(axis="x", rotation=45, labelsize=7)
    ax.tick_params(axis="y", rotation=0, labelsize=10)

    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "coverage_heatmap.png", dpi=150)
    plt.close(fig)
    print(f"  📊 Saved coverage_heatmap.png")


def print_summary(gap_records: list, total_missing: int):
    """Print console summary."""
    print("\n" + "=" * 70)
    print("  GAP ANALYSIS — SUMMARY")
    print("=" * 70)

    total_actual = sum(r["actual_count"] for r in gap_records)
    total_expected = sum(r["expected_count"] for r in gap_records)
    overall_coverage = (total_actual / total_expected * 100) if total_expected > 0 else 0

    print(f"\n  Total cases found:     {total_actual:>10,}")
    print(f"  Total expected (est):  {total_expected:>10,}")
    print(f"  Total estimated gaps:  {total_missing:>10,}")
    print(f"  Overall coverage:      {overall_coverage:>9.1f}%")

    # Per-reporter summary
    reporter_summary = defaultdict(lambda: {"actual": 0, "expected": 0, "missing": 0})
    for r in gap_records:
        reporter_summary[r["reporter"]]["actual"] += r["actual_count"]
        reporter_summary[r["reporter"]]["expected"] += r["expected_count"]
        reporter_summary[r["reporter"]]["missing"] += r["missing_count"]

    print(f"\n  {'Reporter':<10} {'Actual':>8} {'Expected':>10} {'Missing':>9} {'Coverage':>10}")
    print("  " + "-" * 49)
    for rpt in REPORTERS:
        s = reporter_summary.get(rpt, {"actual": 0, "expected": 0, "missing": 0})
        cov = (s["actual"] / s["expected"] * 100) if s["expected"] > 0 else 0
        print(f"  {rpt:<10} {s['actual']:>8,} {s['expected']:>10,} {s['missing']:>9,} {cov:>9.1f}%")

    # Top 20 combos with most gaps
    sorted_gaps = sorted(gap_records, key=lambda x: x["missing_count"], reverse=True)
    print(f"\n  Top 20 Reporter/Year Combos with Most Gaps:")
    print(f"  {'#':>3} {'Reporter':<10} {'Year':>5} {'Have':>6} {'Max#':>6} {'Missing':>8} {'Coverage':>9}")
    print("  " + "-" * 52)
    for i, r in enumerate(sorted_gaps[:20], 1):
        print(f"  {i:>3} {r['reporter']:<10} {r['year']:>5} {r['actual_count']:>6} {r['max_number']:>6} "
              f"{r['missing_count']:>8} {r['coverage_pct']:>8.1f}%")

        # Show some missing numbers (first 10)
        if r["missing_numbers"]:
            nums = r["missing_numbers"]
            sample = nums[:10]
            suffix = f"... +{len(nums) - 10} more" if len(nums) > 10 else ""
            print(f"      Missing: {', '.join(map(str, sample))} {suffix}")

    print("\n" + "=" * 70)


def main():
    print("╔======================================================╗")
    print("║            GAP ANALYSIS ANALYZER                    ║")
    print("╚======================================================╝\n")

    ensure_output_dir()

    print("Loading citations from data_v2/...")
    citations = load_citations()

    if not citations:
        print("  ❌ No citations found. Check data_v2 directory structure.")
        sys.exit(1)

    print("\nAnalyzing gaps...")
    gap_records, total_missing = analyze_gaps(citations)

    print("\nGenerating heatmap...")
    plot_coverage_heatmap(gap_records)

    print_summary(gap_records, total_missing)


if __name__ == "__main__":
    main()
