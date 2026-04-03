#!/usr/bin/env python3
"""
gap_detective.py — Intelligent gap detection in case law collection

Analyses data_v2 to find missing cases using multiple heuristic strategies.
Doesn't just count files — thinks creatively about where gaps might hide.

Strategies:
    1. SEQUENTIAL_GAPS   — Missing page numbers within a reporter/year
    2. YEAR_COVERAGE     — Years with suspiciously few cases vs neighbours  
    3. REPORTER_BALANCE  — Reporters with fewer cases than expected
    4. TEMPORAL_ANOMALY  — Sudden drops in case counts (year-over-year)
    5. CROSS_REPORTER    — Years where some reporters have data but others don't
    6. PAGE_DENSITY      — Expected vs actual page number range
    7. NEW_REPORTER_GAP  — Known reporters on PLS not yet scraped
    8. RECENT_YEAR_GAP   — 2026 cases that may have been added since last scrape

Usage:
    python gap_detective.py                # Full analysis
    python gap_detective.py --reporter PLD # Single reporter
    python gap_detective.py --top 20       # Show top 20 gaps only
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\gap-detective")

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "YLR", "PTD", "PLC", "CLD", "GBLR", "PLCCS"]
NEW_REPORTERS = ["PLC(CS)", "CLCN", "PCRLJN", "PLC(CS)N", "YLRN"]
YEAR_MIN, YEAR_MAX = 1947, 2026


class Gap:
    """Represents a detected gap in the data."""
    def __init__(self, strategy, reporter, year, severity, description, estimated_missing=0):
        self.strategy = strategy
        self.reporter = reporter
        self.year = year
        self.severity = severity  # critical, high, medium, low
        self.description = description
        self.estimated_missing = estimated_missing
        self.priority_score = self._calc_priority()

    def _calc_priority(self):
        severity_weights = {"critical": 100, "high": 70, "medium": 40, "low": 10}
        base = severity_weights.get(self.severity, 10)
        return base + self.estimated_missing

    def to_dict(self):
        return {
            "strategy": self.strategy,
            "reporter": self.reporter,
            "year": self.year,
            "severity": self.severity,
            "description": self.description,
            "estimated_missing": self.estimated_missing,
            "priority_score": self.priority_score,
        }


def load_data_inventory(reporter_filter=None):
    """Build inventory: {reporter: {year: [page_numbers]}}."""
    inventory = defaultdict(lambda: defaultdict(list))
    reporters = [reporter_filter] if reporter_filter else REPORTERS

    for rep in reporters:
        rep_dir = DATA_DIR / rep
        if not rep_dir.exists():
            continue
        for year_dir in sorted(rep_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            for f in year_dir.glob("*.json"):
                # Extract page number from filename: YYYY_REPORTER_PAGE.json
                name = f.stem
                parts = name.split("_")
                try:
                    page = int(parts[-1])
                    inventory[rep][year].append(page)
                except (ValueError, IndexError):
                    # Try extracting from citation inside JSON
                    try:
                        with open(f, "r", encoding="utf-8") as fh:
                            data = json.load(fh)
                        citation = data.get("citation", "")
                        match = re.search(r"(\d+)$", citation)
                        if match:
                            inventory[rep][year].append(int(match.group(1)))
                    except Exception:
                        inventory[rep][year].append(0)

    return inventory


# ── Strategy 1: Sequential Gaps ─────────────────────────────────────────────

def detect_sequential_gaps(inventory):
    """Find missing page numbers within each reporter/year."""
    gaps = []
    for rep in sorted(inventory):
        for year in sorted(inventory[rep]):
            pages = sorted(inventory[rep][year])
            if len(pages) < 3:
                continue
            
            # Check for large jumps in page numbers
            for i in range(1, len(pages)):
                jump = pages[i] - pages[i-1]
                if jump > 50:  # More than 50 pages gap
                    estimated = jump // 10  # Rough estimate: 1 case per 10 pages
                    gaps.append(Gap(
                        "SEQUENTIAL_GAP", rep, year, "medium",
                        f"Page gap: {pages[i-1]} -> {pages[i]} (jump of {jump})",
                        estimated,
                    ))
    return gaps


# ── Strategy 2: Year Coverage ───────────────────────────────────────────────

def detect_year_coverage_gaps(inventory):
    """Find years with suspiciously few cases compared to neighbours."""
    gaps = []
    for rep in sorted(inventory):
        years = sorted(inventory[rep].keys())
        counts = {y: len(inventory[rep][y]) for y in years}

        for i in range(1, len(years) - 1):
            y = years[i]
            prev_count = counts.get(years[i-1], 0)
            next_count = counts.get(years[i+1], 0)
            curr_count = counts[y]
            avg_neighbor = (prev_count + next_count) / 2

            if avg_neighbor > 20 and curr_count < avg_neighbor * 0.3:
                estimated = int(avg_neighbor - curr_count)
                gaps.append(Gap(
                    "YEAR_COVERAGE", rep, y, "high",
                    f"Only {curr_count} cases vs ~{int(avg_neighbor)} avg of neighbours ({years[i-1]}:{prev_count}, {years[i+1]}:{next_count})",
                    estimated,
                ))
    return gaps


# ── Strategy 3: Reporter Balance ────────────────────────────────────────────

def detect_reporter_balance_gaps(inventory):
    """Find reporters with fewer total cases than expected."""
    gaps = []
    totals = {rep: sum(len(v) for v in inventory[rep].values()) for rep in inventory}
    avg_total = sum(totals.values()) / len(totals) if totals else 0

    for rep, total in sorted(totals.items(), key=lambda x: x[1]):
        if total < avg_total * 0.3 and avg_total > 1000:
            estimated = int(avg_total - total)
            gaps.append(Gap(
                "REPORTER_BALANCE", rep, 0, "low",
                f"{total:,} total cases vs {int(avg_total):,} average across reporters",
                estimated,
            ))
    return gaps


# ── Strategy 4: Temporal Anomaly ────────────────────────────────────────────

def detect_temporal_anomalies(inventory):
    """Find sudden year-over-year drops (>60% decline)."""
    gaps = []
    for rep in sorted(inventory):
        years = sorted(inventory[rep].keys())
        for i in range(1, len(years)):
            prev_y, curr_y = years[i-1], years[i]
            prev_c = len(inventory[rep][prev_y])
            curr_c = len(inventory[rep][curr_y])

            if prev_c > 30 and curr_c < prev_c * 0.4:
                estimated = int(prev_c - curr_c)
                gaps.append(Gap(
                    "TEMPORAL_ANOMALY", rep, curr_y, "medium",
                    f"Sharp drop: {prev_y}:{prev_c} -> {curr_y}:{curr_c} (-{int((1-curr_c/prev_c)*100)}%)",
                    estimated,
                ))
    return gaps


# ── Strategy 5: Cross-Reporter Analysis ─────────────────────────────────────

def detect_cross_reporter_gaps(inventory):
    """Find years where most reporters have data but some are empty."""
    gaps = []
    all_years = set()
    for rep in inventory:
        all_years.update(inventory[rep].keys())

    for year in sorted(all_years):
        reps_with_data = [rep for rep in REPORTERS if len(inventory.get(rep, {}).get(year, [])) > 0]
        reps_without = [rep for rep in REPORTERS if len(inventory.get(rep, {}).get(year, [])) == 0]

        # If most reporters have data but some don't
        if len(reps_with_data) >= 7 and len(reps_without) > 0 and year >= 1960:
            for rep in reps_without:
                avg_others = sum(len(inventory[r][year]) for r in reps_with_data) / len(reps_with_data)
                if avg_others > 20:
                    gaps.append(Gap(
                        "CROSS_REPORTER", rep, year, "medium",
                        f"{rep} has 0 cases in {year} but {len(reps_with_data)} other reporters have data (avg {int(avg_others)} each)",
                        int(avg_others * 0.5),
                    ))
    return gaps


# ── Strategy 6: Page Density ────────────────────────────────────────────────

def detect_page_density_gaps(inventory):
    """Compare actual case count vs page range to estimate missing cases."""
    gaps = []
    for rep in sorted(inventory):
        for year in sorted(inventory[rep]):
            pages = sorted(inventory[rep][year])
            if len(pages) < 5:
                continue
            
            page_range = pages[-1] - pages[0]
            actual_count = len(pages)
            
            # Typical density: ~1 case per 8-15 pages
            expected_min = page_range // 15
            expected_max = page_range // 8
            
            if actual_count < expected_min and expected_min > 20:
                estimated = expected_min - actual_count
                gaps.append(Gap(
                    "PAGE_DENSITY", rep, year, "low",
                    f"Page range {pages[0]}-{pages[-1]} ({page_range} pages) but only {actual_count} cases. Expected {expected_min}-{expected_max}",
                    estimated,
                ))
    return gaps


# ── Strategy 7: New Reporter Gap ────────────────────────────────────────────

def detect_new_reporter_gaps():
    """Flag reporters discovered on PLS but not yet scraped."""
    gaps = []
    # Data from probe run Mar 18, 2026
    new_reporter_estimates = {
        "PLC(CS)": {"sampled": 3692, "estimated_total": 9000, "severity": "critical"},
        "PCRLJN": {"sampled": 686, "estimated_total": 1500, "severity": "high"},
        "YLRN": {"sampled": 769, "estimated_total": 1500, "severity": "high"},
        "PLC(CS)N": {"sampled": 299, "estimated_total": 700, "severity": "medium"},
        "CLCN": {"sampled": 248, "estimated_total": 500, "severity": "medium"},
    }

    for rep, data in new_reporter_estimates.items():
        rep_dir = DATA_DIR / rep.replace("(", "").replace(")", "")
        existing = sum(1 for _ in rep_dir.rglob("*.json")) if rep_dir.exists() else 0

        if existing < data["estimated_total"] * 0.5:
            gaps.append(Gap(
                "NEW_REPORTER", rep, 0, data["severity"],
                f"PLS has ~{data['estimated_total']:,} cases ({data['sampled']} in sampled years). We have {existing}.",
                data["estimated_total"] - existing,
            ))
    return gaps


# ── Strategy 8: Recent Year Gap ─────────────────────────────────────────────

def detect_recent_year_gaps(inventory):
    """Check if 2026 and 2025 cases are being kept up to date."""
    gaps = []
    for rep in REPORTERS:
        count_2026 = len(inventory.get(rep, {}).get(2026, []))
        count_2025 = len(inventory.get(rep, {}).get(2025, []))
        count_2024 = len(inventory.get(rep, {}).get(2024, []))

        if count_2024 > 50 and count_2025 < count_2024 * 0.3:
            gaps.append(Gap(
                "RECENT_YEAR", rep, 2025, "medium",
                f"2025 has only {count_2025} vs 2024 has {count_2024}. Likely more cases exist on PLS.",
                count_2024 - count_2025,
            ))

        if count_2024 > 50 and count_2026 < 20:
            gaps.append(Gap(
                "RECENT_YEAR", rep, 2026, "high",
                f"2026 has only {count_2026} cases. PLS likely has more by now.",
                50,
            ))
    return gaps


# ── Main ────────────────────────────────────────────────────────────────────

def run_detective(reporter_filter=None, top_n=50):
    """Run all gap detection strategies."""
    start = time.time()
    today = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"GAP DETECTIVE REPORT - {today}")
    print("=" * 60)

    print("\nLoading data inventory...")
    inventory = load_data_inventory(reporter_filter)
    total_files = sum(len(pages) for rep in inventory for pages in inventory[rep].values())
    print(f"Total cases indexed: {total_files:,}")

    all_gaps = []

    print("\nRunning 8 detection strategies...")

    gaps = detect_sequential_gaps(inventory)
    print(f"  1. Sequential gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_year_coverage_gaps(inventory)
    print(f"  2. Year coverage gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_reporter_balance_gaps(inventory)
    print(f"  3. Reporter balance: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_temporal_anomalies(inventory)
    print(f"  4. Temporal anomalies: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_cross_reporter_gaps(inventory)
    print(f"  5. Cross-reporter gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_page_density_gaps(inventory)
    print(f"  6. Page density gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_new_reporter_gaps()
    print(f"  7. New reporter gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    gaps = detect_recent_year_gaps(inventory)
    print(f"  8. Recent year gaps: {len(gaps)} found")
    all_gaps.extend(gaps)

    # Sort by priority
    all_gaps.sort(key=lambda g: -g.priority_score)

    elapsed = time.time() - start
    total_estimated = sum(g.estimated_missing for g in all_gaps)

    # Results
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {len(all_gaps)} gaps found")
    print(f"Estimated missing cases: {total_estimated:,}")
    print(f"Time: {elapsed:.1f}s")
    print(f"{'=' * 60}")

    # By severity
    by_severity = Counter(g.severity for g in all_gaps)
    print(f"\nBy severity: Critical: {by_severity.get('critical', 0)} | High: {by_severity.get('high', 0)} | Medium: {by_severity.get('medium', 0)} | Low: {by_severity.get('low', 0)}")

    # By strategy
    print("\nBy strategy:")
    by_strategy = Counter(g.strategy for g in all_gaps)
    for strategy, count in by_strategy.most_common():
        est = sum(g.estimated_missing for g in all_gaps if g.strategy == strategy)
        print(f"  {strategy}: {count} gaps (~{est:,} estimated missing)")

    # Top gaps
    print(f"\nTOP {min(top_n, len(all_gaps))} GAPS (by priority):")
    for i, gap in enumerate(all_gaps[:top_n]):
        icon = {"critical": "!!!", "high": "!! ", "medium": "!  ", "low": ".  "}.get(gap.severity, "   ")
        reporter = gap.reporter.ljust(10)
        year = str(gap.year).ljust(5) if gap.year else "ALL  "
        print(f"  {icon} [{gap.strategy}] {reporter} {year} | ~{gap.estimated_missing:,} missing | {gap.description}")

    # Save
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / f"{today}.json"
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump({
            "date": today,
            "total_gaps": len(all_gaps),
            "total_estimated_missing": total_estimated,
            "by_severity": dict(by_severity),
            "by_strategy": dict(by_strategy),
            "gaps": [g.to_dict() for g in all_gaps[:100]],
            "elapsed_seconds": round(elapsed, 1),
        }, f, indent=2, ensure_ascii=False)
    print(f"\nSaved: {result_file}")


def main():
    parser = argparse.ArgumentParser(description="Gap Detective")
    parser.add_argument("--reporter", type=str, help="Filter by reporter")
    parser.add_argument("--top", type=int, default=30, help="Show top N gaps")
    args = parser.parse_args()
    run_detective(reporter_filter=args.reporter, top_n=args.top)


if __name__ == "__main__":
    main()
