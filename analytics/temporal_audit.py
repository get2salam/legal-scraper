"""
Temporal Audit - Cross-validates dates in case data.
Scans data_v2/REPORTER/YEAR/*.json to find citation-date mismatches
and cases with missing/unparseable dates.
"""
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import json
import os
import re
import glob
from collections import defaultdict

# -- Config ----------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DATA_DIR = os.path.join(BASE_DIR, 'data_v2')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']
MISMATCH_THRESHOLD = 2  # Flag if |citation_year - judgment_year| > this

YEAR_RE = re.compile(r'(\d{4})')


def extract_year_from_citation(citation):
    """Extract the first 4-digit year from citation string."""
    if not citation:
        return None
    m = YEAR_RE.search(str(citation))
    if m:
        year = int(m.group(1))
        if 1947 <= year <= 2030:
            return year
    return None


def extract_year_from_date(date_str):
    """Extract a 4-digit year from the date field."""
    if not date_str:
        return None
    # Try to find a 4-digit year
    m = YEAR_RE.search(str(date_str))
    if m:
        year = int(m.group(1))
        if 1947 <= year <= 2030:
            return year
    return None


def scan_cases():
    """Scan all case JSON files and audit dates."""
    results = {
        'total_by_reporter': defaultdict(int),
        'mismatch_by_reporter': defaultdict(int),
        'missing_date_by_reporter': defaultdict(int),
        'mismatches': [],  # (reporter, citation, citation_year, judgment_year, file)
    }

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            print(f"  [!] Reporter dir not found: {reporter}")
            continue

        year_dirs = sorted(glob.glob(os.path.join(reporter_dir, '*')))
        for year_dir in year_dirs:
            if not os.path.isdir(year_dir):
                continue
            dir_name = os.path.basename(year_dir)
            if dir_name == 'original':
                continue

            json_files = glob.glob(os.path.join(year_dir, '*.json'))
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8', errors='replace') as f:
                        data = json.load(f)
                except (json.JSONDecodeError, Exception):
                    continue

                results['total_by_reporter'][reporter] += 1

                citation = data.get('citation', '')
                date_str = data.get('date', '')

                citation_year = extract_year_from_citation(citation)
                judgment_year = extract_year_from_date(date_str)

                # Check for missing/unparseable date
                if judgment_year is None:
                    results['missing_date_by_reporter'][reporter] += 1

                # Check for mismatch
                if citation_year is not None and judgment_year is not None:
                    if abs(citation_year - judgment_year) > MISMATCH_THRESHOLD:
                        results['mismatch_by_reporter'][reporter] += 1
                        results['mismatches'].append({
                            'reporter': reporter,
                            'citation': citation,
                            'citation_year': citation_year,
                            'judgment_year': judgment_year,
                            'diff': abs(citation_year - judgment_year),
                            'file': os.path.basename(json_file),
                        })

    return results


def plot_mismatches(results):
    """Plot mismatch rate by reporter."""
    reporters = []
    rates = []
    counts = []

    for rep in REPORTERS:
        total = results['total_by_reporter'].get(rep, 0)
        mismatches = results['mismatch_by_reporter'].get(rep, 0)
        if total > 0:
            reporters.append(rep)
            rates.append((mismatches / total) * 100)
            counts.append(mismatches)

    if not reporters:
        print("  [!] No data for mismatch chart")
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = sns.color_palette('Reds_d', len(reporters))
    # Sort by rate descending for better visual
    sorted_data = sorted(zip(reporters, rates, counts), key=lambda x: -x[1])
    reporters_s, rates_s, counts_s = zip(*sorted_data) if sorted_data else ([], [], [])

    bars = ax.bar(range(len(reporters_s)), rates_s,
                  color=colors, edgecolor='white', linewidth=0.5)

    # Add count labels on bars
    for i, (rate, count) in enumerate(zip(rates_s, counts_s)):
        if count > 0:
            ax.text(i, rate + 0.1, f'{count}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold')

    ax.set_xlabel('Reporter', fontsize=12)
    ax.set_ylabel('Mismatch Rate (%)', fontsize=12)
    ax.set_title(f'Date Mismatch Rate by Reporter\n(|citation year - judgment year| > {MISMATCH_THRESHOLD})',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(reporters_s)))
    ax.set_xticklabels(reporters_s, fontsize=11)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, 'date_mismatches.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def plot_missing_dates(results):
    """Plot missing date rate by reporter."""
    reporters = []
    rates = []
    counts = []

    for rep in REPORTERS:
        total = results['total_by_reporter'].get(rep, 0)
        missing = results['missing_date_by_reporter'].get(rep, 0)
        if total > 0:
            reporters.append(rep)
            rates.append((missing / total) * 100)
            counts.append(missing)

    if not reporters:
        print("  [!] No data for missing dates chart")
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = sns.color_palette('Oranges_d', len(reporters))
    sorted_data = sorted(zip(reporters, rates, counts), key=lambda x: -x[1])
    reporters_s, rates_s, counts_s = zip(*sorted_data) if sorted_data else ([], [], [])

    bars = ax.bar(range(len(reporters_s)), rates_s,
                  color=colors, edgecolor='white', linewidth=0.5)

    for i, (rate, count) in enumerate(zip(rates_s, counts_s)):
        if count > 0:
            ax.text(i, rate + 0.1, f'{count}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold')

    ax.set_xlabel('Reporter', fontsize=12)
    ax.set_ylabel('Missing Date Rate (%)', fontsize=12)
    ax.set_title('Missing/Unparseable Date Rate by Reporter',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(reporters_s)))
    ax.set_xticklabels(reporters_s, fontsize=11)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, 'missing_dates.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def print_summary(results):
    """Print console summary."""
    total_cases = sum(results['total_by_reporter'].values())
    total_mismatches = sum(results['mismatch_by_reporter'].values())
    total_missing = sum(results['missing_date_by_reporter'].values())

    print("\n" + "=" * 60)
    print("  TEMPORAL AUDIT REPORT")
    print("=" * 60)

    print(f"\n  Total cases scanned:      {total_cases:,}")
    print(f"  Date mismatches (>{MISMATCH_THRESHOLD}yr): {total_mismatches:,}")
    if total_cases > 0:
        print(f"  Mismatch rate:            {(total_mismatches/total_cases)*100:.2f}%")
    print(f"  Missing/unparseable date: {total_missing:,}")
    if total_cases > 0:
        print(f"  Missing date rate:        {(total_missing/total_cases)*100:.2f}%")

    # Per-reporter breakdown
    print(f"\n  {'-' * 50}")
    print(f"  {'Reporter':<10} {'Total':>8} {'Mismatch':>10} {'Missing':>10} {'Mismatch%':>10}")
    print(f"  {'-' * 50}")
    for rep in REPORTERS:
        total = results['total_by_reporter'].get(rep, 0)
        mis = results['mismatch_by_reporter'].get(rep, 0)
        miss = results['missing_date_by_reporter'].get(rep, 0)
        rate = (mis / total * 100) if total > 0 else 0
        if total > 0:
            print(f"  {rep:<10} {total:>8,} {mis:>10,} {miss:>10,} {rate:>9.2f}%")

    # Show sample mismatches
    if results['mismatches']:
        print(f"\n  {'-' * 50}")
        print(f"  SAMPLE MISMATCHES (up to 10):")
        sorted_mm = sorted(results['mismatches'], key=lambda x: -x['diff'])
        for mm in sorted_mm[:10]:
            print(f"    {mm['citation']:<30} "
                  f"cite={mm['citation_year']}  judg={mm['judgment_year']}  "
                  f"D={mm['diff']}yr")

    print("\n" + "=" * 60)


def main():
    print("Temporal Audit - Cross-validating case dates")
    print("-" * 40)

    print(f"  Scanning data in {DATA_DIR}...")
    results = scan_cases()

    total = sum(results['total_by_reporter'].values())
    print(f"  Scanned {total:,} cases across {len([r for r in REPORTERS if results['total_by_reporter'].get(r, 0) > 0])} reporters")

    print(f"\nGenerating charts...")
    plot_mismatches(results)
    plot_missing_dates(results)

    print_summary(results)


if __name__ == '__main__':
    main()
