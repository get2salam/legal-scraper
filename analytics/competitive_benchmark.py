"""
Competitive Benchmark Analysis
================================
Analyzes dataset coverage to identify unique strengths. Produces reporterxyear
coverage metrics, bubble charts, and depth analysis.
"""

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json
import os
import glob
import math
from collections import defaultdict

# --- Configuration -------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data_v2')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_coverage_data():
    """Scan all reporters and years, count cases."""
    reporter_year_counts = defaultdict(lambda: defaultdict(int))
    reporter_totals = defaultdict(int)
    total_cases = 0

    for reporter in REPORTERS:
        reporter_dir = os.path.join(BASE_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue

        year_dirs = [d for d in os.listdir(reporter_dir) if os.path.isdir(os.path.join(reporter_dir, d))]

        for year_str in year_dirs:
            try:
                year = int(year_str)
            except ValueError:
                continue

            year_dir = os.path.join(reporter_dir, year_str)
            json_files = glob.glob(os.path.join(year_dir, '*.json'))
            count = len(json_files)

            if count > 0:
                reporter_year_counts[reporter][year] = count
                reporter_totals[reporter] += count
                total_cases += count

    print(f"Total cases across all reporters: {total_cases:,}")
    print(f"Active reporters: {len(reporter_totals)}")
    print()

    return reporter_year_counts, reporter_totals, total_cases


def build_coverage_matrix(reporter_year_counts):
    """Build reporter x year matrix as a DataFrame."""
    all_years = set()
    for reporter in reporter_year_counts:
        all_years.update(reporter_year_counts[reporter].keys())

    if not all_years:
        return pd.DataFrame()

    min_year = min(all_years)
    max_year = max(all_years)
    all_years_range = list(range(min_year, max_year + 1))

    data = {}
    for reporter in REPORTERS:
        if reporter in reporter_year_counts:
            data[reporter] = [reporter_year_counts[reporter].get(y, 0) for y in all_years_range]

    df = pd.DataFrame(data, index=all_years_range)
    df.index.name = 'Year'
    return df


def create_bubble_chart(reporter_year_counts, reporter_totals):
    """Create a reporter x year bubble chart."""
    rows = []
    for reporter in REPORTERS:
        if reporter not in reporter_year_counts:
            continue
        for year, count in reporter_year_counts[reporter].items():
            rows.append({
                'Reporter': reporter,
                'Year': year,
                'Cases': count
            })

    if not rows:
        print("No data for bubble chart.")
        return

    df = pd.DataFrame(rows)

    # Sort reporters by total cases
    reporter_order = sorted(reporter_totals.items(), key=lambda x: x[1], reverse=True)
    reporter_order = [r[0] for r in reporter_order if r[0] in df['Reporter'].unique()]

    # Assign colors
    palette = sns.color_palette('husl', len(reporter_order))
    color_map = {r: palette[i] for i, r in enumerate(reporter_order)}

    fig, ax = plt.subplots(figsize=(20, 10))

    for reporter in reporter_order:
        subset = df[df['Reporter'] == reporter]
        # Scale bubble sizes: sqrt scale for visual proportionality
        sizes = subset['Cases'].apply(lambda x: max(math.sqrt(x) * 3, 8))
        ax.scatter(
            subset['Year'],
            [reporter] * len(subset),
            s=sizes,
            alpha=0.7,
            color=color_map[reporter],
            edgecolors='white',
            linewidth=0.5,
            label=reporter
        )

    ax.set_title('Dataset Coverage: Reporter x Year\n(Bubble size proportional to case count)',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Year', fontsize=13)
    ax.set_ylabel('Reporter', fontsize=13)

    # Add size legend
    legend_sizes = [10, 100, 500, 1000]
    legend_bubbles = []
    for s in legend_sizes:
        legend_bubbles.append(
            ax.scatter([], [], s=max(math.sqrt(s) * 3, 8), color='gray',
                       alpha=0.7, edgecolors='white', label=f'{s} cases')
        )
    ax.legend(handles=legend_bubbles, loc='upper left', title='Bubble Size',
              framealpha=0.9, fontsize=9)

    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    outpath = os.path.join(OUTPUT_DIR, 'coverage_bubble.png')
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[OK] Bubble chart saved -> {outpath}")


def create_depth_chart(reporter_year_counts, reporter_totals):
    """Create a bar chart showing temporal depth per reporter."""
    depth_data = []

    for reporter in REPORTERS:
        if reporter not in reporter_year_counts:
            continue
        years = list(reporter_year_counts[reporter].keys())
        if not years:
            continue
        min_y = min(years)
        max_y = max(years)
        span = max_y - min_y + 1
        active_years = len(years)
        total = reporter_totals.get(reporter, 0)

        depth_data.append({
            'Reporter': reporter,
            'Start': min_y,
            'End': max_y,
            'Span': span,
            'Active Years': active_years,
            'Total Cases': total
        })

    if not depth_data:
        print("No data for depth chart.")
        return depth_data

    depth_df = pd.DataFrame(depth_data)
    depth_df.sort_values('Span', ascending=True, inplace=True)

    fig, ax = plt.subplots(figsize=(14, 8))

    # Horizontal bar chart showing year range
    colors = sns.color_palette('viridis', len(depth_df))
    for i, (_, row) in enumerate(depth_df.iterrows()):
        ax.barh(
            row['Reporter'],
            row['Span'],
            left=row['Start'],
            color=colors[i],
            alpha=0.8,
            edgecolor='white',
            height=0.6
        )
        # Label with year range and case count
        mid = row['Start'] + row['Span'] / 2
        ax.text(mid, i, f"{row['Start']}-{row['End']} ({row['Total Cases']:,} cases)",
                ha='center', va='center', fontsize=9, fontweight='bold', color='white',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='black', alpha=0.3))

    ax.set_title('Reporter Temporal Depth (Years Covered)', fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel('Year', fontsize=13)
    ax.set_ylabel('Reporter', fontsize=13)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    outpath = os.path.join(OUTPUT_DIR, 'reporter_depth.png')
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[OK] Depth chart saved -> {outpath}")

    return depth_data


def main():
    print("=" * 70)
    print("COMPETITIVE BENCHMARK ANALYSIS")
    print("=" * 70)
    print()

    # Load data
    reporter_year_counts, reporter_totals, total_cases = load_coverage_data()

    if not reporter_year_counts:
        print("ERROR: No data found.")
        return

    # Build matrix
    matrix_df = build_coverage_matrix(reporter_year_counts)

    # --- Charts ----------------------------------------------------------
    create_bubble_chart(reporter_year_counts, reporter_totals)
    depth_data = create_depth_chart(reporter_year_counts, reporter_totals)
    print()

    # --- Console Analysis ------------------------------------------------
    print("-" * 70)
    print("REPORTER RANKING (by total cases)")
    print("-" * 70)

    sorted_reporters = sorted(reporter_totals.items(), key=lambda x: x[1], reverse=True)
    for i, (reporter, count) in enumerate(sorted_reporters, 1):
        years = list(reporter_year_counts[reporter].keys())
        pct = (count / total_cases * 100) if total_cases > 0 else 0
        print(f"  {i:>2}. {reporter:<8s} {count:>7,} cases  ({pct:>5.1f}%)  "
              f"[{min(years)}-{max(years)}, {len(years)} years]")

    print()

    # --- Coverage density ------------------------------------------------
    print("-" * 70)
    print("COVERAGE DENSITY")
    print("-" * 70)

    total_cells = 0
    filled_cells = 0
    dense_cells = 0  # >100 cases

    for reporter in reporter_year_counts:
        years = reporter_year_counts[reporter]
        if not years:
            continue
        min_y = min(years.keys())
        max_y = max(years.keys())
        possible_years = max_y - min_y + 1
        total_cells += possible_years
        filled_cells += len(years)
        dense_cells += sum(1 for c in years.values() if c > 100)

    fill_rate = (filled_cells / total_cells * 100) if total_cells > 0 else 0
    dense_rate = (dense_cells / total_cells * 100) if total_cells > 0 else 0

    print(f"  Total possible reporterxyear cells (within each reporter's range): {total_cells}")
    print(f"  Filled cells (>0 cases): {filled_cells} ({fill_rate:.1f}%)")
    print(f"  Dense cells (>100 cases): {dense_cells} ({dense_rate:.1f}%)")
    print()

    # --- Top reporterxyear combos ----------------------------------------
    print("-" * 70)
    print("TOP 20 STRONGEST REPORTERxYEAR COMBOS")
    print("-" * 70)

    combos = []
    for reporter in reporter_year_counts:
        for year, count in reporter_year_counts[reporter].items():
            combos.append((reporter, year, count))

    combos.sort(key=lambda x: x[2], reverse=True)

    print(f"  {'#':<4} {'Reporter':<10s} {'Year':<6} {'Cases':>8s}")
    print("  " + "-" * 30)
    for i, (reporter, year, count) in enumerate(combos[:20], 1):
        print(f"  {i:<4} {reporter:<10s} {year:<6} {count:>7,}")
    print()

    # --- Temporal depth analysis -----------------------------------------
    print("-" * 70)
    print("TEMPORAL DEPTH ANALYSIS")
    print("-" * 70)

    if depth_data:
        depth_sorted = sorted(depth_data, key=lambda x: x['Start'])
        earliest = depth_sorted[0]
        print(f"  Earliest coverage: {earliest['Reporter']} starting from {earliest['Start']}")

        depth_sorted_span = sorted(depth_data, key=lambda x: x['Span'], reverse=True)
        deepest = depth_sorted_span[0]
        print(f"  Deepest coverage: {deepest['Reporter']} spanning {deepest['Span']} years "
              f"({deepest['Start']}-{deepest['End']})")

        # Completeness: active years / span
        print()
        print(f"  {'Reporter':<10s} {'Span':>6s} {'Active':>8s} {'Completeness':>14s} {'Avg Cases/Yr':>14s}")
        print("  " + "-" * 55)
        for d in sorted(depth_data, key=lambda x: x['Active Years'] / x['Span'] if x['Span'] > 0 else 0, reverse=True):
            completeness = (d['Active Years'] / d['Span'] * 100) if d['Span'] > 0 else 0
            avg = d['Total Cases'] / d['Active Years'] if d['Active Years'] > 0 else 0
            print(f"  {d['Reporter']:<10s} {d['Span']:>6} {d['Active Years']:>8} "
                  f"{completeness:>12.1f}% {avg:>13.0f}")

    print()

    # --- Suggested marketing claims --------------------------------------
    print("-" * 70)
    print("SUGGESTED MARKETING CLAIMS")
    print("-" * 70)

    print()
    print(f"  [DATA] \"{total_cases:,} digitized case law records spanning {len(sorted_reporters)} Pakistani law reporters\"")
    print()

    if depth_data:
        earliest_year = min(d['Start'] for d in depth_data)
        latest_year = max(d['End'] for d in depth_data)
        total_span = latest_year - earliest_year + 1
        print(f"  [TIME] \"Coverage spanning {total_span} years ({earliest_year}-{latest_year})\"")
        print()

    # Strongest reporter
    top_rep = sorted_reporters[0]
    print(f"  [BEST] \"Most comprehensive {top_rep[0]} collection: {top_rep[1]:,} cases\"")
    print()

    # Best year
    top_combo = combos[0]
    print(f"  [PEAK] \"Peak coverage: {top_combo[2]:,} cases in {top_combo[0]} {top_combo[1]}\"")
    print()

    # Multi-reporter years
    years_with_multi = defaultdict(list)
    for reporter in reporter_year_counts:
        for year in reporter_year_counts[reporter]:
            years_with_multi[year].append(reporter)

    full_coverage_years = [y for y, reps in years_with_multi.items() if len(reps) >= len(sorted_reporters) * 0.7]
    if full_coverage_years:
        full_coverage_years.sort()
        print(f"  [LINK] \"Multi-reporter cross-coverage for {len(full_coverage_years)} years "
              f"({min(full_coverage_years)}-{max(full_coverage_years)})\"")
        print()

    # Unique differentiators
    print("  [KEY] Key differentiators:")
    for reporter, count in sorted_reporters[:3]:
        years = reporter_year_counts[reporter]
        active = len(years)
        print(f"     - {reporter}: {count:,} cases across {active} years")

    print()
    print("=" * 70)
    print("Done.")


if __name__ == '__main__':
    main()
