#!/usr/bin/env python3
"""
Statute Lifecycle Analysis
==========================
Tracks statute citation frequency over time across 50,000+ Pakistani court cases.

Identifies:
- Most cited statutes overall (top 30)
- Rising statutes (increasing citation frequency)
- Declining statutes (decreasing citation frequency)
- Trend detection via linear regression slope on yearly counts

Output:
- Top 30 statutes bar chart -> analytics/output/top_statutes.png
- Top 10 statutes timeline (line chart) -> analytics/output/statute_trends.png
- Rising vs declining statutes lists (console)
"""

import json
import os
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# -- Configuration ------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, 'data_v2')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -- Helpers ------------------------------------------------------------
def extract_year(date_str):
    """Extract year from date strings like '16th March, 2022'."""
    if not date_str:
        return None
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else None


def normalize_statute(statute_name):
    """Clean statute names for consistency."""
    if not statute_name:
        return None
    s = str(statute_name).strip()
    if len(s) < 3:
        return None
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s)
    return s


def linear_regression_slope(years, counts):
    """Compute simple linear regression slope."""
    if len(years) < 2:
        return 0.0
    x = np.array(years, dtype=float)
    y = np.array(counts, dtype=float)
    n = len(x)
    if n < 2:
        return 0.0
    x_mean = x.mean()
    y_mean = y.mean()
    num = np.sum((x - x_mean) * (y - y_mean))
    den = np.sum((x - x_mean) ** 2)
    if den == 0:
        return 0.0
    return num / den


def load_all_cases():
    """Load all JSON case files from data_v2 (lightweight - only needed fields)."""
    NEEDED_FIELDS = {'citation', 'date', 'statutes_cited'}
    cases = []
    total_files = 0
    errors = 0
    
    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue
        for year_dir in os.listdir(reporter_dir):
            year_path = os.path.join(reporter_dir, year_dir)
            if not os.path.isdir(year_path):
                continue
            for fname in os.listdir(year_path):
                if not fname.endswith('.json'):
                    continue
                total_files += 1
                fpath = os.path.join(year_path, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    light = {k: data[k] for k in NEEDED_FIELDS if k in data}
                    light['_reporter'] = reporter
                    cases.append(light)
                except Exception:
                    errors += 1
        print(f"  {reporter}: {total_files:,} files so far...", flush=True)
    
    print(f"Loaded {len(cases):,} cases from {total_files:,} files ({errors} errors)", flush=True)
    return cases


# -- Main Analysis ------------------------------------------------------
def main():
    print("=" * 70)
    print("STATUTE LIFECYCLE ANALYSIS")
    print("=" * 70)
    print()
    
    cases = load_all_cases()
    if not cases:
        print("ERROR: No cases loaded.")
        return
    
    # -- Build statute-year counts --
    statute_year_counts = {}  # statute -> {year: count}
    statute_total = {}        # statute -> total count
    
    for case in cases:
        year = extract_year(case.get('date', ''))
        if not year or year < 1947 or year > 2026:
            continue
        
        statutes = case.get('statutes_cited', [])
        if not statutes:
            continue
        
        seen_in_case = set()  # Deduplicate within a single case
        for statute_raw in statutes:
            statute = normalize_statute(statute_raw)
            if not statute or statute in seen_in_case:
                continue
            seen_in_case.add(statute)
            
            if statute not in statute_year_counts:
                statute_year_counts[statute] = {}
                statute_total[statute] = 0
            
            statute_year_counts[statute][year] = statute_year_counts[statute].get(year, 0) + 1
            statute_total[statute] += 1
    
    print(f"Unique statutes found: {len(statute_total):,}")
    print()
    
    # -- Sort by total citations --
    sorted_statutes = sorted(statute_total.items(), key=lambda x: x[1], reverse=True)
    
    # -- Console: Top 30 statutes --
    print("=" * 90)
    print(f"{'Rank':<5} {'Statute':<55} {'Total':>8} {'First':>6} {'Last':>6} {'Trend':>8}")
    print("-" * 90)
    
    # Compute trends for all statutes with sufficient data
    statute_trends = {}
    for statute, total in sorted_statutes:
        year_counts = statute_year_counts[statute]
        years = sorted(year_counts.keys())
        counts = [year_counts[y] for y in years]
        
        # Need at least 3 years of data for meaningful trend
        if len(years) >= 3:
            slope = linear_regression_slope(years, counts)
            statute_trends[statute] = slope
        else:
            statute_trends[statute] = 0.0
    
    for i, (statute, total) in enumerate(sorted_statutes[:30], 1):
        year_counts = statute_year_counts[statute]
        years = sorted(year_counts.keys())
        first_year = years[0]
        last_year = years[-1]
        slope = statute_trends.get(statute, 0.0)
        trend_str = f"{slope:+.2f}"
        
        statute_display = statute[:53] if len(statute) > 53 else statute
        print(f"{i:<5} {statute_display:<55} {total:>8,} {first_year:>6} {last_year:>6} {trend_str:>8}")
    
    print("-" * 90)
    print()
    
    # -- Rising vs Declining Statutes --
    # Filter to statutes with at least 20 total citations and 5+ years of data
    qualified = {s: slope for s, slope in statute_trends.items() 
                 if statute_total[s] >= 20 and len(statute_year_counts[s]) >= 5}
    
    rising = sorted(qualified.items(), key=lambda x: x[1], reverse=True)
    declining = sorted(qualified.items(), key=lambda x: x[1])
    
    print("TOP 15 RISING STATUTES (increasing citation frequency):")
    print("-" * 75)
    for statute, slope in rising[:15]:
        total = statute_total[statute]
        statute_display = statute[:50] if len(statute) > 50 else statute
        print(f"  [+] {statute_display:<50} slope={slope:+.3f}  total={total:,}")
    print()
    
    print("TOP 15 DECLINING STATUTES (decreasing citation frequency):")
    print("-" * 75)
    for statute, slope in declining[:15]:
        if slope >= 0:
            continue
        total = statute_total[statute]
        statute_display = statute[:50] if len(statute) > 50 else statute
        print(f"  [-] {statute_display:<50} slope={slope:+.3f}  total={total:,}")
    print()
    
    # -- Chart 1: Top 30 Statutes Bar Chart --
    fig, ax = plt.subplots(figsize=(14, 12))
    
    top30 = sorted_statutes[:30]
    top30.reverse()  # Bottom to top for horizontal bar
    
    names = []
    for s, _ in top30:
        if len(s) > 45:
            names.append(s[:42] + '...')
        else:
            names.append(s)
    counts = [c for _, c in top30]
    
    # Color by trend direction
    colors = []
    for s, _ in top30:
        slope = statute_trends.get(s, 0)
        if slope > 0.5:
            colors.append('#2ecc71')  # green = rising
        elif slope < -0.5:
            colors.append('#e74c3c')  # red = declining
        else:
            colors.append('#3498db')  # blue = stable
    
    bars = ax.barh(names, counts, color=colors, edgecolor='white', linewidth=0.5)
    
    ax.set_xlabel('Total Citations', fontsize=12)
    ax.set_title('Top 30 Most Cited Statutes in Pakistani Case Law', fontsize=14, fontweight='bold')
    ax.tick_params(axis='y', labelsize=8)
    
    # Legend for colors
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#2ecc71', label='Rising trend'),
        Patch(facecolor='#3498db', label='Stable'),
        Patch(facecolor='#e74c3c', label='Declining trend'),
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=9)
    
    plt.tight_layout()
    chart1_path = os.path.join(OUTPUT_DIR, 'top_statutes.png')
    plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart1_path}")
    
    # -- Chart 2: Top 10 Statutes Timeline --
    fig, ax = plt.subplots(figsize=(16, 8))
    
    top10_statutes = [s for s, _ in sorted_statutes[:10]]
    colors = sns.color_palette('tab10', n_colors=10)
    
    # Build year range across all top 10
    all_years = set()
    for statute in top10_statutes:
        all_years.update(statute_year_counts[statute].keys())
    
    if all_years:
        year_range = range(min(all_years), max(all_years) + 1)
    else:
        year_range = range(1970, 2026)
    
    for i, statute in enumerate(top10_statutes):
        year_counts = statute_year_counts[statute]
        years = sorted(year_range)
        counts = [year_counts.get(y, 0) for y in years]
        
        # Short name for legend
        label = statute if len(statute) <= 35 else statute[:32] + '...'
        ax.plot(years, counts, label=label, color=colors[i], linewidth=1.5, alpha=0.8)
    
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Number of Cases Citing Statute', fontsize=12)
    ax.set_title('Citation Trends for Top 10 Statutes Over Time', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=7, ncol=2)
    ax.set_xlim(min(year_range), max(year_range))
    
    plt.tight_layout()
    chart2_path = os.path.join(OUTPUT_DIR, 'statute_trends.png')
    plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart2_path}")
    
    # -- Summary --
    print()
    print("SUMMARY:")
    print(f"  Total unique statutes:     {len(statute_total):,}")
    print(f"  Most cited statute:        {sorted_statutes[0][0]} ({sorted_statutes[0][1]:,} citations)")
    
    qualified_rising = [s for s, slope in rising if slope > 0]
    qualified_declining = [s for s, slope in declining if slope < 0]
    print(f"  Statutes with rising trend:    {len(qualified_rising):,}")
    print(f"  Statutes with declining trend: {len(qualified_declining):,}")
    print()


if __name__ == '__main__':
    main()
