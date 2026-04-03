#!/usr/bin/env python3
"""
Court Workload Analysis
=======================
Tracks cases per court over time across 50,000+ Pakistani court cases.

Metrics:
- Cases per year per court
- Court share of total cases (pie chart)
- Workload trend over time per court (stacked area chart)
- Busiest and quietest years per court

Output:
- Court distribution pie chart -> analytics/output/court_distribution.png
- Court workload over time (stacked area) -> analytics/output/court_workload_trend.png
- Console summary with stats
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


def normalize_court(court_name):
    """Normalize court names for consistency."""
    if not court_name:
        return 'Unknown'
    court = str(court_name).strip()
    if not court:
        return 'Unknown'
    return court


def load_all_cases():
    """Load all JSON case files from data_v2 (lightweight - only needed fields)."""
    NEEDED_FIELDS = {'citation', 'court', 'date'}
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
    print("COURT WORKLOAD ANALYSIS")
    print("=" * 70)
    print()
    
    cases = load_all_cases()
    if not cases:
        print("ERROR: No cases loaded.")
        return
    
    # -- Build records --
    rows = []
    for case in cases:
        court = normalize_court(case.get('court', ''))
        year = extract_year(case.get('date', ''))
        reporter = case.get('_reporter', '')
        rows.append({'court': court, 'year': year, 'reporter': reporter})
    
    df = pd.DataFrame(rows)
    df = df[df['year'].notna()].copy()
    df['year'] = df['year'].astype(int)
    
    # Filter reasonable years
    df = df[(df['year'] >= 1947) & (df['year'] <= 2026)]
    
    total_cases = len(df)
    unique_courts = df['court'].nunique()
    
    print(f"Total cases with valid year: {total_cases:,}")
    print(f"Unique courts: {unique_courts}")
    print()
    
    # -- Court distribution --
    court_counts = df['court'].value_counts()
    
    # -- Console: Per-court summary --
    print("=" * 100)
    print(f"{'Court':<55} {'Cases':>8} {'Share':>7} {'First':>6} {'Last':>6} {'Busiest':>8} {'Quietest':>9}")
    print("-" * 100)
    
    court_year_matrix = df.groupby(['court', 'year']).size().unstack(fill_value=0)
    
    for court in court_counts.head(25).index:
        count = court_counts[court]
        share = count / total_cases * 100
        court_df = df[df['court'] == court]
        first_year = court_df['year'].min()
        last_year = court_df['year'].max()
        
        if court in court_year_matrix.index:
            row = court_year_matrix.loc[court]
            row_nonzero = row[row > 0]
            if len(row_nonzero) > 0:
                busiest = row_nonzero.idxmax()
                quietest = row_nonzero.idxmin()
            else:
                busiest = quietest = '-'
        else:
            busiest = quietest = '-'
        
        court_display = court[:53] if len(court) > 53 else court
        print(f"{court_display:<55} {count:>8,} {share:>6.1f}% {first_year:>6} {last_year:>6} {str(busiest):>8} {str(quietest):>9}")
    
    if len(court_counts) > 25:
        remaining = court_counts.iloc[25:].sum()
        remaining_pct = remaining / total_cases * 100
        print(f"{'... other courts (' + str(len(court_counts) - 25) + ')':<55} {remaining:>8,} {remaining_pct:>6.1f}%")
    
    print("-" * 100)
    print()
    
    # -- Chart 1: Court Distribution Pie Chart --
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Top 10 courts + "Others"
    top_n = 10
    top_courts = court_counts.head(top_n)
    other_count = court_counts.iloc[top_n:].sum()
    
    labels = list(top_courts.index) + ['Others']
    sizes = list(top_courts.values) + [other_count]
    
    # Truncate long court names for chart
    labels_short = []
    for l in labels:
        if len(l) > 35:
            l = l[:32] + '...'
        labels_short.append(l)
    
    colors = sns.color_palette('Set3', n_colors=len(labels))
    explode = [0.03] * len(labels)
    
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels_short, autopct='%1.1f%%',
        colors=colors, explode=explode, pctdistance=0.85,
        textprops={'fontsize': 8}
    )
    
    for autotext in autotexts:
        autotext.set_fontsize(7)
    
    ax.set_title('Distribution of Cases by Court', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    chart1_path = os.path.join(OUTPUT_DIR, 'court_distribution.png')
    plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart1_path}")
    
    # -- Chart 2: Court Workload Over Time (Stacked Area) --
    # Use top 8 courts for readability
    top_courts_list = court_counts.head(8).index.tolist()
    
    # Create pivot: year × court
    df_top = df[df['court'].isin(top_courts_list)].copy()
    df_other = df[~df['court'].isin(top_courts_list)].copy()
    df_other['court'] = 'Others'
    df_combined = pd.concat([df_top, df_other])
    
    pivot = df_combined.groupby(['year', 'court']).size().unstack(fill_value=0)
    
    # Ensure consistent column order
    col_order = top_courts_list + ['Others']
    col_order = [c for c in col_order if c in pivot.columns]
    pivot = pivot[col_order]
    
    # Truncate column names for legend
    rename_map = {}
    for c in pivot.columns:
        if len(c) > 30:
            rename_map[c] = c[:27] + '...'
    pivot = pivot.rename(columns=rename_map)
    
    fig, ax = plt.subplots(figsize=(16, 8))
    colors = sns.color_palette('tab10', n_colors=len(pivot.columns))
    
    ax.stackplot(pivot.index, pivot.values.T, labels=pivot.columns, colors=colors, alpha=0.8)
    
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('Court Workload Over Time (Stacked Area)', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=7, ncol=2)
    ax.set_xlim(pivot.index.min(), pivot.index.max())
    
    plt.tight_layout()
    chart2_path = os.path.join(OUTPUT_DIR, 'court_workload_trend.png')
    plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart2_path}")
    
    # -- Overall stats --
    print()
    print("OVERALL STATISTICS:")
    cases_by_year = df.groupby('year').size()
    print(f"  Busiest year overall:   {cases_by_year.idxmax()} ({cases_by_year.max():,} cases)")
    print(f"  Quietest year overall:  {cases_by_year.idxmin()} ({cases_by_year.min():,} cases)")
    print(f"  Average cases/year:     {cases_by_year.mean():,.0f}")
    print(f"  Year range:             {df['year'].min()} – {df['year'].max()}")
    print(f"  Top court:              {court_counts.index[0]} ({court_counts.iloc[0]:,} cases, {court_counts.iloc[0]/total_cases*100:.1f}%)")
    print()


if __name__ == '__main__':
    main()
