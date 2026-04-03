#!/usr/bin/env python3
"""
Dataset Valuation Analysis
==========================
Estimates the market value of the Pakistani case law dataset.

Valuation Models:
1. Cost to Reproduce: scraping time + compute + dev hours
2. Market Comparable: per-case pricing from legal APIs
3. Revenue Model: addressable market × adoption × price × multiplier
4. Per-reporter value: rarer reporters = higher per-case value

Output:
- Valuation summary table (console)
- Per-reporter value chart -> analytics/output/dataset_value.png
- Growth in dataset value over time -> analytics/output/value_growth.png
"""

import json
import os
import re
import math
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

# -- Cost Assumptions --
AVG_SCRAPE_TIME_SEC = 3       # seconds per case
COMPUTE_COST_PER_HR = 0.50    # $ per hour
DEV_HOURS = 200               # development hours
DEV_RATE = 50                 # $ per dev hour
LEGAL_API_LOW = 0.50          # $ per case (low estimate)
LEGAL_API_HIGH = 5.00         # $ per case (high estimate)
PK_LAWYERS = 250_000          # total lawyers in Pakistan
ADOPTION_RATE = 0.05          # 5% adoption
SUBSCRIPTION_MONTHLY = 20     # $ per month
REVENUE_MULTIPLIER = 5        # value = 5x annual revenue


# -- Helpers ------------------------------------------------------------
def extract_year(date_str):
    """Extract year from date strings like '16th March, 2022'."""
    if not date_str:
        return None
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else None


def load_all_cases():
    """Load all JSON case files from data_v2 (lightweight - only needed fields)."""
    import sys
    NEEDED_FIELDS = {'citation', 'case_name', 'title', 'court', 'date', 'judges',
                     'statutes_cited', 'cases_cited'}
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
                    # Keep only needed fields to save memory
                    light = {k: data[k] for k in NEEDED_FIELDS if k in data}
                    light['_reporter'] = reporter
                    cases.append(light)
                except Exception:
                    errors += 1
        print(f"  {reporter}: {total_files:,} files so far...", flush=True)
    
    print(f"Loaded {len(cases):,} cases from {total_files:,} files ({errors} errors)", flush=True)
    return cases


def format_usd(amount):
    """Format a dollar amount nicely."""
    if amount >= 1_000_000:
        return f"${amount:,.0f} ({amount/1_000_000:.2f}M)"
    elif amount >= 1_000:
        return f"${amount:,.0f} ({amount/1_000:.1f}K)"
    else:
        return f"${amount:,.2f}"


# -- Main Analysis ------------------------------------------------------
def main():
    print("=" * 70)
    print("DATASET VALUATION ANALYSIS")
    print("=" * 70)
    print()
    
    cases = load_all_cases()
    if not cases:
        print("ERROR: No cases loaded.")
        return
    
    total_cases = len(cases)
    
    # -- Reporter breakdown --
    reporter_counts = {}
    reporter_years = {}
    
    for case in cases:
        reporter = case.get('_reporter', 'Unknown')
        year = extract_year(case.get('date', ''))
        
        reporter_counts[reporter] = reporter_counts.get(reporter, 0) + 1
        
        if year and 1947 <= year <= 2026:
            if reporter not in reporter_years:
                reporter_years[reporter] = {}
            reporter_years[reporter][year] = reporter_years[reporter].get(year, 0) + 1
    
    # ------------------------------------------------------------------
    # MODEL 1: Cost to Reproduce
    # ------------------------------------------------------------------
    scrape_hours = (total_cases * AVG_SCRAPE_TIME_SEC) / 3600
    compute_cost = scrape_hours * COMPUTE_COST_PER_HR
    dev_cost = DEV_HOURS * DEV_RATE
    total_cost_to_reproduce = compute_cost + dev_cost
    
    # ------------------------------------------------------------------
    # MODEL 2: Market Comparable
    # ------------------------------------------------------------------
    market_low = total_cases * LEGAL_API_LOW
    market_high = total_cases * LEGAL_API_HIGH
    market_mid = (market_low + market_high) / 2
    
    # ------------------------------------------------------------------
    # MODEL 3: Revenue Model
    # ------------------------------------------------------------------
    subscribers = PK_LAWYERS * ADOPTION_RATE
    annual_revenue = subscribers * SUBSCRIPTION_MONTHLY * 12
    revenue_valuation = annual_revenue * REVENUE_MULTIPLIER
    
    # ------------------------------------------------------------------
    # MODEL 4: Per-Reporter Value (rarity-based)
    # ------------------------------------------------------------------
    # Rarer reporters get higher per-case value
    # Scale: inverse proportion to count, normalized
    max_count = max(reporter_counts.values())
    reporter_per_case_value = {}
    reporter_total_value = {}
    
    for reporter, count in reporter_counts.items():
        # Rarity multiplier: rarer reporters are worth more per case
        rarity = max_count / count
        # Scale between $1 and $8 per case based on rarity
        per_case = LEGAL_API_LOW + (LEGAL_API_HIGH - LEGAL_API_LOW) * min(rarity / 50, 1.0)
        reporter_per_case_value[reporter] = per_case
        reporter_total_value[reporter] = per_case * count
    
    total_rarity_value = sum(reporter_total_value.values())
    
    # ------------------------------------------------------------------
    # Console Output
    # ------------------------------------------------------------------
    print("=" * 70)
    print("VALUATION SUMMARY")
    print("=" * 70)
    print()
    print(f"Dataset size: {total_cases:,} cases across {len(reporter_counts)} reporters")
    print()
    
    print("+" + "-" * 57 + "+")
    print("|  MODEL 1: COST TO REPRODUCE                            |")
    print("+" + "-" * 57 + "+")
    print(f"|  Scraping time:        {scrape_hours:,.1f} hours ({total_cases:,} x {AVG_SCRAPE_TIME_SEC}s)")
    print(f"|  Compute cost:         {format_usd(compute_cost):>20}")
    print(f"|  Development cost:     {format_usd(dev_cost):>20} ({DEV_HOURS}h x ${DEV_RATE}/h)")
    print(f"|  TOTAL:                {format_usd(total_cost_to_reproduce):>20}")
    print("+" + "-" * 57 + "+")
    print()
    
    print("+" + "-" * 57 + "+")
    print("|  MODEL 2: MARKET COMPARABLE                            |")
    print("+" + "-" * 57 + "+")
    print(f"|  Low  (${LEGAL_API_LOW}/case):    {format_usd(market_low):>20}")
    print(f"|  Mid:                  {format_usd(market_mid):>20}")
    print(f"|  High (${LEGAL_API_HIGH}/case):   {format_usd(market_high):>20}")
    print("+" + "-" * 57 + "+")
    print()
    
    print("+" + "-" * 57 + "+")
    print("|  MODEL 3: REVENUE MODEL                                |")
    print("+" + "-" * 57 + "+")
    print(f"|  Addressable market:   {PK_LAWYERS:,} lawyers")
    print(f"|  Projected subscribers:{subscribers:,.0f} ({ADOPTION_RATE*100:.0f}% adoption)")
    print(f"|  Monthly price:        ${SUBSCRIPTION_MONTHLY}/month")
    print(f"|  Annual revenue:       {format_usd(annual_revenue):>20}")
    print(f"|  Valuation ({REVENUE_MULTIPLIER}x rev):   {format_usd(revenue_valuation):>20}")
    print("+" + "-" * 57 + "+")
    print()
    
    print("+" + "-" * 57 + "+")
    print("|  MODEL 4: RARITY-WEIGHTED VALUE                        |")
    print("+" + "-" * 57 + "+")
    for reporter in sorted(reporter_counts.keys(), key=lambda x: reporter_total_value.get(x, 0), reverse=True):
        count = reporter_counts[reporter]
        pcv = reporter_per_case_value[reporter]
        tv = reporter_total_value[reporter]
        print(f"|  {reporter:<8} {count:>6,} cases x ${pcv:.2f}/case = {format_usd(tv):>15}")
    print(f"|  {'TOTAL':<8} {total_cases:>6,} cases {'':>18} = {format_usd(total_rarity_value):>15}")
    print("+" + "-" * 57 + "+")
    print()
    
    # -- Blended Estimate --
    valuations = [total_cost_to_reproduce, market_mid, revenue_valuation, total_rarity_value]
    blended = np.mean(valuations)
    print("=" * 70)
    print(f"  BLENDED VALUATION ESTIMATE:  {format_usd(blended)}")
    print(f"  Range: {format_usd(min(valuations))} - {format_usd(max(valuations))}")
    print("=" * 70)
    print()
    
    # -- Chart 1: Per-Reporter Value --
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    
    # Sort reporters by total value
    reporters_sorted = sorted(reporter_total_value.items(), key=lambda x: x[1], reverse=True)
    r_names = [r for r, _ in reporters_sorted]
    r_values = [v for _, v in reporters_sorted]
    r_counts = [reporter_counts[r] for r in r_names]
    r_pcv = [reporter_per_case_value[r] for r in r_names]
    
    # Left: total value by reporter
    colors = sns.color_palette('viridis', n_colors=len(r_names))
    bars = ax1.bar(r_names, r_values, color=colors, edgecolor='white', linewidth=0.5)
    ax1.set_ylabel('Total Value ($)', fontsize=11)
    ax1.set_title('Dataset Value by Reporter', fontsize=13, fontweight='bold')
    ax1.tick_params(axis='x', rotation=45)
    
    for bar, val in zip(bars, r_values):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                f'${val:,.0f}', ha='center', va='bottom', fontsize=7)
    
    # Right: per-case value by reporter
    bars2 = ax2.bar(r_names, r_pcv, color=colors, edgecolor='white', linewidth=0.5)
    ax2.set_ylabel('Per-Case Value ($)', fontsize=11)
    ax2.set_title('Per-Case Value by Reporter (Rarity-Based)', fontsize=13, fontweight='bold')
    ax2.tick_params(axis='x', rotation=45)
    
    for bar, val, count in zip(bars2, r_pcv, r_counts):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f'${val:.2f}\n({count:,})', ha='center', va='bottom', fontsize=7)
    
    plt.tight_layout()
    chart1_path = os.path.join(OUTPUT_DIR, 'dataset_value.png')
    plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart1_path}")
    
    # -- Chart 2: Value Growth Over Time --
    # Track cumulative cases and value over years
    all_year_counts = {}
    for case in cases:
        year = extract_year(case.get('date', ''))
        reporter = case.get('_reporter', 'Unknown')
        if year and 1947 <= year <= 2026:
            if year not in all_year_counts:
                all_year_counts[year] = {'cases': 0, 'value': 0}
            all_year_counts[year]['cases'] += 1
            all_year_counts[year]['value'] += reporter_per_case_value.get(reporter, LEGAL_API_LOW)
    
    if all_year_counts:
        years_sorted = sorted(all_year_counts.keys())
        cumulative_cases = []
        cumulative_value = []
        running_cases = 0
        running_value = 0
        
        for year in years_sorted:
            running_cases += all_year_counts[year]['cases']
            running_value += all_year_counts[year]['value']
            cumulative_cases.append(running_cases)
            cumulative_value.append(running_value)
        
        fig, ax1 = plt.subplots(figsize=(14, 7))
        
        color1 = '#2ecc71'
        color2 = '#3498db'
        
        ax1.fill_between(years_sorted, cumulative_value, alpha=0.3, color=color1)
        ax1.plot(years_sorted, cumulative_value, color=color1, linewidth=2, label='Cumulative Value ($)')
        ax1.set_xlabel('Year', fontsize=12)
        ax1.set_ylabel('Cumulative Dataset Value ($)', fontsize=12, color=color1)
        ax1.tick_params(axis='y', labelcolor=color1)
        
        ax2 = ax1.twinx()
        ax2.plot(years_sorted, cumulative_cases, color=color2, linewidth=2, linestyle='--', label='Cumulative Cases')
        ax2.set_ylabel('Cumulative Cases', fontsize=12, color=color2)
        ax2.tick_params(axis='y', labelcolor=color2)
        
        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
        
        ax1.set_title('Dataset Value Growth Over Time', fontsize=14, fontweight='bold')
        ax1.set_xlim(years_sorted[0], years_sorted[-1])
        
        # Add current value annotation
        ax1.annotate(f'Current Value:\n{format_usd(running_value)}',
                    xy=(years_sorted[-1], running_value),
                    xytext=(-100, -40), textcoords='offset points',
                    fontsize=10, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', color='gray'),
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
        
        plt.tight_layout()
        chart2_path = os.path.join(OUTPUT_DIR, 'value_growth.png')
        plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved: {chart2_path}")
    
    print()
    print("Analysis complete.")


if __name__ == '__main__':
    main()
