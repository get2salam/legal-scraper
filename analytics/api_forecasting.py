"""
api_forecasting.py  -  "The Capacity Planner"
=============================================
Model projected API usage, revenue, and costs for a legal
search platform serving Pakistani lawyers.

Usage:
    python api_forecasting.py
"""

import matplotlib
matplotlib.use('Agg')

import sys
import math
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# --- Configuration -----------------------------------------------------------

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Market assumptions
TOTAL_LAWYERS = 250_000
ADOPTION_RATES = {1: 0.01, 2: 0.02, 3: 0.05, 4: 0.10, 5: 0.15}
QUERIES_PER_USER_PER_DAY = 8

# Peak traffic: 80% in 8 peak hours (9-11 AM PKT, 2-4 PM PKT = 6h peak)
# Actually, let's interpret "9-11 AM PKT, 2-4 PM PKT" = 4 peak hours
# But spec says "80% of daily traffic in 8 peak hours" so 8 peak hours total
PEAK_HOURS = 8
PEAK_TRAFFIC_FRACTION = 0.80
TOTAL_DAILY_HOURS = 24

# Revenue model
CONVERSION_RATE = 0.30  # 30% freemium to paid
MONTHLY_SUBSCRIPTION = 20  # $20/month

# Cost model
COST_PER_QUERY = 0.001  # $0.001

# Data growth
INITIAL_CASES = 50_867
CASES_GROWTH_PER_WEEK = 10_000

# Operational costs (additional to compute)
INFRA_BASE_MONTHLY = 500  # Base infrastructure cost $/month
INFRA_SCALE_PER_1000_DAU = 50  # Additional $/month per 1000 DAU

# Working days per month/year
WORKING_DAYS_PER_MONTH = 22  # Lawyers mostly query on working days
DAYS_PER_MONTH = 30  # For cost calculations (some weekend usage)

# --- Styling -----------------------------------------------------------------

COLORS = {
    'primary':    '#1a5276',
    'secondary':  '#2e86c1',
    'accent':     '#e74c3c',
    'bg':         '#fafafa',
    'grid':       '#e0e0e0',
    'text':       '#2c3e50',
    'gold':       '#f39c12',
    'green':      '#27ae60',
    'purple':     '#8e44ad',
    'revenue':    '#27ae60',
    'cost':       '#e74c3c',
    'profit':     '#2e86c1',
}

plt.rcParams.update({
    'figure.facecolor': COLORS['bg'],
    'axes.facecolor':   '#ffffff',
    'axes.edgecolor':   COLORS['grid'],
    'axes.labelcolor':  COLORS['text'],
    'xtick.color':      COLORS['text'],
    'ytick.color':      COLORS['text'],
    'font.size':        11,
    'axes.titlesize':   14,
    'axes.labelsize':   12,
})


def banner(text):
    width = 72
    print("\n" + "=" * width)
    print(f"  {text}")
    print("=" * width)


def sub_banner(text):
    print(f"\n  -- {text} {'-' * max(1, 60 - len(text))}")


def format_money(amount):
    """Format dollar amount nicely."""
    if abs(amount) >= 1_000_000:
        return f"${amount/1_000_000:,.2f}M"
    elif abs(amount) >= 1_000:
        return f"${amount:,.0f}"
    else:
        return f"${amount:,.2f}"


def format_number(n):
    """Format large numbers."""
    if n >= 1_000_000:
        return f"{n/1_000_000:,.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:,.1f}K"
    else:
        return f"{n:,.0f}"


# --- Monthly Projection Model -----------------------------------------------

def build_monthly_projections():
    """Build month-by-month projections for 5 years (60 months)."""

    months = []
    for year in range(1, 6):
        for month in range(1, 13):
            months.append({
                'year': year,
                'month': month,
                'month_index': (year - 1) * 12 + month,
            })

    projections = []

    for m in months:
        year = m['year']
        month_idx = m['month_index']

        # Smooth adoption curve (interpolate between yearly rates using sigmoid)
        if year == 1:
            # Ramp up during year 1
            progress = m['month'] / 12
            adoption = ADOPTION_RATES[1] * progress
        else:
            # Smooth transition between years
            prev_rate = ADOPTION_RATES[year - 1]
            curr_rate = ADOPTION_RATES[year]
            progress = m['month'] / 12
            adoption = prev_rate + (curr_rate - prev_rate) * progress

        # Users
        dau = int(TOTAL_LAWYERS * adoption)
        mau = int(dau * 1.8)  # MAU typically ~1.8x DAU for B2B tools

        # Queries
        daily_queries = dau * QUERIES_PER_USER_PER_DAY
        monthly_queries = daily_queries * DAYS_PER_MONTH  # Include some weekend usage

        # Peak load
        peak_queries_per_hour = (daily_queries * PEAK_TRAFFIC_FRACTION) / PEAK_HOURS
        peak_qps = peak_queries_per_hour / 3600  # queries per second

        # Revenue (only paying users generate revenue)
        paying_users = int(mau * CONVERSION_RATE)
        monthly_revenue = paying_users * MONTHLY_SUBSCRIPTION

        # Costs
        compute_cost = monthly_queries * COST_PER_QUERY
        infra_cost = INFRA_BASE_MONTHLY + (dau / 1000) * INFRA_SCALE_PER_1000_DAU
        total_monthly_cost = compute_cost + infra_cost

        # Profit
        monthly_profit = monthly_revenue - total_monthly_cost

        # Dataset size
        weeks_elapsed = month_idx * 4.33
        total_cases = INITIAL_CASES + int(weeks_elapsed * CASES_GROWTH_PER_WEEK)

        projections.append({
            'year': year,
            'month': m['month'],
            'month_index': month_idx,
            'adoption_rate': adoption,
            'dau': dau,
            'mau': mau,
            'daily_queries': daily_queries,
            'monthly_queries': monthly_queries,
            'peak_qps': peak_qps,
            'peak_queries_per_hour': peak_queries_per_hour,
            'paying_users': paying_users,
            'monthly_revenue': monthly_revenue,
            'compute_cost': compute_cost,
            'infra_cost': infra_cost,
            'total_cost': total_monthly_cost,
            'monthly_profit': monthly_profit,
            'cumulative_profit': 0,  # Will compute after
            'total_cases': total_cases,
        })

    # Cumulative profit
    cumulative = 0
    for p in projections:
        cumulative += p['monthly_profit']
        p['cumulative_profit'] = cumulative

    return projections


# --- Console Output ----------------------------------------------------------

def print_projections(projections):
    """Print rich console output."""

    # -- Assumptions --
    banner("ASSUMPTIONS & MODEL PARAMETERS")
    print(f"""
  Market:
    Total registered lawyers:       {TOTAL_LAWYERS:>10,}
    Queries per user per day:       {QUERIES_PER_USER_PER_DAY:>10}
    Peak hours per day:             {PEAK_HOURS:>10}
    Peak traffic fraction:          {PEAK_TRAFFIC_FRACTION:>10.0%}

  Adoption Curve:
    Year 1:  {ADOPTION_RATES[1]:>5.0%}  ->  {int(TOTAL_LAWYERS * ADOPTION_RATES[1]):>6,} DAU
    Year 2:  {ADOPTION_RATES[2]:>5.0%}  ->  {int(TOTAL_LAWYERS * ADOPTION_RATES[2]):>6,} DAU
    Year 3:  {ADOPTION_RATES[3]:>5.0%}  ->  {int(TOTAL_LAWYERS * ADOPTION_RATES[3]):>6,} DAU
    Year 4:  {ADOPTION_RATES[4]:>5.0%}  ->  {int(TOTAL_LAWYERS * ADOPTION_RATES[4]):>6,} DAU
    Year 5:  {ADOPTION_RATES[5]:>5.0%}  ->  {int(TOTAL_LAWYERS * ADOPTION_RATES[5]):>6,} DAU

  Revenue Model:
    Freemium -> Paid conversion:     {CONVERSION_RATE:>10.0%}
    Monthly subscription:           {format_money(MONTHLY_SUBSCRIPTION):>10}
    Compute cost per query:         {format_money(COST_PER_QUERY):>10}
    Base infrastructure:            {format_money(INFRA_BASE_MONTHLY):>10}/month

  Data:
    Initial case count:             {INITIAL_CASES:>10,}
    Growth rate:                    {format_number(CASES_GROWTH_PER_WEEK):>10}/week
""")

    # -- 5-Year Annual Summary --
    banner("5-YEAR PROJECTION SUMMARY")

    header = (f"  {'Year':>4}  {'DAU':>8}  {'Queries/Day':>13}  "
              f"{'Monthly Cost':>13}  {'Monthly Rev':>13}  {'Monthly Profit':>14}  "
              f"{'Peak QPS':>9}  {'Cases':>10}")
    print(header)
    print("  " + "-" * (len(header.strip())))

    for year in range(1, 6):
        # Get end-of-year numbers (month 12)
        year_end = [p for p in projections if p['year'] == year and p['month'] == 12][0]
        year_data = [p for p in projections if p['year'] == year]
        avg_profit = sum(p['monthly_profit'] for p in year_data) / len(year_data)

        profit_indicator = "[+]" if year_end['monthly_profit'] > 0 else "[-]"

        print(f"  {year:>4}  "
              f"{year_end['dau']:>8,}  "
              f"{year_end['daily_queries']:>13,}  "
              f"{format_money(year_end['total_cost']):>13}  "
              f"{format_money(year_end['monthly_revenue']):>13}  "
              f"{format_money(year_end['monthly_profit']):>14}  "
              f"{year_end['peak_qps']:>8.1f}  "
              f"{format_number(year_end['total_cases']):>10}  {profit_indicator}")

    # -- Detailed Monthly Table (Year 1 only) --
    banner("YEAR 1  -  MONTHLY BREAKDOWN")

    header = (f"  {'Month':>5}  {'DAU':>7}  {'Queries/Day':>12}  "
              f"{'Cost':>10}  {'Revenue':>10}  {'Profit':>11}  {'Cumulative':>11}")
    print(header)
    print("  " + "-" * (len(header.strip())))

    for p in projections:
        if p['year'] != 1:
            continue
        cum_color = "+" if p['cumulative_profit'] > 0 else ""
        print(f"  {p['month']:>5}  "
              f"{p['dau']:>7,}  "
              f"{p['daily_queries']:>12,}  "
              f"{format_money(p['total_cost']):>10}  "
              f"{format_money(p['monthly_revenue']):>10}  "
              f"{format_money(p['monthly_profit']):>11}  "
              f"{cum_color}{format_money(p['cumulative_profit']):>10}")

    # -- Break-Even Analysis --
    banner("BREAK-EVEN ANALYSIS")

    # Monthly break-even
    breakeven_month = None
    for p in projections:
        if p['monthly_profit'] > 0:
            breakeven_month = p
            break

    if breakeven_month:
        print(f"  Monthly Break-Even Point:")
        print(f"    Reached in:     Year {breakeven_month['year']}, Month {breakeven_month['month']} "
              f"(Month {breakeven_month['month_index']})")
        print(f"    DAU at break-even: {breakeven_month['dau']:,}")
        print(f"    Revenue:        {format_money(breakeven_month['monthly_revenue'])}")
        print(f"    Cost:           {format_money(breakeven_month['total_cost'])}")
    else:
        print("  ! Monthly break-even NOT reached within 5 years!")

    # Cumulative break-even
    cum_breakeven = None
    for p in projections:
        if p['cumulative_profit'] > 0:
            cum_breakeven = p
            break

    print()
    if cum_breakeven:
        print(f"  Cumulative Break-Even (ROI positive):")
        print(f"    Reached in:     Year {cum_breakeven['year']}, Month {cum_breakeven['month']} "
              f"(Month {cum_breakeven['month_index']})")
        print(f"    Total invested before profit:  "
              f"{format_money(abs(p['cumulative_profit'] - p['monthly_profit']))}")
    else:
        print("  ! Cumulative break-even NOT reached within 5 years")

    # -- Capacity Planning --
    banner("CAPACITY PLANNING  -  Peak Load Analysis")

    for year in range(1, 6):
        year_end = [p for p in projections if p['year'] == year and p['month'] == 12][0]
        peak_qps = year_end['peak_qps']
        peak_qph = year_end['peak_queries_per_hour']

        # Estimate required infrastructure
        # Assume each server handles ~100 QPS
        servers_needed = max(1, math.ceil(peak_qps / 100))
        # With 2x headroom for spikes
        servers_recommended = servers_needed * 2

        print(f"\n  Year {year} (End of Year):")
        print(f"    Daily queries:           {year_end['daily_queries']:>12,}")
        print(f"    Peak queries/hour:       {peak_qph:>12,.0f}")
        print(f"    Peak QPS:                {peak_qps:>12.1f}")
        print(f"    Servers @ 100 QPS:       {servers_needed:>12} (min)")
        print(f"    Recommended (2x):        {servers_recommended:>12}")
        print(f"    Dataset size:            {format_number(year_end['total_cases']):>12}")

    # -- Revenue Milestones --
    banner("REVENUE MILESTONES")

    milestones = [1_000, 5_000, 10_000, 50_000, 100_000]
    for target in milestones:
        found = False
        for p in projections:
            if p['monthly_revenue'] >= target:
                print(f"  {format_money(target):>10}/month  ->  Year {p['year']}, "
                      f"Month {p['month']}  (DAU: {p['dau']:,})")
                found = True
                break
        if not found:
            print(f"  {format_money(target):>10}/month  ->  Not reached in 5 years")

    # Final 5-year summary
    banner("5-YEAR FINANCIAL SUMMARY")
    total_revenue = sum(p['monthly_revenue'] for p in projections)
    total_cost = sum(p['total_cost'] for p in projections)
    total_profit = total_revenue - total_cost
    final = projections[-1]

    print(f"  Total Revenue (5 years):     {format_money(total_revenue):>15}")
    print(f"  Total Costs (5 years):       {format_money(total_cost):>15}")
    print(f"  Net Profit (5 years):        {format_money(total_profit):>15}")
    print(f"  Final Monthly Revenue:       {format_money(final['monthly_revenue']):>15}")
    print(f"  Final Monthly Profit:        {format_money(final['monthly_profit']):>15}")
    print(f"  Final ARR:                   {format_money(final['monthly_revenue'] * 12):>15}")
    print(f"  Final Dataset Size:          {format_number(final['total_cases']):>15}")

    return projections


# --- Visualizations ----------------------------------------------------------

def create_visualizations(projections):
    """Generate forecast charts."""
    banner("GENERATING VISUALIZATIONS")

    month_indices = [p['month_index'] for p in projections]
    month_labels = [f"Y{p['year']}" if p['month'] == 1 else "" for p in projections]

    # -- 1. API Forecast  -  Query Volume --
    sub_banner("Query Volume Forecast")

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # Query volume over time
    ax = axes[0, 0]
    daily_queries = [p['daily_queries'] for p in projections]
    ax.fill_between(month_indices, daily_queries, alpha=0.3, color=COLORS['secondary'])
    ax.plot(month_indices, daily_queries, color=COLORS['secondary'], linewidth=2)
    ax.set_ylabel('Daily Queries')
    ax.set_title('Daily Query Volume')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    # DAU growth
    ax = axes[0, 1]
    daus = [p['dau'] for p in projections]
    ax.fill_between(month_indices, daus, alpha=0.3, color=COLORS['green'])
    ax.plot(month_indices, daus, color=COLORS['green'], linewidth=2)
    ax.set_ylabel('Daily Active Users')
    ax.set_title('User Adoption Curve')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    # Peak QPS
    ax = axes[1, 0]
    peak_qps = [p['peak_qps'] for p in projections]
    ax.fill_between(month_indices, peak_qps, alpha=0.3, color=COLORS['purple'])
    ax.plot(month_indices, peak_qps, color=COLORS['purple'], linewidth=2)
    ax.set_ylabel('Peak Queries/Second')
    ax.set_title('Peak Load (QPS)')
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    # Dataset growth
    ax = axes[1, 1]
    cases = [p['total_cases'] for p in projections]
    ax.fill_between(month_indices, cases, alpha=0.3, color=COLORS['gold'])
    ax.plot(month_indices, cases, color=COLORS['gold'], linewidth=2)
    ax.set_ylabel('Total Cases')
    ax.set_title('Dataset Growth')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    fig.suptitle('API Capacity Forecast  -  Legal Search Platform (5-Year Projection)',
                 fontsize=15, fontweight='bold', color=COLORS['text'], y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "api_forecast.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close(fig)
    print(f"    + Saved: {path}")

    # -- 2. Revenue vs Cost --
    sub_banner("Revenue vs Cost Forecast")

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={'height_ratios': [2, 1]})

    # Revenue vs Cost
    ax = axes[0]
    revenues = [p['monthly_revenue'] for p in projections]
    costs = [p['total_cost'] for p in projections]
    profits = [p['monthly_profit'] for p in projections]

    ax.fill_between(month_indices, revenues, alpha=0.15, color=COLORS['revenue'])
    ax.fill_between(month_indices, costs, alpha=0.15, color=COLORS['cost'])

    ax.plot(month_indices, revenues, color=COLORS['revenue'], linewidth=2.5,
            label='Monthly Revenue', marker='', zorder=5)
    ax.plot(month_indices, costs, color=COLORS['cost'], linewidth=2.5,
            label='Monthly Cost', linestyle='--', marker='', zorder=5)

    # Break-even line
    breakeven_month = None
    for p in projections:
        if p['monthly_profit'] > 0:
            breakeven_month = p['month_index']
            break
    if breakeven_month:
        ax.axvline(x=breakeven_month, color=COLORS['gold'], linestyle=':',
                   linewidth=2, label=f'Break-even (Month {breakeven_month})')

    ax.set_ylabel('Monthly Amount ($)')
    ax.set_title('Revenue vs Cost  -  Monthly', fontsize=14, fontweight='bold')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_money(x)))
    ax.legend(fontsize=10, loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    # Cumulative profit
    ax = axes[1]
    cumulative = [p['cumulative_profit'] for p in projections]
    colors_bar = [COLORS['revenue'] if c >= 0 else COLORS['cost'] for c in cumulative]
    ax.fill_between(month_indices, cumulative,
                    where=[c >= 0 for c in cumulative],
                    alpha=0.3, color=COLORS['revenue'], interpolate=True)
    ax.fill_between(month_indices, cumulative,
                    where=[c < 0 for c in cumulative],
                    alpha=0.3, color=COLORS['cost'], interpolate=True)
    ax.plot(month_indices, cumulative, color=COLORS['primary'], linewidth=2)
    ax.axhline(y=0, color=COLORS['text'], linewidth=0.8, linestyle='-')
    ax.set_ylabel('Cumulative Profit ($)')
    ax.set_title('Cumulative Profit/Loss', fontsize=12)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_money(x)))
    ax.grid(True, alpha=0.3)
    ax.set_xlabel('Month')
    ax.set_xticks([1, 13, 25, 37, 49])
    ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3', 'Year 4', 'Year 5'])

    fig.suptitle('Financial Projection  -  Revenue, Cost & Profit Over 5 Years',
                 fontsize=15, fontweight='bold', color=COLORS['text'], y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "revenue_forecast.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close(fig)
    print(f"    + Saved: {path}")


# --- Main --------------------------------------------------------------------

def main():
    print("\n" + "#" * 72)
    print("  THE CAPACITY PLANNER - Legal Search Platform API Forecasting")
    print("  Revenue | Cost | Break-Even | Infrastructure Planning")
    print("#" * 72)

    projections = build_monthly_projections()
    print_projections(projections)
    create_visualizations(projections)

    banner("COMPLETE")
    print(f"  Charts saved to: {OUTPUT_DIR}")
    print(f"  Months modeled: {len(projections)}")
    print()


if __name__ == "__main__":
    main()
