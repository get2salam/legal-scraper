"""
Temporal Analysis
Cases per month/quarter, busiest months, YoY growth, seasonal patterns.
"""
import os, json, re
from collections import defaultdict, Counter
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

plt.style.use('dark_background')

BASE = os.path.join(os.path.dirname(__file__), '..', 'data_v2', 'court_cases')
OUTPUT = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT, exist_ok=True)

COURTS = ['SC', 'SHC', 'IHC', 'SST', 'FSC', 'LHC']
COLORS = {
    'SC': '#10b981', 'SHC': '#06b6d4', 'IHC': '#3b82f6',
    'SST': '#8b5cf6', 'FSC': '#f59e0b', 'LHC': '#ef4444'
}
MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
QUARTER_NAMES = ['Q1 (Jan-Mar)', 'Q2 (Apr-Jun)', 'Q3 (Jul-Sep)', 'Q4 (Oct-Dec)']

def extract_date(case):
    """Extract full date from case, return (year, month) tuple or None."""
    for field in ['judgment_date', 'order_date', 'upload_date']:
        val = case.get(field)
        if val and isinstance(val, str) and len(val) >= 7:
            try:
                # Try YYYY-MM-DD format
                parts = val.split('-')
                if len(parts) >= 2:
                    y = int(parts[0])
                    m = int(parts[1])
                    if 1970 <= y <= 2030 and 1 <= m <= 12:
                        return (y, m)
            except:
                pass
            try:
                # Try other date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S', '%d-%m-%Y']:
                    try:
                        dt = datetime.strptime(val[:10], fmt)
                        if 1970 <= dt.year <= 2030:
                            return (dt.year, dt.month)
                    except:
                        continue
            except:
                pass
    
    # created_at for SST (format: "2016-11-02 02:53:51")
    created = case.get('created_at')
    if created and isinstance(created, str) and len(created) >= 7:
        try:
            parts = created.split('-')
            y = int(parts[0])
            m = int(parts[1])
            if 1970 <= y <= 2030 and 1 <= m <= 12:
                return (y, m)
        except:
            pass
    
    return None

def extract_year(case):
    date = extract_date(case)
    if date:
        return date[0]
    y = case.get('year') or case.get('citation_year')
    if y:
        try:
            return int(y)
        except:
            pass
    return None

def load_all_cases():
    cases = defaultdict(list)
    for court in COURTS:
        court_path = os.path.join(BASE, court)
        if not os.path.exists(court_path):
            continue
        for root, dirs, files in os.walk(court_path):
            if 'original' in root.split(os.sep):
                continue
            for f in files:
                if f.endswith('.json'):
                    try:
                        with open(os.path.join(root, f), 'r', encoding='utf-8') as fh:
                            data = json.load(fh)
                            cases[court].append(data)
                    except:
                        pass
    return cases

def main():
    print("=" * 60)
    print("TEMPORAL ANALYSIS")
    print("=" * 60)
    
    cases = load_all_cases()
    
    # Collect dates
    court_dates = defaultdict(list)  # court -> list of (year, month)
    court_years = defaultdict(Counter)  # court -> Counter of years
    
    for court in COURTS:
        dates_found = 0
        for case in cases[court]:
            date = extract_date(case)
            if date:
                court_dates[court].append(date)
                court_years[court][date[0]] += 1
                dates_found += 1
            else:
                year = extract_year(case)
                if year:
                    court_years[court][year] += 1
        total = len(cases[court])
        print(f"  {court}: {dates_found}/{total} cases with month-level dates, "
              f"{sum(court_years[court].values())}/{total} with year data")
    
    # --- Chart 1: Cases per quarter for courts with date data ---
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    
    for idx, court in enumerate(COURTS):
        ax = axes[idx]
        dates = court_dates[court]
        if not dates:
            ax.text(0.5, 0.5, 'No monthly\ndate data', ha='center', va='center', 
                   fontsize=12, color='gray', transform=ax.transAxes)
            ax.set_title(court, fontsize=14, fontweight='bold')
            continue
        
        # Group by year-quarter
        yq = defaultdict(int)
        for y, m in dates:
            q = (m - 1) // 3
            yq[(y, q)] += 1
        
        if yq:
            sorted_keys = sorted(yq.keys())
            labels = [f"{y}Q{q+1}" for y, q in sorted_keys]
            values = [yq[k] for k in sorted_keys]
            
            ax.bar(range(len(labels)), values, color=COLORS[court], edgecolor='white', linewidth=0.3)
            step = max(1, len(labels) // 8)
            ax.set_xticks(range(0, len(labels), step))
            ax.set_xticklabels([labels[i] for i in range(0, len(labels), step)], 
                             rotation=45, ha='right', fontsize=8)
        
        ax.set_title(f'{court} ({len(dates):,} dated cases)', fontsize=13, fontweight='bold')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    plt.suptitle('Cases Per Quarter by Court', fontsize=18, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_cases_per_quarter.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_cases_per_quarter.png")
    
    # --- Chart 2: Monthly seasonality (busiest months) ---
    fig, ax = plt.subplots(figsize=(14, 7))
    
    has_monthly = False
    for court in COURTS:
        dates = court_dates[court]
        if not dates:
            continue
        has_monthly = True
        monthly = Counter()
        for y, m in dates:
            monthly[m] += 1
        
        months = list(range(1, 13))
        values = [monthly.get(m, 0) for m in months]
        ax.plot(MONTH_NAMES, values, marker='o', label=court, color=COLORS[court],
               linewidth=2, markersize=6)
    
    if has_monthly:
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Total Cases', fontsize=12)
        ax.set_title('Seasonal Patterns — Cases by Month (All Years)', fontsize=16, fontweight='bold', pad=15)
        ax.legend(fontsize=10, framealpha=0.8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        # Annotate summer/Ramadan periods
        ax.axvspan(4.5, 7.5, alpha=0.1, color='yellow', label='Summer (May-Aug)')
        ax.text(6, ax.get_ylim()[1]*0.95, 'Summer', ha='center', fontsize=9, color='yellow', alpha=0.7)
        
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_seasonal_patterns.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_seasonal_patterns.png")
        
        # Print busiest months
        print("\nBusiest Months per Court:")
        for court in COURTS:
            dates = court_dates[court]
            if not dates:
                continue
            monthly = Counter()
            for y, m in dates:
                monthly[m] += 1
            top3 = monthly.most_common(3)
            print(f"  {court}: {', '.join(f'{MONTH_NAMES[m-1]}({c})' for m, c in top3)}")
    
    # --- Chart 3: Year-over-year growth rate ---
    fig, ax = plt.subplots(figsize=(14, 7))
    
    for court in COURTS:
        if not court_years[court]:
            continue
        years = sorted(court_years[court].keys())
        if len(years) < 3:
            continue
        
        growth_years = []
        growth_rates = []
        for i in range(1, len(years)):
            if years[i] == years[i-1] + 1:  # consecutive years only
                prev = court_years[court][years[i-1]]
                curr = court_years[court][years[i]]
                if prev > 0:
                    rate = ((curr - prev) / prev) * 100
                    growth_years.append(years[i])
                    growth_rates.append(rate)
        
        if growth_years:
            ax.plot(growth_years, growth_rates, marker='o', label=court, color=COLORS[court],
                   linewidth=2, markersize=5, alpha=0.85)
    
    ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Year-over-Year Growth (%)', fontsize=12)
    ax.set_title('Year-over-Year Growth Rate by Court', fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_yoy_growth.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_yoy_growth.png")
    
    # --- Chart 4: Monthly heatmap for courts with most data ---
    # Pick the court with most monthly data
    best_court = max(COURTS, key=lambda c: len(court_dates[c]))
    dates = court_dates[best_court]
    
    if dates:
        year_month = defaultdict(lambda: defaultdict(int))
        for y, m in dates:
            year_month[y][m] += 1
        
        years = sorted(year_month.keys())
        matrix = np.zeros((len(years), 12))
        for i, y in enumerate(years):
            for m in range(1, 13):
                matrix[i][m-1] = year_month[y].get(m, 0)
        
        fig, ax = plt.subplots(figsize=(14, max(5, len(years)*0.5)))
        im = ax.imshow(matrix, aspect='auto', cmap='viridis', interpolation='nearest')
        
        ax.set_yticks(range(len(years)))
        ax.set_yticklabels([str(y) for y in years], fontsize=9)
        ax.set_xticks(range(12))
        ax.set_xticklabels(MONTH_NAMES, fontsize=10)
        
        for i in range(len(years)):
            for j in range(12):
                val = int(matrix[i][j])
                if val > 0:
                    color = 'white' if val > matrix.max() * 0.5 else 'black'
                    ax.text(j, i, str(val), ha='center', va='center', fontsize=8, color=color)
        
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Cases', fontsize=10)
        ax.set_title(f'{best_court} Monthly Activity Heatmap', fontsize=16, fontweight='bold', pad=15)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_monthly_heatmap.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_monthly_heatmap.png")
    
    print(f"\n{'='*60}")
    print(f"Temporal analysis complete. 4 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
