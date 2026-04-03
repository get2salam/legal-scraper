"""
SHC Bench Deep Dive Analysis
Generates: cases per bench, bench activity over years, bench-year heatmap
"""
import os, json, re
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

plt.style.use('dark_background')

BASE = os.path.join(os.path.dirname(__file__), '..', 'data_v2', 'court_cases', 'SHC')
OUTPUT = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT, exist_ok=True)

BENCHES = ['KHI', 'HYD', 'SUK', 'LAR', 'MIR']
BENCH_NAMES = {
    'KHI': 'Karachi', 'HYD': 'Hyderabad', 'SUK': 'Sukkur',
    'LAR': 'Larkana', 'MIR': 'Mirpurkhas'
}
BENCH_COLORS = {
    'KHI': '#06b6d4', 'HYD': '#10b981', 'SUK': '#3b82f6',
    'LAR': '#f59e0b', 'MIR': '#ef4444'
}

def load_bench_cases():
    """Load all SHC cases organized by bench."""
    bench_cases = defaultdict(list)
    for bench in BENCHES:
        bench_path = os.path.join(BASE, bench)
        if not os.path.exists(bench_path):
            continue
        for root, dirs, files in os.walk(bench_path):
            for f in files:
                if f.endswith('.json'):
                    try:
                        with open(os.path.join(root, f), 'r', encoding='utf-8') as fh:
                            data = json.load(fh)
                            bench_cases[bench].append(data)
                    except:
                        pass
    return bench_cases

def extract_year(case):
    """Extract year from SHC case."""
    for field in ['order_date', 'judgment_date', 'upload_date']:
        val = case.get(field)
        if val and isinstance(val, str) and len(val) >= 4:
            try:
                y = int(val[:4])
                if 1970 <= y <= 2030:
                    return y
            except:
                pass
    cit = case.get('citation', '')
    if cit and isinstance(cit, str):
        m = re.search(r'(20\d{2})', cit)
        if m:
            return int(m.group())
    return None

def main():
    print("=" * 60)
    print("SHC BENCH DEEP DIVE ANALYSIS")
    print("=" * 60)
    
    bench_cases = load_bench_cases()
    
    # --- Summary ---
    total_shc = sum(len(bench_cases[b]) for b in BENCHES)
    print(f"\nTotal SHC cases: {total_shc:,}")
    for b in BENCHES:
        pct = len(bench_cases[b]) / total_shc * 100 if total_shc > 0 else 0
        print(f"  {b} ({BENCH_NAMES[b]}): {len(bench_cases[b]):,} cases ({pct:.1f}%)")
    
    # Find most active bench
    most_active = max(BENCHES, key=lambda b: len(bench_cases[b]))
    print(f"\nMost active bench: {most_active} ({BENCH_NAMES[most_active]}) with {len(bench_cases[most_active]):,} cases")
    
    # --- Chart 1: Cases per bench (bar chart) ---
    fig, ax = plt.subplots(figsize=(10, 6))
    bench_counts = [len(bench_cases[b]) for b in BENCHES]
    bench_labels = [f"{b}\n({BENCH_NAMES[b]})" for b in BENCHES]
    colors = [BENCH_COLORS[b] for b in BENCHES]
    
    bars = ax.bar(bench_labels, bench_counts, color=colors, edgecolor='white', linewidth=0.5, width=0.6)
    for bar, count in zip(bars, bench_counts):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{count:,}', ha='center', va='bottom', fontsize=12, fontweight='bold', color='white')
    
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('SHC Cases by Bench', fontsize=16, fontweight='bold', pad=15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_ylim(0, max(bench_counts) * 1.15)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_shc_cases_per_bench.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_shc_cases_per_bench.png")
    
    # --- Chart 2: Bench activity over years (line chart) ---
    bench_years = defaultdict(lambda: defaultdict(int))
    for bench in BENCHES:
        for case in bench_cases[bench]:
            year = extract_year(case)
            if year:
                bench_years[bench][year] += 1
    
    all_years = set()
    for bench in BENCHES:
        all_years.update(bench_years[bench].keys())
    all_years = sorted(all_years)
    
    if all_years:
        fig, ax = plt.subplots(figsize=(12, 6))
        for bench in BENCHES:
            values = [bench_years[bench].get(y, 0) for y in all_years]
            ax.plot(all_years, values, marker='o', label=f"{bench} ({BENCH_NAMES[bench]})",
                   color=BENCH_COLORS[bench], linewidth=2, markersize=5)
        
        ax.set_xlabel('Year', fontsize=12)
        ax.set_ylabel('Number of Cases', fontsize=12)
        ax.set_title('Bench Activity Over Years', fontsize=16, fontweight='bold', pad=15)
        ax.legend(fontsize=10, framealpha=0.8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_shc_bench_activity_years.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_shc_bench_activity_years.png")
        
        # --- Chart 3: Bench vs Year heatmap ---
        matrix = np.zeros((len(BENCHES), len(all_years)))
        for i, bench in enumerate(BENCHES):
            for j, year in enumerate(all_years):
                matrix[i][j] = bench_years[bench].get(year, 0)
        
        fig, ax = plt.subplots(figsize=(max(10, len(all_years)*1.2), 5))
        im = ax.imshow(matrix, aspect='auto', cmap='cool', interpolation='nearest')
        
        ax.set_yticks(range(len(BENCHES)))
        ax.set_yticklabels([f"{b} ({BENCH_NAMES[b]})" for b in BENCHES], fontsize=11)
        ax.set_xticks(range(len(all_years)))
        ax.set_xticklabels([str(y) for y in all_years], rotation=45, ha='right', fontsize=10)
        
        for i in range(len(BENCHES)):
            for j in range(len(all_years)):
                val = int(matrix[i][j])
                if val > 0:
                    color = 'white' if val > matrix.max() * 0.5 else 'black'
                    ax.text(j, i, str(val), ha='center', va='center', fontsize=9, color=color, fontweight='bold')
        
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Number of Cases', fontsize=10)
        ax.set_title('Bench vs Year Heatmap', fontsize=16, fontweight='bold', pad=15)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_shc_bench_year_heatmap.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_shc_bench_year_heatmap.png")
    
    # --- Growth trends ---
    print("\nGrowth Trends:")
    for bench in BENCHES:
        years_data = sorted(bench_years[bench].items())
        if len(years_data) >= 2:
            first_year, first_count = years_data[0]
            last_year, last_count = years_data[-1]
            if first_count > 0:
                growth = ((last_count - first_count) / first_count) * 100
                print(f"  {bench}: {first_year}({first_count}) → {last_year}({last_count}), growth: {growth:+.1f}%")
            else:
                print(f"  {bench}: {first_year}({first_count}) → {last_year}({last_count})")
    
    print(f"\n{'='*60}")
    print(f"Bench analysis complete. 3 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
