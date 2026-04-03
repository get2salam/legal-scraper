"""
Court Cases Overview Dashboard
Generates: total cases per court, cases per year, court-year heatmap, PDF success rate
"""
import os, json, glob
from collections import defaultdict
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

def load_all_cases():
    """Load all JSON case files, return list of (court, data) tuples."""
    cases = defaultdict(list)
    for court in COURTS:
        court_path = os.path.join(BASE, court)
        if not os.path.exists(court_path):
            continue
        for root, dirs, files in os.walk(court_path):
            # Skip 'original' subdirectories
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

def extract_year(case, court):
    """Extract year from case data, trying multiple fields."""
    for field in ['judgment_date', 'order_date', 'upload_date', 'created_at']:
        val = case.get(field)
        if val and isinstance(val, str) and len(val) >= 4:
            try:
                y = int(val[:4])
                if 1970 <= y <= 2030:
                    return y
            except:
                pass
    # Try year field directly
    y = case.get('year') or case.get('citation_year')
    if y:
        try:
            return int(y)
        except:
            pass
    # Try extracting from citation
    cit = case.get('citation', '')
    if cit and isinstance(cit, str):
        import re
        m = re.search(r'(19|20)\d{2}', cit)
        if m:
            return int(m.group())
    # Try extracting from file path / case_number
    cn = case.get('case_number', '')
    if cn:
        import re
        m = re.search(r'(19|20)\d{2}', cn)
        if m:
            return int(m.group())
    return None

def count_pdfs(court):
    """Count PDF files for a court (including 'original' subdirectories)."""
    court_path = os.path.join(BASE, court)
    if not os.path.exists(court_path):
        return 0
    count = 0
    for root, dirs, files in os.walk(court_path):
        for f in files:
            if f.lower().endswith('.pdf'):
                count += 1
    return count

def has_pdf_info(case):
    """Check if case has PDF URL or local PDF."""
    return bool(case.get('pdf_url') or case.get('pdf_filename') or case.get('pdf_local_path'))

def main():
    print("=" * 60)
    print("COURT CASES OVERVIEW DASHBOARD")
    print("=" * 60)
    
    cases = load_all_cases()
    
    # --- Chart 1: Total cases per court (horizontal bar) ---
    court_counts = {c: len(cases[c]) for c in COURTS}
    total = sum(court_counts.values())
    print(f"\nTotal cases across all courts: {total:,}")
    for c in COURTS:
        print(f"  {c}: {court_counts[c]:,} cases")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    courts_sorted = sorted(COURTS, key=lambda c: court_counts[c])
    counts_sorted = [court_counts[c] for c in courts_sorted]
    colors_sorted = [COLORS[c] for c in courts_sorted]
    
    bars = ax.barh(courts_sorted, counts_sorted, color=colors_sorted, edgecolor='white', linewidth=0.5, height=0.6)
    for bar, count in zip(bars, counts_sorted):
        ax.text(bar.get_width() + 15, bar.get_y() + bar.get_height()/2, 
                f'{count:,}', va='center', fontsize=12, fontweight='bold', color='white')
    
    ax.set_xlabel('Number of Cases', fontsize=12)
    ax.set_title('Total Cases by Court', fontsize=16, fontweight='bold', pad=15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(axis='y', labelsize=12)
    ax.set_xlim(0, max(counts_sorted) * 1.15)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_total_cases_by_court.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_total_cases_by_court.png")
    
    # --- Chart 2: Cases per year across all courts (stacked area) ---
    year_court = defaultdict(lambda: defaultdict(int))
    for court in COURTS:
        for case in cases[court]:
            year = extract_year(case, court)
            if year:
                year_court[year][court] += 1
    
    if year_court:
        all_years = sorted(year_court.keys())
        # Filter to years with reasonable data
        all_years = [y for y in all_years if y >= 1979]
        
        fig, ax = plt.subplots(figsize=(14, 7))
        bottom = np.zeros(len(all_years))
        
        for court in COURTS:
            values = [year_court[y].get(court, 0) for y in all_years]
            ax.fill_between(all_years, bottom, bottom + np.array(values), 
                          alpha=0.7, label=court, color=COLORS[court])
            bottom += np.array(values)
        
        ax.set_xlabel('Year', fontsize=12)
        ax.set_ylabel('Number of Cases', fontsize=12)
        ax.set_title('Cases Per Year Across All Courts', fontsize=16, fontweight='bold', pad=15)
        ax.legend(loc='upper left', fontsize=10, framealpha=0.8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_cases_per_year_stacked.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_cases_per_year_stacked.png")
    
    # --- Chart 3: Court vs Year heatmap ---
    if year_court:
        # Focus on years with substantial data
        year_range = [y for y in all_years if any(year_court[y].get(c, 0) > 0 for c in COURTS)]
        
        matrix = np.zeros((len(COURTS), len(year_range)))
        for i, court in enumerate(COURTS):
            for j, year in enumerate(year_range):
                matrix[i][j] = year_court[year].get(court, 0)
        
        fig, ax = plt.subplots(figsize=(max(14, len(year_range)*0.5), 6))
        im = ax.imshow(matrix, aspect='auto', cmap='YlOrRd', interpolation='nearest')
        
        ax.set_yticks(range(len(COURTS)))
        ax.set_yticklabels(COURTS, fontsize=11)
        
        # Show every Nth year label to avoid clutter
        step = max(1, len(year_range) // 20)
        ax.set_xticks(range(0, len(year_range), step))
        ax.set_xticklabels([str(year_range[i]) for i in range(0, len(year_range), step)], 
                          rotation=45, ha='right', fontsize=9)
        
        # Add text annotations for non-zero cells
        for i in range(len(COURTS)):
            for j in range(len(year_range)):
                val = int(matrix[i][j])
                if val > 0:
                    color = 'white' if val > matrix.max() * 0.5 else 'black'
                    ax.text(j, i, str(val), ha='center', va='center', 
                           fontsize=max(5, min(8, 200//len(year_range))), color=color)
        
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Number of Cases', fontsize=10)
        ax.set_title('Court vs Year Heatmap', fontsize=16, fontweight='bold', pad=15)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_year_heatmap.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_year_heatmap.png")
    
    # --- Chart 4: PDF download success rate ---
    pdf_stats = {}
    for court in COURTS:
        total_cases = len(cases[court])
        actual_pdfs = count_pdfs(court)
        cases_with_pdf_url = sum(1 for c in cases[court] if has_pdf_info(c))
        pdf_stats[court] = {
            'total': total_cases,
            'with_url': cases_with_pdf_url,
            'actual_pdfs': actual_pdfs,
            'rate': (actual_pdfs / total_cases * 100) if total_cases > 0 else 0
        }
    
    print(f"\nPDF Download Success Rates:")
    for c in COURTS:
        s = pdf_stats[c]
        print(f"  {c}: {s['actual_pdfs']:,} PDFs / {s['total']:,} cases = {s['rate']:.1f}%")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    courts_for_pdf = COURTS
    rates = [pdf_stats[c]['rate'] for c in courts_for_pdf]
    bars = ax.bar(courts_for_pdf, rates, color=[COLORS[c] for c in courts_for_pdf], 
                  edgecolor='white', linewidth=0.5, width=0.6)
    
    for bar, rate, court in zip(bars, rates, courts_for_pdf):
        s = pdf_stats[court]
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{rate:.1f}%\n({s["actual_pdfs"]:,}/{s["total"]:,})', 
                ha='center', va='bottom', fontsize=9, fontweight='bold', color='white')
    
    ax.set_ylabel('PDF Success Rate (%)', fontsize=12)
    ax.set_title('PDF Download Success Rate by Court', fontsize=16, fontweight='bold', pad=15)
    ax.set_ylim(0, 115)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.axhline(y=100, color='gray', linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_pdf_success_rate.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_pdf_success_rate.png")
    
    print(f"\n{'='*60}")
    print(f"Overview complete. 4 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
