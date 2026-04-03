"""
Court vs PLS Comparison
Analyzes overlap, unique value, citation formats, and data richness.
"""
import os, json, re
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import numpy as np

plt.style.use('dark_background')

BASE_COURT = os.path.join(os.path.dirname(__file__), '..', 'data_v2', 'court_cases')
BASE_PLS = os.path.join(os.path.dirname(__file__), '..', 'data_v2')
OUTPUT = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT, exist_ok=True)

COURTS = ['SC', 'SHC', 'IHC', 'SST', 'FSC', 'LHC']
COLORS = {
    'SC': '#10b981', 'SHC': '#06b6d4', 'IHC': '#3b82f6',
    'SST': '#8b5cf6', 'FSC': '#f59e0b', 'LHC': '#ef4444'
}

PLS_REPORTERS = ['SCMR', 'PLD', 'CLC', 'CLD', 'MLD', 'PCrLJ', 'PLC', 'PTD', 'YLR', 'GBLR']
PLS_COLORS = {
    'SCMR': '#10b981', 'PLD': '#3b82f6', 'CLC': '#06b6d4', 'CLD': '#8b5cf6',
    'MLD': '#f59e0b', 'PCrLJ': '#ef4444', 'PLC': '#ec4899', 'PTD': '#14b8a6',
    'YLR': '#f97316', 'GBLR': '#6366f1'
}

# Court-to-PLS mapping (which PLS reporters likely cover each court)
COURT_PLS_MAP = {
    'SC': ['SCMR', 'PLD'],
    'SHC': ['CLC', 'MLD', 'YLR', 'PLD'],
    'IHC': ['CLC', 'MLD', 'PLD'],
    'FSC': ['PLD', 'PCrLJ'],
    'LHC': ['CLC', 'MLD', 'PLD', 'YLR'],
    'SST': ['PLC'],
}

def count_pls_files():
    """Count JSON files per PLS reporter."""
    counts = {}
    for reporter in PLS_REPORTERS:
        path = os.path.join(BASE_PLS, reporter)
        if os.path.exists(path):
            count = 0
            for root, dirs, files in os.walk(path):
                count += sum(1 for f in files if f.endswith('.json'))
            counts[reporter] = count
        else:
            counts[reporter] = 0
    return counts

def load_court_cases():
    cases = defaultdict(list)
    for court in COURTS:
        court_path = os.path.join(BASE_COURT, court)
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

def analyze_citation_formats(cases):
    """Analyze citation patterns in court cases."""
    court_citations = defaultdict(Counter)
    for court in COURTS:
        for case in cases[court]:
            citation = case.get('citation', '')
            if not citation or not isinstance(citation, str) or citation == 'Citation Awaited':
                court_citations[court]['No Citation'] += 1
                continue
            
            # Detect PLS-style citations
            pls_found = False
            for reporter in PLS_REPORTERS:
                if reporter in citation.upper() or reporter in citation:
                    court_citations[court][f'PLS ({reporter})'] += 1
                    pls_found = True
                    break
            
            if not pls_found:
                # Court's own citation format
                if re.search(r'\d{4}\s*SHC', citation):
                    court_citations[court]['SHC Format'] += 1
                elif re.search(r'\d{4}\s*SCP', citation):
                    court_citations[court]['SCP Format'] += 1
                elif re.search(r'\d{4}\s*LHC', citation):
                    court_citations[court]['LHC Format'] += 1
                elif re.search(r'\d{4}\s*IHC', citation):
                    court_citations[court]['IHC Format'] += 1
                else:
                    court_citations[court]['Other Format'] += 1
    
    return court_citations

def check_overlap(cases):
    """Estimate cases that likely overlap with PLS."""
    overlap = {}
    for court in COURTS:
        has_pls_citation = 0
        no_pls_citation = 0
        citation_awaited = 0
        
        for case in cases[court]:
            citation = case.get('citation', '') or ''
            citations_list = case.get('citations', []) or []
            
            all_citations = [citation] + (citations_list if isinstance(citations_list, list) else [])
            
            found_pls = False
            for cit in all_citations:
                if isinstance(cit, str):
                    for reporter in PLS_REPORTERS:
                        if reporter in cit:
                            found_pls = True
                            break
                if found_pls:
                    break
            
            if found_pls:
                has_pls_citation += 1
            elif 'awaited' in citation.lower() or not citation.strip():
                citation_awaited += 1
            else:
                no_pls_citation += 1
        
        overlap[court] = {
            'total': len(cases[court]),
            'has_pls': has_pls_citation,
            'no_pls': no_pls_citation,
            'awaited': citation_awaited
        }
    return overlap

def analyze_data_richness(cases):
    """Compare data richness across courts."""
    richness = {}
    fields_to_check = {
        'judgment_text': ['judgment_text', 'full_text', 'judgment_raw'],
        'pdf_url': ['pdf_url'],
        'parties': ['parties', 'case_title', 'title', 'appellant'],
        'judge': ['author_judge', 'judges', 'bench', 'judge_name'],
        'date': ['judgment_date', 'order_date', 'upload_date'],
        'citation': ['citation'],
        'case_subject': ['case_subject', 'matter', 'case_type'],
    }
    
    for court in COURTS:
        court_richness = {}
        total = len(cases[court])
        if total == 0:
            richness[court] = {}
            continue
        
        for field_name, field_variants in fields_to_check.items():
            count = 0
            for case in cases[court]:
                for variant in field_variants:
                    val = case.get(variant)
                    if val:
                        if isinstance(val, str) and len(val.strip()) > 0 and val.strip().lower() not in ('-', 'null', 'none', 'citation awaited'):
                            count += 1
                            break
                        elif isinstance(val, (list, dict)) and len(val) > 0:
                            count += 1
                            break
            court_richness[field_name] = count / total * 100
        
        richness[court] = court_richness
    
    return richness

def main():
    print("=" * 60)
    print("COURT vs PLS COMPARISON")
    print("=" * 60)
    
    cases = load_court_cases()
    pls_counts = count_pls_files()
    
    # --- Summary ---
    total_court = sum(len(cases[c]) for c in COURTS)
    total_pls = sum(pls_counts.values())
    print(f"\nTotal court cases: {total_court:,}")
    print(f"Total PLS cases: {total_pls:,}")
    print(f"\nPLS Reporter Counts:")
    for r in PLS_REPORTERS:
        print(f"  {r}: {pls_counts[r]:,}")
    
    # --- Chart 1: Court vs PLS size comparison ---
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Court data
    court_vals = [len(cases[c]) for c in COURTS]
    x_court = np.arange(len(COURTS))
    
    # PLS data
    pls_vals = [pls_counts[r] for r in PLS_REPORTERS]
    x_pls = np.arange(len(PLS_REPORTERS)) + len(COURTS) + 1
    
    bars1 = ax.bar(x_court, court_vals, color=[COLORS[c] for c in COURTS], 
                   edgecolor='white', linewidth=0.5, width=0.7)
    bars2 = ax.bar(x_pls, pls_vals, color=[PLS_COLORS[r] for r in PLS_REPORTERS],
                   edgecolor='white', linewidth=0.5, width=0.7)
    
    for bar, val in zip(list(bars1) + list(bars2), court_vals + pls_vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                    f'{val:,}', ha='center', va='bottom', fontsize=7, color='white', rotation=45)
    
    all_x = list(x_court) + list(x_pls)
    all_labels = COURTS + PLS_REPORTERS
    ax.set_xticks(all_x)
    ax.set_xticklabels(all_labels, fontsize=9, rotation=45, ha='right')
    
    # Divider
    ax.axvline(x=len(COURTS) + 0.0, color='gray', linestyle='--', alpha=0.5)
    ax.text(len(COURTS)/2, ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1000, 
            'Court Direct', ha='center', fontsize=11, color='#06b6d4', fontweight='bold')
    ax.text(x_pls.mean(), ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1000, 
            'PLS Reporters', ha='center', fontsize=11, color='#f59e0b', fontweight='bold')
    
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('Court Direct Data vs PLS Reporter Data', fontsize=16, fontweight='bold', pad=15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_vs_pls_size.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_vs_pls_size.png")
    
    # --- Chart 2: Overlap analysis ---
    overlap = check_overlap(cases)
    
    print("\nOverlap Analysis (Court cases with PLS citations):")
    for court in COURTS:
        o = overlap[court]
        print(f"  {court}: {o['has_pls']:,} with PLS citation, {o['no_pls']:,} court-only, "
              f"{o['awaited']:,} citation awaited (total: {o['total']:,})")
    
    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(COURTS))
    width = 0.25
    
    pls_vals = [overlap[c]['has_pls'] for c in COURTS]
    court_only = [overlap[c]['no_pls'] for c in COURTS]
    awaited = [overlap[c]['awaited'] for c in COURTS]
    
    ax.bar(x - width, pls_vals, width, label='Has PLS Citation (likely in PLS)', color='#10b981', edgecolor='white', linewidth=0.5)
    ax.bar(x, court_only, width, label='Court-Only Citation (unique value)', color='#3b82f6', edgecolor='white', linewidth=0.5)
    ax.bar(x + width, awaited, width, label='Citation Awaited/Missing', color='#6b7280', edgecolor='white', linewidth=0.5)
    
    ax.set_xticks(x)
    ax.set_xticklabels(COURTS, fontsize=12)
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('Estimated Overlap with PLS Database', fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_pls_overlap.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_pls_overlap.png")
    
    # --- Chart 3: Data richness comparison ---
    richness = analyze_data_richness(cases)
    
    fields = ['judgment_text', 'pdf_url', 'parties', 'judge', 'date', 'citation', 'case_subject']
    field_labels = ['Judgment Text', 'PDF URL', 'Parties', 'Judge', 'Date', 'Citation', 'Subject/Type']
    
    fig, ax = plt.subplots(figsize=(12, 7))
    matrix = np.zeros((len(COURTS), len(fields)))
    for i, court in enumerate(COURTS):
        for j, field in enumerate(fields):
            matrix[i][j] = richness.get(court, {}).get(field, 0)
    
    im = ax.imshow(matrix, aspect='auto', cmap='RdYlGn', vmin=0, vmax=100, interpolation='nearest')
    
    ax.set_yticks(range(len(COURTS)))
    ax.set_yticklabels(COURTS, fontsize=12)
    ax.set_xticks(range(len(fields)))
    ax.set_xticklabels(field_labels, fontsize=10, rotation=30, ha='right')
    
    for i in range(len(COURTS)):
        for j in range(len(fields)):
            val = matrix[i][j]
            color = 'white' if val < 50 else 'black'
            ax.text(j, i, f'{val:.0f}%', ha='center', va='center', fontsize=10, 
                   color=color, fontweight='bold')
    
    cbar = plt.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label('Field Completeness (%)', fontsize=10)
    ax.set_title('Data Richness Comparison Across Courts', fontsize=16, fontweight='bold', pad=15)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_data_richness.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_data_richness.png")
    
    # --- Chart 4: Citation format comparison ---
    citation_formats = analyze_citation_formats(cases)
    
    print("\nCitation Format Distribution:")
    for court in COURTS:
        print(f"  {court}:")
        for fmt, count in citation_formats[court].most_common(5):
            print(f"    {fmt}: {count:,}")
    
    # Unique value summary
    print("\n" + "=" * 40)
    print("UNIQUE VALUE OF COURT DATA:")
    print("=" * 40)
    
    total_unique = sum(overlap[c]['no_pls'] + overlap[c]['awaited'] for c in COURTS)
    print(f"\nCases likely NOT in PLS: {total_unique:,} out of {total_court:,} "
          f"({total_unique/total_court*100:.1f}%)")
    print("\nKey unique values:")
    print("  - SST cases: Tribunal data rarely in PLS reporters")
    print("  - SHC bench-level data: KHI, HYD, SUK, LAR, MIR breakdowns")
    print("  - IHC full judgments with metadata: parties, subjects, landmarks")
    print("  - Original PDFs: Direct from court websites")
    print("  - Cases awaiting citation: Recent/unreported decisions")
    
    print(f"\n{'='*60}")
    print(f"Court vs PLS comparison complete. 3 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
