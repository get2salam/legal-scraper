"""
Data Quality Metrics
Field completeness, PDF extraction, duplicate detection, data freshness.
"""
import os, json, re, hashlib
from collections import defaultdict, Counter
from datetime import datetime
import matplotlib.pyplot as plt
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

# All possible fields across courts
ALL_FIELDS = [
    'case_number', 'case_title', 'parties', 'citation', 'judgment_date',
    'author_judge', 'judges', 'case_subject', 'judgment_text', 'pdf_url',
    'pdf_filename', 'matter', 'bench', 'case_type', 'upload_date',
    'headnote', 'discussed_laws', 'description', 'tagline'
]

DISPLAY_FIELDS = [
    'case_number', 'parties/title', 'citation', 'date', 'judge(s)',
    'subject/matter', 'judgment_text', 'pdf_url', 'headnote/tagline'
]

FIELD_MAP = {
    'case_number': ['case_number', 'case_no', 'appeal'],
    'parties/title': ['parties', 'case_title', 'title', 'appellant'],
    'citation': ['citation'],
    'date': ['judgment_date', 'order_date', 'upload_date', 'created_at'],
    'judge(s)': ['author_judge', 'judges', 'bench', 'judge_name'],
    'subject/matter': ['case_subject', 'matter', 'case_type'],
    'judgment_text': ['judgment_text', 'full_text', 'judgment_raw'],
    'pdf_url': ['pdf_url'],
    'headnote/tagline': ['headnote', 'tagline', 'description'],
}

def is_populated(val):
    """Check if a field value is meaningfully populated."""
    if val is None:
        return False
    if isinstance(val, str):
        cleaned = val.strip().lower()
        return len(cleaned) > 0 and cleaned not in ('-', 'null', 'none', 'n/a', 'citation awaited', '')
    if isinstance(val, (list, dict)):
        return len(val) > 0
    if isinstance(val, bool):
        return True
    if isinstance(val, (int, float)):
        return True
    return False

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

def count_pdfs(court):
    """Count actual PDF files downloaded (including 'original' subdirs)."""
    court_path = os.path.join(BASE, court)
    if not os.path.exists(court_path):
        return 0
    count = 0
    for root, dirs, files in os.walk(court_path):
        for f in files:
            if f.lower().endswith('.pdf'):
                count += 1
    return count

def detect_duplicates(cases_list):
    """Detect potential duplicates based on case_number + parties."""
    seen = defaultdict(list)
    for i, case in enumerate(cases_list):
        # Create a fingerprint
        cn = (case.get('case_number', '') or case.get('case_no', '') or case.get('appeal', '') or '').strip().lower()
        cn = re.sub(r'\s+', '', cn)
        if cn:
            seen[cn].append(i)
    
    duplicates = {k: v for k, v in seen.items() if len(v) > 1}
    return duplicates

def get_date_range(cases_list):
    """Get newest and oldest case dates."""
    dates = []
    for case in cases_list:
        for field in ['judgment_date', 'order_date', 'upload_date', 'created_at']:
            val = case.get(field)
            if val and isinstance(val, str) and len(val) >= 10:
                try:
                    dt = datetime.strptime(val[:10], '%Y-%m-%d')
                    dates.append(dt)
                    break
                except:
                    pass
        else:
            y = case.get('year') or case.get('citation_year')
            if y:
                try:
                    dates.append(datetime(int(y), 1, 1))
                except:
                    pass
    
    if dates:
        return min(dates), max(dates)
    return None, None

def main():
    print("=" * 60)
    print("DATA QUALITY METRICS")
    print("=" * 60)
    
    cases = load_all_cases()
    
    # --- Chart 1: Field completeness heatmap ---
    completeness = np.zeros((len(COURTS), len(DISPLAY_FIELDS)))
    
    for i, court in enumerate(COURTS):
        total = len(cases[court])
        if total == 0:
            continue
        for j, field_name in enumerate(DISPLAY_FIELDS):
            variants = FIELD_MAP[field_name]
            count = 0
            for case in cases[court]:
                for variant in variants:
                    if is_populated(case.get(variant)):
                        count += 1
                        break
            completeness[i][j] = count / total * 100
    
    fig, ax = plt.subplots(figsize=(14, 7))
    im = ax.imshow(completeness, aspect='auto', cmap='RdYlGn', vmin=0, vmax=100, interpolation='nearest')
    
    ax.set_yticks(range(len(COURTS)))
    ax.set_yticklabels(COURTS, fontsize=12)
    ax.set_xticks(range(len(DISPLAY_FIELDS)))
    ax.set_xticklabels(DISPLAY_FIELDS, fontsize=9, rotation=35, ha='right')
    
    for i in range(len(COURTS)):
        for j in range(len(DISPLAY_FIELDS)):
            val = completeness[i][j]
            color = 'white' if val < 50 else 'black'
            ax.text(j, i, f'{val:.0f}%', ha='center', va='center', fontsize=10,
                   color=color, fontweight='bold')
    
    cbar = plt.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label('Field Completeness (%)', fontsize=10)
    ax.set_title('Field Completeness Heatmap by Court', fontsize=16, fontweight='bold', pad=15)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_field_completeness.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_field_completeness.png")
    
    # Print field completeness details
    print("\nField Completeness Summary:")
    for i, court in enumerate(COURTS):
        avg = np.mean(completeness[i])
        print(f"  {court}: avg {avg:.1f}% complete")
        low_fields = [(DISPLAY_FIELDS[j], completeness[i][j]) for j in range(len(DISPLAY_FIELDS)) if completeness[i][j] < 50]
        if low_fields:
            for fname, pct in low_fields:
                print(f"    ! {fname}: {pct:.0f}%")
    
    # --- Chart 2: PDF extraction success rate ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    pdf_data = []
    for court in COURTS:
        total = len(cases[court])
        has_url = sum(1 for c in cases[court] if is_populated(c.get('pdf_url')))
        actual_pdfs = count_pdfs(court)
        has_text = sum(1 for c in cases[court] 
                      for f in ['judgment_text', 'full_text', 'judgment_raw']
                      if is_populated(c.get(f)))
        # Deduplicate — count cases with any text
        cases_with_text = 0
        for c in cases[court]:
            for f in ['judgment_text', 'full_text', 'judgment_raw']:
                if c.get(f) and isinstance(c.get(f), str) and len(c.get(f, '').strip()) > 50:
                    cases_with_text += 1
                    break
        
        pdf_data.append({
            'court': court,
            'total': total,
            'has_url': has_url,
            'actual_pdfs': actual_pdfs,
            'has_text': cases_with_text,
        })
    
    x = np.arange(len(COURTS))
    width = 0.2
    
    ax.bar(x - width*1.5, [d['total'] for d in pdf_data], width, label='Total Cases', color='#6b7280', edgecolor='white', linewidth=0.5)
    ax.bar(x - width*0.5, [d['has_url'] for d in pdf_data], width, label='Has PDF URL', color='#3b82f6', edgecolor='white', linewidth=0.5)
    ax.bar(x + width*0.5, [d['actual_pdfs'] for d in pdf_data], width, label='PDF Downloaded', color='#10b981', edgecolor='white', linewidth=0.5)
    ax.bar(x + width*1.5, [d['has_text'] for d in pdf_data], width, label='Has Judgment Text', color='#f59e0b', edgecolor='white', linewidth=0.5)
    
    ax.set_xticks(x)
    ax.set_xticklabels(COURTS, fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_title('PDF Extraction Pipeline by Court', fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_pdf_extraction_pipeline.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_pdf_extraction_pipeline.png")
    
    print("\nPDF Pipeline:")
    for d in pdf_data:
        c = d['court']
        print(f"  {c}: {d['total']} total → {d['has_url']} w/URL → {d['actual_pdfs']} downloaded → {d['has_text']} w/text")
    
    # --- Chart 3: Duplicate detection ---
    print("\nDuplicate Detection:")
    dup_counts = {}
    for court in COURTS:
        dups = detect_duplicates(cases[court])
        dup_count = sum(len(v) - 1 for v in dups.values())  # extra copies
        dup_counts[court] = dup_count
        total = len(cases[court])
        print(f"  {court}: {dup_count} potential duplicates ({len(dups)} groups) out of {total} cases")
        if dups and len(dups) <= 5:
            for cn, indices in list(dups.items())[:3]:
                print(f"    → '{cn}' appears {len(indices)} times")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    dup_vals = [dup_counts[c] for c in COURTS]
    clean_vals = [len(cases[c]) - dup_counts[c] for c in COURTS]
    
    bars1 = ax.bar(COURTS, clean_vals, color=[COLORS[c] for c in COURTS], 
                   edgecolor='white', linewidth=0.5, width=0.6, label='Unique')
    bars2 = ax.bar(COURTS, dup_vals, bottom=clean_vals, color='#ef4444', 
                   edgecolor='white', linewidth=0.5, width=0.6, alpha=0.7, label='Potential Duplicates')
    
    for court, bar, dups, total in zip(COURTS, bars1, dup_vals, [len(cases[c]) for c in COURTS]):
        if dups > 0:
            pct = dups / total * 100
            ax.text(bar.get_x() + bar.get_width()/2, total + 10,
                    f'{dups} ({pct:.1f}%)', ha='center', va='bottom', fontsize=9, color='#ef4444')
    
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('Duplicate Detection Results by Court', fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_duplicate_detection.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_duplicate_detection.png")
    
    # --- Chart 4: Data freshness ---
    print("\nData Freshness (Oldest → Newest per court):")
    freshness_data = []
    for court in COURTS:
        oldest, newest = get_date_range(cases[court])
        if oldest and newest:
            span_years = (newest - oldest).days / 365.25
            freshness_data.append({
                'court': court,
                'oldest': oldest,
                'newest': newest,
                'span': span_years
            })
            print(f"  {court}: {oldest.strftime('%Y-%m-%d')} → {newest.strftime('%Y-%m-%d')} ({span_years:.1f} years)")
        else:
            print(f"  {court}: No date data available")
    
    if freshness_data:
        fig, ax = plt.subplots(figsize=(12, 6))
        
        for i, d in enumerate(freshness_data):
            ax.barh(d['court'], d['span'], left=d['oldest'].year, 
                   color=COLORS[d['court']], edgecolor='white', linewidth=0.5, height=0.5)
            ax.text(d['oldest'].year - 0.5, i, d['oldest'].strftime('%Y'), 
                   ha='right', va='center', fontsize=9, color='white')
            ax.text(d['newest'].year + 0.5, i, d['newest'].strftime('%Y-%m'), 
                   ha='left', va='center', fontsize=9, color='white')
        
        ax.set_xlabel('Year', fontsize=12)
        ax.set_title('Data Coverage Timeline by Court', fontsize=16, fontweight='bold', pad=15)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_data_freshness.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("\n+ Saved: court_data_freshness.png")
    
    # --- Overall Quality Score ---
    print("\n" + "=" * 40)
    print("OVERALL DATA QUALITY SCORES")
    print("=" * 40)
    for i, court in enumerate(COURTS):
        avg_completeness = np.mean(completeness[i])
        total = len(cases[court])
        actual_pdfs = count_pdfs(court)
        pdf_rate = actual_pdfs / total * 100 if total > 0 else 0
        dup_rate = dup_counts[court] / total * 100 if total > 0 else 0
        quality = (avg_completeness * 0.5 + pdf_rate * 0.3 + (100 - dup_rate) * 0.2)
        print(f"  {court}: Quality Score = {quality:.1f}/100 "
              f"(completeness={avg_completeness:.0f}%, pdf={pdf_rate:.0f}%, dups={dup_rate:.1f}%)")
    
    print(f"\n{'='*60}")
    print(f"Data quality analysis complete. 4 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
