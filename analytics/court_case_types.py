"""
Case Type Distribution Analysis
Extracts case types from case numbers/fields and analyzes distribution per court.
"""
import os, json, re
from collections import defaultdict, Counter
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

# Patterns to classify case types from case_number field
CASE_TYPE_PATTERNS = [
    (r'(?i)crl[\._\s]*a', 'Criminal Appeal'),
    (r'(?i)criminal\s*appeal', 'Criminal Appeal'),
    (r'(?i)crl[\._\s]*p', 'Criminal Petition'),
    (r'(?i)criminal\s*petition', 'Criminal Petition'),
    (r'(?i)crl[\._\s]*m', 'Criminal Misc'),
    (r'(?i)criminal\s*misc', 'Criminal Misc'),
    (r'(?i)crl[\._\s]*r', 'Criminal Revision'),
    (r'(?i)criminal\s*revision', 'Criminal Revision'),
    (r'(?i)c[\._\s]*a|civil[\._\s]*appeal', 'Civil Appeal'),
    (r'(?i)c[\._\s]*p|civil[\._\s]*petition', 'Civil Petition'),
    (r'(?i)c[\._\s]*r(?!l)|civil[\._\s]*revision', 'Civil Revision'),
    (r'(?i)c[\._\s]*m(?!a)|civil[\._\s]*misc', 'Civil Misc'),
    (r'(?i)const[\._\s]*p|constitutional\s*petition|w[\._\s]*p|writ\s*petition', 'Constitutional/Writ Petition'),
    (r'(?i)h[\._\s]*r[\._\s]*c|human\s*rights', 'Human Rights Case'),
    (r'(?i)s[\._\s]*m[\._\s]*c|suo\s*motu', 'Suo Motu Case'),
    (r'(?i)shariat[\._\s]*p|s[\._\s]*p', 'Shariat Petition'),
    (r'(?i)appeal', 'Appeal (General)'),
    (r'(?i)review', 'Review Petition'),
    (r'(?i)family', 'Family'),
    (r'(?i)tax|ptd|customs', 'Tax/Revenue'),
    (r'(?i)labour|labor', 'Labour'),
]

CRIMINAL_TYPES = {'Criminal Appeal', 'Criminal Petition', 'Criminal Misc', 'Criminal Revision'}
CIVIL_TYPES = {'Civil Appeal', 'Civil Petition', 'Civil Revision', 'Civil Misc', 
               'Constitutional/Writ Petition', 'Family'}

def classify_case(case, court):
    """Classify a case into a type category."""
    # First check explicit case_type field
    explicit_type = case.get('case_type', '')
    if explicit_type and isinstance(explicit_type, str):
        explicit_lower = explicit_type.lower().strip()
        if explicit_lower in ('criminal', 'crl'):
            return 'Criminal (General)'
        elif explicit_lower in ('civil',):
            return 'Civil (General)'
        elif explicit_lower in ('family',):
            return 'Family'
        elif explicit_lower in ('s.b.', 'single bench', 'sb'):
            pass  # Not a case type, it's bench composition
        elif explicit_lower in ('d.b.', 'double bench', 'db'):
            pass
        elif len(explicit_lower) > 2:
            return explicit_type.strip()
    
    # Check matter field (SHC)
    matter = case.get('matter', '')
    if matter and isinstance(matter, str):
        matter_lower = matter.lower()
        if 'criminal' in matter_lower:
            return 'Criminal (General)'
        elif 'civil' in matter_lower:
            return 'Civil (General)'
        elif 'family' in matter_lower:
            return 'Family'
        elif 'constitutional' in matter_lower or 'writ' in matter_lower:
            return 'Constitutional/Writ Petition'
        elif 'tax' in matter_lower or 'revenue' in matter_lower or 'customs' in matter_lower:
            return 'Tax/Revenue'
        elif 'labour' in matter_lower or 'labor' in matter_lower or 'service' in matter_lower:
            return 'Service/Labour'
        elif 'banking' in matter_lower:
            return 'Banking'
        elif matter.strip():
            return matter.strip().title()
    
    # Try case_number patterns
    case_num = case.get('case_number', '') or case.get('case_no', '') or case.get('appeal', '')
    if case_num and isinstance(case_num, str):
        for pattern, case_type in CASE_TYPE_PATTERNS:
            if re.search(pattern, case_num):
                return case_type
    
    # Try case_subject
    subject = case.get('case_subject', '')
    if subject and isinstance(subject, str):
        subject_lower = subject.lower()
        if 'criminal' in subject_lower or 'murder' in subject_lower or 'narcotics' in subject_lower:
            return 'Criminal (General)'
        elif 'civil' in subject_lower:
            return 'Civil (General)'
        elif 'writ' in subject_lower or 'constitutional' in subject_lower:
            return 'Constitutional/Writ Petition'
        elif 'tax' in subject_lower or 'customs' in subject_lower:
            return 'Tax/Revenue'
    
    return 'Other/Unclassified'

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
    print("CASE TYPE DISTRIBUTION ANALYSIS")
    print("=" * 60)
    
    cases = load_all_cases()
    
    # Classify all cases
    court_types = defaultdict(Counter)
    for court in COURTS:
        for case in cases[court]:
            case_type = classify_case(case, court)
            court_types[court][case_type] += 1
    
    # Print distributions
    for court in COURTS:
        print(f"\n{court} Case Type Distribution:")
        for ct, count in court_types[court].most_common(10):
            pct = count / len(cases[court]) * 100 if cases[court] else 0
            print(f"  {ct}: {count:,} ({pct:.1f}%)")
    
    # --- Chart 1: Pie charts for each court (2x3 grid) ---
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()
    
    pie_colors = ['#10b981', '#06b6d4', '#3b82f6', '#8b5cf6', '#f59e0b', '#ef4444',
                  '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16', '#a855f7',
                  '#22d3ee', '#fb923c', '#818cf8']
    
    for idx, court in enumerate(COURTS):
        ax = axes[idx]
        types = court_types[court].most_common(8)
        if not types:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center', fontsize=14, color='gray')
            ax.set_title(court, fontsize=14, fontweight='bold')
            continue
        
        # Group small categories into "Other"
        labels = []
        sizes = []
        total = sum(court_types[court].values())
        other = 0
        for ct, count in types:
            if count / total >= 0.03:  # 3% threshold
                labels.append(ct)
                sizes.append(count)
            else:
                other += count
        # Add remaining not in top 8
        remaining = total - sum(sizes) - other
        if remaining > 0:
            other += remaining
        if other > 0:
            labels.append('Other')
            sizes.append(other)
        
        wedges, texts, autotexts = ax.pie(sizes, labels=None, autopct='%1.1f%%',
                                           colors=pie_colors[:len(sizes)],
                                           pctdistance=0.75, startangle=90,
                                           textprops={'fontsize': 8, 'color': 'white'})
        
        # Truncate long labels
        short_labels = [l[:25] + '...' if len(l) > 25 else l for l in labels]
        ax.legend(short_labels, loc='center left', bbox_to_anchor=(-0.3, 0.5), fontsize=7, framealpha=0.5)
        ax.set_title(f'{court} ({total:,} cases)', fontsize=13, fontweight='bold')
    
    plt.suptitle('Case Type Distribution by Court', fontsize=18, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_case_type_distribution.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_case_type_distribution.png")
    
    # --- Chart 2: Criminal vs Civil ratio per court ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    criminal_counts = []
    civil_counts = []
    other_counts = []
    
    for court in COURTS:
        criminal = 0
        civil = 0
        other = 0
        for ct, count in court_types[court].items():
            ct_lower = ct.lower()
            if 'criminal' in ct_lower or 'crl' in ct_lower or 'murder' in ct_lower or 'narcotics' in ct_lower:
                criminal += count
            elif any(w in ct_lower for w in ['civil', 'writ', 'constitutional', 'family', 'banking', 'tax', 'revenue', 'service', 'labour']):
                civil += count
            else:
                other += count
        criminal_counts.append(criminal)
        civil_counts.append(civil)
        other_counts.append(other)
    
    x = np.arange(len(COURTS))
    width = 0.25
    
    bars1 = ax.bar(x - width, criminal_counts, width, label='Criminal', color='#ef4444', edgecolor='white', linewidth=0.5)
    bars2 = ax.bar(x, civil_counts, width, label='Civil/Constitutional', color='#3b82f6', edgecolor='white', linewidth=0.5)
    bars3 = ax.bar(x + width, other_counts, width, label='Other/Unclassified', color='#6b7280', edgecolor='white', linewidth=0.5)
    
    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 3,
                        str(int(h)), ha='center', va='bottom', fontsize=8, color='white')
    
    ax.set_xlabel('Court', fontsize=12)
    ax.set_ylabel('Number of Cases', fontsize=12)
    ax.set_title('Criminal vs Civil Case Ratio by Court', fontsize=16, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(COURTS, fontsize=12)
    ax.legend(fontsize=11, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_criminal_vs_civil.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_criminal_vs_civil.png")
    
    # --- Chart 3: Top case types across all courts (horizontal stacked bar) ---
    all_types = Counter()
    for court in COURTS:
        all_types.update(court_types[court])
    
    top_types = [ct for ct, _ in all_types.most_common(10)]
    
    fig, ax = plt.subplots(figsize=(14, 7))
    y_pos = np.arange(len(top_types))
    left = np.zeros(len(top_types))
    
    for court in COURTS:
        values = [court_types[court].get(ct, 0) for ct in top_types]
        ax.barh(y_pos, values, left=left, label=court, color=COLORS[court], 
                edgecolor='white', linewidth=0.3, height=0.6)
        left += np.array(values)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels([t[:35] for t in top_types], fontsize=10)
    ax.set_xlabel('Number of Cases', fontsize=12)
    ax.set_title('Top Case Types Across All Courts', fontsize=16, fontweight='bold', pad=15)
    ax.legend(loc='lower right', fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_top_case_types_stacked.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("+ Saved: court_top_case_types_stacked.png")
    
    # Summary
    print(f"\nOverall Criminal vs Civil split:")
    total_criminal = sum(criminal_counts)
    total_civil = sum(civil_counts)
    total_other = sum(other_counts)
    grand = total_criminal + total_civil + total_other
    print(f"  Criminal: {total_criminal:,} ({total_criminal/grand*100:.1f}%)")
    print(f"  Civil/Constitutional: {total_civil:,} ({total_civil/grand*100:.1f}%)")
    print(f"  Other/Unclassified: {total_other:,} ({total_other/grand*100:.1f}%)")
    
    print(f"\n{'='*60}")
    print(f"Case type analysis complete. 3 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
