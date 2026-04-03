"""
Judgment Text Analytics
Analyzes judgment lengths, distributions, and trends over time.
"""
import os, json, re
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

def extract_year(case):
    for field in ['judgment_date', 'order_date', 'upload_date', 'created_at']:
        val = case.get(field)
        if val and isinstance(val, str) and len(val) >= 4:
            try:
                y = int(val[:4])
                if 1970 <= y <= 2030:
                    return y
            except:
                pass
    y = case.get('year') or case.get('citation_year')
    if y:
        try:
            return int(y)
        except:
            pass
    cit = case.get('citation', '')
    if cit and isinstance(cit, str):
        m = re.search(r'(19|20)\d{2}', cit)
        if m:
            return int(m.group())
    return None

def get_judgment_text(case):
    """Get judgment text from various fields."""
    for field in ['judgment_text', 'full_text', 'judgment_raw']:
        text = case.get(field)
        if text and isinstance(text, str) and len(text.strip()) > 50:
            return text.strip()
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
    print("JUDGMENT TEXT ANALYTICS")
    print("=" * 60)
    
    cases = load_all_cases()
    
    # Collect judgment lengths
    court_lengths = defaultdict(list)  # court -> list of (year, word_count, char_count, title)
    
    for court in COURTS:
        for case in cases[court]:
            text = get_judgment_text(case)
            if text:
                word_count = len(text.split())
                char_count = len(text)
                year = extract_year(case)
                title = case.get('case_title', '') or case.get('title', '') or case.get('case_number', '') or 'Unknown'
                court_lengths[court].append({
                    'year': year,
                    'words': word_count,
                    'chars': char_count,
                    'title': title[:80]
                })
    
    # Summary stats
    print("\nJudgment Text Statistics:")
    for court in COURTS:
        data = court_lengths[court]
        total = len(cases[court])
        with_text = len(data)
        pct = with_text / total * 100 if total > 0 else 0
        if data:
            words = [d['words'] for d in data]
            avg = np.mean(words)
            median = np.median(words)
            print(f"  {court}: {with_text:,}/{total:,} have text ({pct:.1f}%), "
                  f"avg={avg:,.0f} words, median={median:,.0f} words")
        else:
            print(f"  {court}: {with_text}/{total} have text ({pct:.1f}%)")
    
    # --- Chart 1: Average judgment length per court (bar chart) ---
    fig, ax = plt.subplots(figsize=(10, 6))
    avg_lengths = []
    valid_courts = []
    valid_colors = []
    
    for court in COURTS:
        data = court_lengths[court]
        if data:
            avg = np.mean([d['words'] for d in data])
            avg_lengths.append(avg)
            valid_courts.append(court)
            valid_colors.append(COLORS[court])
    
    if valid_courts:
        bars = ax.bar(valid_courts, avg_lengths, color=valid_colors, edgecolor='white', linewidth=0.5, width=0.6)
        for bar, avg in zip(bars, avg_lengths):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                    f'{avg:,.0f}', ha='center', va='bottom', fontsize=11, fontweight='bold', color='white')
        
        ax.set_ylabel('Average Word Count', fontsize=12)
        ax.set_title('Average Judgment Length by Court', fontsize=16, fontweight='bold', pad=15)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_ylim(0, max(avg_lengths) * 1.15)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_avg_judgment_length.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("\n+ Saved: court_avg_judgment_length.png")
    
    # --- Chart 2: Judgment length distribution (histogram) ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    all_word_counts = {}
    for court in COURTS:
        data = court_lengths[court]
        if data:
            words = [d['words'] for d in data]
            all_word_counts[court] = words
    
    if all_word_counts:
        # Use log-scaled bins for better distribution view
        max_words = max(max(w) for w in all_word_counts.values())
        bins = np.logspace(np.log10(50), np.log10(min(max_words, 100000)), 40)
        
        for court in COURTS:
            if court in all_word_counts:
                ax.hist(all_word_counts[court], bins=bins, alpha=0.6, label=court,
                       color=COLORS[court], edgecolor='white', linewidth=0.3)
        
        ax.set_xscale('log')
        ax.set_xlabel('Word Count (log scale)', fontsize=12)
        ax.set_ylabel('Number of Judgments', fontsize=12)
        ax.set_title('Judgment Length Distribution', fontsize=16, fontweight='bold', pad=15)
        ax.legend(fontsize=10, framealpha=0.8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_judgment_length_distribution.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_judgment_length_distribution.png")
    
    # --- Shortest and longest judgments ---
    print("\nShortest and Longest Judgments:")
    for court in COURTS:
        data = court_lengths[court]
        if data:
            shortest = min(data, key=lambda d: d['words'])
            longest = max(data, key=lambda d: d['words'])
            print(f"  {court}:")
            print(f"    Shortest: {shortest['words']:,} words — {shortest['title']}")
            print(f"    Longest:  {longest['words']:,} words — {longest['title']}")
    
    # --- Chart 3: Year-over-year trend in judgment length ---
    fig, ax = plt.subplots(figsize=(14, 7))
    
    for court in COURTS:
        data = court_lengths[court]
        if not data:
            continue
        year_words = defaultdict(list)
        for d in data:
            if d['year']:
                year_words[d['year']].append(d['words'])
        
        if len(year_words) < 2:
            continue
        
        years = sorted(year_words.keys())
        avg_by_year = [np.mean(year_words[y]) for y in years]
        
        ax.plot(years, avg_by_year, marker='o', label=court, color=COLORS[court],
               linewidth=2, markersize=5, alpha=0.85)
    
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Average Word Count', fontsize=12)
    ax.set_title('Average Judgment Length Over Time', fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT, 'court_judgment_length_trend.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("\n+ Saved: court_judgment_length_trend.png")
    
    # --- Chart 4: Box plot comparison ---
    fig, ax = plt.subplots(figsize=(10, 6))
    box_data = []
    box_labels = []
    box_colors = []
    
    for court in COURTS:
        if court in all_word_counts and all_word_counts[court]:
            box_data.append(all_word_counts[court])
            box_labels.append(court)
            box_colors.append(COLORS[court])
    
    if box_data:
        bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True, showfliers=False,
                       medianprops=dict(color='white', linewidth=2))
        for patch, color in zip(bp['boxes'], box_colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax.set_ylabel('Word Count', fontsize=12)
        ax.set_title('Judgment Length Distribution (Box Plot)', fontsize=16, fontweight='bold', pad=15)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'court_judgment_length_boxplot.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("+ Saved: court_judgment_length_boxplot.png")
    
    print(f"\n{'='*60}")
    print(f"Judgment analysis complete. 4 charts generated.")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
