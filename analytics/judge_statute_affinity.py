"""
Judge-Statute Affinity Analysis
================================
Creates a judge-statute affinity matrix showing which judges most frequently
cite which statutes. Identifies "specialist" judges disproportionately
associated with specific statutes.
"""

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json
import os
import glob
import re
from collections import defaultdict

# --- Configuration -------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data_v2')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']
TOP_N = 25

os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_judge_name(name):
    """Clean and normalize judge names."""
    if not name or not isinstance(name, str):
        return None
    name = name.strip()
    # Remove common prefixes/suffixes
    name = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Justice|Chief Justice|Hon\'?ble?\.?)\s+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+(J\.?|CJ\.?|JJ\.?)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    if len(name) < 3:
        return None
    return name


def normalize_statute(statute):
    """Clean statute names."""
    if not statute or not isinstance(statute, str):
        return None
    statute = statute.strip()
    if len(statute) < 3:
        return None
    return statute


def load_all_cases():
    """Load all case JSON files from data_v2."""
    judge_statute_pairs = []
    judge_counts = defaultdict(int)
    statute_counts = defaultdict(int)
    total_cases = 0
    cases_with_both = 0

    for reporter in REPORTERS:
        reporter_dir = os.path.join(BASE_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue

        pattern = os.path.join(reporter_dir, '*', '*.json')
        files = glob.glob(pattern)

        for fpath in files:
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    case = json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError, IOError):
                continue

            total_cases += 1
            judges = case.get('judges', []) or []
            statutes = case.get('statutes_cited', []) or []

            if not isinstance(judges, list):
                judges = [judges] if judges else []
            if not isinstance(statutes, list):
                statutes = [statutes] if statutes else []

            # Normalize
            clean_judges = []
            for j in judges:
                nj = normalize_judge_name(j)
                if nj:
                    clean_judges.append(nj)

            clean_statutes = []
            for s in statutes:
                ns = normalize_statute(s)
                if ns:
                    clean_statutes.append(ns)

            if clean_judges and clean_statutes:
                cases_with_both += 1

            for j in clean_judges:
                judge_counts[j] += 1

            for s in clean_statutes:
                statute_counts[s] += 1

            # Create pairs
            for j in clean_judges:
                for s in clean_statutes:
                    judge_statute_pairs.append((j, s))

    print(f"Loaded {total_cases:,} total cases")
    print(f"Cases with both judges AND statutes: {cases_with_both:,}")
    print(f"Unique judges: {len(judge_counts):,}")
    print(f"Unique statutes: {len(statute_counts):,}")
    print(f"Judge-statute pairs: {len(judge_statute_pairs):,}")
    print()

    return judge_statute_pairs, judge_counts, statute_counts


def build_affinity_matrix(pairs, judge_counts, statute_counts, top_n=TOP_N):
    """Build the affinity matrix for top judges x top statutes."""
    # Get top judges and statutes by frequency
    top_judges = sorted(judge_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_statutes = sorted(statute_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]

    top_judge_names = [j[0] for j in top_judges]
    top_statute_names = [s[0] for s in top_statutes]

    # Build matrix
    matrix = defaultdict(lambda: defaultdict(int))
    for judge, statute in pairs:
        if judge in top_judge_names and statute in top_statute_names:
            matrix[judge][statute] += 1

    # Convert to DataFrame
    data = []
    for j in top_judge_names:
        row = [matrix[j][s] for s in top_statute_names]
        data.append(row)

    # Truncate long names for display
    display_judges = [j[:30] + '...' if len(j) > 30 else j for j in top_judge_names]
    display_statutes = [s[:35] + '...' if len(s) > 35 else s for s in top_statute_names]

    df = pd.DataFrame(data, index=display_judges, columns=display_statutes)
    return df, top_judge_names, top_statute_names


def find_specialists(pairs, judge_counts, statute_counts, top_n=10):
    """
    Find judges who are disproportionately associated with specific statutes.
    Uses a lift/affinity metric: P(statute|judge) / P(statute|all).
    """
    # Build per-judge statute counts
    judge_statute_count = defaultdict(lambda: defaultdict(int))
    judge_total = defaultdict(int)
    total_pairs = len(pairs)

    if total_pairs == 0:
        return []

    for judge, statute in pairs:
        judge_statute_count[judge][statute] += 1
        judge_total[judge] += 1

    # Global statute frequency
    statute_freq = defaultdict(int)
    for _, statute in pairs:
        statute_freq[statute] += 1

    # Only consider judges with enough data (at least 20 pairs)
    specialists = []
    for judge, statutes in judge_statute_count.items():
        if judge_total[judge] < 20:
            continue
        for statute, count in statutes.items():
            if count < 5:
                continue
            # Lift = P(statute|judge) / P(statute|all)
            p_given_judge = count / judge_total[judge]
            p_global = statute_freq[statute] / total_pairs
            if p_global > 0:
                lift = p_given_judge / p_global
                specialists.append({
                    'judge': judge,
                    'statute': statute,
                    'count': count,
                    'judge_total_pairs': judge_total[judge],
                    'pct_of_judge': round(p_given_judge * 100, 1),
                    'lift': round(lift, 2)
                })

    # Sort by lift (disproportionate association)
    specialists.sort(key=lambda x: x['lift'], reverse=True)
    return specialists[:top_n]


def main():
    print("=" * 70)
    print("JUDGE-STATUTE AFFINITY ANALYSIS")
    print("=" * 70)
    print()

    # Load data
    pairs, judge_counts, statute_counts = load_all_cases()

    if not pairs:
        print("ERROR: No judge-statute pairs found. Check data directory.")
        return

    # Build affinity matrix
    df, top_judges, top_statutes = build_affinity_matrix(pairs, judge_counts, statute_counts)

    # --- Heatmap ---------------------------------------------------------
    fig, ax = plt.subplots(figsize=(22, 14))
    sns.heatmap(
        df,
        annot=True,
        fmt='d',
        cmap='YlOrRd',
        linewidths=0.5,
        linecolor='white',
        ax=ax,
        cbar_kws={'label': 'Citation Count'}
    )
    ax.set_title('Judge-Statute Affinity Matrix (Top 25 x Top 25)', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Statute', fontsize=12)
    ax.set_ylabel('Judge', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=8)
    plt.yticks(fontsize=9)
    plt.tight_layout()

    outpath = os.path.join(OUTPUT_DIR, 'judge_statute_heatmap.png')
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[OK] Heatmap saved -> {outpath}")
    print()

    # --- Top judge's most cited statute ----------------------------------
    print("-" * 70)
    print("MOST COMMONLY CITED STATUTE PER TOP JUDGE")
    print("-" * 70)

    # Build per-judge top statute
    judge_statute_detail = defaultdict(lambda: defaultdict(int))
    for judge, statute in pairs:
        judge_statute_detail[judge][statute] += 1

    top_judges_sorted = sorted(judge_counts.items(), key=lambda x: x[1], reverse=True)[:TOP_N]

    for judge_name, case_count in top_judges_sorted:
        if judge_name in judge_statute_detail:
            top_stat = max(judge_statute_detail[judge_name].items(), key=lambda x: x[1])
            print(f"  {judge_name:<40s} ({case_count:>5,} cases) -> {top_stat[0]} ({top_stat[1]:,}x)")
        else:
            print(f"  {judge_name:<40s} ({case_count:>5,} cases) -> [no statutes]")

    print()

    # --- Specialist judges -----------------------------------------------
    print("-" * 70)
    print("TOP 10 'SPECIALIST' JUDGES (disproportionate statute affinity)")
    print("-" * 70)
    print(f"  {'Judge':<35s} {'Statute':<35s} {'Count':>6s} {'% of Judge':>10s} {'Lift':>6s}")
    print("  " + "-" * 95)

    specialists = find_specialists(pairs, judge_counts, statute_counts)
    for sp in specialists:
        jname = sp['judge'][:34]
        sname = sp['statute'][:34]
        print(f"  {jname:<35s} {sname:<35s} {sp['count']:>6,} {sp['pct_of_judge']:>9.1f}% {sp['lift']:>6.1f}x")

    print()
    print("Lift = how much more likely a judge cites this statute vs. the average judge.")
    print("Higher lift = stronger specialization signal.")
    print()
    print("=" * 70)
    print("Done.")


if __name__ == '__main__':
    main()
