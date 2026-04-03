#!/usr/bin/env python3
"""
Judge Influence Analysis
========================
Ranks judges by influence across 50,000+ Pakistani court cases.

Metrics per judge:
- Total cases authored
- Citation impact (how often their judgments are cited by others)
- Court diversity (unique courts served)
- Career span (years between first and last case)
- Domain breadth (unique statutes cited)
- Composite influence score (weighted combination)

Output:
- Top 50 most influential judges (console table)
- Judge influence bar chart (top 30) -> analytics/output/judge_influence.png
- Judge career timeline (top 20) -> analytics/output/judge_careers.png
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

# -- Helpers ------------------------------------------------------------
def extract_year(date_str):
    """Extract year from date strings like '16th March, 2022' or '30th October, 2023'."""
    if not date_str:
        return None
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else None


def normalize_judge_name(name):
    """Clean and normalize judge names for deduplication."""
    if not name:
        return None
    name = str(name).strip()
    # Remove common suffixes/prefixes
    name = re.sub(r',?\s*(C\.?J\.?|J\.?|JJ\.?|Chief Justice|Justice)\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Hon\.?\s*(Mr\.?)?|Justice)\s+', '', name, flags=re.IGNORECASE)
    name = name.strip(' .,;-')
    if len(name) < 3:
        return None
    return name


def load_all_cases():
    """Load all JSON case files from data_v2 (lightweight - only needed fields)."""
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
                    light = {k: data[k] for k in NEEDED_FIELDS if k in data}
                    light['_reporter'] = reporter
                    light['_file'] = fname
                    cases.append(light)
                except Exception:
                    errors += 1
        print(f"  {reporter}: {total_files:,} files so far...", flush=True)
    
    print(f"Loaded {len(cases):,} cases from {total_files:,} files ({errors} errors)", flush=True)
    return cases


# -- Main Analysis ------------------------------------------------------
def main():
    print("=" * 70)
    print("JUDGE INFLUENCE ANALYSIS")
    print("=" * 70)
    print()
    
    cases = load_all_cases()
    if not cases:
        print("ERROR: No cases loaded. Check data_v2 directory.")
        return
    
    # -- Build citation-to-judge mapping --
    # Map each case citation to its judges
    citation_to_judges = {}
    for case in cases:
        citation = case.get('citation', '')
        judges = case.get('judges', [])
        if citation and judges:
            citation_to_judges[citation] = judges
    
    # -- Build judge metrics --
    judge_data = {}  # judge_name -> {metrics}
    
    for case in cases:
        judges = case.get('judges', [])
        if not judges:
            continue
        
        year = extract_year(case.get('date', ''))
        court = case.get('court', '')
        citation = case.get('citation', '')
        statutes = case.get('statutes_cited', [])
        reporter = case.get('_reporter', '')
        
        for judge_raw in judges:
            judge = normalize_judge_name(judge_raw)
            if not judge:
                continue
            
            if judge not in judge_data:
                judge_data[judge] = {
                    'cases': 0,
                    'citations': [],  # case citations this judge authored
                    'courts': set(),
                    'years': [],
                    'statutes': set(),
                    'reporters': set(),
                    'citation_impact': 0,
                }
            
            jd = judge_data[judge]
            jd['cases'] += 1
            if citation:
                jd['citations'].append(citation)
            if court:
                jd['courts'].add(court)
            if year:
                jd['years'].append(year)
            if statutes:
                for s in statutes:
                    if s:
                        jd['statutes'].add(s)
            if reporter:
                jd['reporters'].add(reporter)
    
    # -- Compute citation impact --
    # For each case, see which judges' earlier cases appear in cases_cited
    judge_citation_set = {}
    for judge, jd in judge_data.items():
        judge_citation_set[judge] = set(jd['citations'])
    
    for case in cases:
        cases_cited = case.get('cases_cited', [])
        if not cases_cited:
            continue
        for cited in cases_cited:
            if not cited:
                continue
            # Check if any judge authored this cited case
            cited_judges = citation_to_judges.get(cited, [])
            for judge_raw in cited_judges:
                judge = normalize_judge_name(judge_raw)
                if judge and judge in judge_data:
                    judge_data[judge]['citation_impact'] += 1
    
    # -- Build DataFrame --
    rows = []
    for judge, jd in judge_data.items():
        years = jd['years']
        if years:
            min_year = min(years)
            max_year = max(years)
            career_span = max_year - min_year + 1
        else:
            min_year = max_year = career_span = 0
        
        rows.append({
            'judge': judge,
            'total_cases': jd['cases'],
            'citation_impact': jd['citation_impact'],
            'court_diversity': len(jd['courts']),
            'career_span': career_span,
            'domain_breadth': len(jd['statutes']),
            'min_year': min_year,
            'max_year': max_year,
            'reporters': len(jd['reporters']),
        })
    
    df = pd.DataFrame(rows)
    
    if df.empty:
        print("No judge data found.")
        return
    
    # -- Compute composite influence score --
    # Normalize each metric to 0-1 range and combine with weights
    metrics = ['total_cases', 'citation_impact', 'court_diversity', 'career_span', 'domain_breadth']
    weights = {'total_cases': 0.25, 'citation_impact': 0.35, 'court_diversity': 0.10, 
               'career_span': 0.10, 'domain_breadth': 0.20}
    
    for m in metrics:
        col_max = df[m].max()
        if col_max > 0:
            df[f'{m}_norm'] = df[m] / col_max
        else:
            df[f'{m}_norm'] = 0.0
    
    df['influence_score'] = sum(df[f'{m}_norm'] * weights[m] for m in metrics)
    df = df.sort_values('influence_score', ascending=False).reset_index(drop=True)
    df.index += 1  # 1-based ranking
    
    # -- Console Output: Top 50 --
    print()
    print("=" * 120)
    print(f"{'Rank':<5} {'Judge':<35} {'Cases':>7} {'Citations':>10} {'Courts':>7} {'Span':>5} {'Statutes':>9} {'Score':>8}")
    print("-" * 120)
    
    for i, row in df.head(50).iterrows():
        print(f"{i:<5} {row['judge']:<35} {row['total_cases']:>7,} {row['citation_impact']:>10,} "
              f"{row['court_diversity']:>7} {row['career_span']:>5} {row['domain_breadth']:>9} "
              f"{row['influence_score']:>8.4f}")
    
    print("-" * 120)
    print(f"Total judges analyzed: {len(df):,}")
    print(f"Total cases with judges: {df['total_cases'].sum():,}")
    print()
    
    # -- Chart 1: Judge Influence Bar Chart (Top 30) --
    fig, ax = plt.subplots(figsize=(14, 10))
    top30 = df.head(30).copy()
    top30 = top30.iloc[::-1]  # Reverse for horizontal bar
    
    colors = sns.color_palette('viridis', n_colors=30)
    bars = ax.barh(top30['judge'], top30['influence_score'], color=colors)
    
    ax.set_xlabel('Composite Influence Score', fontsize=12)
    ax.set_title('Top 30 Most Influential Judges in Pakistani Case Law', fontsize=14, fontweight='bold')
    ax.tick_params(axis='y', labelsize=9)
    
    # Add value labels
    for bar, score in zip(bars, top30['influence_score']):
        ax.text(bar.get_width() + 0.005, bar.get_y() + bar.get_height()/2,
                f'{score:.3f}', va='center', fontsize=7)
    
    plt.tight_layout()
    chart1_path = os.path.join(OUTPUT_DIR, 'judge_influence.png')
    plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart1_path}")
    
    # -- Chart 2: Judge Career Timeline (Top 20) --
    fig, ax = plt.subplots(figsize=(14, 8))
    top20 = df.head(20).copy()
    top20 = top20[top20['min_year'] > 0]
    top20 = top20.iloc[::-1]
    
    y_pos = range(len(top20))
    colors = sns.color_palette('coolwarm', n_colors=len(top20))
    
    for i, (_, row) in enumerate(top20.iterrows()):
        span = row['max_year'] - row['min_year']
        if span == 0:
            span = 1
        ax.barh(i, span, left=row['min_year'], height=0.6, color=colors[i], alpha=0.8,
                edgecolor='white', linewidth=0.5)
        ax.text(row['max_year'] + 0.5, i, f"{int(row['min_year'])}-{int(row['max_year'])}",
                va='center', fontsize=7, color='gray')
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(top20['judge'], fontsize=9)
    ax.set_xlabel('Year', fontsize=12)
    ax.set_title('Career Timelines of Top 20 Most Influential Judges', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    chart2_path = os.path.join(OUTPUT_DIR, 'judge_careers.png')
    plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {chart2_path}")
    
    # -- Summary Stats --
    print()
    print("KEY FINDINGS:")
    print(f"  Most cases authored:    {df.iloc[0]['judge']} ({df.iloc[0]['total_cases']:,} cases)")
    
    top_cited = df.sort_values('citation_impact', ascending=False).iloc[0]
    print(f"  Most cited judge:       {top_cited['judge']} ({int(top_cited['citation_impact']):,} citations)")
    
    top_diverse = df.sort_values('court_diversity', ascending=False).iloc[0]
    print(f"  Most court diversity:   {top_diverse['judge']} ({int(top_diverse['court_diversity'])} courts)")
    
    top_span = df.sort_values('career_span', ascending=False).iloc[0]
    print(f"  Longest career:         {top_span['judge']} ({int(top_span['career_span'])} years)")
    
    top_breadth = df.sort_values('domain_breadth', ascending=False).iloc[0]
    print(f"  Broadest domain:        {top_breadth['judge']} ({int(top_breadth['domain_breadth'])} unique statutes)")
    print()


if __name__ == '__main__':
    main()
