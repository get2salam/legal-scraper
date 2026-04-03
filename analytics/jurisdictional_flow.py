"""
Jurisdictional Flow Analysis
==============================
Maps how cases flow between courts via citations. Builds a court-to-court
citation matrix showing which courts cite which other courts.
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

os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_court(court):
    """Normalize court names into canonical forms."""
    if not court or not isinstance(court, str):
        return None
    court = court.strip()
    # Remove any embedded newlines / carriage returns
    court = re.sub(r'[\r\n]+', ' ', court).strip()
    if not court or len(court) < 3:
        return None

    c = court.lower()

    # Reject garbage entries (long descriptions that aren't court names)
    if len(c) > 80:
        return None

    # Supreme Court of Pakistan
    if 'supreme court' in c and 'azad' not in c and 'ajk' not in c:
        return 'Supreme Court of Pakistan'

    # Federal Shariat Court
    if 'shariat' in c or 'shariah' in c:
        return 'Federal Shariat Court'

    # AJK courts
    if 'azad' in c or 'aj&k' in c or 'ajk' in c:
        if 'supreme' in c:
            return 'AJK Supreme Court'
        return 'AJK High Court'

    # High Courts - map to canonical names
    if 'lahore' in c:
        return 'Lahore High Court'
    if 'sindh' in c or 'karachi' in c:
        return 'Sindh High Court'
    if 'peshawar' in c:
        return 'Peshawar High Court'
    if 'balochistan' in c or 'baluch' in c or 'quetta' in c:
        return 'Balochistan High Court'
    if 'islamabad' in c:
        return 'Islamabad High Court'
    if 'punjab' in c:
        return 'Lahore High Court'

    # Historic: High Court of West/East Pakistan
    if 'high court of west' in c or 'west pakistan' in c:
        return 'High Court of West Pakistan'
    if 'high court of east' in c or 'east pakistan' in c:
        return 'High Court of East Pakistan'

    # Tribunals
    if 'tribunal' in c:
        if 'service' in c or 'services' in c:
            return 'Service Tribunal'
        if 'tax' in c or 'income' in c or 'customs' in c or 'appellate' in c or 'revenue' in c:
            return 'Tax/Revenue Tribunal'
        if 'labour' in c or 'labor' in c:
            return 'Labour Tribunal'
        return 'Other Tribunal'

    # Admiralty
    if 'admiralty' in c:
        return 'Admiralty Jurisdiction'

    # Foreign courts (exclude from our analysis)
    foreign_markers = ['australia', 'england', 'india', 'privy council', 'house of lords',
                       'united kingdom', 'united states', 'canada', 'kenya', 'nigeria']
    for fm in foreign_markers:
        if fm in c:
            return None

    # Generic "high court" without specifics - skip
    if c.startswith('high court') and len(c) < 20:
        return None

    return court.strip()


def load_all_cases():
    """Load all cases and build citation -> court lookup."""
    citation_to_court = {}
    cases_data = []  # (citation, court, cases_cited)
    total = 0

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

            total += 1
            citation = case.get('citation', '')
            court = normalize_court(case.get('court', ''))
            cases_cited = case.get('cases_cited', []) or []

            if not isinstance(cases_cited, list):
                cases_cited = [cases_cited] if cases_cited else []

            if citation and court:
                citation_to_court[citation] = court
                cases_data.append((citation, court, cases_cited))

    print(f"Loaded {total:,} total cases")
    print(f"Cases with citation + court: {len(cases_data):,}")
    print(f"Unique citations in lookup: {len(citation_to_court):,}")

    courts = defaultdict(int)
    for _, court, _ in cases_data:
        courts[court] += 1
    print(f"Unique courts: {len(courts)}")
    print()

    return cases_data, citation_to_court


def build_flow_matrix(cases_data, citation_to_court):
    """Build court-to-court citation flow matrix."""
    flow = defaultdict(lambda: defaultdict(int))
    total_resolved = 0
    total_unresolved = 0
    total_cited = 0

    for citation, source_court, cited_cases in cases_data:
        for cited in cited_cases:
            if not cited or not isinstance(cited, str):
                continue
            cited = cited.strip()
            total_cited += 1

            target_court = citation_to_court.get(cited)
            if target_court:
                flow[source_court][target_court] += 1
                total_resolved += 1
            else:
                total_unresolved += 1

    resolution_rate = (total_resolved / total_cited * 100) if total_cited > 0 else 0
    print(f"Total citations found: {total_cited:,}")
    print(f"  Resolved (in our data): {total_resolved:,} ({resolution_rate:.1f}%)")
    print(f"  Unresolved: {total_unresolved:,}")
    print()

    return flow


def main():
    print("=" * 70)
    print("JURISDICTIONAL FLOW ANALYSIS")
    print("=" * 70)
    print()

    # Load data
    cases_data, citation_to_court = load_all_cases()

    if not cases_data:
        print("ERROR: No case data found.")
        return

    # Build flow matrix
    flow = build_flow_matrix(cases_data, citation_to_court)

    if not flow:
        print("ERROR: No citation flows resolved. Not enough cross-referenced data.")
        return

    # Get all courts involved
    all_courts = set()
    for src, targets in flow.items():
        all_courts.add(src)
        for tgt in targets:
            all_courts.add(tgt)

    # Sort courts by total involvement (citations given + received)
    court_activity = defaultdict(int)
    for src, targets in flow.items():
        for tgt, count in targets.items():
            court_activity[src] += count
            court_activity[tgt] += count

    sorted_courts = sorted(court_activity.items(), key=lambda x: x[1], reverse=True)

    # Limit to top courts for readability
    max_courts = 15
    top_courts = [c[0] for c in sorted_courts[:max_courts]]

    # Build DataFrame
    data = []
    for src in top_courts:
        row = [flow[src].get(tgt, 0) for tgt in top_courts]
        data.append(row)

    # Truncate court names for display
    display_names = []
    for c in top_courts:
        if len(c) > 25:
            display_names.append(c[:22] + '...')
        else:
            display_names.append(c)

    df = pd.DataFrame(data, index=display_names, columns=display_names)

    # --- Heatmap ---------------------------------------------------------
    fig, ax = plt.subplots(figsize=(16, 13))

    # Use log scale for better visualization (many values span orders of magnitude)
    # Add 1 to avoid log(0)
    log_data = np.log1p(df.values)
    log_df = pd.DataFrame(log_data, index=df.index, columns=df.columns)

    sns.heatmap(
        log_df,
        annot=df.values,  # Show actual counts
        fmt='d',
        cmap='Blues',
        linewidths=0.5,
        linecolor='white',
        ax=ax,
        cbar_kws={'label': 'Citation Count (log scale coloring)'},
        square=True
    )
    ax.set_title('Court-to-Court Citation Flow Matrix\n(Row = Citing Court, Column = Cited Court)',
                 fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Cited Court (Target)', fontsize=12)
    ax.set_ylabel('Citing Court (Source)', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=9)
    plt.yticks(rotation=0, fontsize=9)
    plt.tight_layout()

    outpath = os.path.join(OUTPUT_DIR, 'court_flow_heatmap.png')
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[OK] Heatmap saved -> {outpath}")
    print()

    # --- Top 20 court-to-court citation pairs ----------------------------
    print("-" * 70)
    print("TOP 20 COURT-TO-COURT CITATION PAIRS")
    print("-" * 70)
    print(f"  {'#':<4} {'Source Court':<30s} -> {'Target Court':<30s} {'Count':>8s}")
    print("  " + "-" * 76)

    all_pairs = []
    for src, targets in flow.items():
        for tgt, count in targets.items():
            all_pairs.append((src, tgt, count))

    all_pairs.sort(key=lambda x: x[2], reverse=True)

    for i, (src, tgt, count) in enumerate(all_pairs[:20], 1):
        src_disp = src[:29] if len(src) > 29 else src
        tgt_disp = tgt[:29] if len(tgt) > 29 else tgt
        marker = " [SELF]" if src == tgt else ""
        print(f"  {i:<4} {src_disp:<30s} -> {tgt_disp:<30s} {count:>7,}{marker}")

    print()
    print("  [SELF] = self-citation")
    print()

    # --- Self-citation rate per court ------------------------------------
    print("-" * 70)
    print("SELF-CITATION RATE PER COURT")
    print("-" * 70)

    court_total_out = defaultdict(int)
    court_self = defaultdict(int)
    for src, targets in flow.items():
        for tgt, count in targets.items():
            court_total_out[src] += count
            if src == tgt:
                court_self[src] += count

    self_rates = []
    for court in court_total_out:
        total = court_total_out[court]
        self_count = court_self.get(court, 0)
        rate = (self_count / total * 100) if total > 0 else 0
        self_rates.append((court, self_count, total, rate))

    self_rates.sort(key=lambda x: x[3], reverse=True)

    print(f"  {'Court':<40s} {'Self':>7s} {'Total':>7s} {'Rate':>8s}")
    print("  " + "-" * 65)
    for court, self_count, total, rate in self_rates:
        if total < 5:
            continue  # Skip courts with very few citations
        court_disp = court[:39] if len(court) > 39 else court
        print(f"  {court_disp:<40s} {self_count:>7,} {total:>7,} {rate:>7.1f}%")

    print()

    # --- Summary stats ---------------------------------------------------
    total_all = sum(c for _, _, c in all_pairs)
    total_self = sum(c for s, t, c in all_pairs if s == t)
    overall_self_rate = (total_self / total_all * 100) if total_all > 0 else 0

    print("-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total resolved citation flows: {total_all:,}")
    print(f"  Total self-citations: {total_self:,} ({overall_self_rate:.1f}%)")
    print(f"  Total cross-court citations: {total_all - total_self:,} ({100 - overall_self_rate:.1f}%)")
    print(f"  Most-cited court: {sorted_courts[0][0]} ({sorted_courts[0][1]:,} total flows)")
    print()
    print("=" * 70)
    print("Done.")


if __name__ == '__main__':
    main()
