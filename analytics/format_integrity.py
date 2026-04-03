"""
Format Integrity - Checks data format integrity across all cases.
Scans data_v2/REPORTER/YEAR/*.json for HTML contamination,
short judgments, empty judges, duplicate citations, and missing fields.
"""
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import json
import os
import re
import glob
from collections import defaultdict, Counter

# -- Config ----------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DATA_DIR = os.path.join(BASE_DIR, 'data_v2')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

# HTML contamination patterns (in judgment_clean)
HTML_PATTERNS = [
    re.compile(r'<html', re.IGNORECASE),
    re.compile(r'<div[\s>]', re.IGNORECASE),
    re.compile(r'<span[\s>]', re.IGNORECASE),
    re.compile(r'<table[\s>]', re.IGNORECASE),
    re.compile(r'<p\s+class=', re.IGNORECASE),
    re.compile(r'<body[\s>]', re.IGNORECASE),
]

SHORT_JUDGMENT_THRESHOLD = 200  # characters

ISSUE_TYPES = [
    'HTML Contamination',
    'Short Judgment',
    'Empty Judges',
    'Duplicate Citation',
    'Missing Court',
    'Missing Date',
]


def check_html_contamination(judgment_clean):
    """Check if judgment_clean contains HTML tags."""
    if not judgment_clean:
        return False
    text = str(judgment_clean)
    return any(p.search(text) for p in HTML_PATTERNS)


def check_short_judgment(judgment_clean):
    """Check if judgment_clean exists but is very short."""
    if not judgment_clean:
        return False  # Missing judgment is different from short
    return len(str(judgment_clean).strip()) < SHORT_JUDGMENT_THRESHOLD


def check_empty_judges(judges):
    """Check if judges list is empty or missing."""
    if judges is None:
        return True
    if isinstance(judges, list) and len(judges) == 0:
        return True
    if isinstance(judges, list) and all(not j or not str(j).strip() for j in judges):
        return True
    return False


def check_missing_court(court):
    """Check if court field is missing or empty."""
    if not court or not str(court).strip():
        return True
    return False


def check_missing_date(date):
    """Check if date field is missing or empty."""
    if not date or not str(date).strip():
        return True
    return False


def scan_cases():
    """Scan all cases and check integrity."""
    issues_by_reporter = defaultdict(lambda: defaultdict(int))
    total_by_reporter = defaultdict(int)
    citation_registry = {}  # citation -> (reporter, file_path)
    duplicates = []
    issue_examples = defaultdict(list)  # issue_type -> [(citation, detail)]

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            print(f"  [!] Reporter dir not found: {reporter}")
            continue

        year_dirs = sorted(glob.glob(os.path.join(reporter_dir, '*')))
        for year_dir in year_dirs:
            if not os.path.isdir(year_dir):
                continue
            dir_name = os.path.basename(year_dir)
            if dir_name == 'original':
                continue

            json_files = glob.glob(os.path.join(year_dir, '*.json'))
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8', errors='replace') as f:
                        data = json.load(f)
                except (json.JSONDecodeError, Exception):
                    continue

                total_by_reporter[reporter] += 1
                citation = data.get('citation', '')
                fname = os.path.basename(json_file)

                # 1. HTML contamination in judgment_clean
                jc = data.get('judgment_clean', '')
                if check_html_contamination(jc):
                    issues_by_reporter[reporter]['HTML Contamination'] += 1
                    if len(issue_examples['HTML Contamination']) < 5:
                        issue_examples['HTML Contamination'].append(
                            (citation, f"Contains HTML tags in judgment_clean"))

                # 2. Short judgment
                if check_short_judgment(jc):
                    issues_by_reporter[reporter]['Short Judgment'] += 1
                    jc_len = len(str(jc).strip()) if jc else 0
                    if len(issue_examples['Short Judgment']) < 5:
                        issue_examples['Short Judgment'].append(
                            (citation, f"judgment_clean is only {jc_len} chars"))

                # 3. Empty judges
                if check_empty_judges(data.get('judges')):
                    issues_by_reporter[reporter]['Empty Judges'] += 1

                # 4. Duplicate citations
                if citation and str(citation).strip():
                    cit_key = str(citation).strip().upper()
                    if cit_key in citation_registry:
                        prev_rep, prev_file = citation_registry[cit_key]
                        issues_by_reporter[reporter]['Duplicate Citation'] += 1
                        if prev_rep != reporter:
                            issues_by_reporter[prev_rep]['Duplicate Citation'] += 1
                        duplicates.append({
                            'citation': citation,
                            'file1': f"{prev_rep}/{prev_file}",
                            'file2': f"{reporter}/{fname}",
                        })
                    else:
                        citation_registry[cit_key] = (reporter, fname)

                # 5. Missing court
                if check_missing_court(data.get('court')):
                    issues_by_reporter[reporter]['Missing Court'] += 1

                # 6. Missing date
                if check_missing_date(data.get('date')):
                    issues_by_reporter[reporter]['Missing Date'] += 1

    return issues_by_reporter, total_by_reporter, duplicates, issue_examples


def plot_integrity_issues(issues_by_reporter, total_by_reporter):
    """Plot stacked bar chart of integrity issues by reporter."""
    # Prepare data
    active_reporters = [r for r in REPORTERS if total_by_reporter.get(r, 0) > 0]
    if not active_reporters:
        print("  [!] No data for integrity chart")
        return

    # Sort reporters by total issues
    reporter_totals = {r: sum(issues_by_reporter[r].values()) for r in active_reporters}
    active_reporters.sort(key=lambda r: -reporter_totals.get(r, 0))

    fig, ax = plt.subplots(figsize=(14, 7))

    palette = {
        'HTML Contamination': '#e74c3c',
        'Short Judgment': '#e67e22',
        'Empty Judges': '#f1c40f',
        'Duplicate Citation': '#9b59b6',
        'Missing Court': '#3498db',
        'Missing Date': '#1abc9c',
    }

    x = np.arange(len(active_reporters))
    width = 0.6
    bottom = np.zeros(len(active_reporters))

    for issue_type in ISSUE_TYPES:
        values = [issues_by_reporter[r].get(issue_type, 0) for r in active_reporters]
        if sum(values) > 0:
            ax.bar(x, values, width, bottom=bottom,
                   label=issue_type, color=palette.get(issue_type, '#95a5a6'),
                   edgecolor='white', linewidth=0.3)
            bottom += values

    # Add total labels on top
    for i, r in enumerate(active_reporters):
        total = sum(issues_by_reporter[r].values())
        if total > 0:
            ax.text(i, bottom[i] + 5, f'{total:,}', ha='center', va='bottom',
                    fontsize=8, fontweight='bold')

    ax.set_xlabel('Reporter', fontsize=12)
    ax.set_ylabel('Number of Issues', fontsize=12)
    ax.set_title('Data Integrity Issues by Reporter', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(active_reporters, fontsize=11)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, 'integrity_issues.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def print_summary(issues_by_reporter, total_by_reporter, duplicates, issue_examples):
    """Print console summary."""
    total_cases = sum(total_by_reporter.values())

    # Aggregate issues
    all_issues = defaultdict(int)
    for reporter_issues in issues_by_reporter.values():
        for itype, count in reporter_issues.items():
            all_issues[itype] += count

    total_issues = sum(all_issues.values())

    print("\n" + "=" * 60)
    print("  FORMAT INTEGRITY REPORT")
    print("=" * 60)

    print(f"\n  Total cases scanned:  {total_cases:,}")
    print(f"  Total issues found:   {total_issues:,}")
    if total_cases > 0:
        print(f"  Issue rate:           {(total_issues/total_cases)*100:.2f}%")

    # Issue type breakdown
    print(f"\n  {'-' * 45}")
    print(f"  ISSUES BY TYPE:")
    for itype in ISSUE_TYPES:
        count = all_issues.get(itype, 0)
        if count > 0:
            pct = (count / total_cases * 100) if total_cases > 0 else 0
            bar = '#' * min(int(pct), 50)
            print(f"    {itype:<22} {count:>7,}  ({pct:>5.2f}%)  {bar}")

    # Per-reporter summary
    print(f"\n  {'-' * 45}")
    print(f"  WORST REPORTERS (by total issues):")
    reporter_totals = [(r, sum(issues_by_reporter[r].values()), total_by_reporter[r])
                       for r in REPORTERS if total_by_reporter.get(r, 0) > 0]
    reporter_totals.sort(key=lambda x: -x[1])

    for rep, issues, total in reporter_totals[:5]:
        rate = (issues / total * 100) if total > 0 else 0
        print(f"    {rep:<10} {issues:>6,} issues / {total:>6,} cases  ({rate:.1f}%)")

    # Duplicate citations
    if duplicates:
        print(f"\n  {'-' * 45}")
        print(f"  DUPLICATE CITATIONS ({len(duplicates)} found):")
        for dup in duplicates[:5]:
            print(f"    {dup['citation']}")
            print(f"      -> {dup['file1']}")
            print(f"      -> {dup['file2']}")
        if len(duplicates) > 5:
            print(f"    ... and {len(duplicates) - 5} more")

    # Example issues
    for itype, examples in issue_examples.items():
        if examples:
            print(f"\n  Sample {itype}:")
            for citation, detail in examples[:3]:
                print(f"    {citation}: {detail}")

    print("\n" + "=" * 60)


def main():
    print("Format Integrity Check")
    print("-" * 40)

    print(f"  Scanning data in {DATA_DIR}...")
    issues_by_reporter, total_by_reporter, duplicates, issue_examples = scan_cases()

    total = sum(total_by_reporter.values())
    print(f"  Scanned {total:,} cases across "
          f"{len([r for r in REPORTERS if total_by_reporter.get(r, 0) > 0])} reporters")

    print(f"\nGenerating charts...")
    plot_integrity_issues(issues_by_reporter, total_by_reporter)

    print_summary(issues_by_reporter, total_by_reporter, duplicates, issue_examples)


if __name__ == '__main__':
    main()
