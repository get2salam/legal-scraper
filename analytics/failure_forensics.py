"""
Failure Forensics - Analyzes scraper failures from log files.
Scans all log files in logs/ directory for failure patterns,
groups by hour-of-day, and shows failure type breakdown.
"""
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import re
import glob
from collections import defaultdict, Counter

# -- Config ----------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Failure pattern definitions
FAILURE_PATTERNS = {
    'Timeout': [
        re.compile(r'timeout', re.IGNORECASE),
        re.compile(r'timed?\s*out', re.IGNORECASE),
        re.compile(r'TimeoutError', re.IGNORECASE),
    ],
    'Login/Auth': [
        re.compile(r'login\s*fail', re.IGNORECASE),
        re.compile(r'Login\s+failed', re.IGNORECASE),
        re.compile(r'\b401\b'),
        re.compile(r'\b403\b'),
    ],
    'DNS/Connection': [
        re.compile(r'DNS', re.IGNORECASE),
        re.compile(r'resolve', re.IGNORECASE),
        re.compile(r'ConnectionError', re.IGNORECASE),
        re.compile(r'ConnectionReset', re.IGNORECASE),
        re.compile(r'ECONNREFUSED', re.IGNORECASE),
    ],
    'Empty Response': [
        re.compile(r'empty\s*(response|body|result|page)', re.IGNORECASE),
        re.compile(r'(?:got|returned?|received?)\s+None', re.IGNORECASE),
        re.compile(r'no\s*response', re.IGNORECASE),
    ],
    'Skipped': [
        re.compile(r'\bskip(?:ped|ping)?\b', re.IGNORECASE),
    ],
}

# Timestamp patterns commonly found in logs
TIMESTAMP_PATTERNS = [
    # 2026-02-14 01:07:50
    re.compile(r'(\d{4}-\d{2}-\d{2}\s+(\d{2}):\d{2}:\d{2})'),
    # 10:33:49 (time only - no date)
    re.compile(r'^(\d{2}):(\d{2}):(\d{2})\s*\|'),
]


def extract_hour(line):
    """Extract hour-of-day from a log line. Returns int or None."""
    # Full datetime
    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}):\d{2}:\d{2}', line)
    if m:
        return int(m.group(2))
    # Time-only format
    m = re.match(r'^(\d{2}):\d{2}:\d{2}\s*\|', line)
    if m:
        return int(m.group(1))
    return None


def classify_failure(line):
    """Classify a line into failure categories. Returns list of matching types."""
    matches = []
    for category, patterns in FAILURE_PATTERNS.items():
        for pat in patterns:
            if pat.search(line):
                matches.append(category)
                break
    return matches


def scan_logs():
    """Scan all log files and extract failure information."""
    log_files = glob.glob(os.path.join(LOGS_DIR, '*.log'))
    if not log_files:
        print(f"[!] No log files found in {LOGS_DIR}")
        return {}, Counter(), 0, 0

    failures_by_hour = defaultdict(int)
    failure_types = Counter()
    total_lines = 0
    total_failure_lines = 0

    for log_file in sorted(log_files):
        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    total_lines += 1
                    categories = classify_failure(line)
                    if categories:
                        total_failure_lines += 1
                        hour = extract_hour(line)
                        for cat in categories:
                            failure_types[cat] += 1
                        if hour is not None:
                            failures_by_hour[hour] += 1
        except Exception as e:
            print(f"  [!] Error reading {os.path.basename(log_file)}: {e}")

    return failures_by_hour, failure_types, total_lines, total_failure_lines


def plot_timeline(failures_by_hour):
    """Plot failures per hour bar chart."""
    hours = list(range(24))
    counts = [failures_by_hour.get(h, 0) for h in hours]

    fig, ax = plt.subplots(figsize=(14, 6))
    colors = ['#e74c3c' if c > np.mean(counts) + np.std(counts) else
              '#f39c12' if c > np.mean(counts) else '#3498db'
              for c in counts]
    bars = ax.bar(hours, counts, color=colors, edgecolor='white', linewidth=0.5)

    ax.set_xlabel('Hour of Day (UTC)', fontsize=12)
    ax.set_ylabel('Failure Count', fontsize=12)
    ax.set_title('Scraper Failures by Hour of Day', fontsize=14, fontweight='bold')
    ax.set_xticks(hours)
    ax.set_xticklabels([f'{h:02d}' for h in hours], fontsize=9)

    # Add mean line
    mean_val = np.mean(counts)
    ax.axhline(y=mean_val, color='red', linestyle='--', alpha=0.7, label=f'Mean: {mean_val:.0f}')
    ax.legend()

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, 'failure_timeline.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def plot_failure_types(failure_types):
    """Plot failure types pie chart."""
    if not failure_types:
        print("  [!] No failure types to plot")
        return

    labels = list(failure_types.keys())
    sizes = list(failure_types.values())

    # Color palette
    palette = {
        'Timeout': '#e74c3c',
        'Login/Auth': '#9b59b6',
        'DNS/Connection': '#e67e22',
        'Empty Response': '#f1c40f',
        'Skipped': '#95a5a6',
    }
    colors = [palette.get(l, '#3498db') for l in labels]

    fig, ax = plt.subplots(figsize=(10, 8))

    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct='%1.1f%%',
        colors=colors, startangle=140,
        pctdistance=0.85, textprops={'fontsize': 11}
    )
    for autotext in autotexts:
        autotext.set_fontsize(10)
        autotext.set_fontweight('bold')

    # Add center circle for donut style
    centre_circle = plt.Circle((0, 0), 0.55, fc='white')
    ax.add_patch(centre_circle)

    total = sum(sizes)
    ax.text(0, 0, f'{total:,}\nfailures', ha='center', va='center',
            fontsize=16, fontweight='bold', color='#2c3e50')

    ax.set_title('Failure Type Breakdown', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, 'failure_types.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def print_summary(failures_by_hour, failure_types, total_lines, total_failure_lines):
    """Print console summary."""
    print("\n" + "=" * 60)
    print("  FAILURE FORENSICS REPORT")
    print("=" * 60)

    print(f"\n  Total log lines scanned:  {total_lines:,}")
    print(f"  Total failure lines:      {total_failure_lines:,}")
    if total_lines > 0:
        rate = (total_failure_lines / total_lines) * 100
        print(f"  Failure rate:             {rate:.2f}%")

    print(f"\n  {'-' * 40}")
    print(f"  FAILURE TYPE BREAKDOWN:")
    for ftype, count in failure_types.most_common():
        pct = (count / sum(failure_types.values())) * 100 if failure_types else 0
        print(f"    {ftype:<20} {count:>6,}  ({pct:.1f}%)")

    if failures_by_hour:
        print(f"\n  {'-' * 40}")
        print(f"  WORST HOURS (top 5):")
        sorted_hours = sorted(failures_by_hour.items(), key=lambda x: x[1], reverse=True)
        for hour, count in sorted_hours[:5]:
            print(f"    {hour:02d}:00 - {hour:02d}:59   {count:>6,} failures")

        print(f"\n  BEST HOURS (top 5):")
        all_hours = {h: failures_by_hour.get(h, 0) for h in range(24)}
        sorted_best = sorted(all_hours.items(), key=lambda x: x[1])
        for hour, count in sorted_best[:5]:
            print(f"    {hour:02d}:00 - {hour:02d}:59   {count:>6,} failures")

    # Recommendations
    print(f"\n  {'-' * 40}")
    print(f"  RECOMMENDATIONS:")
    if failure_types.get('Timeout', 0) > sum(failure_types.values()) * 0.3:
        print("    [!]  High timeout rate - consider increasing request timeouts")
        print("       or adding exponential backoff")
    if failure_types.get('Login/Auth', 0) > 0:
        print("    [!]  Auth failures detected - check credential rotation")
        print("       and session management")
    if failure_types.get('DNS/Connection', 0) > 0:
        print("    [!]  Connection errors present - PLS server may be unstable")
        print("       during certain hours. Use pls_fingerprint.py for details")
    if failure_types.get('Empty Response', 0) > 0:
        print("    [!]  Empty responses detected - server may be rate-limiting")
        print("       or returning empty pages under load")
    if failures_by_hour:
        worst_hour = max(failures_by_hour, key=failures_by_hour.get)
        best_hour = min({h: failures_by_hour.get(h, 0) for h in range(24)},
                       key=lambda h: failures_by_hour.get(h, 0))
        print(f"    [OK]  Best scraping window: around {best_hour:02d}:00 UTC")
        print(f"    [X]  Worst scraping window: around {worst_hour:02d}:00 UTC")

    print("\n" + "=" * 60)


def main():
    print("Scanning log files...")
    log_count = len(glob.glob(os.path.join(LOGS_DIR, '*.log')))
    print(f"  Found {log_count} log files in {LOGS_DIR}")

    failures_by_hour, failure_types, total_lines, total_failure_lines = scan_logs()

    print(f"\nGenerating charts...")
    plot_timeline(failures_by_hour)
    plot_failure_types(failure_types)

    print_summary(failures_by_hour, failure_types, total_lines, total_failure_lines)


if __name__ == '__main__':
    main()
