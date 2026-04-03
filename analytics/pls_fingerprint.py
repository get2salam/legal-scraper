"""
PLS Fingerprint - Analyzes PLS server response patterns from log data.
Scans historical_stderr.log and scraper_chain.log to identify
server availability patterns and optimal scraping windows.
"""
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import re
from collections import defaultdict
from datetime import datetime

# -- Config ----------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILES = [
    os.path.join(LOGS_DIR, 'historical_stderr.log'),
    os.path.join(LOGS_DIR, 'scraper_chain.log'),
]

# Add any other scraper logs that exist
EXTRA_LOG_PATTERNS = [
    'scraper_*.log', 'bulletproof_*.log', 'marathon_*.log',
    'fill_gaps*.log', 'complete_*.log', 'daily_batch.log',
]

# Success indicators
SUCCESS_PATTERNS = [
    re.compile(r'\bOK\b.*login', re.IGNORECASE),
    re.compile(r'Login\s+successful', re.IGNORECASE),
    re.compile(r'\bnew\s*\(', re.IGNORECASE),  # "10/465 new (0 skipped)"
    re.compile(r'Found\s+\d+\s+cases', re.IGNORECASE),
    re.compile(r'Fetched\s+\d+', re.IGNORECASE),
    re.compile(r'Saved\s+\d+', re.IGNORECASE),
    re.compile(r'successfully', re.IGNORECASE),
    re.compile(r'\d+/\d+\s+new', re.IGNORECASE),  # progress lines like "10/465 new"
]

# Failure indicators
FAILURE_PATTERNS = [
    re.compile(r'timeout', re.IGNORECASE),
    re.compile(r'timed?\s*out', re.IGNORECASE),
    re.compile(r'error', re.IGNORECASE),
    re.compile(r'fail(?:ed|ure)?', re.IGNORECASE),
    re.compile(r'ConnectionError', re.IGNORECASE),
    re.compile(r'refused', re.IGNORECASE),
    re.compile(r'\b5\d{2}\b'),  # 5xx errors
    re.compile(r'down', re.IGNORECASE),
    re.compile(r'unreachable', re.IGNORECASE),
]

# Timestamp extraction
TS_FULL = re.compile(r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})')
TS_TIME_ONLY = re.compile(r'^(\d{2}):(\d{2}):(\d{2})\s*\|')

# Day name mapping
DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def parse_line(line):
    """Parse a log line and return (datetime_or_None, is_success, is_failure)."""
    dt = None

    m = TS_FULL.search(line)
    if m:
        try:
            dt = datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6))
            )
        except ValueError:
            pass

    is_success = any(p.search(line) for p in SUCCESS_PATTERNS)
    is_failure = any(p.search(line) for p in FAILURE_PATTERNS)

    # If both match (e.g., "Login failed" matches both), prefer failure
    if is_success and is_failure:
        # Check if it's truly a failure
        if re.search(r'fail|error|timeout', line, re.IGNORECASE):
            is_success = False
        else:
            is_failure = False

    return dt, is_success, is_failure


def collect_log_files():
    """Collect all relevant log files."""
    import glob
    files = set()
    for f in LOG_FILES:
        if os.path.exists(f):
            files.add(f)
    for pattern in EXTRA_LOG_PATTERNS:
        for f in glob.glob(os.path.join(LOGS_DIR, pattern)):
            files.add(f)
    return sorted(files)


def scan_logs():
    """Scan logs and build availability data."""
    log_files = collect_log_files()
    if not log_files:
        print(f"[!] No log files found")
        return {}, 0, 0

    print(f"  Scanning {len(log_files)} log files...")

    # availability_grid[day_of_week][hour] = (success_count, failure_count)
    grid = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    # For lines without full date, track by hour only
    hour_only_grid = defaultdict(lambda: [0, 0])

    total_success = 0
    total_failure = 0
    dates_seen = set()

    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    dt, is_success, is_failure = parse_line(line)
                    if not (is_success or is_failure):
                        continue

                    if dt:
                        dow = dt.weekday()  # 0=Mon
                        hour = dt.hour
                        dates_seen.add(dt.date())
                        if is_success:
                            grid[dow][hour][0] += 1
                            total_success += 1
                        if is_failure:
                            grid[dow][hour][1] += 1
                            total_failure += 1
                    else:
                        # Time-only: extract hour
                        m = TS_TIME_ONLY.match(line)
                        if m:
                            hour = int(m.group(1))
                            if is_success:
                                hour_only_grid[hour][0] += 1
                                total_success += 1
                            if is_failure:
                                hour_only_grid[hour][1] += 1
                                total_failure += 1
        except Exception as e:
            print(f"    [!] Error reading {os.path.basename(log_file)}: {e}")

    # Merge hour-only data into grid (spread across all days proportionally)
    if hour_only_grid and grid:
        active_days = list(grid.keys()) if grid else list(range(7))
        for hour, (s, f) in hour_only_grid.items():
            for dow in active_days:
                grid[dow][hour][0] += s // max(len(active_days), 1)
                grid[dow][hour][1] += f // max(len(active_days), 1)
    elif hour_only_grid and not grid:
        # All data is time-only, spread evenly
        for hour, (s, f) in hour_only_grid.items():
            for dow in range(7):
                grid[dow][hour][0] += s // 7
                grid[dow][hour][1] += f // 7

    return grid, total_success, total_failure


def compute_availability_matrix(grid):
    """Convert grid to 7x24 availability percentage matrix."""
    matrix = np.full((7, 24), np.nan)

    for dow in range(7):
        for hour in range(24):
            s, f = grid[dow][hour]
            total = s + f
            if total > 0:
                matrix[dow][hour] = (s / total) * 100
            # Leave as NaN if no data

    return matrix


def plot_heatmap(matrix):
    """Plot PLS availability heatmap."""
    fig, ax = plt.subplots(figsize=(16, 6))

    # Custom colormap: red (bad) -> yellow (ok) -> green (good)
    cmap = sns.diverging_palette(10, 130, s=80, l=55, n=256, as_cmap=True)

    # Mask NaN values
    mask = np.isnan(matrix)

    sns.heatmap(
        matrix, ax=ax, mask=mask,
        cmap=cmap, center=50,
        vmin=0, vmax=100,
        annot=True, fmt='.0f',
        linewidths=0.5, linecolor='white',
        yticklabels=DAY_NAMES,
        xticklabels=[f'{h:02d}' for h in range(24)],
        cbar_kws={'label': 'Success Rate (%)', 'shrink': 0.8}
    )

    ax.set_xlabel('Hour of Day (UTC)', fontsize=12)
    ax.set_ylabel('Day of Week', fontsize=12)
    ax.set_title('PLS Server Availability Heatmap\n(Success Rate by Hour × Day of Week)',
                 fontsize=14, fontweight='bold')

    plt.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, 'pls_availability.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] Saved: {out_path}")


def print_summary(grid, total_success, total_failure, matrix):
    """Print console summary."""
    total = total_success + total_failure

    print("\n" + "=" * 60)
    print("  PLS SERVER FINGERPRINT REPORT")
    print("=" * 60)

    print(f"\n  Total events analyzed:    {total:,}")
    print(f"  Successful fetches:       {total_success:,}")
    print(f"  Failed fetches:           {total_failure:,}")
    if total > 0:
        uptime = (total_success / total) * 100
        print(f"  Overall uptime:           {uptime:.1f}%")

    # Find best and worst windows
    valid = ~np.isnan(matrix)
    if valid.any():
        # Best windows (highest availability)
        print(f"\n  {'-' * 40}")
        print(f"  BEST SCRAPING WINDOWS:")
        slots = []
        for dow in range(7):
            for hour in range(24):
                if not np.isnan(matrix[dow][hour]):
                    s, f = grid[dow][hour]
                    slots.append((DAY_NAMES[dow], hour, matrix[dow][hour], s + f))

        # Sort by availability (desc), then by volume (desc)
        slots.sort(key=lambda x: (-x[2], -x[3]))
        seen_hours = set()
        count = 0
        for day, hour, avail, vol in slots:
            if count >= 5:
                break
            if hour not in seen_hours or avail > 90:
                print(f"    {day} {hour:02d}:00  ->  {avail:.0f}% success  ({vol} events)")
                seen_hours.add(hour)
                count += 1

        # Worst windows
        print(f"\n  WORST SCRAPING WINDOWS:")
        slots.sort(key=lambda x: (x[2], -x[3]))
        seen_hours = set()
        count = 0
        for day, hour, avail, vol in slots:
            if count >= 5:
                break
            if vol >= 3:  # Only include windows with meaningful data
                print(f"    {day} {hour:02d}:00  ->  {avail:.0f}% success  ({vol} events)")
                seen_hours.add(hour)
                count += 1

        # Hourly summary
        print(f"\n  {'-' * 40}")
        print(f"  HOURLY AVERAGE AVAILABILITY:")
        hourly_avgs = []
        for hour in range(24):
            vals = [matrix[dow][hour] for dow in range(7)
                    if not np.isnan(matrix[dow][hour])]
            if vals:
                avg = np.mean(vals)
                hourly_avgs.append((hour, avg))

        if hourly_avgs:
            hourly_avgs.sort(key=lambda x: -x[1])
            for hour, avg in hourly_avgs[:5]:
                bar = '#' * int(avg / 5)
                print(f"    {hour:02d}:00  {avg:5.1f}%  {bar}")
            if len(hourly_avgs) > 5:
                print(f"    ...")
                for hour, avg in hourly_avgs[-3:]:
                    bar = '#' * int(avg / 5)
                    print(f"    {hour:02d}:00  {avg:5.1f}%  {bar}")

    print("\n" + "=" * 60)


def main():
    print("PLS Server Fingerprint Analysis")
    print("-" * 40)

    grid, total_success, total_failure = scan_logs()

    if total_success + total_failure == 0:
        print("[!] No success/failure events found in logs.")
        print("    Ensure logs/ contains historical_stderr.log or scraper_chain.log")
        return

    print(f"\n  Found {total_success + total_failure:,} events "
          f"({total_success:,} success, {total_failure:,} failure)")

    matrix = compute_availability_matrix(grid)

    print(f"\nGenerating heatmap...")
    plot_heatmap(matrix)

    print_summary(grid, total_success, total_failure, matrix)


if __name__ == '__main__':
    main()
