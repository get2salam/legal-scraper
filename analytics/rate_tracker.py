"""
Real-time Scraping Rate Tracker
Scans JSON file creation times in data_v2/ and parses log files
to calculate cases scraped per hour for the last 24 hours.
Output: analytics/scraping_rate_24h.json
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

# Project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data_v2")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "scraping_rate_24h.json")

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

HOUR_LABELS = [
    "12AM", "1AM", "2AM", "3AM", "4AM", "5AM",
    "6AM", "7AM", "8AM", "9AM", "10AM", "11AM",
    "12PM", "1PM", "2PM", "3PM", "4PM", "5PM",
    "6PM", "7PM", "8PM", "9PM", "10PM", "11PM"
]


def scan_json_creation_times():
    """Scan data_v2/{REPORTER}/{YEAR}/*.json for file creation times in last 24h."""
    now = time.time()
    cutoff = now - 86400  # 24 hours ago
    hourly_counts = defaultdict(int)

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue
        try:
            with os.scandir(reporter_dir) as year_entries:
                for year_entry in year_entries:
                    if not year_entry.is_dir():
                        continue
                    year_dir = year_entry.path
                    try:
                        with os.scandir(year_dir) as file_entries:
                            for fe in file_entries:
                                if not fe.name.endswith(".json"):
                                    continue
                                if fe.name.startswith("."):
                                    continue
                                try:
                                    stat = fe.stat()
                                    # Use creation time on Windows, mtime as fallback
                                    ctime = getattr(stat, 'st_birthtime', None) or stat.st_ctime
                                    if ctime >= cutoff:
                                        dt = datetime.fromtimestamp(ctime)
                                        hourly_counts[dt.hour] += 1
                                except OSError:
                                    continue
                    except OSError:
                        continue
        except OSError:
            continue

    return hourly_counts


def parse_log_saved_times():
    """Parse log files for 'Saved:' entries to get scraping timestamps."""
    now = datetime.now()
    cutoff = now - timedelta(hours=24)
    hourly_counts = defaultdict(int)

    if not os.path.isdir(LOGS_DIR):
        return hourly_counts

    try:
        with os.scandir(LOGS_DIR) as entries:
            for entry in entries:
                if not entry.name.endswith(".log"):
                    continue
                # Only read recent logs (modified in last 48h for safety)
                try:
                    if entry.stat().st_mtime < (time.time() - 172800):
                        continue
                except OSError:
                    continue

                try:
                    with open(entry.path, "r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            if "Saved:" not in line:
                                continue
                            # Parse timestamp like "10:13:37 | INFO    | Saved: 2018 PCrLJ 570"
                            parts = line.strip().split("|")
                            if len(parts) < 2:
                                continue
                            time_str = parts[0].strip()
                            try:
                                t = datetime.strptime(time_str, "%H:%M:%S")
                                # Assume today's date for recent logs
                                t = t.replace(year=now.year, month=now.month, day=now.day)
                                # If the time is in the future, it was yesterday
                                if t > now:
                                    t -= timedelta(days=1)
                                if t >= cutoff:
                                    hourly_counts[t.hour] += 1
                            except ValueError:
                                continue
                except (OSError, IOError):
                    continue

    except OSError:
        pass

    return hourly_counts


def build_24h_rate():
    """Build the 24-hour rate array starting from current hour - 23."""
    now = datetime.now()
    current_hour = now.hour

    # Get data from both sources
    file_counts = scan_json_creation_times()
    log_counts = parse_log_saved_times()

    # Merge: take the max from each source per hour (avoid double-counting)
    merged = defaultdict(int)
    all_hours = set(list(file_counts.keys()) + list(log_counts.keys()))
    for h in all_hours:
        merged[h] = max(file_counts.get(h, 0), log_counts.get(h, 0))

    # Build array: last 24 hours ending at current hour
    result = []
    for i in range(24):
        hour = (current_hour - 23 + i) % 24
        result.append({
            "hour": HOUR_LABELS[hour],
            "rate": merged.get(hour, 0)
        })

    return result


def main():
    print("[rate_tracker] Scanning file creation times and logs...")
    try:
        rate_data = build_24h_rate()

        total = sum(r["rate"] for r in rate_data)
        active_hours = sum(1 for r in rate_data if r["rate"] > 0)

        output = {
            "generated_at": datetime.now().isoformat(),
            "total_last_24h": total,
            "active_hours": active_hours,
            "avg_per_active_hour": round(total / active_hours, 1) if active_hours > 0 else 0,
            "hourly_rates": rate_data
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print("[OK] Wrote %s (%d cases in last 24h, %d active hours)" % (
            OUTPUT_FILE, total, active_hours))
        return output

    except Exception as e:
        print("[FAIL] rate_tracker error: %s" % str(e))
        # Write empty but valid output
        fallback = {
            "generated_at": datetime.now().isoformat(),
            "total_last_24h": 0,
            "active_hours": 0,
            "avg_per_active_hour": 0,
            "hourly_rates": [{"hour": HOUR_LABELS[h], "rate": 0} for h in range(24)]
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        return fallback


if __name__ == "__main__":
    main()
