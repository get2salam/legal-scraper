"""
Reporter Growth Tracker
For each reporter, tracks growth over time by scanning file creation dates.
Calculates which reporter is growing fastest.
Output: analytics/reporter_growth.json
"""

import os
import sys
import json
import time
from datetime import datetime, date, timedelta
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data_v2")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "reporter_growth.json")

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]


def scan_reporter_dates():
    """Scan each reporter's files and collect creation dates."""
    reporter_daily = {}  # reporter -> {date_str: count}
    reporter_totals = {}
    reporter_years = {}  # reporter -> {year: count}

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        daily = defaultdict(int)
        yearly = defaultdict(int)
        total = 0

        if not os.path.isdir(reporter_dir):
            reporter_daily[reporter] = {}
            reporter_totals[reporter] = 0
            reporter_years[reporter] = {}
            continue

        try:
            with os.scandir(reporter_dir) as year_entries:
                for year_entry in year_entries:
                    if not year_entry.is_dir():
                        continue
                    if year_entry.name == "original":
                        continue
                    year_name = year_entry.name
                    try:
                        with os.scandir(year_entry.path) as file_entries:
                            for fe in file_entries:
                                if not fe.name.endswith(".json"):
                                    continue
                                try:
                                    stat = fe.stat()
                                    ctime = getattr(stat, 'st_birthtime', None) or stat.st_ctime
                                    d = date.fromtimestamp(ctime)
                                    daily[d.isoformat()] += 1
                                    yearly[year_name] += 1
                                    total += 1
                                except OSError:
                                    continue
                    except OSError:
                        continue
        except OSError:
            pass

        reporter_daily[reporter] = dict(daily)
        reporter_totals[reporter] = total
        reporter_years[reporter] = dict(yearly)

    return reporter_daily, reporter_totals, reporter_years


def compute_growth_rates(reporter_daily):
    """Compute growth rates for last 7 days and last 30 days."""
    today = date.today()
    growth = {}

    for reporter, daily in reporter_daily.items():
        last_7 = sum(daily.get((today - timedelta(days=i)).isoformat(), 0) for i in range(7))
        last_30 = sum(daily.get((today - timedelta(days=i)).isoformat(), 0) for i in range(30))

        # Daily trend (last 14 days)
        trend = []
        for i in range(13, -1, -1):
            d = (today - timedelta(days=i)).isoformat()
            trend.append({
                "date": d,
                "count": daily.get(d, 0)
            })

        growth[reporter] = {
            "last_7_days": last_7,
            "last_30_days": last_30,
            "avg_per_day_7d": round(last_7 / 7, 1),
            "avg_per_day_30d": round(last_30 / 30, 1),
            "daily_trend_14d": trend
        }

    return growth


def main():
    print("[reporter_growth] Scanning reporter file dates...")
    try:
        reporter_daily, reporter_totals, reporter_years = scan_reporter_dates()
        growth_rates = compute_growth_rates(reporter_daily)

        # Find fastest growing (by last 7 days)
        fastest = max(growth_rates.items(), key=lambda x: x[1]["last_7_days"])
        fastest_reporter = fastest[0] if fastest[1]["last_7_days"] > 0 else "none"

        # Build per-reporter summary
        reporters = {}
        for r in REPORTERS:
            reporters[r] = {
                "total_cases": reporter_totals.get(r, 0),
                "years_covered": sorted(reporter_years.get(r, {}).keys()),
                "year_distribution": reporter_years.get(r, {}),
                "growth": growth_rates.get(r, {})
            }

        output = {
            "generated_at": datetime.now().isoformat(),
            "fastest_growing_7d": fastest_reporter,
            "fastest_growing_rate": growth_rates.get(fastest_reporter, {}).get("last_7_days", 0),
            "total_all_reporters": sum(reporter_totals.values()),
            "reporters": reporters
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print("[OK] Wrote %s" % OUTPUT_FILE)
        print("     Total: %d cases across %d reporters" % (
            output["total_all_reporters"], len([r for r in REPORTERS if reporter_totals.get(r, 0) > 0])))
        print("     Fastest growing (7d): %s (%d new)" % (
            fastest_reporter, output["fastest_growing_rate"]))
        return output

    except Exception as e:
        print("[FAIL] reporter_growth error: %s" % str(e))
        fallback = {
            "generated_at": datetime.now().isoformat(),
            "fastest_growing_7d": "none",
            "total_all_reporters": 0,
            "reporters": {}
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        return fallback


if __name__ == "__main__":
    main()
