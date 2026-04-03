"""
Historical Scraping Stats
Tracks cumulative cases over time by scanning file creation dates.
Calculates: avg cases/day, best day, current streak, total days active.
Output: analytics/historical_stats.json
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
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "historical_stats.json")

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]


def scan_all_file_dates():
    """Scan all JSON files and collect creation dates."""
    daily_counts = defaultdict(int)
    total_files = 0

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue
        try:
            with os.scandir(reporter_dir) as year_entries:
                for year_entry in year_entries:
                    if not year_entry.is_dir():
                        continue
                    # Skip 'original' subdirectories
                    if year_entry.name == "original":
                        continue
                    try:
                        with os.scandir(year_entry.path) as file_entries:
                            for fe in file_entries:
                                if not fe.name.endswith(".json"):
                                    continue
                                try:
                                    stat = fe.stat()
                                    ctime = getattr(stat, 'st_birthtime', None) or stat.st_ctime
                                    d = date.fromtimestamp(ctime)
                                    daily_counts[d.isoformat()] += 1
                                    total_files += 1
                                except OSError:
                                    continue
                    except OSError:
                        continue
        except OSError:
            continue

    return daily_counts, total_files


def compute_stats(daily_counts):
    """Compute historical statistics from daily counts."""
    if not daily_counts:
        return {
            "total_cases": 0,
            "total_days_active": 0,
            "avg_cases_per_day": 0,
            "best_day": {"date": "N/A", "count": 0},
            "worst_active_day": {"date": "N/A", "count": 0},
            "current_streak_days": 0,
            "longest_streak_days": 0,
            "first_scrape_date": "N/A",
            "last_scrape_date": "N/A",
            "daily_breakdown": []
        }

    sorted_dates = sorted(daily_counts.keys())
    total_cases = sum(daily_counts.values())
    total_days = len(sorted_dates)

    # Best and worst day
    best_date = max(daily_counts, key=daily_counts.get)
    worst_date = min(daily_counts, key=daily_counts.get)

    # Current streak (consecutive days ending today or yesterday)
    today = date.today()
    current_streak = 0
    check_date = today
    for _ in range(365):
        if check_date.isoformat() in daily_counts:
            current_streak += 1
            check_date -= timedelta(days=1)
        else:
            break

    # Longest streak
    longest_streak = 0
    streak = 0
    date_set = set(sorted_dates)
    if sorted_dates:
        d = date.fromisoformat(sorted_dates[0])
        end = date.fromisoformat(sorted_dates[-1])
        while d <= end:
            if d.isoformat() in date_set:
                streak += 1
                longest_streak = max(longest_streak, streak)
            else:
                streak = 0
            d += timedelta(days=1)

    # Build daily breakdown (last 30 days)
    daily_breakdown = []
    for i in range(29, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        daily_breakdown.append({
            "date": d,
            "count": daily_counts.get(d, 0)
        })

    # Cumulative by week (last 12 weeks)
    weekly_cumulative = []
    for w in range(11, -1, -1):
        week_start = today - timedelta(days=today.weekday() + 7 * w)
        week_end = week_start + timedelta(days=6)
        week_total = 0
        d = week_start
        while d <= week_end:
            week_total += daily_counts.get(d.isoformat(), 0)
            d += timedelta(days=1)
        weekly_cumulative.append({
            "week_start": week_start.isoformat(),
            "count": week_total
        })

    return {
        "total_cases": total_cases,
        "total_days_active": total_days,
        "avg_cases_per_day": round(total_cases / total_days, 1) if total_days > 0 else 0,
        "best_day": {"date": best_date, "count": daily_counts[best_date]},
        "worst_active_day": {"date": worst_date, "count": daily_counts[worst_date]},
        "current_streak_days": current_streak,
        "longest_streak_days": longest_streak,
        "first_scrape_date": sorted_dates[0],
        "last_scrape_date": sorted_dates[-1],
        "daily_breakdown": daily_breakdown,
        "weekly_cumulative": weekly_cumulative
    }


def main():
    print("[historical_stats] Scanning all file creation dates...")
    try:
        daily_counts, total_files = scan_all_file_dates()
        stats = compute_stats(daily_counts)
        stats["generated_at"] = datetime.now().isoformat()
        stats["files_scanned"] = total_files

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)

        print("[OK] Wrote %s (%d cases across %d active days)" % (
            OUTPUT_FILE, stats["total_cases"], stats["total_days_active"]))
        if stats["best_day"]["date"] != "N/A":
            print("     Best day: %s with %d cases" % (
                stats["best_day"]["date"], stats["best_day"]["count"]))
            print("     Current streak: %d days" % stats["current_streak_days"])
        return stats

    except Exception as e:
        print("[FAIL] historical_stats error: %s" % str(e))
        fallback = {
            "generated_at": datetime.now().isoformat(),
            "total_cases": 0,
            "total_days_active": 0,
            "avg_cases_per_day": 0,
            "best_day": {"date": "N/A", "count": 0},
            "current_streak_days": 0,
            "daily_breakdown": [],
            "weekly_cumulative": []
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        return fallback


if __name__ == "__main__":
    main()
