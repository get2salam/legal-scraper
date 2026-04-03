"""
Scraper Performance Tracker
Parses log files to extract: success rate, average time per case,
error types, downtime periods, network error patterns.
Output: analytics/performance.json
"""

import os
import sys
import json
import re
import time
from datetime import datetime, timedelta
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "performance.json")

# Regex patterns for log parsing
TIME_PATTERN = re.compile(r'^(\d{2}:\d{2}:\d{2})\s*\|')
FETCH_PATTERN = re.compile(r'Fetching:\s+(\d+)\s+(\w+)\s+(\d+)')
SAVED_PATTERN = re.compile(r'Saved:\s+(\d+)\s+(\w+)\s+(\d+)')
ERROR_PATTERN = re.compile(r'ERROR\s*\|(.+)')
WARNING_PATTERN = re.compile(r'WARNING\s*\|(.+)')
SKIP_PATTERN = re.compile(r'Skipping\s+(\S+)')
FOUND_PATTERN = re.compile(r'Found\s+(\d+)\s+cases')
FAILED_FETCH_PATTERN = re.compile(r'Failed to fetch')
CURL_ERROR_PATTERN = re.compile(r'curl:\s*\((\d+)\)')
TIMEOUT_PATTERN = re.compile(r'timed?\s*out|timeout', re.IGNORECASE)
CONNECTION_PATTERN = re.compile(r'[Cc]onnection\s+(was\s+)?reset|refused|closed')


def parse_log_file(filepath):
    """Parse a single log file and extract performance data."""
    fetches = []
    saves = []
    errors = []
    warnings = []
    skips = 0
    total_cases_found = 0
    timestamps = []
    curl_errors = defaultdict(int)

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            prev_fetch_time = None
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # Extract timestamp
                tm = TIME_PATTERN.match(line)
                ts = None
                if tm:
                    try:
                        ts = datetime.strptime(tm.group(1), "%H:%M:%S")
                        timestamps.append(ts)
                    except ValueError:
                        pass

                # Track fetches
                fm = FETCH_PATTERN.search(line)
                if fm and ts:
                    fetches.append({"time": ts, "year": fm.group(1), "reporter": fm.group(2), "case": fm.group(3)})
                    prev_fetch_time = ts

                # Track saves
                sm = SAVED_PATTERN.search(line)
                if sm and ts:
                    save_info = {"time": ts, "year": sm.group(1), "reporter": sm.group(2), "case": sm.group(3)}
                    if prev_fetch_time:
                        delta = (ts - prev_fetch_time).total_seconds()
                        if 0 < delta < 300:  # Reasonable range
                            save_info["fetch_time_seconds"] = delta
                    saves.append(save_info)

                # Track errors
                em = ERROR_PATTERN.search(line)
                if em:
                    error_msg = em.group(1).strip()
                    errors.append({"time": ts, "message": error_msg[:200]})

                    # Classify curl errors
                    ce = CURL_ERROR_PATTERN.search(error_msg)
                    if ce:
                        curl_errors["curl_%s" % ce.group(1)] += 1
                    elif TIMEOUT_PATTERN.search(error_msg):
                        curl_errors["timeout"] += 1
                    elif CONNECTION_PATTERN.search(error_msg):
                        curl_errors["connection_reset"] += 1

                # Track warnings
                wm = WARNING_PATTERN.search(line)
                if wm:
                    warnings.append(wm.group(1).strip()[:200])

                # Track skips
                if SKIP_PATTERN.search(line):
                    skips += 1

                # Track found cases
                found = FOUND_PATTERN.search(line)
                if found:
                    total_cases_found += int(found.group(1))

                # Failed fetches
                if FAILED_FETCH_PATTERN.search(line):
                    curl_errors["failed_fetch"] += 1

    except (OSError, IOError):
        pass

    return {
        "fetches": len(fetches),
        "saves": len(saves),
        "errors": len(errors),
        "warnings": len(warnings),
        "skips": skips,
        "total_cases_found": total_cases_found,
        "curl_errors": dict(curl_errors),
        "fetch_times": [s.get("fetch_time_seconds") for s in saves if "fetch_time_seconds" in s],
        "timestamps": timestamps,
        "error_messages": errors[:50]  # Keep last 50 for analysis
    }


def find_downtime_gaps(all_timestamps):
    """Find gaps > 30 minutes in activity as downtime periods."""
    if len(all_timestamps) < 2:
        return []

    sorted_ts = sorted(all_timestamps)
    gaps = []
    for i in range(1, len(sorted_ts)):
        delta = (sorted_ts[i] - sorted_ts[i-1]).total_seconds()
        if delta > 1800:  # 30 minutes
            gaps.append({
                "start": sorted_ts[i-1].strftime("%H:%M:%S"),
                "end": sorted_ts[i].strftime("%H:%M:%S"),
                "duration_minutes": round(delta / 60, 1)
            })

    return gaps


def main():
    print("[performance] Parsing scraper log files...")
    start = time.time()

    try:
        if not os.path.isdir(LOGS_DIR):
            print("[WARN] Logs directory not found: %s" % LOGS_DIR)
            raise FileNotFoundError("Logs directory not found")

        all_stats = {
            "total_fetches": 0,
            "total_saves": 0,
            "total_errors": 0,
            "total_warnings": 0,
            "total_skips": 0,
            "all_fetch_times": [],
            "all_curl_errors": defaultdict(int),
            "all_timestamps": [],
            "log_files_parsed": 0,
            "per_log": {}
        }

        with os.scandir(LOGS_DIR) as entries:
            for entry in entries:
                if not entry.name.endswith(".log"):
                    continue
                # Only parse stderr logs and main logs (they have the useful info)
                if "stdout" in entry.name:
                    continue

                result = parse_log_file(entry.path)
                all_stats["total_fetches"] += result["fetches"]
                all_stats["total_saves"] += result["saves"]
                all_stats["total_errors"] += result["errors"]
                all_stats["total_warnings"] += result["warnings"]
                all_stats["total_skips"] += result["skips"]
                all_stats["all_fetch_times"].extend(result["fetch_times"])
                for k, v in result["curl_errors"].items():
                    all_stats["all_curl_errors"][k] += v
                all_stats["all_timestamps"].extend(result["timestamps"])
                all_stats["log_files_parsed"] += 1

                all_stats["per_log"][entry.name] = {
                    "fetches": result["fetches"],
                    "saves": result["saves"],
                    "errors": result["errors"],
                    "warnings": result["warnings"],
                    "success_rate": round(100 * result["saves"] / result["fetches"], 1) if result["fetches"] > 0 else 0
                }

        # Compute aggregates
        fetch_times = all_stats["all_fetch_times"]
        avg_time = round(sum(fetch_times) / len(fetch_times), 1) if fetch_times else 0
        median_time = sorted(fetch_times)[len(fetch_times) // 2] if fetch_times else 0

        total_f = all_stats["total_fetches"]
        total_s = all_stats["total_saves"]
        success_rate = round(100 * total_s / total_f, 1) if total_f > 0 else 0

        downtime_gaps = find_downtime_gaps(all_stats["all_timestamps"])

        output = {
            "generated_at": datetime.now().isoformat(),
            "scan_time_seconds": round(time.time() - start, 1),
            "log_files_parsed": all_stats["log_files_parsed"],
            "overall": {
                "total_fetch_attempts": total_f,
                "total_successful_saves": total_s,
                "total_errors": all_stats["total_errors"],
                "total_warnings": all_stats["total_warnings"],
                "total_skipped": all_stats["total_skips"],
                "success_rate_pct": success_rate,
                "failure_rate_pct": round(100 - success_rate, 1)
            },
            "timing": {
                "avg_seconds_per_case": avg_time,
                "median_seconds_per_case": round(median_time, 1),
                "min_seconds": round(min(fetch_times), 1) if fetch_times else 0,
                "max_seconds": round(max(fetch_times), 1) if fetch_times else 0,
                "samples": len(fetch_times)
            },
            "error_breakdown": dict(all_stats["all_curl_errors"]),
            "downtime_gaps": downtime_gaps[:20],  # Top 20 gaps
            "per_log_summary": all_stats["per_log"]
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print("[OK] Wrote %s (parsed %d logs in %.1fs)" % (
            OUTPUT_FILE, all_stats["log_files_parsed"], output["scan_time_seconds"]))
        print("     Success rate: %.1f%% (%d/%d)" % (success_rate, total_s, total_f))
        print("     Avg time/case: %.1fs | Errors: %d" % (avg_time, all_stats["total_errors"]))
        return output

    except Exception as e:
        print("[FAIL] performance error: %s" % str(e))
        fallback = {
            "generated_at": datetime.now().isoformat(),
            "overall": {
                "total_fetch_attempts": 0,
                "total_successful_saves": 0,
                "success_rate_pct": 0,
                "total_errors": 0
            },
            "timing": {"avg_seconds_per_case": 0},
            "error_breakdown": {},
            "error": str(e)
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        return fallback


if __name__ == "__main__":
    main()
