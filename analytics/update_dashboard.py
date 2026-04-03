"""
Dashboard Integration Script
Reads all analytics JSON files and updates the embeddedData
in dashboard/index.html with real analytics data.
Runnable standalone: python analytics/update_dashboard.py
"""

import os
import sys
import json
import re
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DASHBOARD_DIR = os.path.join(os.path.dirname(PROJECT_ROOT), os.pardir, "dashboard")
# Normalize: workspace/dashboard/index.html
DASHBOARD_DIR = os.path.normpath(os.path.join(PROJECT_ROOT, "..", "..", "dashboard"))
DASHBOARD_FILE = os.path.join(DASHBOARD_DIR, "index.html")

# Analytics JSON files
RATE_FILE = os.path.join(SCRIPT_DIR, "scraping_rate_24h.json")
HISTORICAL_FILE = os.path.join(SCRIPT_DIR, "historical_stats.json")
REPORTER_FILE = os.path.join(SCRIPT_DIR, "reporter_growth.json")
QUALITY_FILE = os.path.join(SCRIPT_DIR, "quality_metrics.json")
PERFORMANCE_FILE = os.path.join(SCRIPT_DIR, "performance.json")


def load_json(filepath):
    """Load a JSON file, return None on failure."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print("[WARN] Could not load %s: %s" % (os.path.basename(filepath), str(e)))
        return None


def update_embedded_data(html_content, analytics):
    """Find and update the embeddedData block in the HTML."""
    # Find the embeddedData assignment
    pattern = re.compile(
        r'(const\s+embeddedData\s*=\s*)\{',
        re.MULTILINE
    )
    match = pattern.search(html_content)
    if not match:
        print("[FAIL] Could not find 'const embeddedData = {' in dashboard")
        return html_content

    # Find the matching closing brace by counting braces
    start_pos = match.end() - 1  # Position of opening {
    brace_count = 0
    pos = start_pos
    while pos < len(html_content):
        if html_content[pos] == '{':
            brace_count += 1
        elif html_content[pos] == '}':
            brace_count -= 1
            if brace_count == 0:
                break
        pos += 1

    if brace_count != 0:
        print("[FAIL] Could not find matching closing brace for embeddedData")
        return html_content

    end_pos = pos + 1  # Include the closing }

    # Parse existing embeddedData
    existing_json_str = html_content[start_pos:end_pos]
    try:
        existing_data = json.loads(existing_json_str)
    except json.JSONDecodeError as e:
        print("[WARN] Could not parse existing embeddedData: %s" % str(e))
        print("       Will only update scrapingRate24h")
        existing_data = {}

    # --- Apply analytics updates ---

    # 1. Update scrapingRate24h from rate tracker
    rate_data = analytics.get("rate")
    if rate_data and "hourly_rates" in rate_data:
        existing_data["scrapingRate24h"] = rate_data["hourly_rates"]

    # 2. Update lastUpdated
    existing_data["lastUpdated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    # 3. Add analytics section with all metrics
    analytics_section = {}

    if rate_data:
        analytics_section["scraping_rate"] = {
            "total_last_24h": rate_data.get("total_last_24h", 0),
            "active_hours": rate_data.get("active_hours", 0),
            "avg_per_active_hour": rate_data.get("avg_per_active_hour", 0)
        }

    hist_data = analytics.get("historical")
    if hist_data:
        analytics_section["historical"] = {
            "total_cases": hist_data.get("total_cases", 0),
            "total_days_active": hist_data.get("total_days_active", 0),
            "avg_cases_per_day": hist_data.get("avg_cases_per_day", 0),
            "best_day": hist_data.get("best_day", {}),
            "current_streak_days": hist_data.get("current_streak_days", 0),
            "longest_streak_days": hist_data.get("longest_streak_days", 0),
            "daily_breakdown": hist_data.get("daily_breakdown", []),
            "weekly_cumulative": hist_data.get("weekly_cumulative", [])
        }

    reporter_data = analytics.get("reporter")
    if reporter_data:
        analytics_section["reporter_growth"] = {
            "fastest_growing_7d": reporter_data.get("fastest_growing_7d", "none"),
            "fastest_growing_rate": reporter_data.get("fastest_growing_rate", 0),
            "reporters": {}
        }
        for r, info in reporter_data.get("reporters", {}).items():
            analytics_section["reporter_growth"]["reporters"][r] = {
                "total_cases": info.get("total_cases", 0),
                "last_7_days": info.get("growth", {}).get("last_7_days", 0),
                "last_30_days": info.get("growth", {}).get("last_30_days", 0)
            }

    quality_data = analytics.get("quality")
    if quality_data:
        analytics_section["quality"] = {
            "avg_judgment_length": quality_data.get("avg_judgment_length", 0),
            "short_judgments": quality_data.get("short_judgments_under_500", 0),
            "empty_judgments": quality_data.get("empty_judgments", 0),
            "unique_judges": quality_data.get("unique_judges", 0),
            "avg_citations_per_case": quality_data.get("avg_citations_per_case", 0),
            "citation_coverage_pct": quality_data.get("citation_coverage_pct", 0),
            "completeness": quality_data.get("top_completeness_fields", {})
        }

    perf_data = analytics.get("performance")
    if perf_data:
        overall = perf_data.get("overall", {})
        timing = perf_data.get("timing", {})
        analytics_section["performance"] = {
            "success_rate_pct": overall.get("success_rate_pct", 0),
            "total_errors": overall.get("total_errors", 0),
            "avg_seconds_per_case": timing.get("avg_seconds_per_case", 0),
            "error_breakdown": perf_data.get("error_breakdown", {})
        }

    if analytics_section:
        existing_data["analytics"] = analytics_section

    # Serialize back to JSON with indentation matching the original style
    new_json = json.dumps(existing_data, indent=4)

    # Replace in HTML
    new_html = html_content[:start_pos] + new_json + html_content[end_pos:]

    return new_html


def main():
    print("[update_dashboard] Loading analytics data...")

    # Load all analytics
    analytics = {
        "rate": load_json(RATE_FILE),
        "historical": load_json(HISTORICAL_FILE),
        "reporter": load_json(REPORTER_FILE),
        "quality": load_json(QUALITY_FILE),
        "performance": load_json(PERFORMANCE_FILE)
    }

    loaded = sum(1 for v in analytics.values() if v is not None)
    print("     Loaded %d/5 analytics files" % loaded)

    if loaded == 0:
        print("[WARN] No analytics data found. Run analytics scripts first.")
        print("       Try: python analytics/run_all.py")
        return

    # Read dashboard HTML
    if not os.path.isfile(DASHBOARD_FILE):
        print("[FAIL] Dashboard not found: %s" % DASHBOARD_FILE)
        return

    try:
        with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
            html = f.read()
    except OSError as e:
        print("[FAIL] Could not read dashboard: %s" % str(e))
        return

    # Update embeddedData
    new_html = update_embedded_data(html, analytics)

    # Write back
    try:
        with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
            f.write(new_html)
        print("[OK] Updated %s" % DASHBOARD_FILE)
        print("     Updated: scrapingRate24h, lastUpdated, analytics section")
    except OSError as e:
        print("[FAIL] Could not write dashboard: %s" % str(e))


if __name__ == "__main__":
    main()
