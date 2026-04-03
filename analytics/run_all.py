"""
Master Analytics Runner
Runs all analytics scripts in sequence, outputs summary.
Can be called by cron or manually:
    python analytics/run_all.py
    python analytics/run_all.py --no-dashboard  (skip dashboard update)
"""

import os
import sys
import time
import importlib
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)


def run_module(name, module_name):
    """Import and run a module's main() function."""
    print("")
    print("=" * 60)
    print("  %s" % name)
    print("=" * 60)
    start = time.time()
    try:
        mod = importlib.import_module(module_name)
        result = mod.main()
        elapsed = time.time() - start
        print("  [DONE] %.1fs" % elapsed)
        return True, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print("  [FAIL] %s: %s (%.1fs)" % (module_name, str(e), elapsed))
        return False, elapsed


def main():
    print("")
    print("#" * 60)
    print("#  Pakistan Legislation Scraper - Analytics Suite")
    print("#  %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("#" * 60)

    skip_dashboard = "--no-dashboard" in sys.argv

    total_start = time.time()
    results = []

    # 1. Rate Tracker
    ok, t = run_module("1/6 - Real-time Scraping Rate Tracker", "rate_tracker")
    results.append(("rate_tracker", ok, t))

    # 2. Historical Stats
    ok, t = run_module("2/6 - Historical Scraping Stats", "historical_stats")
    results.append(("historical_stats", ok, t))

    # 3. Reporter Growth
    ok, t = run_module("3/6 - Reporter Growth Tracker", "reporter_growth")
    results.append(("reporter_growth", ok, t))

    # 4. Quality Metrics
    ok, t = run_module("4/6 - Data Quality Metrics", "quality_metrics")
    results.append(("quality_metrics", ok, t))

    # 5. Performance
    ok, t = run_module("5/6 - Scraper Performance", "performance")
    results.append(("performance", ok, t))

    # 6. Dashboard Update
    if not skip_dashboard:
        ok, t = run_module("6/6 - Dashboard Integration", "update_dashboard")
        results.append(("update_dashboard", ok, t))
    else:
        print("\n  [SKIP] Dashboard update (--no-dashboard flag)")

    total_time = time.time() - total_start

    # Summary
    print("")
    print("#" * 60)
    print("#  SUMMARY")
    print("#" * 60)
    succeeded = sum(1 for _, ok, _ in results if ok)
    failed = sum(1 for _, ok, _ in results if not ok)
    print("  Total time: %.1fs" % total_time)
    print("  Succeeded: %d | Failed: %d" % (succeeded, failed))
    for name, ok, t in results:
        status = "[OK]" if ok else "[FAIL]"
        print("    %s %s (%.1fs)" % (status, name, t))
    print("")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
