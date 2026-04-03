"""
Data Quality Metrics
Scans all JSON case files and computes quality metrics:
- Average judgment length, missing fields, short judgments,
  files per year, citation density, judge coverage.
Output: analytics/quality_metrics.json

Optimized: samples up to 200 files per reporter/year for quality analysis
but counts all files for totals.
"""

import os
import sys
import json
import time
from datetime import datetime
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data_v2")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "quality_metrics.json")

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Fields to check for completeness
REQUIRED_FIELDS = ["title", "judges", "date", "judgment_clean", "court", "headnotes"]

# Sample limit per year directory for deep analysis
SAMPLE_PER_YEAR = 50


def analyze_files():
    """Scan all case JSON files and compute quality metrics."""
    total_files = 0
    sampled_files = 0
    total_judgment_len = 0
    short_judgments = 0  # < 500 chars
    empty_judgments = 0
    missing_field_counts = defaultdict(int)  # field -> count missing
    files_per_year = defaultdict(int)
    files_per_reporter = defaultdict(int)
    total_citations = 0
    files_with_citations = 0
    all_judges = set()
    judge_per_reporter = defaultdict(set)
    files_with_no_judges = 0
    corrupt_files = 0

    for reporter in REPORTERS:
        reporter_dir = os.path.join(DATA_DIR, reporter)
        if not os.path.isdir(reporter_dir):
            continue

        try:
            year_dirs = []
            with os.scandir(reporter_dir) as year_entries:
                for year_entry in year_entries:
                    if year_entry.is_dir() and year_entry.name != "original":
                        year_dirs.append(year_entry)
        except OSError:
            continue

        for year_entry in year_dirs:
            year_name = year_entry.name
            year_sample_count = 0

            try:
                json_files = []
                with os.scandir(year_entry.path) as file_entries:
                    for fe in file_entries:
                        if fe.name.endswith(".json"):
                            json_files.append(fe)
            except OSError:
                continue

            # Count all files for totals
            year_file_count = len(json_files)
            files_per_year[year_name] += year_file_count
            files_per_reporter[reporter] += year_file_count
            total_files += year_file_count

            # Sample subset for deep analysis
            sample = json_files[:SAMPLE_PER_YEAR]

            for fe in sample:
                try:
                    with open(fe.path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                    corrupt_files += 1
                    sampled_files += 1
                    continue

                sampled_files += 1

                # Judgment length
                jc = data.get("judgment_clean") or ""
                jlen = len(jc)
                total_judgment_len += jlen
                if jlen == 0:
                    empty_judgments += 1
                elif jlen < 500:
                    short_judgments += 1

                # Missing fields
                for field in REQUIRED_FIELDS:
                    val = data.get(field)
                    if val is None or val == "" or val == []:
                        missing_field_counts[field] += 1

                # Citations
                cases_cited = data.get("cases_cited") or []
                statutes_cited = data.get("statutes_cited") or []
                citation_count = len(cases_cited) + len(statutes_cited)
                total_citations += citation_count
                if citation_count > 0:
                    files_with_citations += 1

                # Judges
                judges = data.get("judges") or []
                if isinstance(judges, list) and len(judges) > 0:
                    for j in judges:
                        if isinstance(j, str) and j.strip():
                            all_judges.add(j.strip())
                            judge_per_reporter[reporter].add(j.strip())
                else:
                    files_with_no_judges += 1

        sys.stdout.write("  [%s] %d files counted, %d sampled\n" % (reporter, files_per_reporter.get(reporter, 0), sampled_files))
        sys.stdout.flush()

    # Scale sampled metrics to estimate totals
    scale = total_files / sampled_files if sampled_files > 0 else 1

    return {
        "total_files": total_files,
        "sampled_files": sampled_files,
        "sample_scale_factor": round(scale, 2),
        "avg_judgment_length": round(total_judgment_len / sampled_files, 0) if sampled_files > 0 else 0,
        "short_judgments_under_500": short_judgments,
        "short_judgments_estimated": round(short_judgments * scale),
        "empty_judgments": empty_judgments,
        "empty_judgments_estimated": round(empty_judgments * scale),
        "corrupt_files": corrupt_files,
        "missing_fields": dict(missing_field_counts),
        "missing_fields_estimated": {k: round(v * scale) for k, v in missing_field_counts.items()},
        "files_per_year": dict(sorted(files_per_year.items())),
        "files_per_reporter": dict(sorted(files_per_reporter.items())),
        "total_citations_in_sample": total_citations,
        "files_with_citations_in_sample": files_with_citations,
        "avg_citations_per_case": round(total_citations / sampled_files, 2) if sampled_files > 0 else 0,
        "citation_coverage_pct": round(100 * files_with_citations / sampled_files, 1) if sampled_files > 0 else 0,
        "unique_judges": len(all_judges),
        "files_with_no_judges_in_sample": files_with_no_judges,
        "judges_per_reporter": {r: len(s) for r, s in judge_per_reporter.items()},
        "top_completeness_fields": {
            field: round(100 * (1 - missing_field_counts.get(field, 0) / sampled_files), 1) if sampled_files > 0 else 0
            for field in REQUIRED_FIELDS
        }
    }


def main():
    print("[quality_metrics] Scanning case files for quality analysis...")
    start = time.time()
    try:
        metrics = analyze_files()
        metrics["generated_at"] = datetime.now().isoformat()
        metrics["scan_time_seconds"] = round(time.time() - start, 1)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        print("[OK] Wrote %s (sampled %d / %d files in %.1fs)" % (
            OUTPUT_FILE, metrics["sampled_files"], metrics["total_files"], metrics["scan_time_seconds"]))
        print("     Avg judgment: %d chars | %d unique judges" % (
            metrics["avg_judgment_length"], metrics["unique_judges"]))
        print("     Short (<500): %d | Empty: %d | No judges: %d" % (
            metrics["short_judgments_under_500"], metrics["empty_judgments"],
            metrics["files_with_no_judges_in_sample"]))
        return metrics

    except Exception as e:
        elapsed = time.time() - start
        print("[FAIL] quality_metrics error: %s (after %.1fs)" % (str(e), elapsed))
        import traceback
        traceback.print_exc()
        fallback = {
            "generated_at": datetime.now().isoformat(),
            "total_files": 0,
            "avg_judgment_length": 0,
            "unique_judges": 0,
            "error": str(e)
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        return fallback


if __name__ == "__main__":
    main()
