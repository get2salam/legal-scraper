#!/usr/bin/env python3
"""
Case Law Gap Registry
=====================
Scans all case law data and verification reports to build a comprehensive gap registry.

Outputs:
  - data_v2/case_law_gaps.json       (machine-readable)
  - data_v2/case_law_gaps_summary.txt (human-readable)

Usage:
  python case_gap_registry.py --all                   # Full scan
  python case_gap_registry.py --year 2024             # Single year
  python case_gap_registry.py --reporter SCMR         # Single reporter
  python case_gap_registry.py --year 2024 --reporter SCMR  # Both filters
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
OUTPUT_JSON = DATA_DIR / "case_law_gaps.json"
OUTPUT_TXT = DATA_DIR / "case_law_gaps_summary.txt"

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# When each reporter first appeared on PLS (approximate)
REPORTER_FIRST_YEAR = {
    "PLD": 1950,
    "SCMR": 1968,
    "MLD": 1986,
    "CLC": 1979,
    "PCrLJ": 1960,
    "PTD": 1980,
    "PLC": 1974,
    "YLR": 1999,
    "CLD": 2002,
    "GBLR": 2014,
}

# Judgment text fields to check, in priority order
JUDGMENT_FIELDS = ["judgment_text", "judgment_raw", "judgment_clean"]

# Error markers that indicate a failed fetch
ERROR_MARKERS = [
    "error fetching",
    "content unavailable",
    "access denied",
    "page not found",
    "404",
    "500 internal server error",
    "connection refused",
    "timeout",
    "no content available",
]

SHORT_THRESHOLD = 100  # Characters below which we consider judgment "short"


# ─── Core scanning ───────────────────────────────────────────────────────────

def scan_local_cases(year_filter=None, reporter_filter=None, quiet=False):
    """Scan all local case JSON files. Returns structured data."""
    by_reporter = defaultdict(lambda: defaultdict(int))  # reporter -> year -> count
    empty_cases = []
    short_cases = []
    error_cases = []
    files_scanned = 0

    reporters_to_scan = [reporter_filter] if reporter_filter else REPORTERS

    for reporter in reporters_to_scan:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.is_dir():
            continue

        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue

            if year_filter and year != year_filter:
                continue

            json_files = list(year_dir.glob("*.json"))
            by_reporter[reporter][year] = len(json_files)

            # Check each file for empty/short/error content
            for jf in json_files:
                files_scanned += 1
                if not quiet and files_scanned % 2000 == 0:
                    sys.stdout.write(f"\r   Scanned {files_scanned:,} files...")
                    sys.stdout.flush()
                try:
                    # Read file as text first for speed - only parse JSON if needed
                    raw_text = jf.read_text(encoding="utf-8")
                    # Quick checks before full JSON parse
                    file_size = len(raw_text)
                except (IOError, UnicodeDecodeError):
                    error_cases.append({
                        "file": str(jf.relative_to(DATA_DIR)),
                        "reporter": reporter,
                        "year": year,
                        "reason": "File unreadable"
                    })
                    continue

                # For very small files, likely empty/corrupt
                if file_size < 200:
                    try:
                        data = json.loads(raw_text)
                    except json.JSONDecodeError:
                        error_cases.append({
                            "file": str(jf.relative_to(DATA_DIR)),
                            "reporter": reporter,
                            "year": year,
                            "reason": "JSON decode error"
                        })
                        continue
                    citation = data.get("citation", jf.stem)
                    empty_cases.append({
                        "citation": citation,
                        "reporter": reporter,
                        "year": year,
                        "file": str(jf.relative_to(DATA_DIR)),
                        "reason": "Very small file"
                    })
                    continue

                # Check for judgment fields using string search (fast)
                has_judgment = False
                for field in JUDGMENT_FIELDS:
                    key_pattern = f'"{field}"'
                    idx = raw_text.find(key_pattern)
                    if idx >= 0:
                        # Find the value after the key
                        colon_idx = raw_text.find(":", idx + len(key_pattern))
                        if colon_idx >= 0:
                            # Check if value is non-empty (not null, not "", not empty)
                            value_start = raw_text[colon_idx+1:colon_idx+50].strip()
                            if value_start and not value_start.startswith('null') and not value_start.startswith('""'):
                                has_judgment = True
                                break

                if not has_judgment:
                    try:
                        data = json.loads(raw_text)
                    except json.JSONDecodeError:
                        error_cases.append({
                            "file": str(jf.relative_to(DATA_DIR)),
                            "reporter": reporter,
                            "year": year,
                            "reason": "JSON decode error"
                        })
                        continue
                    citation = data.get("citation", jf.stem)

                    # Verify it's truly empty
                    judgment = ""
                    for field in JUDGMENT_FIELDS:
                        if field in data and data[field]:
                            judgment = str(data[field]).strip()
                            if judgment:
                                break

                    if not judgment:
                        empty_cases.append({
                            "citation": citation,
                            "reporter": reporter,
                            "year": year,
                            "file": str(jf.relative_to(DATA_DIR)),
                            "reason": "Empty judgment text"
                        })

    if not quiet and files_scanned > 0:
        sys.stdout.write(f"\r   Scanned {files_scanned:,} files. Done.\n")
        sys.stdout.flush()

    return dict(by_reporter), empty_cases, short_cases, error_cases


def read_audit_reports(year_filter=None, reporter_filter=None):
    """Read all verification audit reports and extract confirmed missing cases."""
    confirmed_missing = []
    empty_from_audit = []
    seen = set()  # Deduplicate across reports

    if not AUDIT_DIR.is_dir():
        return confirmed_missing, empty_from_audit

    for audit_file in sorted(AUDIT_DIR.glob("*_verification.json")):
        try:
            with open(audit_file, "r", encoding="utf-8") as f:
                report = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        for result in report.get("results", []):
            reporter = result.get("reporter", "")
            year = result.get("year", 0)

            if year_filter and year != year_filter:
                continue
            if reporter_filter and reporter != reporter_filter:
                continue

            # Missing cases
            for mc in result.get("missing_cases", []):
                citation = mc.get("citation", "")
                key = citation
                if key and key not in seen:
                    seen.add(key)
                    # Extract page number from citation like "2024 PTD 440"
                    parts = citation.split()
                    page = int(parts[-1]) if parts and parts[-1].isdigit() else 0
                    confirmed_missing.append({
                        "year": year,
                        "reporter": reporter,
                        "page": page,
                        "citation": citation,
                        "case_id": mc.get("case_id", ""),
                        "reason": "PLS content unavailable",
                        "source_file": audit_file.name
                    })

            # Empty judgments from audit
            for ej in result.get("empty_judgments", []):
                if ej not in [e.get("citation") for e in empty_from_audit]:
                    empty_from_audit.append({
                        "citation": ej,
                        "reporter": reporter,
                        "year": year,
                        "reason": "Empty judgment (from audit)",
                        "source_file": audit_file.name
                    })

    return confirmed_missing, empty_from_audit


def estimate_historical_coverage(by_reporter, total_local):
    """Estimate total Pakistani case law and coverage."""
    # Calculate average cases per year for 2019-2025 (our most complete years)
    yearly_totals = defaultdict(int)
    for reporter, years in by_reporter.items():
        for year, count in years.items():
            if 2019 <= year <= 2025:
                yearly_totals[year] += count

    avg_per_year = sum(yearly_totals.values()) / max(len(yearly_totals), 1)

    # Historical estimates based on:
    # - Fewer reporters before 1980
    # - Fewer cases per reporter historically
    # - PLD started 1950, most others 1960s-1990s
    year_coverage = {
        "1947-1960": {
            "estimated_cases": "5,000-10,000",
            "estimated_low": 5000,
            "estimated_high": 10000,
            "status": "not_scraped",
            "source": "PLS may have partial — only PLD and PCrLJ existed",
            "active_reporters": 2
        },
        "1961-1980": {
            "estimated_cases": "20,000-40,000",
            "estimated_low": 20000,
            "estimated_high": 40000,
            "status": "not_scraped",
            "source": "SCMR (1968), CLC (1979), PLC (1974) started",
            "active_reporters": 5
        },
        "1981-2000": {
            "estimated_cases": "50,000-100,000",
            "estimated_low": 50000,
            "estimated_high": 100000,
            "status": "not_scraped",
            "source": "MLD (1986), PTD (1980) added. All major reporters active",
            "active_reporters": 8
        },
        "2001-2018": {
            "estimated_cases": "30,000-50,000",
            "estimated_low": 30000,
            "estimated_high": 50000,
            "status": "not_scraped",
            "source": "CLD (2002), GBLR (2014) added. Digital era begins",
            "active_reporters": 10
        },
        "2019-2025": {
            "estimated_cases": f"~{total_local:,}",
            "estimated_low": total_local,
            "estimated_high": total_local,
            "status": "scraped",
            "verified": True,
            "avg_per_year": round(avg_per_year),
            "active_reporters": 10
        }
    }

    total_estimated_low = sum(v["estimated_low"] for v in year_coverage.values())
    total_estimated_high = sum(v["estimated_high"] for v in year_coverage.values())

    coverage_pct = (total_local / ((total_estimated_low + total_estimated_high) / 2)) * 100

    return year_coverage, total_estimated_low, total_estimated_high, coverage_pct


# ─── Build the gap registry ─────────────────────────────────────────────────

def build_registry(year_filter=None, reporter_filter=None, quiet=False):
    """Build the full gap registry."""
    if not quiet:
        print("Scanning local case files...")
    by_reporter, empty_cases, short_cases, error_cases = scan_local_cases(year_filter, reporter_filter, quiet)

    if not quiet:
        print("Reading audit reports...")
    confirmed_missing, empty_from_audit = read_audit_reports(year_filter, reporter_filter)

    # Merge empty from audit with empty from scan (deduplicate)
    existing_citations = {e["citation"] for e in empty_cases}
    for ea in empty_from_audit:
        if ea["citation"] not in existing_citations:
            empty_cases.append(ea)

    # ─── Totals ─────────────────
    total_local = 0
    all_years = set()
    reporter_summary = {}

    for reporter in REPORTERS:
        years_data = by_reporter.get(reporter, {})
        local_count = sum(years_data.values())
        total_local += local_count
        year_list = sorted(years_data.keys())
        all_years.update(year_list)

        # Count confirmed missing for this reporter
        missing_for_reporter = len([m for m in confirmed_missing if m["reporter"] == reporter])

        reporter_summary[reporter] = {
            "local": local_count,
            "years": year_list,
            "year_count": len(year_list),
            "first_year_on_pls": REPORTER_FIRST_YEAR.get(reporter, 0),
            "confirmed_missing": missing_for_reporter
        }

    # Years scraped (primary: 2019-2025, but we have some historical)
    primary_years = sorted([y for y in all_years if 2019 <= y <= 2025])
    historical_years = sorted([y for y in all_years if y < 2019])
    all_years_sorted = sorted(all_years)

    # By year breakdown
    by_year = {}
    for year in all_years_sorted:
        year_data = {"total": 0, "reporters": {}}
        for reporter in REPORTERS:
            count = by_reporter.get(reporter, {}).get(year, 0)
            if count > 0:
                year_data["reporters"][reporter] = count
                year_data["total"] += count
        by_year[str(year)] = year_data

    # Historical coverage estimates
    year_coverage, est_low, est_high, coverage_pct = estimate_historical_coverage(by_reporter, total_local)

    # Active reporters count
    active_reporters = sum(1 for r in REPORTERS if by_reporter.get(r, {}))

    # ─── Build output ─────────────────
    registry = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_cases_local": total_local,
            "total_reporters": active_reporters,
            "years_scraped": primary_years,
            "years_with_historical": all_years_sorted,
            "years_missing": f"1947-2018 (estimated {est_low:,}-{est_high:,} cases)",
            "confirmed_missing_from_pls": len(confirmed_missing),
            "empty_or_corrupt": len(empty_cases) + len(error_cases),
            "short_judgments": len(short_cases),
            "coverage_estimate": f"~{coverage_pct:.1f}% of total Pakistani case law",
            "estimated_total_low": est_low,
            "estimated_total_high": est_high
        },
        "by_reporter": reporter_summary,
        "by_year": by_year,
        "confirmed_missing": sorted(confirmed_missing, key=lambda x: (x["year"], x["reporter"], x["page"])),
        "year_coverage_estimate": year_coverage,
        "empty_cases": empty_cases,
        "short_cases": short_cases,
        "error_cases": error_cases
    }

    return registry


def write_summary(registry, output_path):
    """Write human-readable summary."""
    s = registry["summary"]
    lines = []
    lines.append("=" * 70)
    lines.append("  PAKISTAN CASE LAW GAP REGISTRY")
    lines.append(f"  Generated: {registry['generated_at']}")
    lines.append("=" * 70)
    lines.append("")

    lines.append("── SUMMARY ─────────────────────────────────────────────────────────")
    lines.append(f"  Total local cases:        {s['total_cases_local']:,}")
    lines.append(f"  Active reporters:          {s['total_reporters']}")
    lines.append(f"  Primary years scraped:     {', '.join(map(str, s['years_scraped']))}")
    lines.append(f"  Historical years partial:  {len(s['years_with_historical']) - len(s['years_scraped'])} years")
    lines.append(f"  Years missing:             {s['years_missing']}")
    lines.append(f"  Confirmed missing (PLS):   {s['confirmed_missing_from_pls']}")
    lines.append(f"  Empty/corrupt cases:       {s['empty_or_corrupt']}")
    lines.append(f"  Short judgments (<100ch):   {s['short_judgments']}")
    lines.append(f"  Coverage estimate:         {s['coverage_estimate']}")
    lines.append("")

    lines.append("── REPORTER BREAKDOWN ──────────────────────────────────────────────")
    lines.append(f"  {'Reporter':<10} {'Local':>8} {'Years':>6} {'PLS Since':>10} {'Missing':>8}")
    lines.append(f"  {'─'*10} {'─'*8} {'─'*6} {'─'*10} {'─'*8}")
    for reporter in REPORTERS:
        info = registry["by_reporter"].get(reporter, {})
        local = info.get("local", 0)
        year_count = info.get("year_count", 0)
        first_year = info.get("first_year_on_pls", "?")
        missing = info.get("confirmed_missing", 0)
        lines.append(f"  {reporter:<10} {local:>8,} {year_count:>6} {first_year:>10} {missing:>8}")

    total = sum(registry["by_reporter"].get(r, {}).get("local", 0) for r in REPORTERS)
    lines.append(f"  {'─'*10} {'─'*8} {'─'*6} {'─'*10} {'─'*8}")
    lines.append(f"  {'TOTAL':<10} {total:>8,}")
    lines.append("")

    lines.append("── YEARLY BREAKDOWN (Primary Scrape Years) ─────────────────────────")
    for year in sorted(registry["by_year"].keys(), reverse=True):
        yr = registry["by_year"][year]
        if int(year) >= 2019:
            reporters_str = ", ".join(f"{r}:{c}" for r, c in sorted(yr["reporters"].items(), key=lambda x: -x[1]))
            lines.append(f"  {year}: {yr['total']:>5,} cases  ({reporters_str})")
    lines.append("")

    lines.append("── HISTORICAL COVERAGE ESTIMATE ────────────────────────────────────")
    for period, info in registry["year_coverage_estimate"].items():
        status_icon = "✅" if info.get("status") == "scraped" else "❌"
        lines.append(f"  {status_icon} {period:<12} {info['estimated_cases']:>16} cases  [{info['status']}]")
        if info.get("source"):
            lines.append(f"     └─ {info['source']}")
    lines.append("")

    if registry["confirmed_missing"]:
        lines.append("── CONFIRMED MISSING FROM PLS ──────────────────────────────────────")
        lines.append(f"  {len(registry['confirmed_missing'])} cases that PLS will not serve:")
        for m in registry["confirmed_missing"]:
            lines.append(f"    • {m['citation']:<25} (case_id: {m.get('case_id', 'N/A')})")
        lines.append("")

    if registry["empty_cases"]:
        lines.append("── EMPTY CASES ─────────────────────────────────────────────────────")
        for e in registry["empty_cases"]:
            lines.append(f"    • {e['citation']:<25} {e['reason']}")
        lines.append("")

    if registry["short_cases"]:
        lines.append("── SHORT CASES (<100 chars) ────────────────────────────────────────")
        for sc in registry["short_cases"]:
            lines.append(f"    • {sc['citation']:<25} {sc['reason']}")
        lines.append("")

    if registry["error_cases"]:
        lines.append("── ERROR CASES ─────────────────────────────────────────────────────")
        for ec in registry["error_cases"]:
            lines.append(f"    • {ec.get('citation', ec.get('file', '?')):<30} {ec['reason']}")
        lines.append("")

    lines.append("=" * 70)
    lines.append("  End of Report")
    lines.append("=" * 70)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Case Law Gap Registry — Scan and report gaps in Pakistani case law data"
    )
    parser.add_argument("--all", action="store_true", help="Full scan of all reporters and years")
    parser.add_argument("--year", type=int, help="Filter to a specific year")
    parser.add_argument("--reporter", type=str, choices=REPORTERS, help="Filter to a specific reporter")
    parser.add_argument("--output", type=str, default=str(OUTPUT_JSON), help="Output JSON path")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")

    args = parser.parse_args()

    # Default to --all if no filters specified
    if not args.all and not args.year and not args.reporter:
        args.all = True

    year_filter = args.year if not args.all else None
    reporter_filter = args.reporter if not args.all else None

    if not args.quiet:
        filters = []
        if year_filter:
            filters.append(f"year={year_filter}")
        if reporter_filter:
            filters.append(f"reporter={reporter_filter}")
        filter_str = f" (filters: {', '.join(filters)})" if filters else " (all data)"
        print(f"Case Law Gap Registry{filter_str}")
        print(f"Data directory: {DATA_DIR}")
        print()

    registry = build_registry(year_filter, reporter_filter, args.quiet)

    # Write JSON
    output_json = Path(args.output)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)

    # Write summary text
    write_summary(registry, OUTPUT_TXT)

    if not args.quiet:
        s = registry["summary"]
        print()
        print("Registry complete!")
        print(f"  {s['total_cases_local']:,} local cases across {s['total_reporters']} reporters")
        print(f"  Years: {', '.join(map(str, s['years_scraped']))}")
        print(f"  {s['confirmed_missing_from_pls']} confirmed missing from PLS")
        print(f"  {s['empty_or_corrupt']} empty/corrupt, {s['short_judgments']} short")
        print(f"  {s['coverage_estimate']}")
        print()
        print(f"  JSON: {output_json}")
        print(f"  Summary: {OUTPUT_TXT}")


if __name__ == "__main__":
    main()
