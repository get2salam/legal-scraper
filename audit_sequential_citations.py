#!/usr/bin/env python3
"""
AUDIT SCRIPT 2: Sequential Citation Crawl
===========================================
For each reporter/year where Script 1 found gaps:
- Get the list of all citation numbers PLS has for that reporter/year
- Compare with our local citation numbers
- Output exact citation IDs we're missing

Depends on: audit_pls_counts.py output (pls_counts_progress.json)

Output: data_v2/audit/missing_citations.json
"""

import os
import sys
import json
import re
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
OUTPUT_FILE = AUDIT_DIR / "missing_citations.json"
PLS_PROGRESS_FILE = AUDIT_DIR / "pls_counts_progress.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]


def get_local_citations(reporter, year):
    """Get set of citation strings we have locally."""
    year_dir = DATA_DIR / reporter / str(year)
    if not year_dir.exists():
        return set()
    citations = set()
    for f in year_dir.glob("*.json"):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            cit = data.get("citation", "")
            if cit:
                citations.add(cit)
        except:
            pass
    return citations


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 2: Sequential Citation Comparison")
    print("=" * 60)

    # Load PLS counts progress which has the PLS citation lists
    if not PLS_PROGRESS_FILE.exists():
        print("ERROR: pls_counts_progress.json not found! Run audit_pls_counts.py first.")
        sys.exit(1)

    with open(PLS_PROGRESS_FILE, 'r', encoding='utf-8') as f:
        pls_data = json.load(f)

    completed = pls_data.get("completed", {})
    pls_citations = pls_data.get("pls_citations", {})

    all_missing = {}
    total_missing = 0
    total_extra_local = 0
    total_gaps_checked = 0

    for reporter in REPORTERS:
        reporter_counts = completed.get(reporter, {})
        reporter_pls_cits = pls_citations.get(reporter, {})
        reporter_missing = []

        for year_str, counts in sorted(reporter_counts.items()):
            missing_count = counts.get("missing", 0)
            pls_count = counts.get("pls", 0)
            local_count = counts.get("local", 0)

            # Check all years with PLS data, not just gaps
            if pls_count == 0 and local_count == 0:
                continue

            pls_cit_list = reporter_pls_cits.get(year_str, [])
            if not pls_cit_list and pls_count == 0:
                continue

            local_cits = get_local_citations(reporter, int(year_str))
            pls_cit_set = set(pls_cit_list)

            # Find what's in PLS but not local
            missing = pls_cit_set - local_cits
            # Find what's local but not in PLS (extra)
            extra = local_cits - pls_cit_set

            if missing:
                for m in sorted(missing):
                    reporter_missing.append({
                        "citation": m,
                        "year": int(year_str),
                        "reporter": reporter,
                    })
                total_missing += len(missing)
                print(f"  {year_str} {reporter}: {len(missing)} missing citations ({len(extra)} extra local)")

            if extra and pls_count > 0:
                total_extra_local += len(extra)

            total_gaps_checked += 1

        if reporter_missing:
            all_missing[reporter] = reporter_missing

    # Summary
    stats = {
        "total_missing_citations": total_missing,
        "total_extra_local": total_extra_local,
        "years_checked": total_gaps_checked,
        "missing_by_reporter": {r: len(v) for r, v in all_missing.items()},
    }

    # Flatten for easy consumption
    flat_missing = []
    for reporter, items in all_missing.items():
        flat_missing.extend(items)
    flat_missing.sort(key=lambda x: (x["reporter"], x["year"], x["citation"]))

    output = {
        "audit": "sequential_citations",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": stats,
        "missing_citations": flat_missing,
        "by_reporter": all_missing,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    print("\n" + "=" * 60)
    print("SEQUENTIAL CITATION COMPARISON COMPLETE")
    print("=" * 60)
    print(f"Total missing citations: {total_missing:,}")
    print(f"Extra local (not in PLS): {total_extra_local:,}")
    print()
    print("Missing by reporter:")
    for r, items in all_missing.items():
        print(f"  {r}: {len(items):,}")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
