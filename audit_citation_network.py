#!/usr/bin/env python3
"""
AUDIT SCRIPT 7: Linked Case Discovery — Citation Network Analysis (LOCAL)
==========================================================================
Uses the cross-reference data from Script 3 to:
- Build the full citation network
- Identify clusters of cases that cite each other
- Find "orphan" citations — cases referenced 5+ times but missing from our dataset
- These are HIGH PRIORITY missing cases

Depends on: audit_citation_crossref.py output (all_citation_references.json)

Output: data_v2/audit/orphan_citations.json
"""

import sys
import json
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
ALL_REFS_FILE = AUDIT_DIR / "all_citation_references.json"
OUTPUT_FILE = AUDIT_DIR / "orphan_citations.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]


def build_local_citation_set():
    """Build set of all citations we have locally."""
    local_citations = set()
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        for year_dir in reporter_dir.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            for json_file in year_dir.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    citation = data.get("citation", "")
                    if citation:
                        local_citations.add(citation)
                except:
                    pass
    return local_citations


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 7: Citation Network / Orphan Detection")
    print("=" * 60)
    
    start_time = time.time()

    # Load cross-reference data
    if not ALL_REFS_FILE.exists():
        print("ERROR: all_citation_references.json not found! Run audit_citation_crossref.py first.")
        sys.exit(1)

    print("Loading citation reference data...")
    with open(ALL_REFS_FILE, 'r', encoding='utf-8') as f:
        all_references = json.load(f)
    
    print(f"  {len(all_references):,} unique citations referenced")

    # Build local citation set
    print("Building local citation set...")
    local_citations = build_local_citation_set()
    print(f"  {len(local_citations):,} local citations")

    # Identify orphan citations (missing, referenced 5+ times)
    print("\nAnalyzing citation network...")
    
    orphans = []
    missing_all = []
    ref_count_distribution = defaultdict(int)
    
    for citation, referencing_cases in all_references.items():
        ref_count = len(referencing_cases)
        ref_count_distribution[min(ref_count, 50)] += 1
        
        if citation not in local_citations:
            # Parse citation
            parts = citation.split()
            if len(parts) >= 3:
                year = parts[0]
                reporter = parts[1]
                if reporter in REPORTERS:
                    entry = {
                        "citation": citation,
                        "year": int(year),
                        "reporter": reporter,
                        "reference_count": ref_count,
                        "referenced_by": list(referencing_cases)[:20],
                        "priority": "HIGH" if ref_count >= 5 else "MEDIUM" if ref_count >= 3 else "LOW"
                    }
                    missing_all.append(entry)
                    if ref_count >= 5:
                        orphans.append(entry)
    
    # Sort by reference count
    orphans.sort(key=lambda x: -x["reference_count"])
    missing_all.sort(key=lambda x: -x["reference_count"])

    # Network statistics
    # How many citations in our dataset reference other citations in our dataset?
    internal_edges = 0
    external_edges = 0
    for citation, refs in all_references.items():
        if citation in local_citations:
            internal_edges += len(refs)
        else:
            external_edges += len(refs)

    # Orphan statistics by reporter and year
    orphan_by_reporter = defaultdict(int)
    orphan_by_year = defaultdict(int)
    for o in orphans:
        orphan_by_reporter[o["reporter"]] += 1
        orphan_by_year[o["year"]] += 1

    missing_by_reporter = defaultdict(int)
    missing_by_year = defaultdict(int)
    for m in missing_all:
        missing_by_reporter[m["reporter"]] += 1
        missing_by_year[m["year"]] += 1

    elapsed = time.time() - start_time

    stats = {
        "total_unique_citations_in_network": len(all_references),
        "citations_in_local_dataset": len(local_citations),
        "total_missing_from_network": len(missing_all),
        "high_priority_orphans_5plus_refs": len(orphans),
        "medium_priority_3plus_refs": len([m for m in missing_all if m["reference_count"] >= 3]),
        "internal_edges": internal_edges,
        "external_edges": external_edges,
        "orphan_by_reporter": dict(orphan_by_reporter),
        "orphan_by_year": {str(k): v for k, v in sorted(orphan_by_year.items())},
        "missing_by_reporter": dict(missing_by_reporter),
        "elapsed_seconds": round(elapsed, 1),
    }

    output = {
        "audit": "citation_network",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": stats,
        "high_priority_orphans": orphans,
        "all_missing_referenced_citations": missing_all[:5000],  # Top 5000 by ref count
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    # Print summary
    print("\n" + "=" * 60)
    print("CITATION NETWORK ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Total unique citations in network: {len(all_references):,}")
    print(f"Citations in our dataset: {len(local_citations):,}")
    print(f"Missing citations (referenced but not in dataset): {len(missing_all):,}")
    print(f"HIGH PRIORITY orphans (referenced 5+ times): {len(orphans):,}")
    print(f"MEDIUM PRIORITY (referenced 3+ times): {len([m for m in missing_all if m['reference_count'] >= 3]):,}")
    print(f"Time: {elapsed:.1f}s")
    print()
    print("High-priority orphans by reporter:")
    for reporter in REPORTERS:
        count = orphan_by_reporter.get(reporter, 0)
        if count > 0:
            print(f"  {reporter}: {count:,}")
    print()
    print("Top 20 most-referenced missing cases:")
    for o in orphans[:20]:
        print(f"  {o['citation']} (referenced by {o['reference_count']} cases)")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
