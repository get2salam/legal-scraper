#!/usr/bin/env python3
"""
AUDIT SCRIPT 3: Citation Cross-Reference Discovery (LOCAL ONLY)
================================================================
Parses ALL JSON files, extracts every case citation mentioned in judgment text,
builds a master set of referenced citations, and checks which we DON'T have locally.

This discovers cases that exist (referenced by other cases) but we don't have.

Output: data_v2/audit/crossref_missing.json
"""

import os
import sys
import json
import re
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = AUDIT_DIR / "crossref_missing.json"
PROGRESS_FILE = AUDIT_DIR / "crossref_progress.json"
ALL_REFS_FILE = AUDIT_DIR / "all_citation_references.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

# Citation patterns to match in judgment text
# Standard: "2005 SCMR 123" or "PLD 1990 SC 345"  
# We need to handle both YYYY REPORTER PAGE and REPORTER YYYY COURT PAGE formats
CITATION_PATTERN = re.compile(
    r'\b(\d{4})\s+(SCMR|PLD|PCrLJ|MLD|CLC|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)\b'
)

# PLD has a special format: "PLD 1990 Supreme Court 345" or "PLD 1990 SC 345"  
# But in citation form it's "PLD 1990 SC 345" which becomes "1990 PLD 345" in our system
# Actually, our files use "YYYY REPORTER PAGE" format based on citation field
# Let's also catch PLD-style: "PLD 2015 SC 808"
PLD_PATTERN = re.compile(
    r'\bPLD\s+(\d{4})\s+(?:Supreme\s+Court|SC|Lahore|Lah|Karachi|Kar|Peshawar|Pesh|Quetta|Quet|Sindh|Balochistan|Federal\s+Shariat\s+Court|FSC|AJ&K|Islamabad|ISB)\s+(\d+)\b',
    re.IGNORECASE
)


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"processed_files": [], "phase": "scanning"}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')


def normalize_citation(year, reporter, page):
    """Normalize a citation to standard format: YYYY REPORTER PAGE"""
    return f"{year} {reporter} {page}"


def build_local_citation_set():
    """Build set of all citations we have locally."""
    local_citations = set()
    local_citation_files = {}  # citation -> file path
    
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
                        local_citation_files[citation] = str(json_file.relative_to(DATA_DIR))
                except:
                    pass
    
    return local_citations, local_citation_files


def extract_citations_from_text(text):
    """Extract all case citations from judgment text."""
    citations = set()
    
    if not text:
        return citations
    
    # Standard pattern: YYYY REPORTER PAGE
    for match in CITATION_PATTERN.finditer(text):
        year, reporter, page = match.groups()
        year_int = int(year)
        if 1947 <= year_int <= 2026:
            citations.add(normalize_citation(year, reporter, page))
    
    # PLD special pattern: PLD YYYY COURT PAGE -> normalize to YYYY PLD PAGE
    for match in PLD_PATTERN.finditer(text):
        year, page = match.groups()
        year_int = int(year)
        if 1947 <= year_int <= 2026:
            citations.add(normalize_citation(year, "PLD", page))
    
    return citations


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 3: Citation Cross-Reference Discovery")
    print("=" * 60)

    progress = load_progress()
    start_time = time.time()

    # Phase 1: Build local citation set
    print("\nPhase 1: Building local citation set...")
    local_citations, local_citation_files = build_local_citation_set()
    print(f"  Local citations found: {len(local_citations):,}")

    # Phase 2: Scan all judgments for cited cases
    print("\nPhase 2: Scanning judgment texts for citations...")
    
    all_references = defaultdict(set)  # cited_citation -> set of citing files
    processed_set = set(progress.get("processed_files", []))
    
    total_files = 0
    total_processed = 0
    total_citations_found = 0
    
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        
        reporter_citations = 0
        year_dirs = sorted([d for d in reporter_dir.iterdir() if d.is_dir() and d.name.isdigit()])
        
        for year_dir in year_dirs:
            json_files = list(year_dir.glob("*.json"))
            total_files += len(json_files)
            
            for json_file in json_files:
                file_key = f"{reporter}/{year_dir.name}/{json_file.name}"
                
                if file_key in processed_set:
                    total_processed += 1
                    continue
                
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract from judgment text
                    judgment = data.get("judgment", "")
                    judgment_raw = data.get("judgment_raw", "")
                    headnotes = data.get("headnotes", "")
                    
                    # Combine all text fields
                    full_text = f"{judgment}\n{judgment_raw}\n{headnotes}"
                    
                    cited = extract_citations_from_text(full_text)
                    
                    # Also use cases_cited field if available
                    cases_cited = data.get("cases_cited", [])
                    for cc in cases_cited:
                        cited.add(cc)
                    
                    source_citation = data.get("citation", file_key)
                    
                    for c in cited:
                        # Don't count self-references
                        if c != source_citation:
                            all_references[c].add(source_citation)
                    
                    reporter_citations += len(cited)
                    total_citations_found += len(cited)
                    
                except Exception as e:
                    pass
                
                processed_set.add(file_key)
                total_processed += 1
                
                # Progress update every 5000 files
                if total_processed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_processed / elapsed if elapsed > 0 else 0
                    print(f"  Processed {total_processed:,}/{total_files:,} files | "
                          f"Found {total_citations_found:,} citation refs | "
                          f"Unique cited: {len(all_references):,} | {rate:.0f} files/sec")
                    
                    # Save progress (convert sets to lists for JSON)
                    progress["processed_files"] = list(processed_set)
                    save_progress(progress)
        
        print(f"  {reporter}: {reporter_citations:,} citation references extracted")

    # Phase 3: Find missing citations
    print("\nPhase 3: Identifying missing citations...")
    
    missing_citations = {}
    missing_by_reporter = defaultdict(list)
    
    for cited_citation, citing_sources in all_references.items():
        if cited_citation not in local_citations:
            # Parse the citation to get reporter
            parts = cited_citation.split()
            if len(parts) >= 3:
                year = parts[0]
                reporter = parts[1]
                if reporter in REPORTERS:
                    ref_count = len(citing_sources)
                    missing_citations[cited_citation] = {
                        "citation": cited_citation,
                        "year": int(year),
                        "reporter": reporter,
                        "referenced_by_count": ref_count,
                        "referenced_by_sample": list(citing_sources)[:10],
                    }
                    missing_by_reporter[reporter].append(cited_citation)
    
    # Sort by reference count (most referenced first)
    sorted_missing = sorted(
        missing_citations.values(),
        key=lambda x: -x["referenced_by_count"]
    )

    # Stats
    elapsed = time.time() - start_time
    
    stats = {
        "total_files_processed": total_processed,
        "total_local_citations": len(local_citations),
        "total_unique_citations_referenced": len(all_references),
        "total_missing_citations": len(missing_citations),
        "missing_by_reporter": {r: len(v) for r, v in missing_by_reporter.items()},
        "top_20_most_referenced_missing": [
            {"citation": m["citation"], "ref_count": m["referenced_by_count"]}
            for m in sorted_missing[:20]
        ],
        "elapsed_seconds": round(elapsed, 1),
    }

    # Save output
    output = {
        "audit": "citation_crossref",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": stats,
        "missing_citations": sorted_missing,
    }
    
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    # Save all references for Script 7
    refs_serializable = {k: list(v) for k, v in all_references.items()}
    ALL_REFS_FILE.write_text(json.dumps(refs_serializable, ensure_ascii=False), encoding='utf-8')

    # Save progress
    progress["processed_files"] = list(processed_set)
    progress["phase"] = "complete"
    save_progress(progress)

    # Print summary
    print("\n" + "=" * 60)
    print("CITATION CROSS-REFERENCE ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Files processed: {total_processed:,}")
    print(f"Local citations: {len(local_citations):,}")
    print(f"Unique citations referenced in text: {len(all_references):,}")
    print(f"Missing citations (referenced but not in dataset): {len(missing_citations):,}")
    print(f"Time: {elapsed:.1f}s")
    print()
    print("Missing by reporter:")
    for reporter in REPORTERS:
        count = len(missing_by_reporter.get(reporter, []))
        if count > 0:
            print(f"  {reporter}: {count:,}")
    print()
    print("Top 10 most-referenced missing citations:")
    for m in sorted_missing[:10]:
        print(f"  {m['citation']} (referenced by {m['referenced_by_count']} cases)")
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    print(f"All references saved to: {ALL_REFS_FILE}")


if __name__ == "__main__":
    main()
