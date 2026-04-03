#!/usr/bin/env python3
"""
Migrate old scraper data to progress.json
==========================================
Reads case files from data/pakistanlawsite/cases/ and adds their citations
to data_v2/progress.json so they won't be re-scraped.
"""

import json
import re
from pathlib import Path
from datetime import datetime

# Paths
OLD_CASES_DIR = Path(__file__).parent / "data" / "pakistanlawsite" / "cases"
PROGRESS_FILE = Path(__file__).parent / "data_v2" / "progress.json"

# Reporter code mapping (internal ID prefix -> citation format)
REPORTER_MAP = {
    "S": "SCMR",      # Supreme Court Monthly Review
    "P": "PLD",       # Pakistan Legal Decisions (but also PTD...)
    "M": "MLD",       # Monthly Law Digest
    "C": "CLC",       # Civil Law Cases
    "Cr": "PCrLJ",    # Pakistan Criminal Law Journal
    "T": "PTD",       # Pakistan Tax Decisions
    "L": "PLC",       # Pakistan Labour Cases
    "Y": "YLR",       # Yearly Law Reporter
    "CL": "CLD",      # Company Law Decisions
    "G": "GBLR",      # Gilgit-Baltistan Law Reports
}


def extract_citation_from_text(text: str, case_name: str) -> str | None:
    """Try to extract a proper citation from the case text."""
    if not text:
        return None
    
    # Common citation patterns at start of text
    # e.g., "2024 SCMR 123", "2023 PLD 456", "2024 P T D 789"
    patterns = [
        r"(\d{4})\s+(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d+)",
        r"(\d{4})\s+P\s*T\s*D\s+\(Trib\.?\)\s+(\d+)",  # PTD (Trib.)
        r"(\d{4})\s+P\s*T\s*D\s+(\d+)",  # PTD without parens
        r"(\d{4})\s+P\s*L\s*D\s+(\d+)",  # PLD with spaces
        r"(\d{4})\s+S\s*C\s*M\s*R\s+(\d+)",  # SCMR with spaces
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text[:500])  # Search in first 500 chars
        if match:
            groups = match.groups()
            if len(groups) == 3:
                year, reporter, page = groups
                return f"{year} {reporter} {page}"
            elif len(groups) == 2:
                # PTD pattern
                year, page = groups
                return f"{year} PTD {page}"
    
    return None


def convert_case_id_to_citation(case_id: str) -> str | None:
    """
    Convert old case ID format to citation format.
    e.g., "2024S701" -> "2024 SCMR 701"
    """
    # Pattern: YYYYXnnn where X is reporter code, nnn is page
    match = re.match(r"(\d{4})([A-Za-z]+)(\d+)", case_id)
    if not match:
        return None
    
    year, code, page = match.groups()
    code = code.upper()
    
    # Map code to reporter
    reporter = REPORTER_MAP.get(code)
    if reporter:
        return f"{year} {reporter} {page}"
    
    # Try longer codes
    for prefix, rep in REPORTER_MAP.items():
        if code.startswith(prefix.upper()):
            return f"{year} {rep} {page}"
    
    return None


def load_progress() -> dict:
    """Load existing progress file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed_searches": [], "cases_fetched": [], "total_cases": 0}


def save_progress(progress: dict):
    """Save progress file."""
    progress["last_updated"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)


def main():
    print("=" * 60)
    print("Migrating old scraper data to progress.json")
    print("=" * 60)
    
    # Load current progress
    progress = load_progress()
    existing_cases = set(progress.get("cases_fetched", []))
    
    print(f"Current progress: {len(existing_cases)} cases tracked")
    
    # Find all old case files
    if not OLD_CASES_DIR.exists():
        print(f"ERROR: Old cases directory not found: {OLD_CASES_DIR}")
        return
    
    old_files = list(OLD_CASES_DIR.glob("*.json"))
    print(f"Found {len(old_files)} old case files")
    
    # Process each old case
    added = 0
    skipped = 0
    failed = 0
    
    for case_file in old_files:
        try:
            with open(case_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            case_name = data.get("caseName", case_file.stem)
            text = data.get("text", "") or data.get("title", "")
            
            # Try to extract citation from text first
            citation = extract_citation_from_text(text, case_name)
            
            # Fallback to converting case ID
            if not citation:
                citation = convert_case_id_to_citation(case_name)
            
            if citation:
                if citation in existing_cases:
                    skipped += 1
                else:
                    existing_cases.add(citation)
                    progress["cases_fetched"].append(citation)
                    added += 1
                    if added <= 10:  # Show first 10
                        print(f"  + {citation} (from {case_name})")
            else:
                failed += 1
                if failed <= 5:  # Show first 5 failures
                    print(f"  ? Could not parse: {case_name}")
        
        except Exception as e:
            failed += 1
            print(f"  ERROR reading {case_file.name}: {e}")
    
    # Update totals
    progress["total_cases"] = len(progress["cases_fetched"])
    
    print()
    print(f"Results:")
    print(f"  Added: {added}")
    print(f"  Skipped (already tracked): {skipped}")
    print(f"  Failed to parse: {failed}")
    print(f"  Total tracked now: {progress['total_cases']}")
    
    # Save
    if added > 0:
        save_progress(progress)
        print(f"\nSaved to {PROGRESS_FILE}")
    else:
        print("\nNo changes to save.")


if __name__ == "__main__":
    main()
