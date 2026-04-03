#!/usr/bin/env python3
"""
Migrate old scraper data to progress.json v2
=============================================
Better parsing - uses title field and handles spaced reporter names.
"""

import json
import re
from pathlib import Path
from datetime import datetime

# Paths
OLD_CASES_DIR = Path(__file__).parent / "data" / "pakistanlawsite" / "cases"
PROGRESS_FILE = Path(__file__).parent / "data_v2" / "progress.json"


def normalize_reporter(text: str) -> str:
    """Normalize reporter names with spaces to standard format."""
    # Remove spaces within reporter abbreviations
    replacements = {
        "P L D": "PLD",
        "S C M R": "SCMR", 
        "M L D": "MLD",
        "C L C": "CLC",
        "P Cr L J": "PCrLJ",
        "P Cr.L J": "PCrLJ",
        "P Cr. L J": "PCrLJ",
        "P Cr. LJ": "PCrLJ",
        "P Cr.LJ": "PCrLJ",
        "P Cr": "PCrLJ",  # Handle truncated
        "PCr.LJ": "PCrLJ",
        "P T D (Trib.)": "PTD",
        "P T D (Trib": "PTD",
        "P T D": "PTD",
        "PTD (Trib.)": "PTD",
        "PTD (Trib": "PTD",
        "P L C": "PLC",
        "Y L R": "YLR",
        "C L D": "CLD",
        "G B L R": "GBLR",
    }
    for spaced, normal in replacements.items():
        text = text.replace(spaced, normal)
    return text


def extract_from_metadata(data: dict) -> str | None:
    """Extract citation from book/year metadata fields."""
    book = data.get("book", "")
    year = data.get("year")
    case_id = data.get("id") or data.get("caseName", "")
    
    if not book or not year:
        return None
    
    # Map book codes to reporters
    book_map = {
        "SCMR": "SCMR", "PLD": "PLD", "MLD": "MLD", "CLC": "CLC",
        "PCrLJ": "PCrLJ", "PTD": "PTD", "PLC": "PLC", "YLR": "YLR",
        "CLD": "CLD", "GBLR": "GBLR"
    }
    
    reporter = book_map.get(book.upper())
    if not reporter:
        return None
    
    # Try to extract page from case_id (e.g., "2024S890" -> 890)
    match = re.search(r"\d{4}[A-Za-z]+(\d+)", case_id)
    if match:
        page = match.group(1)
        return f"{year} {reporter} {page}"
    
    return None


def extract_citation_from_title(title: str) -> str | None:
    """Extract citation from title field."""
    if not title:
        return None
    
    # Normalize spaces in reporter names
    title = normalize_reporter(title)
    
    # Pattern: YEAR REPORTER PAGE or REPORTER YEAR ... PAGE
    # e.g., "2023 PLD 84", "PLD 2023 Sindh 1", "2023 CLC 207"
    
    # Try: YEAR REPORTER PAGE
    match = re.search(r"(\d{4})\s+(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d+)", title)
    if match:
        year, reporter, page = match.groups()
        return f"{year} {reporter} {page}"
    
    # Try: REPORTER YEAR ... PAGE (for "PLD 2023 Sindh 1" format)
    match = re.search(r"(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d{4})\s+.*?(\d+)\s*$", title)
    if match:
        reporter, year, page = match.groups()
        return f"{year} {reporter} {page}"
    
    # Try: REPORTER YEAR ... PAGE (anywhere in string, less strict)
    match = re.search(r"(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d{4})\D+(\d+)", title)
    if match:
        reporter, year, page = match.groups()
        return f"{year} {reporter} {page}"
    
    return None


def extract_citation_from_text(text: str) -> str | None:
    """Fallback: extract citation from judgment text."""
    if not text:
        return None
    
    # Check first 1000 chars
    text = normalize_reporter(text[:1000])
    
    # Same patterns as above
    match = re.search(r"(\d{4})\s+(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d+)", text)
    if match:
        year, reporter, page = match.groups()
        return f"{year} {reporter} {page}"
    
    match = re.search(r"(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d{4})\D+(\d+)", text)
    if match:
        reporter, year, page = match.groups()
        return f"{year} {reporter} {page}"
    
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
    print("Migrating old scraper data to progress.json (v2)")
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
    failed = []
    
    for case_file in old_files:
        try:
            with open(case_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            case_name = data.get("caseName") or data.get("id") or case_file.stem
            title = data.get("title", "")
            text = data.get("text", "") or data.get("judgment", "") or data.get("headnotes", "")
            
            # Skip failed scrapes
            if "window.location" in text or len(text) < 100:
                failed.append(f"{case_name} (empty/failed)")
                continue
            
            # Try title first (most reliable)
            citation = extract_citation_from_title(title)
            
            # Fallback to text
            if not citation:
                citation = extract_citation_from_text(text)
            
            # Fallback to metadata (book/year fields)
            if not citation:
                citation = extract_from_metadata(data)
            
            if citation:
                if citation in existing_cases:
                    skipped += 1
                else:
                    existing_cases.add(citation)
                    progress["cases_fetched"].append(citation)
                    added += 1
                    if added <= 15:  # Show first 15
                        print(f"  + {citation}")
            else:
                failed.append(case_name)
        
        except Exception as e:
            failed.append(f"{case_file.name}: {e}")
    
    # Update totals
    progress["total_cases"] = len(progress["cases_fetched"])
    
    print()
    print(f"Results:")
    print(f"  Added: {added}")
    print(f"  Skipped (already tracked): {skipped}")
    print(f"  Failed to parse: {len(failed)}")
    print(f"  Total tracked now: {progress['total_cases']}")
    
    if failed and len(failed) <= 20:
        print(f"\nFailed cases:")
        for f in failed:
            print(f"  - {f}")
    elif failed:
        print(f"\nFirst 20 failed cases:")
        for f in failed[:20]:
            print(f"  - {f}")
    
    # Save
    if added > 0:
        save_progress(progress)
        print(f"\nSaved to {PROGRESS_FILE}")
    else:
        print("\nNo new cases to add.")


if __name__ == "__main__":
    main()
