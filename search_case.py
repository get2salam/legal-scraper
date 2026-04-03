#!/usr/bin/env python3
"""Search for a specific case in PLS and get its correct case_id."""

import sys
from pls_scraper_v2 import PLSScraperV2

def main():
    if len(sys.argv) != 4:
        print("Usage: python search_case.py <reporter> <year> <page>")
        sys.exit(1)
    
    reporter = sys.argv[1].upper()
    year = int(sys.argv[2])
    target_page = sys.argv[3]
    
    print(f"Searching for {year} {reporter} {target_page}...")
    
    scraper = PLSScraperV2(ignore_hours=True)
    if not scraper.login():
        print("Login failed")
        sys.exit(1)
    
    cases = scraper.citation_search(year, reporter)
    print(f"Found {len(cases)} cases for {year} {reporter}")
    
    # Find the target case
    for case in cases:
        citation = case.get("citation", "")
        if citation.endswith(f" {target_page}"):
            print(f"\nFound target case:")
            print(f"  Citation: {citation}")
            print(f"  Case ID: {case.get('case_name', 'N/A')}")
            print(f"  Full data: {case}")
            
            # Try to fetch it
            print("\nAttempting to fetch...")
            result = scraper.fetch_case(case.get("case_name", ""), citation)
            if result:
                print(f"[OK] Fetched successfully!")
                print(f"  Title: {result.title[:80]}..." if result.title else "  Title: (empty)")
                print(f"  Judgment: {len(result.judgment)} chars")
            else:
                print("[FAIL] Could not fetch case content")
            return
    
    # Not found, show pages around target
    print(f"\n[WARN] Case {year} {reporter} {target_page} not found in search results")
    print("Available pages around target:")
    
    target_num = int(target_page)
    nearby = []
    for case in cases:
        citation = case.get("citation", "")
        parts = citation.split()
        if len(parts) >= 3:
            try:
                page = int(parts[-1])
                if abs(page - target_num) <= 20:
                    nearby.append((page, case.get("case_name", "")))
            except:
                pass
    
    for page, case_id in sorted(nearby):
        print(f"  {year} {reporter} {page} -> {case_id}")

if __name__ == "__main__":
    main()
