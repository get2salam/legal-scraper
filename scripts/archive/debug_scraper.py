#!/usr/bin/env python3
"""Debug the legislation scraper v2."""

import traceback
from legislation_scraper_v2 import LegislationScraperV2

try:
    scraper = LegislationScraperV2(ignore_hours=True)
    
    if scraper.login():
        print("[OK] Login successful")
        
        statutes = scraper.get_statutes_by_letter("A")
        print(f"[OK] Got {len(statutes)} statutes for 'A'")
        
        if statutes:
            first = statutes[0]
            print(f"First statute: {first.get('title', 'N/A')[:60]}")
            print(f"Statute ID: {first.get('id', 'N/A')}")
            
            # Try to scrape just the first one
            print("\nScraping first statute...")
            result = scraper.scrape_statute(first)
            
            if result:
                print(f"[OK] Scraped successfully!")
                print(f"  Sections: {len(result.get('sections', []))}")
                print(f"  Cases cited: {len(result.get('cases_cited', []))}")
            else:
                print("[FAIL] Scrape returned None")
    else:
        print("[FAIL] Login failed")

except Exception as e:
    print(f"[ERROR] {e}")
    traceback.print_exc()
