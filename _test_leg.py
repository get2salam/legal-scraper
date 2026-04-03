"""Quick test of legislation scraper - verify sections and case links work."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from legislation_scraper import LegislationScraper

scraper = LegislationScraper()
if not scraper.login():
    print("LOGIN FAILED")
    sys.exit(1)
print("Login OK")

# Test with letter D (first un-scraped letter)
import time, random
time.sleep(random.uniform(2, 4))
statutes = scraper.get_statutes_by_letter("D")
print(f"Letter D: {len(statutes)} statutes found")

if statutes:
    # Test scraping first statute 
    first = statutes[0]
    print(f"Testing: {first['name']}")
    
    time.sleep(random.uniform(2, 4))
    sections = scraper.get_statute_sections(first['name'])
    print(f"  Sections: {len(sections)}")
    
    if sections:
        s = sections[0]
        print(f"  First section: #{s.get('number')} - {s.get('definition', '')[:60]}")
        
        # Test section content
        sid = s.get('section_id', '')
        if sid:
            time.sleep(random.uniform(1, 3))
            html, text = scraper.get_section_content(sid)
            print(f"  Content length: {len(text)} chars")
            print(f"  Content preview: {text[:100]}..." if text else "  Content: EMPTY")
        
        # Test case links
        case_id = s.get('case_type_id', '')
        if case_id:
            time.sleep(random.uniform(1, 3))
            links = scraper.get_section_case_links(case_id)
            print(f"  Case links: {len(links)}")
            for cl in links[:3]:
                print(f"    - {cl.get('citation', 'N/A')}")

# Also check counts for B, C (verify we didn't miss any)
for letter in ['B', 'C', 'E', 'F']:
    time.sleep(random.uniform(2, 4))
    sts = scraper.get_statutes_by_letter(letter)
    existing = len([f for f in os.listdir(f'data_v2/legislation/{letter}') if f.endswith('.json')]) if os.path.isdir(f'data_v2/legislation/{letter}') else 0
    gap = len(sts) - existing
    status = "OK" if gap <= 0 else f"MISSING {gap}"
    print(f"Letter {letter}: PLS={len(sts)}, Local={existing} — {status}")

print("\nDONE")
