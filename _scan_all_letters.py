"""Scan all 26 letters to get PLS statute counts vs local counts."""
import sys, os, time, random, json
sys.path.insert(0, os.path.dirname(__file__))
from legislation_scraper import LegislationScraper

scraper = LegislationScraper()
if not scraper.login():
    print("LOGIN FAILED")
    sys.exit(1)

results = {}
grand_pls = 0
grand_local = 0
grand_gap = 0

for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    time.sleep(random.uniform(2, 4))
    statutes = scraper.get_statutes_by_letter(letter)
    pls_count = len(statutes)
    
    local_dir = f'data_v2/legislation/{letter}'
    local_count = len([f for f in os.listdir(local_dir) if f.endswith('.json')]) if os.path.isdir(local_dir) else 0
    
    gap = max(0, pls_count - local_count)
    grand_pls += pls_count
    grand_local += local_count
    grand_gap += gap
    
    status = "OK" if gap == 0 else f"GAP {gap}"
    print(f"{letter}: PLS={pls_count:>4}, Local={local_count:>4}, Gap={gap:>4} — {status}")
    
    results[letter] = {"pls": pls_count, "local": local_count, "gap": gap}

print(f"\nTOTAL: PLS={grand_pls}, Local={grand_local}, Gap={grand_gap}")
print(f"Coverage: {grand_local/grand_pls*100:.1f}%" if grand_pls else "N/A")

# Save results
with open('data_v2/legislation/pls_legislation_counts.json', 'w') as f:
    json.dump(results, f, indent=2)
print("Saved to pls_legislation_counts.json")
