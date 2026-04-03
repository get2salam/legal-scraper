import json, sys
from pathlib import Path
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

base = Path('data_v2/pls_extras')

# ===== 1. Fix Dictionary =====
print("=== DICTIONARY ===")
raw_defs = json.load(open(base/'dictionary'/'definitions.json', encoding='utf-8'))
print(f"Raw entries: {len(raw_defs)}")
print(f"Sample raw: {raw_defs[0]}")
# Fix: 'definition' field is actually the term, 'citation' is the full definition text
fixed_defs = []
for d in raw_defs:
    fixed_defs.append({
        "id": d.get("term",""),
        "term": d.get("definition",""),
        "definition": d.get("citation","")
    })
print(f"Fixed sample: term={fixed_defs[1]['term']}, def={fixed_defs[1]['definition'][:60]}")
with open(base/'dictionary'/'definitions_fixed.json', 'w', encoding='utf-8') as f:
    json.dump(fixed_defs, f, ensure_ascii=False, indent=2)
print(f"Saved {len(fixed_defs)} fixed definitions")

# ===== 2. Check Words & Phrases structure =====
print("\n=== WORDS & PHRASES ===")
wp_html = (base/'words_phrases'/'listing.html').read_text(encoding='utf-8')
soup = BeautifulSoup(wp_html, 'html.parser')
all_rows = soup.find_all('tr')
print(f"Total rows: {len(all_rows)}")

# Get row classes
from collections import Counter
classes = Counter(' '.join(r.get('class',[])) for r in all_rows)
print(f"Row classes: {dict(classes)}")

# Show first few rows with TDs
for row in all_rows[:5]:
    tds = row.find_all('td')
    if tds:
        btn = row.find('input')
        cid = btn.get('casetypeid','') if btn else ''
        print(f"  Row cls={row.get('class',[])}: {len(tds)} tds, id={cid}")
        for i, td in enumerate(tds[:3]):
            print(f"    td[{i}]: {td.get_text(strip=True)[:80]}")
        break

# ===== 3. Check legal_terms structure =====
print("\n=== LEGAL TERMS ===")
lt = (base/'legal_terms'/'legal_terms.txt').read_text(encoding='utf-8', errors='replace')
lines = [l.strip() for l in lt.split('\n') if l.strip()]
print(f"Lines: {len(lines)}")
for l in lines[:5]:
    print(f"  {l[:100]}")
