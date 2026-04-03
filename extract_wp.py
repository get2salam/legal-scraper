import json, sys
from pathlib import Path
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

base = Path('data_v2/pls_extras')

# Words & Phrases — rows use class "searchCase" not "caseType"
print("=== EXTRACTING WORDS & PHRASES ===")
wp_html = (base/'words_phrases'/'listing.html').read_text(encoding='utf-8')
soup = BeautifulSoup(wp_html, 'html.parser')

entries = []
for row in soup.find_all('tr', class_='searchCase'):
    tds = row.find_all('td')
    btn = row.find('input', attrs={'casetypeid': True})
    cid = btn.get('casetypeid','') if btn else ''
    
    phrase = tds[0].get_text(strip=True) if len(tds) >= 1 else ''
    citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ''
    snippet = tds[2].get_text(strip=True) if len(tds) >= 3 else ''
    
    if phrase:
        entries.append({
            "phrase": phrase,
            "citation": citation,
            "snippet": snippet,
            "case_id": cid
        })

print(f"Entries from listing: {len(entries)}")
for e in entries[:3]:
    print(f"  phrase={e['phrase'][:40]}, citation={e['citation'][:40]}, id={e['case_id']}")

# Now go through all letter files too
all_entries = list(entries)
for letter_file in sorted((base/'words_phrases').glob('letter_*.html')):
    letter_soup = BeautifulSoup(letter_file.read_text(encoding='utf-8'), 'html.parser')
    for row in letter_soup.find_all('tr', class_='searchCase'):
        tds = row.find_all('td')
        btn = row.find('input', attrs={'casetypeid': True})
        cid = btn.get('casetypeid','') if btn else ''
        phrase = tds[0].get_text(strip=True) if len(tds) >= 1 else ''
        citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ''
        snippet = tds[2].get_text(strip=True) if len(tds) >= 3 else ''
        if phrase:
            all_entries.append({"phrase": phrase, "citation": citation, "snippet": snippet, "case_id": cid})

print(f"Total W&P entries (all files): {len(all_entries)}")

# Save
with open(base/'words_phrases'/'words_phrases.json', 'w', encoding='utf-8') as f:
    json.dump(all_entries, f, ensure_ascii=False, indent=2)

# Also check legal_terms HTML for proper structure
print("\n=== LEGAL TERMS HTML ===")
lt_html = (base/'legal_terms'/'legal_terms.html').read_text(encoding='utf-8')
lt_soup = BeautifulSoup(lt_html, 'html.parser')
# Check what row classes it uses
from collections import Counter
classes = Counter(' '.join(r.get('class',[])) for r in lt_soup.find_all('tr'))
print(f"Row classes in legal_terms: {dict(classes)}")
# Try searchCase
lt_rows = lt_soup.find_all('tr', class_='searchCase')
print(f"searchCase rows: {len(lt_rows)}")
if lt_rows:
    tds = lt_rows[0].find_all('td')
    for i, td in enumerate(tds[:3]):
        print(f"  td[{i}]: {td.get_text(strip=True)[:80]}")
