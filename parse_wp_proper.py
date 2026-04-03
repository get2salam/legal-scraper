import json, sys, re
from pathlib import Path
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

base = Path('data_v2/pls_extras')

# Check char_words_A.html - this is the actual scraped data
wp_html = (base/'words_phrases'/'char_words_A.html').read_text(encoding='utf-8')
soup = BeautifulSoup(wp_html, 'html.parser')

# Look for script tags with data
for script in soup.find_all('script'):
    text = script.string or ''
    if ('phrase' in text.lower() or 'word' in text.lower() or 'data' in text.lower()):
        if len(text) > 200:
            print(f"Script with data ({len(text)} chars):")
            print(text[:1000])
            print("---")

# Tables and rows
tables = soup.find_all('table')
print(f"\nTables: {len(tables)}")
for i, t in enumerate(tables[:3]):
    cls = t.get('class', [])
    trs = t.find_all('tr')
    print(f"Table {i} class={cls}: {len(trs)} rows")
    for tr in trs[:3]:
        print(f"  tr class={tr.get('class',[])} text={tr.get_text(strip=True)[:80]}")
        for inp in tr.find_all('input'):
            print(f"    input attrs: {dict(inp.attrs)}")

# Look for any element with casetypeid
elements = soup.find_all(attrs={'casetypeid': True})
print(f"\nElements with casetypeid: {len(elements)}")
for el in elements[:5]:
    print(f"  {el.name}: {dict(el.attrs)}")

# Check the words_phrases.json structure
print("\n\n=== words_phrases.json ===")
with open(base/'words_phrases'/'words_phrases.json', encoding='utf-8') as f:
    data = json.load(f)
print(f"Type: {type(data)}, Count: {len(data) if isinstance(data, list) else 'dict'}")
for item in data[:10]:
    print(f"  {json.dumps(item, ensure_ascii=False)[:200]}")

# Check words_phrases_all.json
print("\n=== words_phrases_all.json ===")
with open(base/'words_phrases'/'words_phrases_all.json', encoding='utf-8') as f:
    data2 = json.load(f)
print(f"Type: {type(data2)}, Count: {len(data2) if isinstance(data2, list) else 'dict'}")
for item in data2[:10]:
    print(f"  {json.dumps(item, ensure_ascii=False)[:200]}")
