import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup
from pathlib import Path

# 1. Check article listing - find ALL rows with casetypeid
print("=== ARTICLES ===")
with open('data_v2/pls_extras/articles/listing.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()
soup = BeautifulSoup(html, 'html.parser')

# Find all elements with casetypeid
for el in soup.find_all(attrs={"casetypeid": True}):
    print(f"  tag={el.name}, casetypeid={el.get('casetypeid')}, text={el.get_text(strip=True)[:80]}")

table = soup.find("table", id="articleSearchTable")
if table:
    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr")
        print(f"  tbody rows: {len(rows)}")
        for row in rows[:10]:
            print(f"    casetypeid={row.get('casetypeid')}, text={row.get_text(strip=True)[:100]}")
    # Check if there are more rows hidden or in different state
    all_rows = table.find_all("tr")
    print(f"  All table rows (incl header): {len(all_rows)}")
    for row in all_rows:
        cid = row.get("casetypeid")
        if cid:
            print(f"    casetypeid={cid}: {row.get_text(strip=True)[:100]}")

# 2. Check Topics - what kind of data is in there
print("\n=== TOPICS ===")
with open('data_v2/pls_extras/topics/listing.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()
soup = BeautifulSoup(html, 'html.parser')

# Find the main content area
content = soup.find("div", id="rightmenu")
if content:
    # Find all clickable topic items
    all_btns = content.find_all(["button", "input"], attrs={"casetypeid": True})
    print(f"  Topic buttons with casetypeid: {len(all_btns)}")
    for btn in all_btns[:5]:
        print(f"    {btn}")
    
    # Find tables
    tables = content.find_all("table")
    print(f"  Tables: {len(tables)}")
    for tbl in tables[:2]:
        rows = tbl.find_all("tr")
        print(f"    Table rows: {len(rows)}")
        for row in rows[:5]:
            print(f"      {row.get_text(strip=True)[:100]}")
    
    # Find any list items
    items = content.find_all(["li", "a"])
    print(f"  List items/links: {len(items)}")
    unique = set()
    for item in items[:30]:
        txt = item.get_text(strip=True)
        href = item.get("href","")
        if txt and txt not in unique:
            unique.add(txt)
            print(f"    [{txt[:50]}] -> {href}")

# Check scripts for topic AJAX
print("\n  Topic scripts:")
for sc in soup.find_all("script"):
    content_txt = sc.get_text()
    if "topic" in content_txt.lower() or "Topic" in content_txt:
        if "ajax" in content_txt.lower() or "url" in content_txt.lower():
            print(f"  {content_txt[:1000]}")
            break

# 3. Check W&P letter A structure
print("\n=== W&P Letter A ===")
with open('data_v2/pls_extras/words_phrases/letter_A.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()
soup = BeautifulSoup(html, 'html.parser')

# Find the actual W&P entries
table = soup.find("table")
if table:
    rows = table.find_all("tr")
    print(f"  Table rows: {len(rows)}")
    for row in rows[:10]:
        tds = row.find_all("td")
        if tds:
            print(f"    cols={len(tds)}: {[td.get_text(strip=True)[:40] for td in tds]}")

# Check for links to individual W&P entries
links = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(strip=True)
    if "words" in href.lower() or "phrase" in href.lower() or "words" in txt.lower():
        links.append(f"[{txt[:40]}] -> {href}")
print(f"\n  W&P links: {len(links)}")
for l in links[:10]:
    print(f"    {l}")

# Check for script endpoints
print("\n  W&P scripts:")
for sc in soup.find_all("script"):
    content_txt = sc.get_text()
    if "ajax" in content_txt.lower() and ("words" in content_txt.lower() or "phrase" in content_txt.lower()):
        print(f"  {content_txt[:1000]}")
        break

# 4. Check Maxims
print("\n=== MAXIMS ===")
with open('data_v2/pls_extras/maxims/maxims.txt', 'r', encoding='utf-8', errors='replace') as f:
    content_txt = f.read()
print(f"  Size: {len(content_txt)} chars")
# Show first 50 lines
lines = [l for l in content_txt.split('\n') if l.strip()][:30]
for line in lines:
    print(f"  {line}")

# 5. Check Legal Terms
print("\n=== LEGAL TERMS ===")
with open('data_v2/pls_extras/legal_terms/legal_terms.txt', 'r', encoding='utf-8', errors='replace') as f:
    content_txt = f.read()
print(f"  Size: {len(content_txt)} chars")
lines = [l for l in content_txt.split('\n') if l.strip()][:30]
for line in lines:
    print(f"  {line}")
