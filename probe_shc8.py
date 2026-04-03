"""Extract row data from SHC search results - get case info from listing."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://cases.shc.gov.pk'

# Get page 1 of 2024 results and extract all data
resp = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fsearch-result&CasesSearch[CASEYEAR]=2024', timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

# Print all table headers
table = soup.find('table')
if table:
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    print(f"Headers: {headers}")
    rows = table.find_all('tr')[1:]  # skip header
    print(f"Rows: {len(rows)}")
    for row in rows[:5]:
        cells = [td.get_text(strip=True)[:60] for td in row.find_all('td')]
        links = [a.get('href','') for a in row.find_all('a')]
        print(f"  Cells: {cells}")
        print(f"  Links: {links}")
else:
    print("No table found")
    # Find the actual data containers
    all_text = soup.get_text()
    print(f"Text sample 2000-3000: {all_text[2000:3000]}")

# Also check if there's judgment text in case view with a different ID
print("\n=== Try different case IDs ===")
for cid in [69809, 71847, 69903]:
    resp2 = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fview&id={cid}', timeout=12)
    soup2 = BeautifulSoup(resp2.text, 'html.parser')
    # Find all definition lists
    dls = soup2.find_all('dl')
    for dl in dls:
        dts = dl.find_all('dt')
        dds = dl.find_all('dd')
        for dt, dd in zip(dts, dds):
            k = dt.get_text(strip=True)
            v = dd.get_text(strip=True)[:80]
            if k and v:
                print(f"  {k}: {v}")
    # Any section with 'order' or 'judgment'
    for tag in soup2.find_all(string=re.compile(r'judgment|order|decision', re.I)):
        parent = tag.parent
        print(f"  Found '{tag.strip()[:30]}' in <{parent.name}> with class={parent.get('class')}")
    break
