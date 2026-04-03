"""Fetch a case from SHC and understand full data + pagination."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://cases.shc.gov.pk'

# 1. Fetch a case view
print("=== Case View ===")
resp = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fview&id=69809', timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')
print(f"Status: {resp.status_code} ({len(resp.text)} chars)")
# Extract all text content
details = {}
for dt in soup.find_all('dt'):
    dd = dt.find_next_sibling('dd')
    if dd:
        details[dt.get_text(strip=True)] = dd.get_text(strip=True)[:100]
print(f"Fields: {details}")
# Look for judgment text
judgment_divs = soup.find_all(['div', 'p'], class_=re.compile(r'judgment|text|content|body', re.I))
print(f"Judgment divs: {len(judgment_divs)}")
if judgment_divs:
    print(f"Sample: {judgment_divs[0].get_text()[:300]}")

# Links in the case view (PDF, full text, etc.)
links = [(a.get_text(strip=True)[:30], a.get('href','')[:80]) for a in soup.find_all('a')]
print(f"Links: {links[:10]}")

time.sleep(1)

# 2. Check pagination for year 2024
print("\n=== Pagination check ===")
resp2 = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fsearch-result&CasesSearch[CASEYEAR]=2024&page=2', timeout=15)
soup2 = BeautifulSoup(resp2.text, 'html.parser')
rows = soup2.find_all('tr')
total_hint = re.search(r'(\d[\d,]+)\s*(?:total|result|record)', resp2.text, re.I)
page_links = soup2.find_all('a', href=re.compile(r'page=\d+'))
print(f"Page 2: {len(rows)} rows")
print(f"Total: {total_hint.group(0) if total_hint else 'not shown'}")
print(f"Page links: {[l.get('href','')[-20:] for l in page_links[:5]]}")

# Count how many pages/cases for 2024
# Try high page number
resp3 = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fsearch-result&CasesSearch[CASEYEAR]=2024&per-page=100', timeout=15)
rows3 = BeautifulSoup(resp3.text, 'html.parser').find_all('tr')
links3 = re.findall(r'cases%2Fview&id=(\d+)', resp3.text)
print(f"\nWith per-page=100: {len(rows3)} rows, {len(links3)} case IDs")
print(f"Sample IDs: {links3[:10]}")
