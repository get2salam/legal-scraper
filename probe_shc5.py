"""Submit SHC search and extract case results."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://cases.shc.gov.pk'

# First GET to get session cookie
s.get(f'{BASE}/lar/web/index.php?r=cases%2Fsearch', timeout=15)
print(f"Cookies after GET: {dict(s.cookies)}")

# Submit search for all 2024 cases
payload = {
    'r': 'cases/search-result',
    'CasesSearch[CASENO]': '',
    'CasesSearch[CASEYEAR]': '2024',
    'CasesSearch[CASENAMECODE]': '',
    'CasesSearch[BENCH]': '',
    'CasesSearch[CIRCUITCODE]': '',
    'CasesSearch[MATTERCODE]': '',
    'CasesSearch[PARTY]': '',
    'CasesSearch[GOVT_AGENCY_CODE]': '',
    'CasesSearch[FIRNO]': '',
    'CasesSearch[FIRYEAR]': '',
    'CasesSearch[POLICESTATIONCODE]': '',
    'CasesSearch[ADVOCATECODE]': '',
    'CasesSearch[isPending]': '',
}

print("\n=== Submitting search (year=2024) ===")
resp = s.post(
    f'{BASE}/lar/web/index.php?r=cases%2Fsearch-result',
    data=payload,
    headers={'Referer': f'{BASE}/lar/web/index.php?r=cases%2Fsearch'},
    timeout=20
)
print(f"Status: {resp.status_code} ({len(resp.text)} chars)")

soup = BeautifulSoup(resp.text, 'html.parser')
# Find table rows
rows = soup.find_all('tr')
print(f"Table rows: {len(rows)}")
for row in rows[:10]:
    cells = [td.get_text(strip=True)[:50] for td in row.find_all('td')]
    links = [a.get('href','')[:60] for a in row.find_all('a') if a.get('href')]
    if cells:
        print(f"  Cells: {cells}")
        if links:
            print(f"  Links: {links}")

# Count total results
total = re.search(r'(\d+)\s*(?:total|results?|records?)', resp.text, re.I)
print(f"Total hint: {total.group(0) if total else 'not found'}")
