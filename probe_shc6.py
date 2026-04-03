"""Try GET search and inspect actual result structure."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://cases.shc.gov.pk'

# Try GET with query params (Yii2 often supports both)
urls_to_try = [
    f'{BASE}/lar/web/index.php?r=cases%2Fsearch-result&CasesSearch[CASEYEAR]=2024',
    f'{BASE}/lar/web/index.php?r=cases%2Fsearch&CasesSearch[CASEYEAR]=2024',
    f'{BASE}/lar/web/index.php?r=cases/index&year=2024',
    f'{BASE}/lar/web/index.php?r=ocp/index',
]

for url in urls_to_try:
    try:
        resp = s.get(url, timeout=12)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all('tr')
        links = soup.find_all('a', href=re.compile(r'cases.*view|cases.*detail|view.*case', re.I))
        print(f"GET {url[-60:]}: {resp.status_code} ({len(resp.text)} chars), rows={len(rows)}, case_links={len(links)}")
        if links:
            print(f"  Sample links: {[l.get('href','')[:80] for l in links[:3]]}")
        # look for JSON in response
        json_data = re.findall(r'\{["\'](?:data|cases|results|items)["\']:\s*\[', resp.text)
        if json_data:
            print(f"  JSON hint: {json_data[:2]}")
        time.sleep(0.5)
    except Exception as e:
        print(f"  ERROR: {str(e)[:60]}")

# Also check the KHI portal
print("\n=== KHI portal ===")
try:
    resp2 = s.get('https://cases.shc.gov.pk/khi/web/index.php?r=ocp/index', timeout=12)
    print(f"KHI: {resp2.status_code} ({len(resp2.text)} chars)")
    soup2 = BeautifulSoup(resp2.text, 'html.parser')
    links2 = soup2.find_all('a')[:10]
    print(f"Links: {[l.get('href','')[:60] for l in links2]}")
except Exception as e:
    print(f"KHI: {e}")
