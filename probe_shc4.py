"""Dig into the SHC cases portal search form and AJAX endpoints."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://cases.shc.gov.pk'

# Get search page and extract form fields + CSRF
resp = s.get(f'{BASE}/lar/web/index.php?r=cases%2Fsearch', timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

# Find CSRF token
csrf = soup.find('meta', {'name': 'csrf-token'})
csrf_token = csrf['content'] if csrf else None
print(f"CSRF: {csrf_token}")

# Find all form inputs
form = soup.find('form')
if form:
    inputs = form.find_all(['input', 'select', 'textarea'])
    print(f"Form fields: {[(i.get('name'), i.get('value','')[:30]) for i in inputs]}")

# Find AJAX endpoints
ajax_urls = re.findall(r'url["\']?\s*[:=]\s*["\']([^"\']+)["\']', resp.text)
select2_urls = re.findall(r'select2.*?url.*?["\']([^"\']+)["\']', resp.text, re.S)
print(f"AJAX URLs: {ajax_urls[:8]}")

# Find the actual search submit URL
search_forms = re.findall(r'action=["\']([^"\']*search[^"\']*)["\']', resp.text, re.I)
print(f"Search forms: {search_forms}")

# Try submitting the search form with CSRF
if csrf_token:
    headers = {
        'X-CSRF-Token': csrf_token,
        'Referer': f'{BASE}/lar/web/index.php?r=cases%2Fsearch',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    # Typical Yii2 form submission
    for payload in [
        {'_csrf': csrf_token, 'CasesSearch[case_no]': '', 'CasesSearch[year]': '2024', 'CasesSearch[court_type]': ''},
        {'_csrf': csrf_token, 'CasesSearch[petitioner]': 'Ahmed', 'CasesSearch[year]': '2024'},
    ]:
        print(f"\nSubmitting search: {list(payload.keys())}")
        r2 = s.post(f'{BASE}/lar/web/index.php?r=cases%2Fsearch', data=payload, headers=headers, timeout=15)
        print(f"Response: {r2.status_code} ({len(r2.text)} chars)")
        # Look for case results
        soup2 = BeautifulSoup(r2.text, 'html.parser')
        rows = soup2.find_all('tr')
        print(f"Table rows: {len(rows)}")
        for row in rows[:5]:
            cells = [td.get_text(strip=True)[:40] for td in row.find_all('td')]
            if cells:
                print(f"  Row: {cells}")
        time.sleep(1)
