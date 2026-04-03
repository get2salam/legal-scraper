"""
PLS Search Probe Final - Focused investigation
Already know: login works, notes reporters CLCN/PCRLJN/YLRN/PLCN exist
Now investigating: 387K source, SCMR total pages, notes counts by year
"""
import os, sys, re, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup

BASE_URL = 'https://www.pakistanlawsite.com'
PLS_USER = os.getenv('PLS_USER', '')
PLS_PASS = os.getenv('PLS_PASS', '')

def login():
    s = Session(impersonate=BrowserType.chrome120)
    s.headers.update({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Upgrade-Insecure-Requests': '1',
    })
    resp0 = s.get(f'{BASE_URL}/', timeout=30)
    csrf = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp0.text)
    if not csrf:
        print('ERROR: No CSRF token'); return None
    s.post(f'{BASE_URL}/Login/ClearLoginHistory', data={
        'Login.UserName': PLS_USER, 'Login.Password': PLS_PASS,
        '__RequestVerificationToken': csrf.group(1)
    }, timeout=30)
    time.sleep(3)
    check = s.get(f'{BASE_URL}/Login/Check', timeout=30)
    if 'Logout' in check.text:
        print('Login: OK'); return s
    print('Login: FAILED'); return None

s = login()
if not s:
    sys.exit(1)

findings = {}

# === A: Inspect the main post-login page for search forms ===
print('\n=== A: Main page structure ===')
time.sleep(3)
check_resp = s.get(f'{BASE_URL}/Login/Check', timeout=30)
# Save it for inspection
with open('probe_mainpage.html', 'w', encoding='utf-8') as f:
    f.write(check_resp.text)
print(f'Saved main page: {len(check_resp.text)} chars')

soup = BeautifulSoup(check_resp.text, 'html.parser')
# Find ALL forms
forms = soup.find_all('form')
print(f'Forms: {len(forms)}')
for form in forms:
    print(f'\n  Form: action={form.get("action","")} method={form.get("method","")}')
    for inp in form.find_all(['input', 'select', 'textarea']):
        print(f'    {inp.name}: name={inp.get("name","")} id={inp.get("id","")} type={inp.get("type","")} value="{inp.get("value","")[:30]}"')

# Find any JS that has search endpoint
js_scripts = re.findall(r'(?:ajax|fetch|post|get)\s*\(["\']([^"\']+)["\']', check_resp.text, re.I)
print(f'\nJS endpoints found: {js_scripts[:20]}')

# Find any URL patterns
urls_in_page = re.findall(r'["\']/(Login/[A-Za-z]+)["\']', check_resp.text)
unique_urls = sorted(set(urls_in_page))
print(f'\nAll /Login/ URLs in page: {unique_urls}')

time.sleep(3)

# === B: Try the Keywords/Words & Phrases search ===
print('\n=== B: Keywords search exploration ===')
# First GET the page
resp_wp = s.get(f'{BASE_URL}/Login/WordsAndPhrases', timeout=30)
print(f'WordsAndPhrases GET: {resp_wp.status_code}, {len(resp_wp.text)} chars')
if resp_wp.status_code == 200:
    soup_wp = BeautifulSoup(resp_wp.text, 'html.parser')
    forms = soup_wp.find_all('form')
    for form in forms:
        print(f'  Form: action={form.get("action","")}')
        for inp in form.find_all(['input','select']):
            print(f'    {inp.name}: name={inp.get("name","")} value="{inp.get("value","")[:50]}"')
    text = soup_wp.get_text()
    big_nums = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text)
    print(f'  Big numbers: {big_nums[:5]}')
    print(f'  First 500: {text[:500]}')

time.sleep(3)

# Try searching Words & Phrases 
resp_wp_s = s.post(f'{BASE_URL}/Login/WordsAndPhrases',
    data={'SearchText': 'a', 'type': 'words', 'page': '1'}, timeout=30)
print(f'WordsAndPhrases search: {resp_wp_s.status_code}, {len(resp_wp_s.text)} chars')
soup_wp_s = BeautifulSoup(resp_wp_s.text, 'html.parser')
rows = soup_wp_s.find_all('tr', class_='caseType')
text = soup_wp_s.get_text()
big_nums = re.findall(r'(?<!\d)(\d{4,})(?!\d)', text)
print(f'  Rows: {len(rows)}, big_nums: {big_nums[:10]}')
if rows:
    for row in rows[:3]:
        print(f'  Row: {row.get_text(strip=True)[:100]}')

time.sleep(3)

# === C: Try keyword-based search with GET params ===
print('\n=== C: Keyword/full-text search approaches ===')
# The 387K might be a GET-based search
fts_attempts = [
    (f'{BASE_URL}/Login/WordsAndPhrases?SearchText=contract&type=words', 'GET'),
    (f'{BASE_URL}/Login/WordsAndPhrases?q=contract', 'GET'),
    (f'{BASE_URL}/Login/KeyWords?SearchText=a', 'GET'),
    (f'{BASE_URL}/Login/SearchCaseLaw?SearchText=a', 'GET'),
]
for url, method in fts_attempts:
    time.sleep(2)
    try:
        if method == 'GET':
            resp = s.get(url, timeout=20)
        else:
            resp = s.post(url, timeout=20)
        soup2 = BeautifulSoup(resp.text, 'html.parser')
        rows = soup2.find_all('tr', class_='caseType')
        text = soup2.get_text()
        big_nums = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text)
        print(f'{url.split("/Login/")[1][:40]}: {resp.status_code}, rows={len(rows)}, big={big_nums[:3]}')
    except Exception as e:
        print(f'{url}: {e}')

time.sleep(3)

# === D: Count notes reporters by year ===
print('\n=== D: Notes reporters by year (2015-2025) ===')
notes_data = {}
for reporter in ['CLCN', 'PCRLJN', 'YLRN', 'PLCN']:
    notes_data[reporter] = {}
    total = 0
    for year in range(2015, 2026):
        time.sleep(2)
        resp = s.post(f'{BASE_URL}/Login/CitationSearch',
            data={'year': year, 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
            timeout=30)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all('tr', class_='caseType')
        count = len(rows)
        notes_data[reporter][year] = count
        total += count
        if count > 0:
            print(f'  {reporter} {year}: {count} rows')
    notes_data[reporter]['total'] = total
    print(f'{reporter} TOTAL: {total}')

print(f'\nAll notes totals: {json.dumps({r: d["total"] for r, d in notes_data.items()}, indent=2)}')
findings['notes_reporters'] = notes_data

time.sleep(3)

# === E: Check if our existing data has these notes reporters ===
print('\n=== E: Local data check for notes reporters ===')
data_dir = 'data_v2'
import os
for reporter in ['CLCN', 'PCRLJN', 'YLRN', 'PLCN']:
    reporter_dir = os.path.join(data_dir, reporter)
    if os.path.exists(reporter_dir):
        years = os.listdir(reporter_dir)
        total_files = sum(len(os.listdir(os.path.join(reporter_dir, y))) for y in years if os.path.isdir(os.path.join(reporter_dir, y)))
        print(f'{reporter}: EXISTS in local data, {len(years)} years, ~{total_files} files')
    else:
        print(f'{reporter}: NOT in local data')

time.sleep(3)

# === F: Check SCMR pagination to understand total counts ===
print('\n=== F: SCMR pagination depth ===')
# Check pages for SCMR all years to understand how many total SCMR cases PLS has
scmr_pages = {}
for pg in [1, 2, 3, 4, 5, 10, 50, 100, 200, 500]:
    time.sleep(2)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': pg},
        timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    if rows:
        first_td = rows[0].find_all('td')
        last_td = rows[-1].find_all('td')
        first = first_td[1].get_text(strip=True) if len(first_td) > 1 else '?'
        last = last_td[1].get_text(strip=True) if len(last_td) > 1 else '?'
        print(f'  page={pg}: {len(rows)} rows, first={first}, last={last}')
        scmr_pages[pg] = {'count': len(rows), 'first': first, 'last': last}
    else:
        print(f'  page={pg}: 0 rows')
        scmr_pages[pg] = {'count': 0}

findings['scmr_pages'] = scmr_pages

time.sleep(3)

# === G: Check how many total pages exist for SCMR by finding page boundaries ===
print('\n=== G: Find SCMR total pages ===')
# Binary search to find max page
low, high = 1, 2000
last_good_page = 1
for pg in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]:
    time.sleep(2)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': pg},
        timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    print(f'  SCMR page={pg}: {len(rows)} rows')
    if len(rows) > 0:
        last_good_page = pg
        td = rows[-1].find_all('td')
        print(f'    Last: {td[1].get_text(strip=True) if len(td) > 1 else "?"}')
    else:
        print(f'    -> No more data after page {last_good_page}')
        break

findings['scmr_last_page'] = last_good_page
print(f'Estimated SCMR total cases: ~{last_good_page * 280} (at 280/page)')

time.sleep(3)

# === H: Check total per-reporter case counts ===
print('\n=== H: Per-reporter totals (first/last pages) ===')
reporter_totals = {}
for reporter in ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD']:
    time.sleep(3)
    # Page 1
    resp1 = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': 1},
        timeout=30)
    soup1 = BeautifulSoup(resp1.text, 'html.parser')
    rows1 = soup1.find_all('tr', class_='caseType')
    
    # Try page 1000 to see if it has data
    time.sleep(2)
    resp1000 = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': 1000},
        timeout=30)
    soup1000 = BeautifulSoup(resp1000.text, 'html.parser')
    rows1000 = soup1000.find_all('tr', class_='caseType')
    
    reporter_totals[reporter] = {
        'page1_count': len(rows1),
        'page1000_count': len(rows1000),
    }
    print(f'{reporter}: page1={len(rows1)}, page1000={len(rows1000)}')

findings['reporter_totals'] = reporter_totals

# === I: Save all findings ===
print('\n=== SAVING FINDINGS ===')
import json
with open('probe_findings.json', 'w', encoding='utf-8') as f:
    json.dump(findings, f, indent=2, ensure_ascii=False)
print('Saved to probe_findings.json')
print('\n=== DONE ===')
print(json.dumps(findings, indent=2, ensure_ascii=False))
