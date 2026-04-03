"""
PLS Search Probe Part 2 - After successful login
Focus on: reporters, note series, full-text search structure, page counts
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
        print('ERROR: No CSRF token')
        return None
    s.post(f'{BASE_URL}/Login/ClearLoginHistory', data={
        'Login.UserName': PLS_USER, 'Login.Password': PLS_PASS,
        '__RequestVerificationToken': csrf.group(1)
    }, timeout=30)
    time.sleep(3)
    check = s.get(f'{BASE_URL}/Login/Check', timeout=30)
    if 'Logout' in check.text:
        print('Login: OK')
        return s
    print('Login: FAILED')
    return None

s = login()
if not s:
    sys.exit(1)

results = {}

# === A: Explore PLS site navigation to find the 387K search ===
print('\n=== A: Site navigation exploration ===')
nav_pages = [
    '/Login/Home',
    '/Login/Dashboard', 
    '/Login/Index',
    '/Login/CaseLawSearch',
    '/Login/SearchCaseLaw',
    '/Login/Judgment',
    '/Login/JudgmentList',
]
for url in nav_pages:
    time.sleep(2)
    try:
        resp = s.get(f'{BASE_URL}{url}', timeout=20)
        print(f'{url}: {resp.status_code} ({len(resp.text)} chars) -> {resp.url}')
        if resp.status_code == 200 and len(resp.text) > 2000:
            soup = BeautifulSoup(resp.text, 'html.parser')
            # Check for search forms
            forms = [(f.get('action',''), f.get('method','')) for f in soup.find_all('form')]
            print(f'  Forms: {forms[:5]}')
            # Look for large numbers
            text = soup.get_text()
            big_nums = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text)
            print(f'  Big numbers: {big_nums[:5]}')
    except Exception as e:
        print(f'{url}: {e}')

time.sleep(3)

# === B: Check what the homepage shows after login ===
print('\n=== B: Check homepage after login ===')
resp_home = s.get(f'{BASE_URL}/Login/Check', timeout=30)
soup_home = BeautifulSoup(resp_home.text, 'html.parser')
# Find all links
links = [(a.get('href',''), a.get_text(strip=True)) for a in soup_home.find_all('a')]
print(f'Links on Check page:')
for href, text in links[:30]:
    if href and not href.startswith('#'):
        print(f'  {href} -> {text}')
# Find forms
forms = soup_home.find_all('form')
for form in forms:
    print(f'  Form action={form.get("action","")} inputs={[i.get("name","") for i in form.find_all("input")]}')

time.sleep(3)

# === C: Try various search endpoints with "contract" keyword ===
print('\n=== C: Search endpoint probe ===')
search_endpoints = [
    ('/Login/CitationSearch', {'SearchText': 'contract', 'page': '1'}),
    ('/Login/CitationSearch', {'year': '2023', 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'SearchText': 'contract'}),
    ('/Login/JudgmentSearch', {'SearchText': 'contract', 'page': '1'}),
    ('/Login/KeywordSearch', {'SearchText': 'contract', 'page': '1'}),
    ('/Login/SearchResult', {'SearchText': 'contract', 'page': '1'}),
    ('/Login/FullText', {'SearchText': 'contract'}),
    ('/Login/Search', {'query': 'contract', 'page': '1'}),
    ('/Login/CaseLawSearch', {'SearchText': 'contract', 'page': '1'}),
]
for endpoint, data in search_endpoints:
    time.sleep(2)
    try:
        resp = s.post(f'{BASE_URL}{endpoint}', data=data, timeout=20)
        soup2 = BeautifulSoup(resp.text, 'html.parser')
        rows = soup2.find_all('tr', class_='caseType')
        text2 = soup2.get_text()
        big_nums = re.findall(r'(?<!\d)(\d{4,})(?!\d)', text2)
        print(f'{endpoint}: {resp.status_code}, rows={len(rows)}, big_nums={big_nums[:5]}')
        if resp.status_code == 200 and len(resp.text) > 5000:
            print(f'  First 300: {resp.text[:300]}')
    except Exception as e:
        print(f'{endpoint}: ERROR {e}')

time.sleep(3)

# === D: CitationSearch with specific year only (no book) ===
print('\n=== D: CitationSearch per-year (no reporter filter) ===')
# This is critical - check what "year only" searches return
for year in [2023, 2022, 2024]:
    time.sleep(3)
    try:
        resp = s.post(f'{BASE_URL}/Login/CitationSearch',
            data={'year': year, 'book': '', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
            timeout=60)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all('tr', class_='caseType')
        text = soup.get_text()
        # Look for total count
        counts = re.findall(r'(\d[\d,]+)\s*(record|result|case|total|found)', text, re.I)
        # Look for page info
        pages = re.findall(r'Page\s+(\d+)\s+of\s+(\d+)', text, re.I)
        reporters_on_page = {}
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                cit = tds[1].get_text(strip=True)
                parts = cit.split()
                if len(parts) >= 2:
                    rep = parts[1]
                    reporters_on_page[rep] = reporters_on_page.get(rep, 0) + 1
        print(f'Year {year}: {len(rows)} rows, counts={counts[:3]}, pages={pages}, reporters={reporters_on_page}')
    except Exception as e:
        print(f'Year {year}: {e}')

time.sleep(3)

# === E: Notes reporters - do they actually work? ===
print('\n=== E: Notes reporters ===')
note_reporters = ['CLCN', 'PCRLJN', 'YLRN', 'PLCCSN', 'SCMRN', 'PLDN', 'MLRN', 'PLCN']
note_results = {}
for reporter in note_reporters:
    time.sleep(3)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
        timeout=60)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    text = soup.get_text()
    counts = re.findall(r'(\d[\d,]+)\s*(record|result|case|total|found)', text, re.I)
    print(f'{reporter} (all years): {len(rows)} rows, counts={counts[:3]}', end='')
    if rows:
        tds = rows[0].find_all('td')
        sample = tds[1].get_text(strip=True) if len(tds) > 1 else '?'
        print(f', sample={sample}', end='')
    print()
    note_results[reporter] = len(rows)

time.sleep(3)

# === F: Check PLCCS and other known reporters we might have missed years ===
print('\n=== F: Check PLCCS coverage ===')
for reporter in ['PLCCS', 'PCRLJN', 'YLRN', 'PLCCSN', 'CLCN']:
    time.sleep(3)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
        timeout=60)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    text = soup.get_text()
    counts = re.findall(r'(\d[\d,]+)\s*(record|result|case|total|found)', text, re.I)
    pages_info = re.findall(r'(\d+)\s*/\s*(\d+)', text)
    print(f'{reporter} (all years): {len(rows)} rows, counts={counts[:3]}, page_info={pages_info[:3]}')
    if rows:
        for row in rows[:3]:
            tds = row.find_all('td')
            if len(tds) >= 2:
                print(f'  {tds[1].get_text(strip=True)}')

time.sleep(3)

# === G: Understand the pagination mechanism ===
print('\n=== G: Pagination probe ===')
# Try SCMR 2023 with page parameter
for page_param in [1, 2, 3]:
    time.sleep(3)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': 2023, 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': page_param},
        timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    text = soup.get_text()
    # Pagination elements
    pag_links = soup.find_all(class_=re.compile(r'pag', re.I))
    print(f'Page {page_param}: {len(rows)} rows')
    for p in pag_links[:3]:
        print(f'  Pag: {p.get_text(strip=True)[:80]}')
    # Any "next page" links
    next_links = [a.get('href','') for a in soup.find_all('a') if 'next' in a.get_text('').lower() or 'page' in a.get('href','').lower()]
    print(f'  Next links: {next_links[:5]}')

time.sleep(3)

# === H: Check if there are unreported judgment sections ===
print('\n=== H: Unreported judgments probe ===')
# Try fetching an unreported case via party search with common terms
for party in ['Khan', 'Government', 'Bank', 'Pakistan']:
    time.sleep(3)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': '', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': party},
        timeout=60)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    reporters_found = {}
    for row in rows:
        tds = row.find_all('td')
        if len(tds) >= 2:
            cit = tds[1].get_text(strip=True)
            parts = cit.split()
            if len(parts) >= 2:
                rep = parts[1]
                reporters_found[rep] = reporters_found.get(rep, 0) + 1
    print(f'Party "{party}": {len(rows)} rows, reporters={reporters_found}')

print('\n=== DONE ===')
print(json.dumps({'note_results': note_results}, indent=2))
