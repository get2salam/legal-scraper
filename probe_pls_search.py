"""
PLS Search Probe - Investigate the 208K gap between our 178K cases and PLS's 387K search results
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

print(f'User: {PLS_USER[:3]}*** Pass: {"set" if PLS_PASS else "NOT SET"}')

s = Session(impersonate=BrowserType.chrome120)
s.headers.update({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Upgrade-Insecure-Requests': '1',
})

# === STEP 1: GET homepage for CSRF token ===
print('\n=== STEP 1: Login ===')
resp0 = s.get(f'{BASE_URL}/', timeout=30)
print(f'Homepage: {resp0.status_code}, {len(resp0.text)} chars')

csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp0.text)
if not csrf_match:
    print('ERROR: No CSRF token found on homepage!')
    sys.exit(1)
csrf_token = csrf_match.group(1)
print(f'CSRF token: {csrf_token[:30]}...')

time.sleep(3)

# === STEP 2: ClearLoginHistory ===
resp1 = s.post(f'{BASE_URL}/Login/ClearLoginHistory', data={
    'Login.UserName': PLS_USER,
    'Login.Password': PLS_PASS,
    '__RequestVerificationToken': csrf_token,
}, timeout=30)
print(f'ClearLoginHistory: {resp1.status_code}, {len(resp1.text)} chars')

time.sleep(3)

# === STEP 3: Check if logged in ===
check_resp = s.get(f'{BASE_URL}/Login/Check', timeout=30)
print(f'Login/Check: {check_resp.status_code}')
print(f'Logged in: {"Logout" in check_resp.text}')
print(f'Check response (first 300): {check_resp.text[:300]}')

if 'Logout' not in check_resp.text:
    print('Not logged in via ClearLoginHistory, trying Login/Login...')
    time.sleep(2)
    # Get fresh CSRF
    resp_home2 = s.get(f'{BASE_URL}/', timeout=30)
    csrf_match2 = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp_home2.text)
    if csrf_match2:
        csrf_token = csrf_match2.group(1)
    
    time.sleep(2)
    login_resp = s.post(f'{BASE_URL}/Login/Login', data={
        'Login.UserName': PLS_USER,
        'Login.Password': PLS_PASS,
        '__RequestVerificationToken': csrf_token,
    }, timeout=30)
    print(f'Login/Login: {login_resp.status_code}, {len(login_resp.text)} chars')
    print(f'Login URL: {login_resp.url}')
    print(f'Logged in: {"Logout" in login_resp.text}')

print(f'\nCookies: {dict(s.cookies)}')

# === STEP 4: Probe known search endpoint structure ===
print('\n=== STEP 2: Probe full-text search endpoints ===')
time.sleep(3)

# The PLS site uses a specific search structure - let's find it
# First, let's look at the homepage to see what search forms exist
resp_home = s.get(f'{BASE_URL}/Login/CaseLaw', timeout=30)
print(f'CaseLaw page: {resp_home.status_code}, {len(resp_home.text)} chars, URL: {resp_home.url}')
if resp_home.status_code == 200:
    # Find form actions
    forms = re.findall(r'<form[^>]*action=["\']([^"\']+)["\']', resp_home.text, re.I)
    print(f'Forms: {forms}')
    # Find search inputs
    inputs = re.findall(r'<input[^>]+name=["\']([^"\']+)["\']', resp_home.text, re.I)
    print(f'Input names: {inputs[:20]}')
    print(f'Sample HTML: {resp_home.text[:500]}')

time.sleep(3)

# === STEP 5: Try CitationSearch with empty params ===
print('\n=== STEP 3: CitationSearch with empty params ===')
resp_empty = s.post(f'{BASE_URL}/Login/CitationSearch',
    data={'year': '', 'book': '', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
    timeout=30)
print(f'CitationSearch empty: {resp_empty.status_code}, {len(resp_empty.text)} chars')

soup = BeautifulSoup(resp_empty.text, 'html.parser')

# Look for total count
count_text = soup.find(string=re.compile(r'\d+.*record|result|case', re.I))
print(f'Count text: {count_text}')

# Find all table rows
rows = soup.find_all('tr', class_='caseType')
print(f'Rows found: {len(rows)}')

# Look for pagination info
pagination = soup.find_all(class_=re.compile(r'pag|page|count|total', re.I))
for p in pagination[:5]:
    print(f'Pagination element: {p.get_text(strip=True)[:100]}')

# All text nodes with numbers
all_text = soup.get_text()
numbers = re.findall(r'(\d[\d,]+)\s*(cases?|records?|results?|judgments?)', all_text, re.I)
print(f'Numbers found: {numbers[:10]}')

# Check for any count/total fields
print(f'First 1000 chars: {resp_empty.text[:1000]}')

time.sleep(3)

# === STEP 6: Try PLS full-text search (the one showing 387K) ===
print('\n=== STEP 4: Full-text search for "a" ===')
# Try various endpoints that might handle full-text search
fts_endpoints = [
    ('/Login/CitationSearch', {'SearchText': 'a', 'page': '1'}),
    ('/Login/CitationSearch', {'year': '', 'book': '', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': 'a'}),
    ('/Login/Search', {'q': 'a', 'page': '1'}),
    ('/Login/SearchJudgments', {'SearchText': 'a', 'page': '1'}),
    ('/Login/KeywordSearch', {'SearchText': 'a', 'page': '1'}),
    ('/Login/JudgmentSearch', {'SearchText': 'a', 'page': '1'}),
    ('/Login/FullText', {'SearchText': 'a', 'page': '1'}),
]

for endpoint, data in fts_endpoints:
    time.sleep(2)
    try:
        resp = s.post(f'{BASE_URL}{endpoint}', data=data, timeout=15)
        soup2 = BeautifulSoup(resp.text, 'html.parser')
        rows2 = soup2.find_all('tr', class_='caseType')
        text2 = soup2.get_text()
        nums2 = re.findall(r'(\d[\d,]+)\s*(cases?|records?|results?|judgments?|found)', text2, re.I)
        print(f'{endpoint} ({list(data.values())[0]}): {resp.status_code}, rows={len(rows2)}, nums={nums2[:3]}')
    except Exception as e:
        print(f'{endpoint}: ERROR {e}')

time.sleep(3)

# === STEP 7: Check what reporters CitationSearch returns with empty search ===
print('\n=== STEP 5: Check reporters in empty CitationSearch ===')
known_reporters = {'SCMR','PLD','PCrLJ','MLD','CLC','YLR','PTD','PLC','CLD','GBLR','PLCCS','PCRLJN','YLRN','PLCCSN','CLCN'}

reporters_found = {}
if rows:
    for row in rows:
        tds = row.find_all('td')
        if len(tds) >= 2:
            cit = tds[1].get_text(strip=True)
            parts = cit.split()
            if len(parts) >= 2:
                rep = parts[1]
                reporters_found[rep] = reporters_found.get(rep, 0) + 1

print(f'Reporters found in empty search: {reporters_found}')
new_reps = {k: v for k, v in reporters_found.items() if k not in known_reporters}
print(f'NEW reporters not in our list: {new_reps}')

time.sleep(3)

# === STEP 8: Try Notes reporters ===
print('\n=== STEP 6: Notes reporters (CLCN, PCRLJN, YLRN, PLCCSN) ===')
for reporter in ['CLCN', 'PCRLJN', 'YLRN', 'PLCCSN', 'SCMRN', 'PLDN', 'MLRN']:
    time.sleep(2)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': 2023, 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
        timeout=15)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    print(f'{reporter} 2023: {len(rows)} results', end='')
    if rows:
        tds = rows[0].find_all('td')
        print(f' | Sample: {tds[1].get_text(strip=True) if len(tds) > 1 else "?"}', end='')
    print()

time.sleep(3)

# === STEP 9: Party name "Khan" - check for unreported cases ===
print('\n=== STEP 7: Party name search ===')
resp_khan = s.post(f'{BASE_URL}/Login/CitationSearch',
    data={'year': '', 'book': '', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': 'Khan'},
    timeout=30)
soup = BeautifulSoup(resp_khan.text, 'html.parser')
rows = soup.find_all('tr', class_='caseType')
print(f'Party "Khan": {len(rows)} results')
for row in rows[:15]:
    tds = row.find_all('td')
    if len(tds) >= 2:
        print(f'  {tds[1].get_text(strip=True)}')

time.sleep(3)

# === STEP 10: Look at actual PLS case search HTML to understand the 387K source ===
print('\n=== STEP 8: Explore PLS site structure ===')
# Visit the main search page to understand structure
for url in [f'{BASE_URL}/Login/CaseLaw', f'{BASE_URL}/Login/JudgmentIndex', f'{BASE_URL}/Login/Dashboard']:
    time.sleep(2)
    try:
        resp = s.get(url, timeout=15)
        print(f'{url}: {resp.status_code}, {len(resp.text)} chars, URL: {resp.url}')
        if resp.status_code == 200 and len(resp.text) > 1000:
            # Look for any mention of numbers
            text = BeautifulSoup(resp.text, 'html.parser').get_text()
            nums = re.findall(r'(\d[\d,]{3,})', text)
            print(f'  Large numbers: {nums[:10]}')
    except Exception as e:
        print(f'{url}: ERROR {e}')

time.sleep(3)

# === STEP 11: Count per-reporter with all years ===
print('\n=== STEP 9: Per-reporter totals via CitationSearch ===')
# Check some reporters across all years
all_reporters = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR', 'PLCCS']
for reporter in all_reporters[:5]:
    time.sleep(2)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
        timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    text = soup.get_text()
    # Look for total count in page
    nums = re.findall(r'(\d[\d,]+)\s*(record|result|case|found|total)', text, re.I)
    print(f'{reporter} (all years): {len(rows)} rows on page, count mentions: {nums[:3]}')

print('\n=== DONE ===')
