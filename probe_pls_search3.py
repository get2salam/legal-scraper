"""
PLS Search Probe Part 3 - Deep investigation
Key questions:
1. Why does empty CitationSearch default to CLC?
2. What endpoint returns the 387K full-text results?
3. Are there notes reporters we missed?
4. What pages does CitationSearch actually show?
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

# === A: Inspect CitationSearch form structure ===
print('\n=== A: CitationSearch form HTML ===')
time.sleep(3)
# GET the CitationSearch page
resp_form = s.get(f'{BASE_URL}/Login/Check', timeout=30)
soup_form = BeautifulSoup(resp_form.text, 'html.parser')
# The form is on the main page - find it
forms = soup_form.find_all('form')
print(f'Forms found: {len(forms)}')
for form in forms:
    action = form.get('action', '')
    print(f'\nForm action: {action}')
    for inp in form.find_all(['input', 'select']):
        name = inp.get('name', '')
        val = inp.get('value', '')
        inp_type = inp.get('type', 'text')
        if name:
            print(f'  {name} ({inp_type}) = "{val}"')
        if inp.name == 'select':
            options = [(o.get('value',''), o.get_text(strip=True)) for o in inp.find_all('option')]
            print(f'  SELECT {name}: {options[:10]}')

time.sleep(3)

# === B: Look at the actual books/reporters dropdown ===
print('\n=== B: Books dropdown in CitationSearch ===')
# Need to GET the CitationSearch page to see the form
resp_cs = s.get(f'{BASE_URL}/Login/CitationSearch', timeout=30)
print(f'CitationSearch GET: {resp_cs.status_code}, {len(resp_cs.text)} chars, URL: {resp_cs.url}')
soup_cs = BeautifulSoup(resp_cs.text, 'html.parser')
selects = soup_cs.find_all('select')
for sel in selects:
    name = sel.get('name', sel.get('id', ''))
    options = [(o.get('value',''), o.get_text(strip=True)) for o in sel.find_all('option')]
    print(f'SELECT {name}: {options}')

# Also check input fields
inputs = soup_cs.find_all('input')
for inp in inputs:
    name = inp.get('name', inp.get('id', ''))
    val = inp.get('value', '')
    if name:
        print(f'INPUT {name} = "{val}"')

time.sleep(3)

# === C: CitationSearch with proper book selection ===
print('\n=== C: CitationSearch proper - SCMR all years ===')
resp_scmr = s.post(f'{BASE_URL}/Login/CitationSearch',
    data={'year': '', 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
    timeout=60)
soup_scmr = BeautifulSoup(resp_scmr.text, 'html.parser')
rows = soup_scmr.find_all('tr', class_='caseType')
print(f'SCMR all years: {len(rows)} rows')
text = soup_scmr.get_text()
big_nums = re.findall(r'(?<!\w)(\d{4,})(?!\w)', text)
print(f'Big numbers on page: {big_nums[:20]}')
# Check pagination
pag = soup_scmr.find_all(class_=re.compile(r'pag|page', re.I))
for p in pag[:5]:
    print(f'Pag: {p}')
# Find "showing X of Y" text
showing = re.findall(r'show\w*\s+\d+[\s-]+\d+\s+of\s+\d+|total.*?\d+', text, re.I)
print(f'Showing: {showing[:5]}')
if rows:
    for row in rows[:5]:
        tds = row.find_all('td')
        if len(tds) >= 2:
            print(f'  {tds[1].get_text(strip=True)}')

time.sleep(3)

# === D: Check CitationSearch with page=2 for SCMR ===
print('\n=== D: SCMR pagination ===')
for pg in [1, 2, 3, 100]:
    time.sleep(3)
    resp = s.post(f'{BASE_URL}/Login/CitationSearch',
        data={'year': '', 'book': 'SCMR', 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': '', 'page': pg},
        timeout=60)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    if rows:
        first_cit = rows[0].find_all('td')[1].get_text(strip=True) if len(rows[0].find_all('td')) > 1 else '?'
        last_cit = rows[-1].find_all('td')[1].get_text(strip=True) if len(rows[-1].find_all('td')) > 1 else '?'
        print(f'page={pg}: {len(rows)} rows, first={first_cit}, last={last_cit}')
    else:
        print(f'page={pg}: 0 rows')

time.sleep(3)

# === E: Explore the full-text search (the 387K source) ===
print('\n=== E: Full-text search exploration ===')
# The 387K might come from the WordsAndPhrases or Topics search
for url_path in ['/Login/WordsAndPhrases', '/Login/TopicPage', '/Login/ArticlePage']:
    time.sleep(2)
    resp = s.get(f'{BASE_URL}{url_path}', timeout=30)
    print(f'{url_path}: {resp.status_code}, {len(resp.text)} chars')
    if resp.status_code == 200 and len(resp.text) > 2000:
        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text()
        big_nums = re.findall(r'(?<!\w)(\d{5,})(?!\w)', text)
        print(f'  Big numbers: {big_nums[:5]}')
        forms = [(f.get('action',''), [i.get('name','') for i in f.find_all('input')]) for f in soup.find_all('form')]
        print(f'  Forms: {forms[:3]}')
        print(f'  First 300: {text[:300]}')

time.sleep(3)

# === F: Try GET the main content page after login ===
print('\n=== F: Explore post-login content pages ===')
# After login, the main page should show content
check_resp = s.get(f'{BASE_URL}/Login/Check', timeout=30)
print(f'Check page HTML (first 3000):')
print(check_resp.text[:3000])

time.sleep(3)

# === G: Count notes reporters comprehensively ===
print('\n=== G: Notes reporters - year by year ===')
# CLCN, PCRLJN, YLRN seem to exist from 2016
# Check range of years
notes_reporters = ['CLCN', 'PCRLJN', 'YLRN', 'PLCN']
for reporter in notes_reporters:
    print(f'\n{reporter}:')
    for year in range(2015, 2026):
        time.sleep(2)
        resp = s.post(f'{BASE_URL}/Login/CitationSearch',
            data={'year': year, 'book': reporter, 'code': '', 'court': '', 'judge': '', 'lawyer': '', 'party': ''},
            timeout=30)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all('tr', class_='caseType')
        if rows:
            sample = rows[0].find_all('td')[1].get_text(strip=True) if len(rows[0].find_all('td')) > 1 else '?'
            print(f'  {year}: {len(rows)} rows, sample={sample}')
        else:
            print(f'  {year}: 0 rows')

print('\n=== DONE ===')
