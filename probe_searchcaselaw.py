"""
Probe SearchCaseLaw, AdvanceSearch, IndexSearch - these are the 387K source
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

# === A: SearchCaseLaw - this is the KEYWORD/full-text search ===
print('\n=== A: SearchCaseLaw endpoint ===')
# From layoutScript.js: GET /Login/SearchCaseLaw with pageindex, pagesize, year, book, code, court, searchType
time.sleep(3)

# Try with no filters to get all cases
resp = s.get(f'{BASE_URL}/Login/SearchCaseLaw',
    params={'pageindex': 0, 'pagesize': 20, 'year': '', 'book': '', 'code': '', 'court': '', 'searchType': ''},
    timeout=60)
print(f'SearchCaseLaw GET (empty): {resp.status_code}, {len(resp.text)} chars')
if resp.status_code == 200:
    with open('probe_searchcaselaw.html', 'w', encoding='utf-8') as f:
        f.write(resp.text)
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    text = soup.get_text()
    big_nums = re.findall(r'(?<!\d)(\d{4,})(?!\d)', text)
    counts = re.findall(r'(\d[\d,]+)\s*(?:record|result|case|total|found)', text, re.I)
    print(f'  Rows: {len(rows)}, big_nums: {big_nums[:10]}, counts: {counts[:5]}')
    print(f'  First 500: {resp.text[:500]}')

time.sleep(3)

# Try with keyword "a"
resp2 = s.get(f'{BASE_URL}/Login/SearchCaseLaw',
    params={'pageindex': 0, 'pagesize': 20, 'year': '', 'book': '', 'code': '', 'court': '', 'searchType': 'caselaw'},
    timeout=60)
print(f'SearchCaseLaw GET (searchType=caselaw): {resp2.status_code}, {len(resp2.text)} chars')
if resp2.status_code == 200:
    soup2 = BeautifulSoup(resp2.text, 'html.parser')
    rows2 = soup2.find_all('tr', class_='caseType')
    text2 = soup2.get_text()
    big_nums2 = re.findall(r'(?<!\d)(\d{4,})(?!\d)', text2)
    print(f'  Rows: {len(rows2)}, big_nums: {big_nums2[:10]}')
    print(f'  First 300: {resp2.text[:300]}')

time.sleep(3)

# === B: SearchCaseLaw via POST ===
print('\n=== B: SearchCaseLaw POST ===')
resp3 = s.post(f'{BASE_URL}/Login/SearchCaseLaw',
    data={'pageindex': 0, 'pagesize': 20, 'year': '', 'book': '', 'code': '', 'court': '', 'searchType': ''},
    timeout=60)
print(f'SearchCaseLaw POST (empty): {resp3.status_code}, {len(resp3.text)} chars')
print(f'  First 500: {resp3.text[:500]}')

time.sleep(3)

# === C: SearchCaseLaw with searchType="statutes" ===
print('\n=== C: SearchCaseLaw searchType variants ===')
for stype in ['', 'caselaw', 'statutes', 'keyword', 'fulltext', 'citation']:
    time.sleep(2)
    try:
        r = s.get(f'{BASE_URL}/Login/SearchCaseLaw',
            params={'pageindex': 0, 'pagesize': 20, 'year': '', 'book': '', 'code': 'a', 'court': '', 'searchType': stype},
            timeout=30)
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.find_all('tr', class_='caseType')
        text = soup.get_text()
        big_nums = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text)
        print(f'  searchType="{stype}": {r.status_code}, rows={len(rows)}, big={big_nums[:3]}, len={len(r.text)}')
    except Exception as e:
        print(f'  searchType="{stype}": ERROR {e}')

time.sleep(3)

# === D: AdvanceSearch endpoint ===
print('\n=== D: AdvanceSearch endpoint ===')
resp_adv = s.post(f'{BASE_URL}/Login/AdvanceSearch',
    data={
        'court': '', 'judge': '', 'lawyer': '', 'appelant': '',
        'nd': '', 'rule': '', 'act': '', 'actSection': '',
        'act1': '', 'act1Section': '', 'rowNo': 0
    },
    timeout=60)
print(f'AdvanceSearch POST: {resp_adv.status_code}, {len(resp_adv.text)} chars')
if resp_adv.status_code == 200:
    with open('probe_advancesearch.html', 'w', encoding='utf-8') as f:
        f.write(resp_adv.text)
    soup_adv = BeautifulSoup(resp_adv.text, 'html.parser')
    rows_adv = soup_adv.find_all('tr', class_='caseType')
    text_adv = soup_adv.get_text()
    big_nums_adv = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text_adv)
    counts_adv = re.findall(r'(\d[\d,]+)\s*(?:record|result|case|total|found)', text_adv, re.I)
    print(f'  Rows: {len(rows_adv)}, big: {big_nums_adv[:5]}, counts: {counts_adv[:3]}')
    print(f'  First 500: {resp_adv.text[:500]}')

time.sleep(3)

# === E: IndexSearch endpoint ===
print('\n=== E: IndexSearch endpoint ===')
resp_idx = s.post(f'{BASE_URL}/Login/IndexSearch',
    data={'year': 2023, 'book': 'SCMR', 'court': ''},
    timeout=30)
print(f'IndexSearch POST (SCMR 2023): {resp_idx.status_code}, {len(resp_idx.text)} chars')
if resp_idx.status_code == 200:
    with open('probe_indexsearch.html', 'w', encoding='utf-8') as f:
        f.write(resp_idx.text)
    soup_idx = BeautifulSoup(resp_idx.text, 'html.parser')
    rows_idx = soup_idx.find_all('tr', class_='caseType')
    text_idx = soup_idx.get_text()
    big_nums_idx = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text_idx)
    print(f'  Rows: {len(rows_idx)}, big: {big_nums_idx[:5]}')
    print(f'  First 300: {resp_idx.text[:300]}')
    if rows_idx:
        for row in rows_idx[:3]:
            print(f'  Row: {row.get_text(strip=True)[:100]}')

time.sleep(3)

# === F: LoadMoreCaseLaw - what does it return? ===
print('\n=== F: LoadMoreCaseLaw endpoint ===')
resp_lmc = s.get(f'{BASE_URL}/Login/LoadMoreCaseLaw',
    params={'book': 'SCMR', 'court': '', 'row': 0, 'year': 2023, 'caseTypeId': ''},
    timeout=30)
print(f'LoadMoreCaseLaw GET: {resp_lmc.status_code}, {len(resp_lmc.text)} chars')
if resp_lmc.status_code == 200:
    soup_lmc = BeautifulSoup(resp_lmc.text, 'html.parser')
    rows_lmc = soup_lmc.find_all('tr', class_='caseType')
    text_lmc = soup_lmc.get_text()
    big_nums_lmc = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text_lmc)
    print(f'  Rows: {len(rows_lmc)}, big: {big_nums_lmc[:5]}')
    print(f'  First 300: {resp_lmc.text[:300]}')

time.sleep(3)

# === G: searchStatuteCaseLaw endpoint ===
print('\n=== G: searchStatuteCaseLaw endpoint ===')
resp_ssc = s.post(f'{BASE_URL}/Login/searchStatuteCaseLaw',
    data={'book': 'SCMR', 'book': 'SCMR', 'caseTypeId': '', 'court': ''},
    timeout=30)
print(f'searchStatuteCaseLaw POST: {resp_ssc.status_code}, {len(resp_ssc.text)} chars')
if resp_ssc.status_code == 200:
    soup_ssc = BeautifulSoup(resp_ssc.text, 'html.parser')
    rows_ssc = soup_ssc.find_all('tr', class_='caseType')
    text_ssc = soup_ssc.get_text()
    big_nums_ssc = re.findall(r'(?<!\d)(\d{5,})(?!\d)', text_ssc)
    print(f'  Rows: {len(rows_ssc)}, big: {big_nums_ssc[:5]}')
    print(f'  First 300: {resp_ssc.text[:300]}')

time.sleep(3)

# === H: GetCaseFile - what does it return for a known caseName? ===
print('\n=== H: GetCaseFile endpoint ===')
# From the homepage: caseName="2026S706" = SCMR case, "2026P204" = another reporter
test_cases = ['2026S706', '2026P204', '2026I5004', '2026K4001']
for case_id in test_cases:
    time.sleep(2)
    resp_gcf = s.post(f'{BASE_URL}/Login/GetCaseFile',
        data={'caseName': case_id, 'headNotes': 0},
        timeout=30)
    print(f'GetCaseFile({case_id}): {resp_gcf.status_code}, {len(resp_gcf.text)} chars')
    if resp_gcf.status_code == 200 and len(resp_gcf.text) > 100:
        soup_gcf = BeautifulSoup(resp_gcf.text, 'html.parser')
        text_gcf = soup_gcf.get_text(strip=True)
        print(f'  Text: {text_gcf[:300]}')
        # Check if this has citation
        cit_match = re.search(r'\d{4}\s+(?:SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR|PLCCS)\s+\d+', text_gcf)
        if cit_match:
            print(f'  Citation: {cit_match.group(0)}')
        # Save first one for inspection
        if case_id == '2026S706':
            with open('probe_getcasefile.html', 'w', encoding='utf-8') as f:
                f.write(resp_gcf.text)

time.sleep(3)

# === I: Try GetCaseFile with headNotes=1 ===
print('\n=== I: GetCaseFile with headNotes=1 ===')
resp_gcf_hn = s.post(f'{BASE_URL}/Login/GetCaseFile',
    data={'caseName': '2026S706', 'headNotes': 1},
    timeout=30)
print(f'GetCaseFile(headNotes=1): {resp_gcf_hn.status_code}, {len(resp_gcf_hn.text)} chars')
if resp_gcf_hn.status_code == 200:
    soup = BeautifulSoup(resp_gcf_hn.text, 'html.parser')
    print(f'  Text: {soup.get_text(strip=True)[:300]}')
    with open('probe_getcasefile_hn.html', 'w', encoding='utf-8') as f:
        f.write(resp_gcf_hn.text)

print('\n=== DONE ===')
