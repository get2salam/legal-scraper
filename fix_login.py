import os
import re
from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session, BrowserType
import time

BASE_URL = 'https://www.pakistanlawsite.com'
PLS_USER = os.getenv('PLS_USER')
PLS_PASS = os.getenv('PLS_PASS')

session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
})

# Step 1: Get home page + CSRF token
home = session.get(f'{BASE_URL}/', timeout=30)
token_match = re.search(r'<input name="__RequestVerificationToken"[^>]+value=["\']([^"\']+)["\']', home.text)
token = token_match.group(1) if token_match else ''
print(f'Step 1: Got home page, CSRF token: {token[:20]}...')

# Step 2: Normal login attempt (will trigger multi-login modal)
login_headers = {
    'Referer': f'{BASE_URL}/',
    'Origin': BASE_URL,
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest',
}
resp = session.post(
    f'{BASE_URL}/Login/Login',
    data={
        'Login.UserName': PLS_USER,
        'Login.Password': PLS_PASS,
        '__RequestVerificationToken': token,
    },
    headers=login_headers,
    timeout=30
)
print(f'Step 2: Login POST status: {resp.status_code}')

# Check if we got the multi-login modal
if 'ClearLoginHistory' in resp.text:
    print('Step 2: Multi-login modal detected! Clearing login history...')
    time.sleep(2)
    
    # Step 3: POST to ClearLoginHistory
    clear_resp = session.post(
        f'{BASE_URL}/Login/ClearLoginHistory',
        data={
            'Login.UserName': PLS_USER,
            'Login.Password': PLS_PASS,
        },
        headers={
            'Referer': f'{BASE_URL}/',
            'Origin': BASE_URL,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
        },
        timeout=30
    )
    print(f'Step 3: ClearLoginHistory status: {clear_resp.status_code}')
    print(f'Response (first 500): {clear_resp.text[:500]}')
    
    # Step 4: Now try logging in again
    time.sleep(2)
    home2 = session.get(f'{BASE_URL}/', timeout=30)
    token2_match = re.search(r'<input name="__RequestVerificationToken"[^>]+value=["\']([^"\']+)["\']', home2.text)
    token2 = token2_match.group(1) if token2_match else token
    
    login2 = session.post(
        f'{BASE_URL}/Login/Login',
        data={
            'Login.UserName': PLS_USER,
            'Login.Password': PLS_PASS,
            '__RequestVerificationToken': token2,
        },
        headers=login_headers,
        timeout=30
    )
    print(f'Step 4: Second login attempt status: {login2.status_code}')
    print(f'Response (first 300): {login2.text[:300]}')
    
    # Step 5: Check if logged in
    time.sleep(2)
    check = session.get(f'{BASE_URL}/Login/Check', timeout=30)
    print(f'Step 5: Check URL: {check.url}')
    print(f'Check status: {check.status_code}')
    # If redirected to home, not logged in; if stays at /Login/Check, logged in
    if 'Check' in check.url:
        print('SUCCESS: Still on /Login/Check = LOGGED IN')
    else:
        print(f'FAILED: Redirected to {check.url}')
else:
    print('No multi-login modal - checking if already logged in')
    check = session.get(f'{BASE_URL}/Login/Check', timeout=30)
    print(f'Check URL: {check.url}')
