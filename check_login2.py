import os
import re
from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session, BrowserType

BASE_URL = 'https://www.pakistanlawsite.com'
PLS_USER = os.getenv('PLS_USER')
PLS_PASS = os.getenv('PLS_PASS')

session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
})

# Get home page (which has login form)
home = session.get(f'{BASE_URL}/', timeout=30)
print(f'Home status: {home.status_code}')
print(f'Cookies after home: {dict(session.cookies)}')

# Extract any CSRF/anti-forgery tokens
token_match = re.search(r'__RequestVerificationToken[^>]+value=["\']([^"\']+)["\']', home.text)
if token_match:
    token = token_match.group(1)
    print(f'Found CSRF token: {token[:30]}...')
else:
    print('No CSRF token found')
    token = None

# Extract the login form inputs more carefully
login_section = re.search(r'<form[^>]*action=["\'][^"\']*Login[^"\']*["\'][^>]*>.*?</form>', home.text, re.DOTALL)
if login_section:
    print('\nLogin form HTML (first 1000 chars):')
    print(login_section.group(0)[:1000])
else:
    print('\nNo login form found on homepage')
    # Check what forms exist
    all_forms = re.findall(r'<form[^>]*>.*?</form>', home.text, re.DOTALL)
    print(f'Total forms on page: {len(all_forms)}')
    for i, f in enumerate(all_forms[:3]):
        print(f'\nForm {i}: {f[:200]}')

# Try POST with Referer header
print('\n\nAttempting login POST...')
data = {
    'Login.UserName': PLS_USER,
    'Login.Password': PLS_PASS,
}
if token:
    data['__RequestVerificationToken'] = token

resp = session.post(
    f'{BASE_URL}/Login/Login',
    data=data,
    headers={
        'Referer': f'{BASE_URL}/',
        'Origin': BASE_URL,
        'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout=30
)
print(f'Login POST status: {resp.status_code}')
print(f'Final URL: {resp.url}')
print(f'Response (first 500): {resp.text[:500]}')
