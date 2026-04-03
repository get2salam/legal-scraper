"""Find actual caseName format from dashboard + full AdvanceSearch parameters."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

s = curl_requests.Session(impersonate='chrome')

# Login
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None
s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username, "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
time.sleep(2)
dash = s.get(f"{BASE_URL}/Login/Check", timeout=15)

from bs4 import BeautifulSoup
soup = BeautifulSoup(dash.text, 'html.parser')

# Find elements with caseName attribute
case_elements = soup.find_all(attrs={"casename": True})
print(f"Elements with caseName attr: {len(case_elements)}")
for el in case_elements[:20]:
    print(f"  caseName='{el.get('casename')}' text='{el.get_text(strip=True)[:80]}'")

# Also check for latestCaseLaw class
latest = soup.find_all(class_='latestCaseLaw')
print(f"\nlatestCaseLaw elements: {len(latest)}")
for el in latest[:20]:
    print(f"  caseName='{el.get('casename')}' text='{el.get_text(strip=True)[:80]}'")

# Check for any data attributes
all_with_data = soup.find_all(lambda tag: any(attr.startswith('data-') or attr == 'casename' for attr in tag.attrs))
print(f"\nElements with data- attrs: {len(all_with_data)}")
for el in list(all_with_data)[:10]:
    attrs = {k:v for k,v in el.attrs.items() if k.startswith('data-') or k == 'casename'}
    if attrs:
        print(f"  {el.name}: {attrs} text='{el.get_text(strip=True)[:60]}'")

# Now get layoutScript.js and find full AdvanceSearch params
js_resp = s.get(f"{BASE_URL}/Scripts/layoutScript.js", timeout=15)
lines = js_resp.text.split('\n')

# Find AdvanceSearch AJAX call with full data payload
for i, line in enumerate(lines):
    if 'Login/AdvanceSearch' in line or 'Login/LoadMoreAdvanceSearch' in line:
        start = max(0, i-30)
        end = min(len(lines), i+10)
        print(f"\n=== AdvanceSearch AJAX (line {i}) ===")
        for j in range(start, end):
            print(f"  {j}: {lines[j]}")

# Find Citation_Search_btn click handler with full AJAX
for i, line in enumerate(lines):
    if 'Citation_Search_btn' in line and 'click' in line:
        start = max(0, i-5)
        end = min(len(lines), i+50)
        print(f"\n=== Citation_Search_btn handler (line {i}) ===")
        for j in range(start, end):
            print(f"  {j}: {lines[j]}")
        break
