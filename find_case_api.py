"""Find the new PLS case API parameters by checking JS files."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

s = curl_requests.Session(impersonate='chrome')

# Login via clear history flow
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None
s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username, "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
time.sleep(2)
dash = s.get(f"{BASE_URL}/Login/Check", timeout=15)
print(f"Logged in: {len(dash.text) > 10000}")

# Fetch key JS files
for js_path in ['/Scripts/common.js', '/Scripts/layoutScript.js']:
    print(f"\n{'='*60}")
    print(f"Fetching {js_path}...")
    js_resp = s.get(f"{BASE_URL}{js_path}", timeout=15)
    if js_resp.status_code == 200:
        js_text = js_resp.text
        # Find all references to GetCaseFile, GetStatuesSearch, or any API-like call
        for pattern in ['GetCaseFile', 'GetStatuesSearch', 'GetArticle', 'opt=', 'sbn', 'citation', 'val=', 'yr=', 'pg=', 'ajax', 'url:']:
            matches = [line.strip() for line in js_text.split('\n') if pattern in line]
            if matches:
                print(f"\n  '{pattern}' found in {len(matches)} lines:")
                for m in matches[:10]:
                    print(f"    {m[:200]}")

# Also check the dashboard HTML for inline JS with API patterns
print(f"\n{'='*60}")
print("Checking dashboard inline JS...")
for pattern in ['GetCaseFile', 'GetStatuesSearch', 'opt=', 'sbn', 'citation', 'val=', 'yr=', 'pg=']:
    matches = [line.strip() for line in dash.text.split('\n') if pattern in line]
    if matches:
        print(f"\n  '{pattern}' found in {len(matches)} lines:")
        for m in matches[:10]:
            print(f"    {m[:200]}")
