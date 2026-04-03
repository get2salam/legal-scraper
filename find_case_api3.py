"""Extract AdvanceSearch, citation search, and browse parameters from PLS JS."""
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

# Get layoutScript.js
js_resp = s.get(f"{BASE_URL}/Scripts/layoutScript.js", timeout=15)
js = js_resp.text
lines = js.split('\n')

# Find AdvanceSearch context
for i, line in enumerate(lines):
    if 'AdvanceSearch' in line:
        start = max(0, i-10)
        end = min(len(lines), i+20)
        print(f"=== AdvanceSearch context (line {i}) ===")
        for j in range(start, end):
            print(f"  {j}: {lines[j]}")
        print()

# Find citation search
for i, line in enumerate(lines):
    if 'citationSearch' in line.lower() or 'citation_search' in line.lower():
        start = max(0, i-5)
        end = min(len(lines), i+15)
        print(f"=== Citation search context (line {i}) ===")
        for j in range(start, end):
            print(f"  {j}: {lines[j]}")
        print()

# Now try the API with test data
print("\n=== Testing GetCaseFile API ===")
# Try with a known citation format
test_cases = [
    "2024 SCMR 1",
    "2024_SCMR_1",
    "PLD 2024 SC 1",
    "2024 SCMR 001",
    "2024SCMR1",
]
for cn in test_cases:
    resp = s.post(f"{BASE_URL}/Login/GetCaseFile", data={"caseName": cn, "headNotes": 0}, timeout=10)
    print(f"  caseName='{cn}' -> status={resp.status_code}, len={len(resp.text)}, has_content={len(resp.text) > 100}")
    if len(resp.text) > 100 and len(resp.text) < 500:
        print(f"    Response: {resp.text[:300]}")

# Try AdvanceSearch API
print("\n=== Testing AdvanceSearch API ===")
search_params = [
    {"searchString": "SCMR", "year": "2024", "pageNumber": "1"},
    {"searchString": "", "year": "2024", "reporter": "SCMR", "pageNumber": "1"},
    {"opt": "sbn", "val": "SCMR", "yr": "2024", "pg": "1"},
]
for params in search_params:
    resp = s.post(f"{BASE_URL}/Login/AdvanceSearch", data=params, timeout=10)
    print(f"  params={params} -> status={resp.status_code}, len={len(resp.text)}")
    if resp.status_code == 200 and len(resp.text) > 100 and len(resp.text) < 500:
        print(f"    Response: {resp.text[:300]}")
