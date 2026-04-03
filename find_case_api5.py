"""Get full citation search AJAX + test it."""
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

# Get layoutScript.js - lines 1250-1320 (citation search handler)
js_resp = s.get(f"{BASE_URL}/Scripts/layoutScript.js", timeout=15)
lines = js_resp.text.split('\n')
print("=== Citation Search handler (lines 1250-1340) ===")
for j in range(1250, min(len(lines), 1340)):
    print(f"  {j}: {lines[j]}")

# Test citation search
print("\n\n=== Testing Citation Search ===")
# Based on the code: book=SCMR, year=2024, code=1 (page number)
test_params = [
    {"book": "SCMR", "year": "2024", "code": "1", "court": "", "judge": "", "lawyer": "", "party": "", "rowNo": "0"},
    {"book": "SCMR", "year": "2024", "code": "", "court": "", "judge": "", "lawyer": "", "party": "", "rowNo": "0"},
    {"nd": "SCMR", "year": "2024", "code": "", "court": "", "judge": "", "lawyer": "", "appelant": "", "rowNo": "0"},
]

# Find the URL for citation search first
for i, line in enumerate(lines):
    if i > 1253 and i < 1320 and ('url' in line.lower() and 'Login' in line):
        print(f"  Found URL at line {i}: {line.strip()}")

for params in test_params:
    for url in ["/Login/AdvanceSearch", "/Login/CitationSearch", "/Login/GetCaseFile"]:
        resp = s.post(f"{BASE_URL}{url}", data=params, timeout=10)
        if resp.status_code == 200 and len(resp.text) > 100:
            print(f"  {url} with {params} -> status={resp.status_code}, len={len(resp.text)} ✅")
            if len(resp.text) < 1000:
                print(f"    Preview: {resp.text[:300]}")
            else:
                print(f"    First 300 chars: {resp.text[:300]}")
            break
