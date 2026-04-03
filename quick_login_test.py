"""Quick PLS login test."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
s = curl_requests.Session(impersonate='chrome')
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

# Get homepage + CSRF
r = s.get(f"{BASE_URL}/", timeout=15)
print(f"Homepage: {r.status_code}, len={len(r.text)}")
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None
print(f"CSRF: {bool(csrf)}")

time.sleep(2)

# Try ClearLoginHistory
print("\nTrying ClearLoginHistory...")
clear = s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username, "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
print(f"Clear status: {clear.status_code}, response: {clear.text[:200]}")

time.sleep(2)

# Follow to Check
print("\nFollowing to /Login/Check...")
check = s.get(f"{BASE_URL}/Login/Check", timeout=15)
print(f"Check status: {check.status_code}, len={len(check.text)}")
print(f"Has Logout: {'Logout' in check.text}")
print(f"Account in use: {'Account Already In Use' in check.text}")

if 'Logout' in check.text and len(check.text) > 10000:
    print("\n✅ LOGGED IN! Testing citation search...")
    time.sleep(2)
    search = s.post(f"{BASE_URL}/Login/CitationSearch", data={
        "book": "SCMR", "year": "2010", "code": "", "court": "",
        "judge": "", "lawyer": "", "party": "", "rowNo": "0"
    }, timeout=15)
    print(f"Search status: {search.status_code}, len={len(search.text)}")
    if search.status_code == 200 and len(search.text) > 100:
        print("✅ Citation search works!")
    else:
        print(f"❌ Search failed. Response: {search.text[:300]}")
else:
    print("\n❌ Not logged in")
    # Try direct login
    print("Trying direct /Login/Login...")
    login = s.post(f"{BASE_URL}/Login/Login", data={
        "Login.UserName": username, "Login.Password": password,
        "__RequestVerificationToken": csrf,
    }, timeout=15)
    print(f"Login status: {login.status_code}, len={len(login.text)}")
    print(f"Account in use: {'Account Already In Use' in login.text}")
    print(f"Has Logout: {'Logout' in login.text}")
