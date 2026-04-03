"""Force logout all PLS sessions, then login fresh and verify."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
s = curl_requests.Session(impersonate='chrome')

username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

# Step 1: Get homepage + CSRF
print("1. Getting homepage...")
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None
print(f"   CSRF: {bool(csrf)}")

# Step 2: Try login (will get "Account Already In Use")
print("2. Attempting login...")
login_resp = s.post(f"{BASE_URL}/Login/Login", data={
    "Login.UserName": username,
    "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
print(f"   Account in use: {'Account Already In Use' in login_resp.text}")

# Step 3: Force logout from all devices
print("3. Forcing logout from all devices...")
clear_resp = s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username,
    "Login.Password": password,
}, timeout=15)
print(f"   Clear response status: {clear_resp.status_code}")
print(f"   Clear response: {clear_resp.text[:200]}")

time.sleep(3)

# Step 4: Fresh session + re-login
print("4. Fresh session + re-login...")
s = curl_requests.Session(impersonate='chrome')
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None

login_resp = s.post(f"{BASE_URL}/Login/Login", data={
    "Login.UserName": username,
    "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
print(f"   Status: {login_resp.status_code}")
print(f"   Account in use: {'Account Already In Use' in login_resp.text}")
print(f"   Has Logout: {'Logout' in login_resp.text}")
print(f"   Response length: {len(login_resp.text)}")

if 'Account Already In Use' not in login_resp.text and len(login_resp.text) > 10000:
    print("\n✅ LOGIN SUCCESSFUL! PLS session is active.")
    
    # Verify by fetching a case
    time.sleep(2)
    test = s.get(f"{BASE_URL}/LawOnline/law?opt=sbn&val=SCMR&yr=2024&pg=1", timeout=15)
    print(f"   Case fetch test: status={test.status_code}, len={len(test.text)}")
    if test.status_code == 200 and len(test.text) > 5000:
        print("   ✅ Case data accessible!")
    else:
        print(f"   ❌ Case fetch issue. First 200 chars: {test.text[:200]}")
else:
    print("\n❌ Still blocked. May need manual intervention.")
    print(f"   First 500 chars: {login_resp.text[:500]}")
