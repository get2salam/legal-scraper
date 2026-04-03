"""Force logout all PLS sessions — follow the full flow."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

# Use same session throughout
s = curl_requests.Session(impersonate='chrome')

# Step 1: Homepage + CSRF
print("1. Getting homepage...")
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None

# Step 2: Force clear login history (same session, with CSRF)
print("2. Clearing login history...")
clear_resp = s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username,
    "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
print(f"   Response: {clear_resp.text[:200]}")

# Step 3: Follow the redirect to /Login/Check
print("3. Following redirect to /Login/Check...")
check = s.get(f"{BASE_URL}/Login/Check", timeout=15)
print(f"   Status: {check.status_code}, Length: {len(check.text)}")
# Check if we're now logged in
if len(check.text) > 10000:
    print("   Looks like we're in! Checking for case data access...")
    
    # Try fetching a case
    time.sleep(2)
    test = s.get(f"{BASE_URL}/LawOnline/law?opt=sbn&val=SCMR&yr=2024&pg=1", timeout=15)
    print(f"   Case test: status={test.status_code}, len={len(test.text)}")
    if test.status_code == 200 and len(test.text) > 3000:
        print("   ✅ WE'RE IN! Case data accessible!")
    else:
        # Maybe URL changed - try to find it
        print(f"   Case URL might have changed. Exploring...")

# Step 4: If not in, wait and retry with completely fresh session
print("\n4. Waiting 10s then trying completely fresh session...")
time.sleep(10)
s2 = curl_requests.Session(impersonate='chrome')
r2 = s2.get(f"{BASE_URL}/", timeout=15)
csrf2_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r2.text)
csrf2 = csrf2_match.group(1) if csrf2_match else None

login2 = s2.post(f"{BASE_URL}/Login/Login", data={
    "Login.UserName": username,
    "Login.Password": password,
    "__RequestVerificationToken": csrf2,
}, timeout=15)
print(f"   Status: {login2.status_code}, Length: {len(login2.text)}")
print(f"   Account in use: {'Account Already In Use' in login2.text}")

if 'Account Already In Use' not in login2.text:
    print("   ✅ LOGGED IN!")
    # Follow to /Login/Check
    check2 = s2.get(f"{BASE_URL}/Login/Check", timeout=15)
    print(f"   /Login/Check: status={check2.status_code}, len={len(check2.text)}")
    
    # Find navigation/case URLs in the dashboard
    import re as regex
    all_urls = regex.findall(r'(?:href|action|url)\s*[=:]\s*["\']([^"\']+)["\']', check2.text)
    interesting = [u for u in set(all_urls) if any(kw in u.lower() for kw in ['law', 'case', 'search', 'online', 'report', 'judgment', 'cite'])]
    print(f"   Interesting URLs: {interesting[:20]}")
    
    # Save dashboard HTML
    with open('pls_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(check2.text)
    print("   Saved dashboard to pls_dashboard.html")
else:
    print("   ❌ Still blocked after 10s wait")
    
    # Try longer wait
    print("\n5. Waiting 30s more...")
    time.sleep(30)
    s3 = curl_requests.Session(impersonate='chrome')
    r3 = s3.get(f"{BASE_URL}/", timeout=15)
    csrf3_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r3.text)
    csrf3 = csrf3_match.group(1) if csrf3_match else None
    
    login3 = s3.post(f"{BASE_URL}/Login/Login", data={
        "Login.UserName": username,
        "Login.Password": password,
        "__RequestVerificationToken": csrf3,
    }, timeout=15)
    print(f"   Status: {login3.status_code}, Length: {len(login3.text)}")
    print(f"   Account in use: {'Account Already In Use' in login3.text}")
    if 'Account Already In Use' not in login3.text:
        print("   ✅ FINALLY LOGGED IN!")
    else:
        print("   ❌ Still blocked. PLS may need longer cooldown or manual browser logout.")
