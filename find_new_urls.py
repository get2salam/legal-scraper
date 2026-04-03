"""Login via ClearLoginHistory flow, then explore the new PLS URL structure."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.pakistanlawsite.com"
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

s = curl_requests.Session(impersonate='chrome')

# Get homepage + CSRF
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None

# Clear login history (force logout others)
s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username,
    "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)

# Follow to dashboard
time.sleep(2)
dash = s.get(f"{BASE_URL}/Login/Check", timeout=15)
print(f"Dashboard: status={dash.status_code}, len={len(dash.text)}")

# Save it
with open('pls_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(dash.text)

soup = BeautifulSoup(dash.text, 'html.parser')

# Find ALL links
all_links = soup.find_all('a', href=True)
print(f"\nAll links ({len(all_links)}):")
seen = set()
for a in all_links:
    href = a['href']
    text = a.get_text(strip=True)[:80]
    if href not in seen and href != '#' and not href.startswith('javascript'):
        seen.add(href)
        print(f"  {href} -> {text}")

# Find all form actions
forms = soup.find_all('form')
print(f"\nForms ({len(forms)}):")
for f in forms:
    print(f"  action={f.get('action')} method={f.get('method')}")

# Find script sources and inline JS with URLs
scripts = soup.find_all('script')
print(f"\nScript tags ({len(scripts)}):")
for sc in scripts:
    src = sc.get('src')
    if src:
        print(f"  src: {src}")
    elif sc.string:
        # Find URLs in inline JS
        urls = re.findall(r'["\']/([\w/-]+(?:\?[^"\']*)?)["\']', sc.string)
        for u in set(urls):
            if len(u) > 3 and not u.startswith(('css/', 'js/', 'fonts/', 'images/', 'lib/')):
                print(f"  JS URL: /{u}")

# Find data attributes that might contain URLs
elements_with_data = soup.find_all(attrs={"data-url": True})
for el in elements_with_data:
    print(f"  data-url: {el['data-url']}")

# Try common new URL patterns
print("\n\nTesting new URL patterns...")
test_urls = [
    "/Home/CaseLaw",
    "/CaseLaw",
    "/CaseLaw/Search",
    "/CaseLaw/Index",
    "/Home/Index",
    "/Home",
    "/Search/CaseLaw",
    "/Search/Index",
    "/PLSOnline/law",
    "/PLSOnline",
    "/Law/Online",
    "/Login/Home",
    "/Login/LawOnline",
]
for url in test_urls:
    try:
        resp = s.get(f"{BASE_URL}{url}", timeout=10, allow_redirects=False)
        if resp.status_code != 404:
            print(f"  {url} -> {resp.status_code} (len={len(resp.text)})")
    except:
        pass

# CRITICAL: Try the old case URL format with this logged-in session
print("\nTesting old case URL formats...")
old_urls = [
    "/LawOnline/law?opt=sbn&val=SCMR&yr=2024&pg=1",
    "/LawOnline/law?opt=citation&val=2024+SCMR+1",
    "/LawOnline/Law?opt=sbn&val=SCMR&yr=2024&pg=1",
]
for url in old_urls:
    resp = s.get(f"{BASE_URL}{url}", timeout=10)
    print(f"  {url} -> {resp.status_code} (len={len(resp.text)})")
