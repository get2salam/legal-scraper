import os, sys, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = "chrome"
B = "https://www.pakistanlawsite.com"
u = os.getenv("PLS_USER",""); p = os.getenv("PLS_PASS","")
s.post(f"{B}/Login/ClearLoginHistory", data={"Login.UserName":u,"Login.Password":p}, timeout=30)
time.sleep(3)

# Get the main logged-in page and find ALL forms and search-related links
resp = s.get(f"{B}/Login/Home", timeout=15)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"Home page: {resp.status_code}, {len(resp.text)} chars")

# Find all forms
print("\n=== All forms on home page ===")
for form in soup.find_all("form"):
    print(f"  Form: action={form.get('action','')} method={form.get('method','')}")
    for inp in form.find_all(["input","select","textarea"]):
        print(f"    {inp.name}: name={inp.get('name','')} id={inp.get('id','')} type={inp.get('type','')}")

# Find search-related links/scripts
print("\n=== Search-related JS/links ===")
for script in soup.find_all("script"):
    src = script.get("src","")
    if "search" in src.lower():
        print(f"  Script: {src}")
    text = script.string or ""
    if "search" in text.lower() and len(text) < 2000:
        print(f"  Inline script (search): {text[:300]}")

# Find any nav links mentioning search/keyword
for a in soup.find_all("a"):
    href = a.get("href","")
    text = a.get_text(strip=True)
    if any(w in (href+text).lower() for w in ["search","keyword","fulltext","judgment"]):
        print(f"  Link: {text} → {href}")
