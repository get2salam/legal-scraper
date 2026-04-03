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

resp = s.get(f"{B}/", timeout=15)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"Homepage: {resp.status_code}, {len(resp.text)} chars")

# All forms
print("\n=== FORMS ===")
for form in soup.find_all("form"):
    print(f"  action={form.get('action','')} method={form.get('method','get')}")
    for inp in form.find_all(["input","select"]):
        print(f"    {inp.get('name','')} [{inp.get('type','text')}] = {inp.get('value','')[:40]}")

# All links
print("\n=== NAV LINKS ===")
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(strip=True)[:50]
    if txt: print(f"  {txt} → {href}")

# Inline scripts mentioning search
print("\n=== SEARCH IN SCRIPTS ===")
for sc in soup.find_all("script"):
    t = sc.string or ""
    if "search" in t.lower() or "keyword" in t.lower():
        print(t[:500])
