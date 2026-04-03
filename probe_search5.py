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

# Try the root after login
for path in ["/", "/Login", "/Login/Dashboard", "/Login/Welcome", "/Login/Main"]:
    time.sleep(1)
    try:
        resp = s.get(f"{B}{path}", timeout=10)
        if resp.status_code == 200 and len(resp.text) > 2000:
            print(f"{path}: {resp.status_code} ({len(resp.text)} chars)")
            soup = BeautifulSoup(resp.text, "html.parser")
            # Find all forms
            for form in soup.find_all("form"):
                print(f"  Form action={form.get('action','')} method={form.get('method','')}")
            # Find keyword search links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                txt = a.get_text(strip=True)
                if any(w in (href+txt).lower() for w in ["search","keyword","full"]):
                    print(f"  → {txt}: {href}")
        else:
            print(f"{path}: {resp.status_code} ({len(resp.text)} chars)")
    except Exception as e:
        print(f"{path}: {e}")
