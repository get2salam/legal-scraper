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

known = {"SCMR","PLD","PCrLJ","MLD","CLC","YLR","PTD","PLC","CLD","GBLR","PLCCS","PCRLJN","YLRN","PLCCSN","CLCN"}

# The 387K search - try keyword/fulltext endpoints
print("=== Probing keyword search endpoints ===")
endpoints = [
    ("/Login/KeywordSearch",  {"keyword":"contract","page":1}),
    ("/Login/SearchCase",     {"SearchText":"contract"}),
    ("/Login/CaseKeyword",    {"keyword":"contract"}),
    ("/Login/SearchByKeyword",{"keyword":"a","page":1}),
    ("/Login/GlobalSearch",   {"q":"a"}),
    ("/Login/SearchAll",      {"SearchText":"a"}),
]
for ep, data in endpoints:
    time.sleep(1)
    try:
        resp = s.post(f"{B}{ep}", data=data, timeout=10)
        if resp.status_code == 200 and len(resp.text) > 1000:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("tr", class_="caseType")
            print(f"  {ep}: {resp.status_code} ({len(resp.text)} chars, {len(rows)} case rows)")
            if rows:
                tds = rows[0].find_all("td")
                print(f"  Sample: {tds[1].get_text(strip=True) if len(tds)>1 else '?'}")
        else:
            print(f"  {ep}: {resp.status_code} ({len(resp.text)} chars)")
    except Exception as e:
        print(f"  {ep}: {e}")

# Also check what the login page HTML reveals about search form action
time.sleep(2)
print("\n=== Check login page for search form ===")
resp = s.get(f"{B}/Login/Index", timeout=10)
soup = BeautifulSoup(resp.text, "html.parser")
forms = soup.find_all("form")
for form in forms:
    action = form.get("action","")
    if "search" in action.lower():
        print(f"  Search form: action={action}")
        for inp in form.find_all("input"):
            print(f"    input: name={inp.get('name','')} type={inp.get('type','')} value={inp.get('value','')[:30]}")
