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

# 1. Full text search - what endpoint does the website use?
print("=== Testing /Login/Search ===")
resp = s.post(f"{B}/Login/Search", data={"SearchText":"contract"}, timeout=20)
print(f"Status: {resp.status_code}, {len(resp.text)} chars")
print(f"Sample: {resp.text[:400]}")

time.sleep(3)
# 2. Party search with smaller scope
print("\n=== Party search (2024 only) ===")
resp2 = s.post(f"{B}/Login/CitationSearch",
    data={"year":"2024","book":"","code":"","court":"","judge":"","lawyer":"","party":"Khan"},
    timeout=20)
soup2 = BeautifulSoup(resp2.text, "html.parser")
rows2 = soup2.find_all("tr", class_="caseType")
print(f"Results: {len(rows2)}")
found = set()
for row in rows2[:30]:
    tds = row.find_all("td")
    if len(tds) >= 2:
        cit = tds[1].get_text(strip=True)
        parts = cit.split()
        if len(parts) >= 2: found.add(parts[1])
print(f"Reporters: {found}")
print(f"NEW: {found - known}")
for row in rows2[:5]:
    tds = row.find_all("td")
    if len(tds) >= 2: print(f"  {tds[1].get_text(strip=True)}")
