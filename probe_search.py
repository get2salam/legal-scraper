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
time.sleep(2)

known = {"SCMR","PLD","PCrLJ","MLD","CLC","YLR","PTD","PLC","CLD","GBLR","PLCCS","PCRLJN","YLRN","PLCCSN","CLCN"}

# 1. Try full-text search endpoint
print("=== Full-text search ===")
for ep in ["/Login/Search", "/Login/FullTextSearch"]:
    time.sleep(1)
    try:
        resp = s.post(f"{B}{ep}", data={"SearchText":"contract","page":1}, timeout=10)
        print(f"  {ep}: {resp.status_code} ({len(resp.text)} chars)")
        if resp.status_code == 200 and len(resp.text) > 500:
            print(f"  SAMPLE: {resp.text[:300]}")
    except Exception as e:
        print(f"  {ep}: {e}")

# 2. Empty citation search - what reporter does it default to?
print("\n=== Empty CitationSearch ===")
time.sleep(2)
resp = s.post(f"{B}/Login/CitationSearch",
    data={"year":"","book":"","code":"","court":"","judge":"","lawyer":"","party":""},
    timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")
rows = soup.find_all("tr", class_="caseType")
print(f"Results: {len(rows)} on first page")
found_reporters = set()
for row in rows[:30]:
    tds = row.find_all("td")
    if len(tds) >= 2:
        cit = tds[1].get_text(strip=True)
        parts = cit.split()
        if len(parts) >= 2:
            found_reporters.add(parts[1])
print(f"Reporters in results: {found_reporters}")
print(f"UNKNOWN reporters: {found_reporters - known}")

# 3. Party search - any unreported cases?
print("\n=== Party name search ===")
time.sleep(2)
resp2 = s.post(f"{B}/Login/CitationSearch",
    data={"year":"","book":"","code":"","court":"","judge":"","lawyer":"","party":"Khan"},
    timeout=30)
soup2 = BeautifulSoup(resp2.text, "html.parser")
rows2 = soup2.find_all("tr", class_="caseType")
print(f"Party 'Khan' results: {len(rows2)}")
unknown_found = []
for row in rows2[:50]:
    tds = row.find_all("td")
    if len(tds) >= 2:
        cit = tds[1].get_text(strip=True)
        parts = cit.split()
        if len(parts) >= 2 and parts[1] not in known:
            unknown_found.append(cit)
if unknown_found:
    print(f"UNKNOWN reporter citations: {unknown_found}")
else:
    print("All reporters known")
    # Show sample of what came back
    for row in rows2[:5]:
        tds = row.find_all("td")
        if len(tds) >= 2:
            print(f"  {tds[1].get_text(strip=True)}")
