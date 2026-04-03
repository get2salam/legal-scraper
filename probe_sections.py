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

# Test all the sections found on homepage
sections = [
    ("/Login/WordsAndPhrases", {"type":"words"}, "GET"),
    ("/Login/LegalTerms", {}, "GET"),
    ("/Login/Maxim", {"type":"maxim"}, "GET"),
    ("/Login/ArticlePage", {}, "GET"),
    ("/Login/TopicPage", {}, "GET"),
    ("/Login/DictionaryPage", {}, "GET"),
    ("/Login/GetCurrentUserHistory", {}, "GET"),
    ("/Login/Check", {}, "GET"),
]

known = {"SCMR","PLD","PCrLJ","MLD","CLC","YLR","PTD","PLC","CLD","GBLR","PLCCS","PCRLJN","YLRN","PLCCSN","CLCN"}

for path, params, method in sections:
    time.sleep(1)
    try:
        if method == "GET":
            resp = s.get(f"{B}{path}", params=params, timeout=12)
        else:
            resp = s.post(f"{B}{path}", data=params, timeout=12)
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find_all("tr", class_="caseType")
        title = soup.find("title")
        print(f"{path}: {resp.status_code} ({len(resp.text)} chars) | caseRows={len(rows)} | title={title.get_text()[:40] if title else '?'}")
        if len(rows) > 0:
            tds = rows[0].find_all("td")
            if len(tds) >= 2:
                cit = tds[1].get_text(strip=True)
                parts = cit.split()
                reporter = parts[1] if len(parts) >= 2 else ""
                if reporter not in known:
                    print(f"  *** NEW REPORTER: {cit}")
    except Exception as e:
        print(f"{path}: ERROR {e}")

# Now try the keyword search — it's likely GET with query param
time.sleep(2)
print("\n=== Keyword search via GET ===")
for ep in ["/Login/KeywordSearch", "/Login/SearchJudgment", "/Login/JudgmentSearch"]:
    try:
        resp = s.get(f"{B}{ep}", params={"q":"contract","keyword":"contract","SearchText":"contract"}, timeout=10)
        print(f"  GET {ep}: {resp.status_code} ({len(resp.text)} chars)")
        if resp.status_code == 200 and len(resp.text) > 2000:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("tr", class_="caseType")
            print(f"    Cases: {len(rows)}")
    except Exception as e:
        print(f"  GET {ep}: {e}")
