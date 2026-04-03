# -*- coding: utf-8 -*-
import sys
import os
import re
import time
import json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
print("Starting PTD 1961 scraper...")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

from curl_cffi import requests as r
from bs4 import BeautifulSoup

print("Imports OK")

s = r.Session()
s.impersonate = "chrome"
BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))

print(f"User: {user[:5]}...")

resp = s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
print(f"Login: {resp.status_code}")
time.sleep(2)

resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": 1961, "book": "PTD", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
print(f"Search: {resp.status_code}, {len(resp.text)} bytes")

soup = BeautifulSoup(resp.text, "html.parser")
rows = soup.find_all("tr", class_="caseType")
print(f"Rows: {len(rows)}")

cases = []
for row in rows:
    tds = row.find_all("td")
    cit = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
    if not re.match(r"\d{4}\s+\w+\s+\d+", cit):
        continue
    btn = row.find("input", attrs={"casetypeid": True})
    cid = btn.get("casetypeid", "") if btn else ""
    cases.append({"c": cit, "id": cid})

print(f"Valid cases: {len(cases)}")
if cases:
    print(f"Sample: {cases[0]}")

out = Path("data_v2/PTD/1961")
out.mkdir(parents=True, exist_ok=True)

scraped = 0
for case in cases:
    fn = case["c"].replace(" ", "_")
    if (out / f"{fn}.json").exists():
        continue
    if not case["id"]:
        continue
    time.sleep(3)
    try:
        r2 = s.post(f"{BASE}/Login/GetCaseFile", data={"caseName": case["id"], "headNotes": 0}, timeout=60)
        if r2.status_code == 200 and len(r2.text) > 100:
            content = r2.text
            try:
                content = json.loads(content)
            except Exception:
                pass
            data = {"citation": case["c"], "reporter": "PTD", "year": 1961, "case_name": case["id"],
                    "judgment": content if isinstance(content, str) else json.dumps(content)}
            with open(out / f"{fn}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            scraped += 1
            if scraped % 10 == 0:
                print(f"  Scraped {scraped}")
    except Exception as e:
        print(f"  Err: {e}")

print(f"DONE: {scraped} scraped, {len(list(out.glob('*.json')))} total files")
