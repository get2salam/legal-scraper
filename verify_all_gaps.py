"""Verify all gaps - compare file-by-file against PLS."""
import sys, os, re, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = "chrome"
BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)

for reporter, year in [("PLD", 2026), ("PLD", 2025), ("PLD", 2024), ("PTD", 1961)]:
    time.sleep(2.5)
    resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": year, "book": reporter, "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Get unique citations from PLS
    pls_citations = set()
    for row in soup.find_all("tr", class_="caseType"):
        tds = row.find_all("td")
        if len(tds) >= 2:
            cit = tds[1].get_text(strip=True)
            if re.match(r"\d{4}\s+\w+\s+\d+", cit):
                pls_citations.add(cit)
    
    # Get our files
    our_dir = Path(f"data_v2/{reporter}/{year}")
    our_files = set(f.stem.replace("_", " ") for f in our_dir.glob("*.json")) if our_dir.exists() else set()
    
    missing = pls_citations - our_files
    extra = our_files - pls_citations
    
    status = "COMPLETE" if len(missing) == 0 else f"MISSING {len(missing)}"
    print(f"{reporter} {year}: PLS={len(pls_citations)} unique | Ours={len(our_files)} | {status}")
    if missing:
        for m in sorted(missing)[:5]:
            print(f"  Missing: {m}")
