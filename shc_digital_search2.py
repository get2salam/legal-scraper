#!/usr/bin/env python3
"""Search digital.shc.gov.pk for missing cases using correct CSRF token."""
import requests
import re
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup

BASE = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\court_cases\SHC")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
})

# First explore the journal structure (the search is by citation year + journal)
r = session.get("https://digital.shc.gov.pk/search-citation", timeout=10)
soup = BeautifulSoup(r.text, "html.parser")

# Get CSRF token from form
token = ""
token_input = soup.find("input", {"name": "_token"})
if token_input:
    token = token_input.get("value", "")
    print(f"CSRF token: {token[:20]}...")

# Get journal codes from the select element
journal_select = soup.find("select", {"name": "journal_code"})
if journal_select:
    options = journal_select.find_all("option")
    print("Journal codes available:")
    for opt in options[:20]:
        print(f"  {opt.get('value', '')} -> {opt.get_text(strip=True)}")

# The search requires: citation_year, journal_code, journal_part, page_no
# For SHC cases, we need to figure out the journal code
# SHC judgments appear in PLD, SCMR, CLC, NLR, etc.
# But the 9 missing cases might be in a special SHC journal

# Try the get-journal-parts endpoint
print("\n=== Testing get-journal-parts ===")
r2 = session.get(
    "https://digital.shc.gov.pk/get-journal-parts",
    params={"journal_code": "SHC"},
    timeout=5
)
print(f"SHC parts: {r2.status_code} -> {r2.text[:300]}")

# Try with SHC code
for code in ["SHC", "PLD", "SCMR", "MLD", "CLC", "PCrLJ", "2009 SHC"]:
    try:
        r3 = session.get(
            "https://digital.shc.gov.pk/get-journal-parts",
            params={"journal_code": code},
            timeout=5
        )
        if r3.status_code == 200 and len(r3.text) > 5:
            print(f"{code}: {r3.status_code} -> {r3.text[:200]}")
    except Exception as e:
        print(f"{code}: error - {e}")

# Now try a search with the full form data  
# These older cases (2006-2013) likely use old-style IDs
# Try to search for a simple known case
print("\n=== Attempting form search ===")
search_data = {
    "_token": token,
    "action": "submit",
    "citation_year": "2009",
    "journal_code": "PLD",
    "journal_part": "Sindh",
    "page_no": "1",
    "recaptcha_token": "",
}

r4 = session.post(
    "https://digital.shc.gov.pk/search-citation",
    data=search_data,
    timeout=15
)
print(f"Search result: {r4.status_code} ({len(r4.text)} bytes)")
if r4.status_code == 200:
    soup4 = BeautifulSoup(r4.text, "html.parser")
    # Look for results
    tables = soup4.find_all("table")
    print(f"Tables found: {len(tables)}")
    for t in tables[:2]:
        print(t.get_text()[:300])
    links = soup4.find_all("a", href=True)
    pdf_links = [l["href"] for l in links if "pdf" in l["href"].lower() or "download" in l["href"].lower()]
    print(f"PDF/download links: {pdf_links[:5]}")

# Check if the 9 missing cases have JSON with citation info
print("\n=== Checking citation fields of missing cases ===")
missing_jsons = [
    (r"KHI\2005\SHC_KHI_MTYyNDY0Y2Ztcy1kYzgz.json", "162464cfms-dc83"),
    (r"KHI\2012\SHC_KHI_NzIyNDljZm1zLWRjODM.json", "72249cfms-dc83"),
    (r"HYD\2010\SHC_HYD_MzczNDdjZm1zLWRjODM.json", "37347cfms-dc83"),
    (r"LAR\2009\SHC_LAR_OTkyOWNmbXMtZGM4Mw.json", "9929cfms-dc83"),
]

for rel_path, decoded in missing_jsons:
    full_path = BASE / rel_path
    if full_path.exists():
        with open(full_path, encoding="utf-8") as f:
            data = json.load(f)
        citation = data.get("citation", "")
        shc_cit = data.get("shc_citation_id", "")
        print(f"\n{decoded}:")
        print(f"  citation: {citation}")
        print(f"  shc_citation_id: {shc_cit}")
        print(f"  case_number: {data.get('case_number', '')}")
        print(f"  order_date: {data.get('order_date', '')}")
        print(f"  downloads_count: {data.get('downloads_count', 0)}")
