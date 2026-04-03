#!/usr/bin/env python3
"""Fetch the 2 specific missing IHC cases identified by deep verification."""
import json, os, sys, time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

try:
    from curl_cffi import requests
except ImportError:
    print("curl_cffi not installed")
    sys.exit(1)

DATA = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\court_cases\IHC")

MISSING = [
    {"year": 2018, "case_type": "Writ Petition", "case_no": 201, "o_id": 30472,
     "title": "M. Shahbaz Ullah & others VS FOP & others"},
    {"year": 2019, "case_type": "Criminal Appeal", "case_no": 1, "o_id": 13478,
     "title": "Mian Muhammad Nawaz Sharif VS State Through Chairman NAB etc"},
]

API_URL = "https://mis.ihc.gov.pk/frmJgmnt.asmx/srchDecisionClms"

session = requests.Session(impersonate="chrome")

for case in MISSING:
    print(f"\nFetching: {case['case_type']}-{case['case_no']}-{case['year']} ({case['title'][:50]}...)")
    
    try:
        # Try API
        payload = {
            "CaseType": case["case_type"],
            "CaseNo": str(case["case_no"]),
            "CaseYear": str(case["year"]),
            "DecisionDateFrom": "",
            "DecisionDateTo": "",
            "JudgeName": "",
            "Keywords": ""
        }
        
        resp = session.post(API_URL, json=payload, timeout=30,
                           headers={"Content-Type": "application/json"})
        
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("d", [])
            if isinstance(records, str):
                records = json.loads(records)
            
            if records:
                record = records[0] if isinstance(records, list) else records
                
                # Save JSON
                year_dir = DATA / str(case["year"])
                year_dir.mkdir(parents=True, exist_ok=True)
                
                case_id = f"{case['case_type'].replace(' ', '_')}-{case['case_no']}-{case['year']}"
                json_path = year_dir / f"{case_id}.json"
                
                case_data = {
                    "case_number": f"{case['case_type']}-{case['case_no']}-{case['year']}",
                    "case_title": case["title"],
                    "case_type": case["case_type"],
                    "year": case["year"],
                    "court": "IHC",
                    "source": "ihc.gov.pk",
                    "o_id": case["o_id"],
                    "raw_data": record,
                    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(case_data, f, indent=2, ensure_ascii=False)
                
                print(f"  Saved: {json_path}")
                
                # Try PDF
                pdf_url = record.get("PdfPath") or record.get("pdfPath") or record.get("JudgmentFile")
                if pdf_url:
                    if not pdf_url.startswith("http"):
                        pdf_url = f"https://mis.ihc.gov.pk/{pdf_url}"
                    
                    orig_dir = year_dir / "original"
                    orig_dir.mkdir(parents=True, exist_ok=True)
                    pdf_path = orig_dir / f"{case_id}.pdf"
                    
                    pdf_resp = session.get(pdf_url, timeout=30)
                    if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                        pdf_path.write_bytes(pdf_resp.content)
                        print(f"  PDF saved: {pdf_path} ({len(pdf_resp.content)} bytes)")
                    else:
                        print(f"  PDF download failed: {pdf_resp.status_code}")
                else:
                    print(f"  No PDF URL in record")
            else:
                print(f"  No records returned from API")
        else:
            print(f"  API error: {resp.status_code}")
    
    except Exception as e:
        print(f"  Error: {e}")
    
    time.sleep(3)

print("\nDone!")
