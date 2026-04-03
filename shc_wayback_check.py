#!/usr/bin/env python3
"""Check Wayback Machine for the 9 missing SHC PDFs."""
import urllib.request
import json
import time
import base64
from pathlib import Path

DOC_IDS = [
    ("MTYyNDY0Y2Ztcy1kYzgz", "KHI", "162464cfms-dc83", "H.C.A 107/2012"),
    ("NzIyNDljZm1zLWRjODM", "KHI", "72249cfms-dc83", "Const. P. 2167/2012"),
    ("Nzg5NTJjZm1zLWRjODM", "KHI", "78952cfms-dc83", "Const. P. 2158/2012"),
    ("NzY5ODFjZm1zLWRjODM", "KHI", "76981cfms-dc83", "Const. P. 2753/2009"),
    ("NzYyNTNjZm1zLWRjODM", "KHI", "76253cfms-dc83", "Const. P. 425/2013"),
    ("MzczNDdjZm1zLWRjODM", "HYD", "37347cfms-dc83", "Const. P. 188/2010"),
    ("OTQ1NjZjZm1zLWRjODM", "HYD", "94566cfms-dc83", "Const. P. 612/2013"),
    ("OTUyNjVjZm1zLWRjODM", "HYD", "95265cfms-dc83", "Const. P. 925/2013"),
    ("OTkyOWNmbXMtZGM4Mw", "LAR", "9929cfms-dc83", "Const. P. 324/2006"),
]

BASE_SHC = "caselaw.shc.gov.pk/caselaw"

def check_wayback(doc_id, case_num):
    """Check if Wayback Machine has a snapshot."""
    urls_to_check = [
        f"{BASE_SHC}/download-file.php%3Fdoc%3D{doc_id}",
        f"{BASE_SHC}/view-file/{doc_id}",
    ]
    
    for path in urls_to_check:
        wb_url = f"https://archive.org/wayback/available?url={path}"
        try:
            req = urllib.request.Request(wb_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read())
                snap = data.get("archived_snapshots", {}).get("closest", {})
                if snap.get("available"):
                    return snap.get("url", ""), snap.get("timestamp", "")
        except Exception as e:
            print(f"  Wayback error for {path}: {e}")
        time.sleep(0.5)
    return None, None


def try_download_wayback(wb_url, save_path):
    """Download from Wayback Machine URL."""
    try:
        req = urllib.request.Request(wb_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            content = r.read()
            ct = r.headers.get("Content-Type", "")
            if len(content) > 500:  # At least something substantive
                save_path.parent.mkdir(parents=True, exist_ok=True)
                # Save with appropriate extension
                if content[:4] == b"%PDF":
                    pdf_path = save_path
                else:
                    # HTML/other content - still save it
                    pdf_path = save_path.parent / (save_path.stem + ".html")
                pdf_path.write_bytes(content)
                return True, len(content), str(pdf_path)
    except Exception as e:
        print(f"  Download error: {e}")
    return False, 0, ""


BASE = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\court_cases\SHC")
HTML_BASE = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\html\court_cases\SHC")

results = []

for doc_id, bench, decoded, case_num in DOC_IDS:
    print(f"\n=== {bench}/{case_num} ({decoded}) ===")
    
    # Find the JSON to get the right year/stem
    matches = list(BASE.glob(f"**/{bench}/**/*{doc_id}*.json"))
    if not matches:
        print("  JSON not found!")
        results.append({"doc_id": doc_id, "status": "json_not_found"})
        continue
    
    json_path = matches[0]
    year = json_path.parent.parent.name  # e.g. 2012
    stem = json_path.stem
    orig_dir = json_path.parent / "original"
    pdf_path = orig_dir / f"{stem}.pdf"
    
    print(f"  Year: {year}, Stem: {stem}")
    print(f"  PDF path: {pdf_path}")
    print(f"  PDF exists: {pdf_path.exists()}")
    
    if pdf_path.exists():
        print("  Already have PDF, skipping")
        results.append({"doc_id": doc_id, "status": "already_exists"})
        continue
    
    # Check Wayback Machine
    print(f"  Checking Wayback Machine...")
    wb_url, wb_ts = check_wayback(doc_id, case_num)
    
    if wb_url:
        print(f"  FOUND in Wayback: ts={wb_ts}")
        print(f"  URL: {wb_url}")
        
        # Try to download from Wayback
        success, size, saved_path = try_download_wayback(wb_url, pdf_path)
        if success:
            print(f"  SAVED: {saved_path} ({size} bytes)")
            results.append({"doc_id": doc_id, "status": "downloaded_wayback", "ts": wb_ts, "size": size})
        else:
            print(f"  Download failed from Wayback")
            results.append({"doc_id": doc_id, "status": "wayback_download_failed", "wb_url": wb_url})
    else:
        print(f"  NOT found in Wayback Machine")
        results.append({"doc_id": doc_id, "status": "not_in_wayback", "case": case_num, "bench": bench})

print("\n\n=== SUMMARY ===")
for r in results:
    print(f"  {r}")
