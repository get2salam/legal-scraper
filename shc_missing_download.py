#!/usr/bin/env python3
"""
shc_missing_download.py - Download the 9 specifically missing SHC PDFs.
Tries multiple URL patterns and techniques.
"""

import os
import sys
import time
import json
from pathlib import Path

# The 9 missing cases (bench, year, file_stem, download_url, source_url, case_number)
MISSING = [
    # KHI (5)
    {
        "bench": "KHI", "year": "2012",
        "stem": "SHC_KHI_MTYyNDY0Y2Ztcy1kYzgz",
        "doc_id": "MTYyNDY0Y2Ztcy1kYzgz",
        "case": "H.C.A 107/2012",
    },
    {
        "bench": "KHI", "year": "2012",
        "stem": "SHC_KHI_NzIyNDljZm1zLWRjODM",
        "doc_id": "NzIyNDljZm1zLWRjODM",
        "case": "Const. P. 2167/2012",
    },
    {
        "bench": "KHI", "year": "2012",
        "stem": "SHC_KHI_Nzg5NTJjZm1zLWRjODM",
        "doc_id": "Nzg5NTJjZm1zLWRjODM",
        "case": "Const. P. 2158/2012",
    },
    {
        "bench": "KHI", "year": "2009",
        "stem": "SHC_KHI_NzY5ODFjZm1zLWRjODM",
        "doc_id": "NzY5ODFjZm1zLWRjODM",
        "case": "Const. P. 2753/2009",
    },
    {
        "bench": "KHI", "year": "2013",
        "stem": "SHC_KHI_NzYyNTNjZm1zLWRjODM",
        "doc_id": "NzYyNTNjZm1zLWRjODM",
        "case": "Const. P. 425/2013",
    },
    # HYD (3)
    {
        "bench": "HYD", "year": "2010",
        "stem": "SHC_HYD_MzczNDdjZm1zLWRjODM",
        "doc_id": "MzczNDdjZm1zLWRjODM",
        "case": "Const. P. 188/2010",
    },
    {
        "bench": "HYD", "year": "2015",
        "stem": "SHC_HYD_OTQ1NjZjZm1zLWRjODM",
        "doc_id": "OTQ1NjZjZm1zLWRjODM",
        "case": "Const. P. 612/2013",
    },
    {
        "bench": "HYD", "year": "2015",
        "stem": "SHC_HYD_OTUyNjVjZm1zLWRjODM",
        "doc_id": "OTUyNjVjZm1zLWRjODM",
        "case": "Const. P. 925/2013",
    },
    # LAR (1)
    {
        "bench": "LAR", "year": "2009",
        "stem": "SHC_LAR_OTkyOWNmbXMtZGM4Mw",
        "doc_id": "OTkyOWNmbXMtZGM4Mw",
        "case": "Const. P. 324/2006",
    },
]

BASE = Path(__file__).parent / "data_v2" / "court_cases" / "SHC"
BASE_URL = "https://caselaw.shc.gov.pk/caselaw"

def try_download(doc_id, timeout=15):
    """Try multiple URL patterns to download PDF."""
    import base64
    
    # Decode the doc_id to get numeric ID
    # Add padding if needed
    padded = doc_id
    while len(padded) % 4 != 0:
        padded += "="
    try:
        decoded = base64.b64decode(padded).decode("utf-8")
        print(f"  doc_id decoded: {decoded}")
    except Exception:
        decoded = ""
    
    urls_to_try = [
        f"{BASE_URL}/download-file.php?doc={doc_id}",
        f"{BASE_URL}/view-file/{doc_id}",
    ]
    
    # If decoded has numeric prefix, try direct numeric ID
    if decoded:
        num = decoded.split("c")[0] if "c" in decoded else decoded.split("-")[0]
        if num.isdigit():
            urls_to_try.append(f"{BASE_URL}/download-file.php?doc={num}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Referer": f"{BASE_URL}/public/home",
    }
    
    # Try with requests first
    try:
        import requests
        for url in urls_to_try:
            try:
                r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
                print(f"  requests: {url} -> {r.status_code} ({len(r.content)} bytes)")
                if r.status_code == 200 and len(r.content) > 1000:
                    if r.content[:4] == b"%PDF":
                        return r.content, "pdf"
                    ct = r.headers.get("Content-Type", "")
                    if "pdf" in ct.lower():
                        return r.content, "pdf"
                    print(f"    Not PDF: ContentType={ct}, starts with: {r.content[:20]}")
            except Exception as e:
                print(f"  requests error for {url}: {e}")
    except ImportError:
        pass
    
    # Try with curl_cffi (Chrome impersonation)
    try:
        from curl_cffi.requests import Session, BrowserType
        sess = Session(impersonate="chrome")
        for url in urls_to_try:
            try:
                r = sess.get(url, timeout=timeout, headers=headers)
                print(f"  curl_cffi: {url} -> {r.status_code} ({len(r.content)} bytes)")
                if r.status_code == 200 and len(r.content) > 1000:
                    if r.content[:4] == b"%PDF":
                        return r.content, "pdf"
                    ct = r.headers.get("Content-Type", "")
                    if "pdf" in ct.lower():
                        return r.content, "pdf"
            except Exception as e:
                print(f"  curl_cffi error for {url}: {e}")
    except ImportError:
        print("  curl_cffi not available")
    
    return None, None


def main():
    results = {"downloaded": [], "failed": [], "site_down": False}
    
    # Quick connectivity check
    print("Checking SHC connectivity...")
    try:
        import requests
        r = requests.get("https://caselaw.shc.gov.pk/", timeout=5)
        print(f"SHC site status: {r.status_code}")
    except Exception as e:
        print(f"SHC site unreachable: {e}")
        results["site_down"] = True
        print("Site appears to be down. Recording status.")
    
    for item in MISSING:
        bench = item["bench"]
        year = item["year"]
        stem = item["stem"]
        doc_id = item["doc_id"]
        case_num = item["case"]
        
        orig_dir = BASE / bench / year / "original"
        pdf_path = orig_dir / f"{stem}.pdf"
        
        print(f"\n--- {stem} ({case_num}) ---")
        
        if pdf_path.exists():
            print(f"  Already exists: {pdf_path}")
            results["downloaded"].append({"stem": stem, "status": "already_exists"})
            continue
        
        if results["site_down"]:
            print(f"  Skipping (site down)")
            results["failed"].append({"stem": stem, "reason": "site_down", "case": case_num})
            continue
        
        content, ctype = try_download(doc_id)
        
        if content:
            orig_dir.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(content)
            print(f"  SAVED: {pdf_path} ({len(content)} bytes)")
            results["downloaded"].append({"stem": stem, "case": case_num, "size": len(content)})
        else:
            print(f"  FAILED to download")
            results["failed"].append({"stem": stem, "reason": "download_failed", "case": case_num})
        
        time.sleep(1)
    
    print(f"\n=== RESULTS ===")
    print(f"Downloaded: {len(results['downloaded'])}")
    print(f"Failed: {len(results['failed'])}")
    
    return results


if __name__ == "__main__":
    main()
