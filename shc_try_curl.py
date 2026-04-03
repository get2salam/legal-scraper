#!/usr/bin/env python3
"""Quick test of SHC connectivity and download attempt."""
import sys
import time

URL = "https://caselaw.shc.gov.pk/caselaw/download-file.php?doc=MTYyNDY0Y2Ztcy1kYzgz"
URL2 = "https://caselaw.shc.gov.pk/caselaw/view-file/MTYyNDY0Y2Ztcy1kYzgz"

try:
    from curl_cffi.requests import Session
    print("Using curl_cffi with chrome124 impersonation")
    for browser in ["chrome124", "chrome110", "firefox110"]:
        try:
            sess = Session(impersonate=browser)
            r = sess.get(URL, timeout=12)
            ct = r.headers.get("Content-Type", "")
            print(f"{browser}: status={r.status_code} content_type={ct} size={len(r.content)}")
            if r.content[:4] == b"%PDF":
                print("GOT PDF!")
                sys.exit(0)
            elif r.status_code != 521:
                print(f"Response: {r.content[:200]}")
        except Exception as e:
            print(f"{browser}: error - {e}")
        time.sleep(2)
except ImportError:
    print("curl_cffi not available")

try:
    import requests
    print("Using requests")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"}
    r = requests.get(URL, timeout=10, headers=headers)
    ct = r.headers.get("Content-Type", "")
    print(f"requests: status={r.status_code} content_type={ct} size={len(r.content)}")
except Exception as e:
    print(f"requests error: {e}")
