#!/usr/bin/env python3
"""Check digital.shc.gov.pk for missing PDF cases."""
import requests
import re
import json
import time
from pathlib import Path

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
})

# Explore the digital portal
print("=== digital.shc.gov.pk ===")
r = session.get("https://digital.shc.gov.pk/", timeout=10)
print(f"Status: {r.status_code}, Size: {len(r.text)}")

# Find all links
links = re.findall(r'href=["\']([^"\']+)["\']', r.text)
print("Links found:")
for l in sorted(set(links)):
    if "shc" in l or l.startswith("/"):
        print(f"  {l}")

print()

# Try the citation search page
print("=== search-citation page ===")
r2 = session.get("https://digital.shc.gov.pk/search-citation", timeout=10)
print(f"Status: {r2.status_code}, Size: {len(r2.text)}")
print(r2.text[:2000])

print()

# Try searching for one of our cases
print("=== Searching for case ===")
# Try to post a search
search_url = "https://digital.shc.gov.pk/search-citation"
# Extract CSRF token
csrf_match = re.search(r'<meta name="csrf-token" content="([^"]+)"', r2.text)
csrf = csrf_match.group(1) if csrf_match else ""
print(f"CSRF: {csrf}")

# Also try direct API
for endpoint in ["/api/cases", "/api/search", "/caselaw/search", "/search"]:
    try:
        resp = session.get(f"https://digital.shc.gov.pk{endpoint}", timeout=5)
        print(f"{endpoint}: {resp.status_code} ({len(resp.content)} bytes)")
    except Exception as e:
        print(f"{endpoint}: error - {e}")

# Try the cases.shc.gov.pk
print()
print("=== cases.shc.gov.pk ===")
r3 = session.get("https://cases.shc.gov.pk/", timeout=10)
print(f"Status: {r3.status_code}, Size: {len(r3.text)}")
print(r3.text[:2000])
