#!/usr/bin/env python3
"""Search digital.shc.gov.pk for the 9 missing cases, and explore cases.shc.gov.pk."""
import requests
import re
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
})

# Get the search-citation page to understand the form
r = session.get("https://digital.shc.gov.pk/search-citation", timeout=10)
soup = BeautifulSoup(r.text, "html.parser")

# Find the search form
forms = soup.find_all("form")
print(f"Found {len(forms)} forms")
for f in forms:
    print(f"  Form action: {f.get('action', '')} method: {f.get('method', '')}")
    inputs = f.find_all(["input", "select", "textarea"])
    for inp in inputs:
        print(f"    {inp.name}: name={inp.get('name', '')} type={inp.get('type', '')} value={inp.get('value', '')}")

# Look for Vue.js or AJAX endpoints in the JS
scripts = soup.find_all("script")
for s in scripts:
    src = s.get("src", "")
    if src:
        continue  # Skip external scripts
    text = s.string or ""
    if "api" in text.lower() or "search" in text.lower() or "ajax" in text.lower():
        print(f"\nScript with API references:")
        print(text[:500])

# Try the Livewire component which seems to be used
print("\n=== Livewire check ===")
livewire_data = soup.find_all(attrs={"wire:id": True})
for lw in livewire_data:
    print(f"Livewire component: {lw.get('wire:id', '')}")

# Try to search via Livewire
print("\n=== Trying Livewire search ===")
# First get the livewire data from the page
wire_scripts = re.findall(r'window\.livewire_token\s*=\s*["\']([^"\']+)["\']', r.text)
print(f"Livewire token: {wire_scripts}")

# Try the form directly
form_data_match = re.search(r'<form[^>]+action="([^"]*)"', r.text)
if form_data_match:
    print(f"Form action: {form_data_match.group(1)}")

# Check page for API calls in built JS
r_js = session.get("https://digital.shc.gov.pk/build/assets/app-DBdvkBoY.js", timeout=10)
if r_js.status_code == 200:
    js = r_js.text
    # Find API endpoints
    endpoints = re.findall(r'["\']/(api|search|caselaw)[^"\']*["\']', js)
    print(f"\nJS API endpoints: {list(set(endpoints))[:20]}")
    
    # Look for specific patterns
    if "search-citation" in js:
        idx = js.find("search-citation")
        print(f"search-citation context: ...{js[max(0,idx-200):idx+200]}...")

# Try direct POST to search endpoint  
print("\n=== Trying search POST ===")
case_numbers = [
    "H.C.A 107/2012",
    "Const. P. 2167/2012", 
    "Const. P. 188/2010",
    "Const. P. 324/2006",
]

# Get CSRF token if any
csrf = ""
csrf_match = re.search(r'name="csrf[_-]token"[^>]*value="([^"]+)"', r.text)
if csrf_match:
    csrf = csrf_match.group(1)
    print(f"CSRF from form: {csrf}")
else:
    # Try meta tag
    csrf_meta = re.search(r'<meta[^>]*name="csrf-token"[^>]*content="([^"]+)"', r.text)
    if csrf_meta:
        csrf = csrf_meta.group(1)
        print(f"CSRF from meta: {csrf}")

# Explore routes
print("\n=== Trying various routes ===")
routes = [
    "/search-citation",
    "/citation-search",
    "/caselaw",
    "/download",
    "/get-citation",
]
for route in routes:
    try:
        resp = session.post(
            f"https://digital.shc.gov.pk{route}",
            data={"case_number": "H.C.A 107/2012", "_token": csrf},
            timeout=5
        )
        print(f"POST {route}: {resp.status_code} ({len(resp.content)} bytes)")
        if resp.status_code == 200 and len(resp.text) > 100:
            if "pdf" in resp.headers.get("Content-Type", "").lower() or resp.content[:4] == b"%PDF":
                print("  -> GOT PDF!")
            elif "json" in resp.headers.get("Content-Type", "").lower():
                try:
                    d = resp.json()
                    print(f"  JSON: {str(d)[:200]}")
                except:
                    pass
    except Exception as e:
        print(f"POST {route}: error - {e}")
