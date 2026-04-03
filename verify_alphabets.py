#!/usr/bin/env python3
"""Verify alphabets A and B are complete with all formats."""

import json
import re
from pathlib import Path
from datetime import datetime

base = Path("data_v2")

def verify_alphabet(letter):
    print(f"\n{'='*60}")
    print(f"VERIFYING ALPHABET {letter}")
    print("="*60)
    
    leg_dir = base / "legislation" / letter
    html_dir = base / "legislation" / "html" / letter
    
    # 1. Count legislation
    leg_files = list(leg_dir.glob("*.json")) if leg_dir.exists() else []
    html_files = list(html_dir.glob("*.html")) if html_dir.exists() else []
    print(f"\nLegislation:")
    print(f"  JSON files: {len(leg_files)}")
    print(f"  HTML files: {len(html_files)}")
    
    # 2. Extract all citations
    all_citations = []
    for jf in leg_files:
        try:
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)
            cases = data.get("case_links", []) or data.get("cases_cited", [])
            for c in cases:
                year = c.get("year", "")
                reporter = c.get("reporter", "")
                page = c.get("page", "")
                if year and reporter and page:
                    all_citations.append({
                        "year": str(year),
                        "reporter": reporter.upper(),
                        "page": str(page),
                        "citation": f"{year} {reporter} {page}"
                    })
        except:
            pass
    
    # Deduplicate
    seen = set()
    unique_citations = []
    for c in all_citations:
        key = c["citation"]
        if key not in seen:
            seen.add(key)
            unique_citations.append(c)
    
    print(f"\nCitations found: {len(unique_citations)}")
    
    # 3. Check each citation for all 4 formats
    complete = 0
    missing_json = []
    missing_original = []
    missing_readable = []
    
    for c in unique_citations:
        year = c["year"]
        reporter = c["reporter"]
        page = c["page"]
        cite = c["citation"]
        
        json_path = base / reporter / year / f"{year}_{reporter}_{page}.json"
        orig_path = base / reporter / year / "original" / f"{year}_{reporter}_{page}.html"
        read_path = base / "html" / reporter / year / f"{year}_{reporter}_{page}.html"
        
        has_json = json_path.exists()
        has_orig = orig_path.exists()
        has_read = read_path.exists()
        
        if has_json and has_orig and has_read:
            complete += 1
        else:
            if not has_json:
                missing_json.append(cite)
            if not has_orig:
                missing_original.append(cite)
            if not has_read:
                missing_readable.append(cite)
    
    print(f"\nCase Format Verification:")
    print(f"  Complete (all 3 formats): {complete}/{len(unique_citations)}")
    print(f"  Missing JSON: {len(missing_json)}")
    print(f"  Missing Original HTML: {len(missing_original)}")
    print(f"  Missing Readable HTML: {len(missing_readable)}")
    
    # 4. Show some missing cases
    if missing_json:
        print(f"\n  Sample missing JSON: {missing_json[:5]}")
    
    # 5. Verify HTML links
    broken_links = []
    working_links = 0
    for hf in html_files:
        try:
            content = hf.read_text(encoding="utf-8")
            links = re.findall(r'href="([^"]*html/[^"]+\.html)"', content)
            for link in links:
                full_path = (hf.parent / link).resolve()
                if full_path.exists():
                    working_links += 1
                else:
                    broken_links.append(f"{hf.name} -> {link}")
        except:
            pass
    
    print(f"\nHTML Link Verification:")
    print(f"  Working links: {working_links}")
    print(f"  Broken links: {len(broken_links)}")
    if broken_links:
        print(f"  Sample broken: {broken_links[:3]}")
    
    # Summary
    pct = (complete / len(unique_citations) * 100) if unique_citations else 100
    status = "READY" if len(broken_links) == 0 else "HAS ISSUES"
    print(f"\n>>> STATUS: {status} ({pct:.1f}% cases complete)")
    
    return {
        "letter": letter,
        "legislation": len(leg_files),
        "citations": len(unique_citations),
        "complete": complete,
        "missing": len(unique_citations) - complete,
        "broken_links": len(broken_links),
        "status": status
    }

# Verify both
results = []
for letter in ["A", "B"]:
    r = verify_alphabet(letter)
    results.append(r)

print(f"\n\n{'='*60}")
print("SUMMARY")
print("="*60)
print(f"{'Letter':<8} {'Leg':<6} {'Citations':<10} {'Complete':<10} {'Missing':<10} {'Broken':<8} {'Status'}")
print("-"*70)
for r in results:
    print(f"{r['letter']:<8} {r['legislation']:<6} {r['citations']:<10} {r['complete']:<10} {r['missing']:<10} {r['broken_links']:<8} {r['status']}")

print(f"\nVerification completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
