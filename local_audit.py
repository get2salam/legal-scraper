#!/usr/bin/env python3
"""
Local Data Audit - No PLS login required.
Checks file integrity, format consistency, and counts across all data.
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

BASE = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

def audit_cases():
    """Audit all case law files."""
    print("=" * 70)
    print("CASE LAW AUDIT")
    print("=" * 70)
    
    total_cases = 0
    total_original = 0
    total_html = 0
    issues = []
    by_reporter = {}
    by_year = defaultdict(int)
    empty_judgments = []
    small_judgments = []
    missing_fields = []
    
    for reporter in REPORTERS:
        reporter_dir = BASE / reporter
        if not reporter_dir.exists():
            continue
        
        reporter_total = 0
        reporter_original = 0
        reporter_html = 0
        
        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir() or year_dir.name == "original":
                continue
            
            year = year_dir.name
            jsons = list(year_dir.glob("*.json"))
            originals = list((year_dir / "original").glob("*.html")) if (year_dir / "original").exists() else []
            html_dir = BASE / "html" / reporter / year
            htmls = list(html_dir.glob("*.html")) if html_dir.exists() else []
            
            reporter_total += len(jsons)
            reporter_original += len(originals)
            reporter_html += len(htmls)
            by_year[year] += len(jsons)
            
            # Check format mismatches
            if len(jsons) != len(originals):
                issues.append(f"  {reporter}/{year}: {len(jsons)} JSON vs {len(originals)} original HTML (diff: {len(jsons) - len(originals)})")
            if len(jsons) != len(htmls):
                issues.append(f"  {reporter}/{year}: {len(jsons)} JSON vs {len(htmls)} readable HTML (diff: {len(jsons) - len(htmls)})")
            
            # Spot-check JSON integrity (sample 5 per year)
            for jf in jsons[:5]:
                try:
                    with open(jf, encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Check for required fields
                    if not data.get('judgment_text') and not data.get('judgment_raw'):
                        empty_judgments.append(str(jf))
                    elif data.get('judgment_text') and len(data['judgment_text']) < 100:
                        small_judgments.append(f"{jf} ({len(data['judgment_text'])} chars)")
                    
                    required = ['citation', 'title']
                    for field in required:
                        if not data.get(field):
                            missing_fields.append(f"{jf}: missing '{field}'")
                            
                except json.JSONDecodeError:
                    issues.append(f"  CORRUPT JSON: {jf}")
                except Exception as e:
                    issues.append(f"  ERROR reading {jf}: {e}")
        
        by_reporter[reporter] = {
            'json': reporter_total,
            'original': reporter_original,
            'html': reporter_html
        }
        total_cases += reporter_total
        total_original += reporter_original
        total_html += reporter_html
    
    # Print results
    print(f"\n📊 TOTALS: {total_cases} cases | {total_original} original HTML | {total_html} readable HTML")
    print(f"\nBy Reporter:")
    for r, counts in sorted(by_reporter.items()):
        match = "✅" if counts['json'] == counts['original'] == counts['html'] else "⚠️"
        print(f"  {match} {r:8s}: {counts['json']:5d} JSON | {counts['original']:5d} original | {counts['html']:5d} readable")
    
    print(f"\nBy Year (top 10):")
    for year, count in sorted(by_year.items(), key=lambda x: -x[1])[:10]:
        print(f"  {year}: {count} cases")
    
    print(f"\nYear range: {min(by_year.keys())} - {max(by_year.keys())} ({len(by_year)} years)")
    
    if issues:
        print(f"\n⚠️ FORMAT MISMATCHES ({len(issues)}):")
        for issue in issues[:20]:
            print(issue)
        if len(issues) > 20:
            print(f"  ... and {len(issues) - 20} more")
    else:
        print(f"\n✅ All formats perfectly aligned!")
    
    if empty_judgments:
        print(f"\n🔴 EMPTY JUDGMENTS ({len(empty_judgments)}):")
        for ej in empty_judgments[:10]:
            print(f"  {ej}")
    
    if small_judgments:
        print(f"\n🟡 SUSPICIOUSLY SHORT JUDGMENTS ({len(small_judgments)}):")
        for sj in small_judgments[:10]:
            print(f"  {sj}")
    
    if missing_fields:
        print(f"\n🟡 MISSING REQUIRED FIELDS ({len(missing_fields)}):")
        for mf in missing_fields[:10]:
            print(f"  {mf}")
    
    return total_cases, by_reporter, issues

def audit_legislation():
    """Audit all legislation files."""
    print("\n" + "=" * 70)
    print("LEGISLATION AUDIT")
    print("=" * 70)
    
    leg_dir = BASE / "legislation"
    total_statutes = 0
    total_sections = 0
    unavailable_sections = 0
    by_letter = {}
    issues = []
    
    for letter_dir in sorted(leg_dir.iterdir()):
        if not letter_dir.is_dir() or len(letter_dir.name) != 1:
            continue
        
        letter = letter_dir.name
        jsons = list(letter_dir.glob("*.json"))
        
        if not jsons:
            continue
        
        letter_sections = 0
        letter_unavailable = 0
        
        for jf in jsons:
            try:
                with open(jf, encoding='utf-8') as f:
                    data = json.load(f)
                
                sections = data.get('sections', [])
                letter_sections += len(sections)
                
                for sec in sections:
                    content = sec.get('content', '')
                    if '[Content not available' in content or content == '-1':
                        letter_unavailable += 1
                        
            except Exception as e:
                issues.append(f"  ERROR: {jf}: {e}")
        
        by_letter[letter] = {
            'files': len(jsons),
            'sections': letter_sections,
            'unavailable': letter_unavailable
        }
        total_statutes += len(jsons)
        total_sections += letter_sections
        unavailable_sections += letter_unavailable
    
    print(f"\n📊 TOTALS: {total_statutes} statutes | {total_sections} sections | {unavailable_sections} unavailable ({unavailable_sections*100//max(total_sections,1)}%)")
    print(f"\nBy Letter:")
    for letter, counts in sorted(by_letter.items()):
        avail_pct = ((counts['sections'] - counts['unavailable']) * 100 // max(counts['sections'], 1))
        print(f"  {letter}: {counts['files']:4d} statutes | {counts['sections']:5d} sections | {counts['unavailable']:5d} unavailable ({avail_pct}% available)")
    
    # Check progress.json
    progress_file = leg_dir / "progress.json"
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
        print(f"\nProgress.json: completed_alphabets = {progress.get('completed_alphabets', [])}")
        print(f"  statutes_scraped: {len(progress.get('statutes_scraped', []))} entries")
    
    if issues:
        print(f"\n⚠️ ISSUES ({len(issues)}):")
        for issue in issues[:10]:
            print(issue)
    
    return total_statutes, total_sections

def audit_linked_cases():
    """Audit linked cases data."""
    print("\n" + "=" * 70)
    print("LINKED CASES AUDIT")
    print("=" * 70)
    
    progress_file = BASE / "linked_cases_progress.json"
    index_file = BASE / "legislation" / "linked_cases_index.json"
    links_file = BASE / "legislation" / "statute_case_links.jsonl"
    
    if progress_file.exists():
        with open(progress_file) as f:
            data = json.load(f)
        print(f"\nProgress:")
        print(f"  Fetched: {len(data.get('fetched', []))}")
        print(f"  Not found: {len(data.get('not_found', []))}")
        print(f"  Errors: {len(data.get('errors', []))}")
        print(f"  Groups processed: {len(data.get('searched_groups', []))}")
        print(f"  Last updated: {data.get('last_updated', 'N/A')}")
    
    if index_file.exists():
        with open(index_file) as f:
            idx = json.load(f)
        print(f"  Index: {len(idx)} statutes with linked cases")
    
    if links_file.exists():
        with open(links_file) as f:
            lines = f.readlines()
        citations = set()
        for line in lines:
            try:
                d = json.loads(line)
                citations.add(d.get('citation', ''))
            except:
                pass
        print(f"  Links file: {len(lines)} entries, {len(citations)} unique citations")

def main():
    print(f"🔍 FULL LOCAL DATA AUDIT — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Base directory: {BASE}")
    
    total_cases, by_reporter, case_issues = audit_cases()
    total_statutes, total_sections = audit_legislation()
    audit_linked_cases()
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  📁 Cases: {total_cases}")
    print(f"  📜 Statutes: {total_statutes}")
    print(f"  📝 Sections: {total_sections}")
    print(f"  ⚠️ Format issues: {len(case_issues)}")
    print(f"  💾 Disk: {BASE}")
    
    # Save report
    report_path = Path(r"C:\Users\gempo\.openclaw\workspace\memory\audit-2026-02-10.md")
    with open(report_path, 'w') as f:
        f.write(f"# Data Audit Report — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n")
        f.write(f"Total cases: {total_cases}\n")
        f.write(f"Total statutes: {total_statutes}\n")
        f.write(f"Total sections: {total_sections}\n")
        f.write(f"Format issues: {len(case_issues)}\n\n")
        f.write("## By Reporter\n")
        for r, c in sorted(by_reporter.items()):
            f.write(f"- {r}: {c['json']} JSON / {c['original']} original / {c['html']} readable\n")
    
    print(f"\n📄 Report saved to {report_path}")

if __name__ == "__main__":
    main()
