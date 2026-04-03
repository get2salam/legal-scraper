#!/usr/bin/env python3
"""Quick local audit — no PLS requests, just analyze what's on disk."""

import json, re, os, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
REPORT_DIR = DATA_DIR / "audit"
REPORT_DIR.mkdir(exist_ok=True)

def is_corrupt(text):
    """Check if a section's text is corrupt/empty."""
    if not text or not isinstance(text, str):
        return "empty"
    t = text.strip()
    if t == '':
        return "empty"
    if t in ('"-1"', '-1', '"-1', '-1"', '"−1"', '-1.'):
        return "neg1"
    if len(t) < 5:
        return "tiny"  
    return None  # valid

def audit_letter(letter):
    letter_dir = DATA_DIR / letter
    if not letter_dir.exists():
        return None
    
    files = sorted(letter_dir.glob("*.json"))
    
    stats = {
        "letter": letter,
        "total_files": len(files),
        "total_sections": 0,
        "valid_sections": 0,
        "neg1_sections": 0,
        "empty_sections": 0,
        "tiny_sections": 0,
        "files_all_valid": 0,
        "files_some_corrupt": 0,
        "files_all_corrupt": 0,
        "files_no_sections": 0,
        "statutes_needing_rescrape": [],
        "section_details": defaultdict(lambda: {"total": 0, "valid": 0, "neg1": 0, "empty": 0, "tiny": 0}),
    }
    
    for f in files:
        try:
            data = json.load(open(f, encoding='utf-8'))
        except:
            stats["files_all_corrupt"] += 1
            stats["statutes_needing_rescrape"].append({"file": f.stem, "reason": "invalid_json", "sections": 0, "corrupt": 0})
            continue
        
        sections = data.get("sections", [])
        if not sections:
            stats["files_no_sections"] += 1
            continue
        
        file_valid = 0
        file_corrupt = 0
        
        for s in sections:
            text = s.get("text", "")
            status = is_corrupt(text)
            stats["total_sections"] += 1
            
            if status is None:
                stats["valid_sections"] += 1
                file_valid += 1
            elif status == "neg1":
                stats["neg1_sections"] += 1
                file_corrupt += 1
            elif status == "empty":
                stats["empty_sections"] += 1
                file_corrupt += 1
            elif status == "tiny":
                stats["tiny_sections"] += 1
                file_corrupt += 1
        
        if file_corrupt == 0:
            stats["files_all_valid"] += 1
        elif file_valid == 0:
            stats["files_all_corrupt"] += 1
            stats["statutes_needing_rescrape"].append({
                "file": f.stem[:80],
                "title": data.get("title", "")[:80],
                "sections": len(sections),
                "corrupt": file_corrupt,
                "reason": "all_corrupt"
            })
        else:
            stats["files_some_corrupt"] += 1
            stats["statutes_needing_rescrape"].append({
                "file": f.stem[:80],
                "title": data.get("title", "")[:80],
                "sections": len(sections),
                "valid": file_valid,
                "corrupt": file_corrupt,
                "reason": "partial_corrupt"
            })
    
    return stats

def get_pls_missing(letter):
    """Check progress.json for what PLS has that we don't."""
    progress_file = DATA_DIR / "progress.json"
    if not progress_file.exists():
        return []
    
    progress = json.load(open(progress_file, encoding='utf-8'))
    scraped = progress.get("statutes_scraped", [])
    
    # Get names starting with this letter
    pls_names = [n for n in scraped if n and n[0].upper() == letter.upper()]
    
    # Get disk names
    letter_dir = DATA_DIR / letter
    disk_names = set()
    if letter_dir.exists():
        for f in letter_dir.glob("*.json"):
            disk_names.add(f.stem)
    
    # Find ones in PLS list that might not be on disk
    # (This is approximate - name sanitization makes exact match hard)
    return pls_names, len(disk_names)

def main():
    letters = [l.upper() for l in (sys.argv[1:] if sys.argv[1:] else ["A", "B"])]
    
    print("=" * 70)
    print("LEGISLATION AUDIT REPORT")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)
    
    full_report = {}
    
    for letter in letters:
        stats = audit_letter(letter)
        if not stats:
            print(f"\n{letter}: No data directory found")
            continue
        
        full_report[letter] = stats
        
        total_corrupt = stats["neg1_sections"] + stats["empty_sections"] + stats["tiny_sections"]
        total = stats["total_sections"]
        valid_pct = (stats["valid_sections"] / total * 100) if total else 0
        corrupt_pct = (total_corrupt / total * 100) if total else 0
        
        print(f"\n{'='*60}")
        print(f"LETTER {letter}")
        print(f"{'='*60}")
        print(f"")
        print(f"  FILES:")
        print(f"    Total JSON files:       {stats['total_files']}")
        print(f"    All sections valid:     {stats['files_all_valid']}")
        print(f"    Partially corrupt:      {stats['files_some_corrupt']}")
        print(f"    Fully corrupt:          {stats['files_all_corrupt']}")
        print(f"    No sections:            {stats['files_no_sections']}")
        print(f"")
        print(f"  SECTIONS:")
        print(f"    Total sections:         {total}")
        print(f"    Valid (good text):      {stats['valid_sections']} ({valid_pct:.1f}%)")
        print(f"    Corrupt '-1':           {stats['neg1_sections']}")
        print(f"    Empty text:             {stats['empty_sections']}")
        print(f"    Tiny (<5 chars):        {stats['tiny_sections']}")
        print(f"    TOTAL CORRUPT:          {total_corrupt} ({corrupt_pct:.1f}%)")
        print(f"")
        
        needs_rescrape = stats["statutes_needing_rescrape"]
        all_corrupt = [s for s in needs_rescrape if s.get("reason") == "all_corrupt"]
        partial = [s for s in needs_rescrape if s.get("reason") == "partial_corrupt"]
        
        print(f"  RESCRAPE NEEDED:")
        print(f"    Files fully corrupt:    {len(all_corrupt)}")
        print(f"    Files partially corrupt:{len(partial)}")
        print(f"    Total files to fix:     {len(needs_rescrape)}")
        
        corrupt_sections_to_fix = sum(s.get("corrupt", 0) for s in needs_rescrape)
        print(f"    Sections to re-fetch:   {corrupt_sections_to_fix}")
        
        # Show worst offenders
        worst = sorted(needs_rescrape, key=lambda x: x.get("corrupt", 0), reverse=True)[:10]
        if worst:
            print(f"\n  WORST FILES (most corrupt sections):")
            for w in worst:
                c = w.get("corrupt", 0)
                t = w.get("sections", 0)
                v = w.get("valid", 0)
                title = w.get("title", w["file"])[:60]
                print(f"    {title}: {c}/{t} corrupt ({v} valid)")
    
    # Save report
    report_path = REPORT_DIR / f"audit_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    # Convert defaultdicts for serialization
    for letter, stats in full_report.items():
        if "section_details" in stats:
            del stats["section_details"]
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, indent=2, ensure_ascii=False)
    print(f"\nFull report saved to: {report_path}")
    
    # PLS comparison (from progress.json)
    print(f"\n{'='*60}")
    print("PLS COVERAGE (from progress.json)")
    print(f"{'='*60}")
    
    progress_file = DATA_DIR / "progress.json"
    if progress_file.exists():
        progress = json.load(open(progress_file, encoding='utf-8'))
        scraped = progress.get("statutes_scraped", [])
        
        for letter in letters:
            pls_names = [n for n in scraped if n and n[0].upper() == letter]
            letter_dir = DATA_DIR / letter
            disk_count = len(list(letter_dir.glob("*.json"))) if letter_dir.exists() else 0
            
            print(f"  {letter}: PLS progress claims {len(pls_names)} | Disk has {disk_count}")
            if len(pls_names) > disk_count:
                diff = len(pls_names) - disk_count
                print(f"    -> {diff} statutes in progress but not on disk")
                print(f"    -> These were likely 'no sections found' statutes (valid)")
    
    print(f"\n{'='*70}")
    print("AUDIT COMPLETE")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
