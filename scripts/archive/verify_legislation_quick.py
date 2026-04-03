#!/usr/bin/env python3
"""Quick verification of legislation data for a given letter."""

import json
import sys
from pathlib import Path

def verify_letter(letter="A"):
    data_dir = Path(f"data_v2/legislation/{letter}")
    
    if not data_dir.exists():
        print(f"Directory not found: {data_dir}")
        return
    
    statutes = list(data_dir.glob("*.json"))
    
    print(f"=== LEGISLATION LETTER {letter} VERIFICATION ===")
    print(f"Total statute files: {len(statutes)}")
    print()
    
    issues = []
    stats = {
        "total": len(statutes),
        "complete": 0,
        "missing_sections": 0,
        "empty_sections": 0,
        "no_case_links": 0,
        "total_sections": 0,
        "total_case_links": 0,
    }
    
    for f in sorted(statutes):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            title = data.get("title", f.stem)[:60]
            sections = data.get("sections", [])
            
            stats["total_sections"] += len(sections)
            
            # Check for issues
            if not sections:
                issues.append(f"NO SECTIONS: {title}")
                stats["missing_sections"] += 1
                continue
            
            empty_count = 0
            case_count = 0
            for sec in sections:
                text = sec.get("text", "")
                if not text or len(text) < 50:
                    empty_count += 1
                case_links = sec.get("case_links", [])
                case_count += len(case_links)
            
            stats["total_case_links"] += case_count
            
            if empty_count > 0:
                issues.append(f"EMPTY SECTIONS ({empty_count}): {title}")
                stats["empty_sections"] += 1
            else:
                stats["complete"] += 1
            
            if case_count == 0:
                stats["no_case_links"] += 1
                
        except Exception as e:
            issues.append(f"ERROR reading {f.name}: {e}")
    
    print("SUMMARY:")
    print(f"  Complete statutes: {stats['complete']}/{stats['total']}")
    print(f"  With section issues: {stats['empty_sections']}")
    print(f"  Missing sections entirely: {stats['missing_sections']}")
    print(f"  Statutes with no case links: {stats['no_case_links']}")
    print(f"  Total sections across all: {stats['total_sections']}")
    print(f"  Total case citations: {stats['total_case_links']}")
    print()
    
    if issues:
        print(f"ISSUES FOUND ({len(issues)}):")
        for issue in issues[:30]:
            print(f"  - {issue}")
        if len(issues) > 30:
            print(f"  ... and {len(issues)-30} more")
    else:
        print("NO ISSUES - All statutes complete!")
    
    return stats, issues

if __name__ == "__main__":
    letter = sys.argv[1] if len(sys.argv) > 1 else "A"
    verify_letter(letter)
