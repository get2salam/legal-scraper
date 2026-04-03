#!/usr/bin/env python3
"""
AUDIT SCRIPT 6: Local Integrity Check
======================================
Checks every JSON file in data_v2 for integrity issues:
- Valid JSON?
- Has citation field?
- Has judgment or judgment_raw?
- judgment_raw length > 100 chars?
- File size > 500 bytes?
- Filename matches citation inside file?
- Duplicate citations within same reporter/year?

Output: data_v2/audit/local_integrity.json
"""

import os
import sys
import json
import re
import time
from pathlib import Path
from collections import defaultdict

# Reconfigure stdout
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = AUDIT_DIR / "local_integrity.json"
PROGRESS_FILE = AUDIT_DIR / "local_integrity_progress.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"checked": [], "issues": {}, "stats": {}}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')


def check_file(file_path):
    """Check a single JSON file for integrity issues. Returns list of issues."""
    issues = []
    file_size = file_path.stat().st_size

    # Check file size
    if file_size < 500:
        issues.append({"type": "small_file", "size": file_size})

    # Try to parse JSON
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        issues.append({"type": "invalid_json", "error": str(e)[:200]})
        return issues, None
    except Exception as e:
        issues.append({"type": "read_error", "error": str(e)[:200]})
        return issues, None

    # Check citation field
    citation = data.get("citation", "")
    if not citation:
        issues.append({"type": "missing_citation"})

    # Check judgment fields
    judgment = data.get("judgment", "")
    judgment_raw = data.get("judgment_raw", "")
    
    if not judgment and not judgment_raw:
        issues.append({"type": "no_judgment_content"})
    elif judgment_raw and len(judgment_raw) < 100:
        issues.append({"type": "stub_judgment", "raw_length": len(judgment_raw)})
    elif not judgment_raw and judgment and len(judgment) < 100:
        issues.append({"type": "stub_judgment", "judgment_length": len(judgment)})

    # Check filename matches citation
    if citation:
        expected_filename = re.sub(r'[^\w\-]', '_', citation) + ".json"
        actual_filename = file_path.name
        if expected_filename != actual_filename:
            issues.append({
                "type": "filename_mismatch",
                "expected": expected_filename,
                "actual": actual_filename
            })

    # Check required fields exist
    for field in ["year", "reporter", "page"]:
        if not data.get(field):
            issues.append({"type": f"missing_{field}"})

    return issues, citation


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 6: Local Integrity Check")
    print("=" * 60)

    progress = load_progress()
    checked_set = set(progress.get("checked", []))
    all_issues = progress.get("issues", {})

    # Track citations per reporter/year for duplicate detection
    citations_by_ry = defaultdict(list)

    # Stats
    total_files = 0
    total_checked = 0
    total_issues = 0
    issue_counts = defaultdict(int)

    start_time = time.time()

    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            print(f"  {reporter}: directory not found, skipping")
            continue

        year_dirs = sorted([d for d in reporter_dir.iterdir() if d.is_dir() and d.name.isdigit()])
        
        for year_dir in year_dirs:
            year = year_dir.name
            ry_key = f"{reporter}/{year}"
            
            json_files = list(year_dir.glob("*.json"))
            total_files += len(json_files)

            for json_file in json_files:
                file_key = f"{reporter}/{year}/{json_file.name}"
                
                if file_key in checked_set:
                    # Still need to track citation for duplicate detection
                    if file_key in all_issues:
                        for issue in all_issues.get(file_key, []):
                            issue_counts[issue["type"]] += 1
                    total_checked += 1
                    continue

                file_issues, citation = check_file(json_file)
                
                if citation:
                    citations_by_ry[ry_key].append((citation, json_file.name))

                if file_issues:
                    all_issues[file_key] = file_issues
                    total_issues += len(file_issues)
                    for issue in file_issues:
                        issue_counts[issue["type"]] += 1

                checked_set.add(file_key)
                total_checked += 1

                # Progress update every 5000 files
                if total_checked % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_checked / elapsed if elapsed > 0 else 0
                    print(f"  Checked {total_checked:,} files | {len(all_issues):,} files with issues | {rate:.0f} files/sec")
                    
                    # Save progress
                    progress["checked"] = list(checked_set)
                    progress["issues"] = all_issues
                    save_progress(progress)

    # Check for duplicate citations
    duplicate_issues = {}
    for ry_key, citations in citations_by_ry.items():
        seen = {}
        for citation, filename in citations:
            if citation in seen:
                dup_key = f"{ry_key}/DUPLICATE"
                if dup_key not in duplicate_issues:
                    duplicate_issues[dup_key] = []
                duplicate_issues[dup_key].append({
                    "type": "duplicate_citation",
                    "citation": citation,
                    "files": [seen[citation], filename]
                })
                issue_counts["duplicate_citation"] += 1
            else:
                seen[citation] = filename

    all_issues.update(duplicate_issues)

    # Final stats
    elapsed = time.time() - start_time

    stats = {
        "total_files_found": total_files,
        "total_files_checked": total_checked,
        "files_with_issues": len([k for k, v in all_issues.items() if v]),
        "total_issues": sum(len(v) for v in all_issues.values()),
        "issue_breakdown": dict(issue_counts),
        "elapsed_seconds": round(elapsed, 1),
        "reporters_checked": REPORTERS,
    }

    # Build per-reporter stats
    reporter_stats = {}
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        r_files = 0
        r_issues = 0
        for year_dir in reporter_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                r_files += len(list(year_dir.glob("*.json")))
                for f in year_dir.glob("*.json"):
                    fk = f"{reporter}/{year_dir.name}/{f.name}"
                    if fk in all_issues:
                        r_issues += 1
        reporter_stats[reporter] = {"files": r_files, "issues": r_issues}
    
    stats["per_reporter"] = reporter_stats

    # Save final output
    output = {
        "audit": "local_integrity",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": stats,
        "issues": all_issues,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    # Save progress
    progress["checked"] = list(checked_set)
    progress["issues"] = all_issues
    progress["stats"] = stats
    save_progress(progress)

    # Print summary
    print("\n" + "=" * 60)
    print("LOCAL INTEGRITY CHECK COMPLETE")
    print("=" * 60)
    print(f"Total files found: {total_files:,}")
    print(f"Total files checked: {total_checked:,}")
    print(f"Files with issues: {stats['files_with_issues']:,}")
    print(f"Total issues: {stats['total_issues']:,}")
    print(f"Time: {elapsed:.1f}s")
    print()
    print("Issue breakdown:")
    for issue_type, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {issue_type}: {count:,}")
    print()
    print("Per-reporter:")
    for reporter, rs in reporter_stats.items():
        pct = (rs['issues'] / rs['files'] * 100) if rs['files'] > 0 else 0
        print(f"  {reporter}: {rs['files']:,} files, {rs['issues']:,} issues ({pct:.1f}%)")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
