#!/usr/bin/env python3
"""
Sync case data between JSONL and individual JSON formats.
Ensures both formats have all cases.
"""

import json
from pathlib import Path

DATA_DIR = Path("data/pakistanlawsite")
CASES_DIR = DATA_DIR / "cases"
JSONL_DIR = DATA_DIR / "jsonl"

def sync_jsonl_to_json():
    """Create individual JSON files for any cases only in JSONL."""
    CASES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get existing JSON files
    existing_json = {f.stem for f in CASES_DIR.glob("*.json")}
    print(f"Existing individual JSON files: {len(existing_json)}")
    
    # Read all JSONL entries
    created = 0
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    case = json.loads(line)
                    case_id = case.get("id", case.get("case_id", ""))
                    
                    if case_id and case_id not in existing_json:
                        # Create individual JSON file
                        json_path = CASES_DIR / f"{case_id}.json"
                        with open(json_path, "w", encoding="utf-8") as jf:
                            json.dump(case, jf, ensure_ascii=False, indent=2)
                        print(f"  Created: {case_id}.json")
                        created += 1
                        existing_json.add(case_id)
                except json.JSONDecodeError:
                    pass
    
    print(f"\nCreated {created} new JSON files from JSONL")
    return created

def sync_json_to_jsonl():
    """Ensure all individual JSON files are in JSONL (run convert_to_jsonl.py)."""
    print("\nTo sync JSON -> JSONL, run: python convert_to_jsonl.py")

def main():
    print("=" * 60)
    print("SYNCING CASE DATA FORMATS")
    print("=" * 60)
    
    # Sync JSONL → individual JSON
    print("\n1. JSONL -> Individual JSON")
    print("-" * 40)
    sync_jsonl_to_json()
    
    # Count final totals
    print("\n" + "=" * 60)
    print("FINAL COUNTS")
    print("=" * 60)
    
    jsonl_count = 0
    for f in JSONL_DIR.glob("cases_*.jsonl"):
        with open(f, "r", encoding="utf-8") as jf:
            jsonl_count += sum(1 for line in jf if line.strip())
    
    json_count = len(list(CASES_DIR.glob("*.json")))
    
    print(f"  JSONL entries:     {jsonl_count}")
    print(f"  Individual JSON:   {json_count}")
    print(f"  In sync:           {'YES' if jsonl_count == json_count else 'NO'}")

if __name__ == "__main__":
    main()
