#!/usr/bin/env python3
"""
Fix JSONL Duplicates
====================
Removes duplicate entries from JSONL files based on citation.
Run this after any re-scraping incident.
"""

import json
from pathlib import Path
from collections import OrderedDict

DATA_DIR = Path(__file__).parent / "data_v2"

def deduplicate_jsonl(filepath: Path) -> tuple[int, int]:
    """Remove duplicates from a JSONL file. Returns (original_count, new_count)."""
    if not filepath.exists():
        return 0, 0
    
    seen = OrderedDict()  # Preserves order, keeps last occurrence
    original_count = 0
    
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            original_count += 1
            try:
                data = json.loads(line)
                citation = data.get("citation", "")
                if citation:
                    seen[citation] = line  # Keep last occurrence
            except json.JSONDecodeError:
                continue
    
    new_count = len(seen)
    
    if new_count < original_count:
        # Rewrite file without duplicates
        with open(filepath, "w", encoding="utf-8") as f:
            for line in seen.values():
                f.write(line + "\n")
        print(f"  {filepath.name}: {original_count} -> {new_count} (removed {original_count - new_count} duplicates)")
    else:
        print(f"  {filepath.name}: {original_count} entries (no duplicates)")
    
    return original_count, new_count

def main():
    print("=== Fixing JSONL Duplicates ===\n")
    
    total_removed = 0
    
    # Fix reporter JSONL files
    print("Reporter JSONL files:")
    for jsonl_file in DATA_DIR.glob("*_*.jsonl"):
        orig, new = deduplicate_jsonl(jsonl_file)
        total_removed += (orig - new)
    
    # Fix master JSONL
    print("\nMaster JSONL:")
    master = DATA_DIR / "all_cases.jsonl"
    orig, new = deduplicate_jsonl(master)
    total_removed += (orig - new)
    
    print(f"\n=== Total duplicates removed: {total_removed} ===")

if __name__ == "__main__":
    main()
