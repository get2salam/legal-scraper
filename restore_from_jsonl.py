#!/usr/bin/env python3
"""
Restore JSON files from JSONL backups
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data_v2"

def restore():
    """Restore all JSON files from JSONL backups."""
    
    # Use all_cases.jsonl as the master source
    jsonl_file = DATA_DIR / "all_cases.jsonl"
    
    if not jsonl_file.exists():
        print(f"Error: {jsonl_file} not found!")
        return
    
    print(f"Reading from {jsonl_file}...")
    
    restored = 0
    errors = 0
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                case = json.loads(line.strip())
                
                citation = case.get('citation', '')
                case_name = case.get('case_name', '')
                
                if not citation or not case_name:
                    continue
                
                # Parse citation to get reporter and year
                # Format: "2024 SCMR 1" -> reporter=SCMR, year=2024
                parts = citation.split()
                if len(parts) < 2:
                    continue
                
                year = parts[0]
                reporter = parts[1]
                
                # Create directory
                output_dir = DATA_DIR / reporter / year
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate filename
                filename = f"{year}_{reporter}_{parts[2] if len(parts) > 2 else case_name}.json"
                output_path = output_dir / filename
                
                # Write file
                with open(output_path, 'w', encoding='utf-8') as out:
                    json.dump(case, out, indent=2, ensure_ascii=False)
                
                restored += 1
                
                if restored % 100 == 0:
                    print(f"Restored: {restored} files")
                    
            except Exception as e:
                errors += 1
                if errors < 5:
                    print(f"Error: {e}")
    
    print(f"\nComplete: {restored} restored, {errors} errors")

if __name__ == "__main__":
    restore()
