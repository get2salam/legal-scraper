#!/usr/bin/env python3
"""
Clean up "-1" sections — replace with proper marker.
Also fix progress.json to reflect reality.
"""

import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
PROGRESS_FILE = DATA_DIR / "progress.json"

MARKER = "[Content not available on source]"

def cleanup_letter(letter):
    letter_dir = DATA_DIR / letter
    if not letter_dir.exists():
        print(f"No directory for {letter}")
        return
    
    files = sorted(letter_dir.glob("*.json"))
    fixed_files = 0
    fixed_sections = 0
    
    for f in files:
        try:
            data = json.load(open(f, encoding='utf-8'))
        except:
            continue
        
        changed = False
        for s in data.get("sections", []):
            text = s.get("text", "")
            if isinstance(text, str):
                t = text.strip()
                if t in ('"-1"', '-1', '"-1', '-1"', '') or (t and len(t) < 5 and '-1' in t):
                    s["text"] = MARKER
                    s["content_status"] = "unavailable"
                    changed = True
                    fixed_sections += 1
                elif t == '' or not t:
                    s["text"] = MARKER
                    s["content_status"] = "unavailable"
                    changed = True
                    fixed_sections += 1
        
        if changed:
            with open(f, 'w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            fixed_files += 1
    
    print(f"  {letter}: Fixed {fixed_sections} sections in {fixed_files} files")
    return fixed_sections


def fix_progress():
    """Fix progress.json — mark B as incomplete."""
    if not PROGRESS_FILE.exists():
        return
    
    progress = json.load(open(PROGRESS_FILE, encoding='utf-8'))
    
    old_completed = progress.get("completed_alphabets", [])
    
    # A is genuinely complete (624/642, rest have no sections)
    # B is NOT complete (183/725)
    if "B" in old_completed:
        old_completed.remove("B")
        print("  Removed B from completed_alphabets (only 25% done)")
    
    progress["completed_alphabets"] = old_completed
    progress["last_updated"] = datetime.now().isoformat()
    
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)
    
    print(f"  Progress updated: completed = {old_completed}")


if __name__ == "__main__":
    print("Cleaning up -1 sections...")
    total = 0
    for letter in ["A", "B"]:
        n = cleanup_letter(letter)
        if n:
            total += n
    
    print(f"\nTotal fixed: {total} sections")
    
    print("\nFixing progress.json...")
    fix_progress()
    
    print("\nDone!")
