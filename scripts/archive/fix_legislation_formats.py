#!/usr/bin/env python3
"""
Fix legislation to have all 4 formats like case law:
1. JSON (structured) - already exists
2. Original HTML (raw PLS) - extract from JSON full_text
3. Readable HTML - already exists in html/{letter}/
4. JSONL - append to master file
"""

import json
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent / "data_v2" / "legislation"
JSONL_FILE = Path(__file__).parent / "data_v2" / "all_legislation.jsonl"

def fix_alphabet(letter: str):
    """Fix all 4 formats for a single alphabet."""
    letter_dir = BASE_DIR / letter.upper()
    if not letter_dir.exists():
        print(f"[SKIP] No directory for letter {letter}")
        return 0, 0
    
    # Create original folder
    original_dir = letter_dir / "original"
    original_dir.mkdir(exist_ok=True)
    
    json_files = list(letter_dir.glob("*.json"))
    print(f"[INFO] Processing {len(json_files)} statutes for letter {letter}")
    
    extracted = 0
    jsonl_added = 0
    
    # Load existing JSONL entries to avoid duplicates
    existing_ids = set()
    if JSONL_FILE.exists():
        with open(JSONL_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if 'id' in entry:
                        existing_ids.add(entry['id'])
                except:
                    pass
    
    jsonl_entries = []
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            statute_id = data.get('id', json_file.stem)
            
            # 1. Extract original HTML from full_text or sections
            raw_html = data.get('full_text', '')
            
            # If no full_text, try to reconstruct from sections
            if not raw_html and 'sections' in data:
                parts = []
                for section in data['sections']:
                    if isinstance(section, dict):
                        content = section.get('content', section.get('text', ''))
                        if content:
                            parts.append(content)
                raw_html = '\n'.join(parts)
            
            if raw_html:
                original_file = original_dir / f"{json_file.stem}.html"
                if not original_file.exists():
                    with open(original_file, 'w', encoding='utf-8') as f:
                        f.write(raw_html)
                    extracted += 1
            
            # 2. Add to JSONL if not already there
            if statute_id not in existing_ids:
                jsonl_entry = {
                    'id': statute_id,
                    'type': 'legislation',
                    'alphabet': letter.upper(),
                    'title': data.get('title', ''),
                    'short_title': data.get('short_title', ''),
                    'jurisdiction': data.get('jurisdiction', ''),
                    'enactment_date': data.get('enactment_date', ''),
                    'status': data.get('status', ''),
                    'sections_count': len(data.get('sections', [])),
                    'case_links_count': len(data.get('case_links', data.get('cases_cited', []))),
                    'scraped_at': datetime.now().isoformat(),
                    'source_file': str(json_file.name)
                }
                jsonl_entries.append(jsonl_entry)
                jsonl_added += 1
                
        except Exception as e:
            print(f"[ERROR] {json_file.name}: {e}")
    
    # Append to JSONL
    if jsonl_entries:
        with open(JSONL_FILE, 'a', encoding='utf-8') as f:
            for entry in jsonl_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    
    print(f"[OK] Letter {letter}: {extracted} original HTML extracted, {jsonl_added} JSONL entries added")
    return extracted, jsonl_added

def main():
    import sys
    
    if len(sys.argv) > 1:
        letters = [l.upper() for l in sys.argv[1:]]
    else:
        # All letters
        letters = [chr(i) for i in range(ord('A'), ord('Z') + 1)]
    
    total_html = 0
    total_jsonl = 0
    
    for letter in letters:
        html, jsonl = fix_alphabet(letter)
        total_html += html
        total_jsonl += jsonl
    
    print(f"\n[DONE] Total: {total_html} original HTML files, {total_jsonl} JSONL entries")

if __name__ == "__main__":
    main()
