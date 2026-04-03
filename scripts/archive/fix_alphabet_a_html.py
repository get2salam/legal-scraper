#!/usr/bin/env python3
"""Fix Alphabet A - decode escaped HTML to proper original HTML files."""

import json
import codecs
from pathlib import Path

BASE_DIR = Path(__file__).parent / "data_v2" / "legislation" / "A"
ORIGINAL_DIR = BASE_DIR / "original"

def main():
    ORIGINAL_DIR.mkdir(exist_ok=True)
    
    fixed = 0
    for json_file in BASE_DIR.glob("*.json"):
        try:
            data = json.load(open(json_file, 'r', encoding='utf-8'))
            full_text = data.get('full_text', '')
            
            if not full_text:
                continue
            
            # Check if it's escaped HTML
            if '\\u003c' in full_text or '<' not in full_text:
                # Decode escaped unicode
                try:
                    decoded = codecs.decode(full_text, 'unicode_escape')
                except:
                    decoded = full_text
                
                # Extract just the HTML parts (remove [Section X] markers)
                html_parts = []
                current_html = []
                for line in decoded.split('\n'):
                    if line.startswith('[Section '):
                        if current_html:
                            html_parts.append('\n'.join(current_html))
                            current_html = []
                    else:
                        current_html.append(line)
                if current_html:
                    html_parts.append('\n'.join(current_html))
                
                raw_html = '\n\n'.join(html_parts)
                
                # Save to original folder
                html_file = ORIGINAL_DIR / f"{json_file.stem}.html"
                html_file.write_text(raw_html, encoding='utf-8')
                
                # Also update the JSON full_text with decoded version
                data['full_text'] = decoded
                json_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
                
                fixed += 1
                
        except Exception as e:
            print(f"Error {json_file.name}: {e}")
    
    print(f"Fixed {fixed} statutes in Alphabet A")

if __name__ == "__main__":
    main()
