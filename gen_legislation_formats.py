#!/usr/bin/env python3
"""
gen_legislation_formats.py — Generate Readable HTML + JSONL for legislation data.
Reads existing JSON files, produces:
  1. Readable HTML at data_v2/html/legislation/{LETTER}/{name}.html
  2. JSONL at data_v2/legislation_{LETTER}.jsonl (one JSON per line)

Safe to run alongside the scraper (read-only on source JSONs).
"""

import json
import os
import glob
import time
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / 'data_v2'
LEG_DIR = DATA_DIR / 'legislation'
HTML_DIR = DATA_DIR / 'html' / 'legislation'

READABLE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
    body {{ font-family: Georgia, 'Times New Roman', serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; color: #333; background: #fafafa; }}
    .header {{ border-bottom: 2px solid #2c3e50; padding-bottom: 15px; margin-bottom: 20px; }}
    .title {{ font-size: 1.4em; font-weight: bold; color: #2c3e50; }}
    .meta {{ color: #666; margin: 5px 0; }}
    .status {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; }}
    .status-in_force {{ background: #d4edda; color: #155724; }}
    .status-repealed {{ background: #f8d7da; color: #721c24; }}
    .sections {{ margin-top: 20px; }}
    .section {{ margin-bottom: 15px; padding: 10px; border-left: 3px solid #2c3e50; background: #fff; }}
    .section-title {{ font-weight: bold; color: #2c3e50; margin-bottom: 5px; }}
    .full-text {{ margin-top: 20px; }}
    .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #999; font-size: 0.85em; }}
</style>
</head>
<body>
<div class="header">
    <div class="title">{title}</div>
    <div class="meta"><strong>Short Title:</strong> {short_title}</div>
    <div class="meta"><strong>Date:</strong> {enactment_date}</div>
    <div class="meta"><strong>Jurisdiction:</strong> {jurisdiction}</div>
    <div class="meta"><strong>Status:</strong> <span class="status status-{status}">{status}</span></div>
    <div class="meta"><strong>Sections:</strong> {section_count} | <strong>Case Links:</strong> {case_link_count}</div>
</div>
<div class="full-text">
{full_text_html}
</div>
<div class="footer">
    Generated from legal research data &bull; {timestamp}
</div>
</body>
</html>"""


def generate_formats():
    start = time.time()
    readable_count = 0
    jsonl_count = 0
    errors = 0
    
    letters = sorted([d for d in os.listdir(LEG_DIR) 
                      if os.path.isdir(LEG_DIR / d) and len(d) == 1])
    
    total_jsons = sum(len(glob.glob(str(LEG_DIR / l / '*.json'))) for l in letters)
    processed = 0
    
    print(f"Generating formats for {total_jsons} legislation files across {len(letters)} letters...")
    print()
    
    for letter in letters:
        letter_dir = LEG_DIR / letter
        json_files = sorted(glob.glob(str(letter_dir / '*.json')))
        
        if not json_files:
            continue
        
        # Prepare JSONL file
        jsonl_path = DATA_DIR / f'legislation_{letter}.jsonl'
        jsonl_lines = []
        
        # Prepare readable HTML dir
        html_letter_dir = HTML_DIR / letter
        html_letter_dir.mkdir(parents=True, exist_ok=True)
        
        for jf in json_files:
            processed += 1
            try:
                with open(jf, encoding='utf-8') as f:
                    data = json.load(f)
                
                name = Path(jf).stem
                
                # 1. Generate Readable HTML
                full_text = data.get('full_text', '')
                # Clean up the full_text - it contains [Section X] markers with HTML
                full_text_html = full_text
                if full_text_html.startswith('[Section'):
                    # It's sectioned text with embedded HTML
                    pass  # Keep as-is, browser will render HTML parts
                
                html_content = READABLE_TEMPLATE.format(
                    title=data.get('title', 'Unknown'),
                    short_title=data.get('short_title', ''),
                    enactment_date=data.get('enactment_date', 'Unknown'),
                    jurisdiction=data.get('jurisdiction', 'Not specified') or 'Not specified',
                    status=data.get('status', 'unknown'),
                    section_count=len(data.get('sections', [])),
                    case_link_count=len(data.get('case_links', [])),
                    full_text_html=full_text_html,
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M')
                )
                
                html_path = html_letter_dir / f'{name}.html'
                html_path.write_text(html_content, encoding='utf-8')
                readable_count += 1
                
                # 2. Collect for JSONL
                jsonl_lines.append(json.dumps(data, ensure_ascii=False))
                jsonl_count += 1
                
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ERROR: {jf}: {e}")
            
            if processed % 200 == 0:
                print(f"  Progress: {processed}/{total_jsons} ({processed/total_jsons*100:.0f}%)")
        
        # Write JSONL for this letter
        if jsonl_lines:
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(jsonl_lines) + '\n')
        
        print(f"  {letter}: {len(json_files)} JSONs -> {len(jsonl_lines)} JSONL + readable HTML")
    
    elapsed = time.time() - start
    
    print(f"\n{'='*60}")
    print(f"  Legislation Format Generator — COMPLETE")
    print(f"{'='*60}")
    print(f"  Readable HTML generated: {readable_count}")
    print(f"  JSONL entries written:   {jsonl_count}")
    print(f"  Errors:                  {errors}")
    print(f"  Elapsed:                 {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == '__main__':
    generate_formats()
