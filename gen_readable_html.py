#!/usr/bin/env python3
"""Generate readable HTML for all cases missing it."""

import json
import glob
from pathlib import Path

base = Path(__file__).parent / "data_v2"

def generate_html(case):
    citation = case.get('citation', '')
    case_name = case.get('case_name', '')
    court = case.get('court', 'N/A')
    judge = case.get('judge', 'N/A')
    date = case.get('date', 'N/A')
    judgment = case.get('judgment', '')
    scraped_at = case.get('scraped_at', 'N/A')
    
    return f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{citation}</title>
<style>body{{font-family:Georgia,serif;max-width:900px;margin:0 auto;padding:20px;line-height:1.6}}.header{{background:#1a365d;color:white;padding:20px;margin:-20px -20px 20px}}.citation{{font-size:1.4em;font-weight:bold}}.meta{{background:#e2e8f0;padding:15px;margin-bottom:20px}}.judgment{{background:white;padding:20px;border:1px solid #e2e8f0}}</style></head>
<body><div class="header"><div class="citation">{citation}</div><div>{case_name}</div></div>
<div class="meta"><b>Court:</b> {court} | <b>Judge:</b> {judge} | <b>Date:</b> {date}</div>
<div class="judgment">{judgment}</div></body></html>'''


def main():
    reporters = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD']
    years = ['2022', '2021', '2020', '2019', '2018']
    
    generated = 0
    for reporter in reporters:
        for year in years:
            json_dir = base / reporter / year
            if not json_dir.exists():
                continue
                
            for json_file in json_dir.glob("*.json"):
                readable_dir = base / "html" / reporter / year
                readable_path = readable_dir / json_file.name.replace('.json', '.html')
                
                if readable_path.exists():
                    continue
                
                readable_dir.mkdir(parents=True, exist_ok=True)
                
                with open(json_file, 'r', encoding='utf-8') as f:
                    case = json.load(f)
                
                html = generate_html(case)
                readable_path.write_text(html, encoding='utf-8')
                generated += 1
    
    print(f"Generated {generated} readable HTML files")


if __name__ == "__main__":
    main()
