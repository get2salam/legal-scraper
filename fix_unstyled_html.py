#!/usr/bin/env python3
"""Fix readable HTML files that exist but have no <style> tag (pre-template legacy files)."""
import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / 'data_v2'
HTML_DIR = DATA_DIR / 'html'
REPORTERS = ['SCMR','PLD','PCrLJ','MLD','CLC','YLR','PTD','PLC','CLD','GBLR']

def make_html(citation, court, judges, date, judgment_html, timestamp):
    return (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n'
        f'<title>{citation}</title>\n'
        '<style>\n'
        '    body { font-family: Georgia, serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; color: #333; background: #fafafa; }\n'
        '    .header { border-bottom: 2px solid #2c3e50; padding-bottom: 15px; margin-bottom: 20px; }\n'
        '    .citation { font-size: 1.4em; font-weight: bold; color: #2c3e50; }\n'
        '    .meta { color: #666; margin: 5px 0; }\n'
        '    .judgment { margin-top: 20px; }\n'
        '    .footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #999; font-size: 0.85em; }\n'
        '</style>\n</head>\n<body>\n'
        '<div class="header">\n'
        f'    <div class="citation">{citation}</div>\n'
        f'    <div class="meta"><strong>Court:</strong> {court}</div>\n'
        f'    <div class="meta"><strong>Judges:</strong> {judges}</div>\n'
        f'    <div class="meta"><strong>Date:</strong> {date}</div>\n'
        '</div>\n'
        f'<div class="judgment">{judgment_html}</div>\n'
        f'<div class="footer">Generated from legal research data &bull; {timestamp}</div>\n'
        '</body>\n</html>'
    )

fixed = 0
already_ok = 0
no_json = 0
errors = 0

for rep in REPORTERS:
    rep_html_dir = HTML_DIR / rep
    if not rep_html_dir.is_dir():
        continue
    for year_dir in rep_html_dir.iterdir():
        if not year_dir.is_dir():
            continue
        for hf in year_dir.glob('*.html'):
            try:
                content = hf.read_text(encoding='utf-8', errors='ignore')
                if '<style>' in content[:1000]:
                    already_ok += 1
                    continue
                json_path = DATA_DIR / rep / year_dir.name / (hf.stem + '.json')
                if not json_path.exists():
                    no_json += 1
                    continue
                with open(json_path, encoding='utf-8') as f:
                    data = json.load(f)
                judges = data.get('judges', [])
                if isinstance(judges, list):
                    judges = ', '.join(judges)
                html = make_html(
                    citation=data.get('citation', hf.stem),
                    court=data.get('court', ''),
                    judges=judges,
                    date=data.get('date_decided', data.get('date', '')),
                    judgment_html=data.get('judgment_html', data.get('judgment_raw', data.get('judgment', ''))),
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M')
                )
                hf.write_text(html, encoding='utf-8')
                fixed += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"Error: {hf}: {e}")

print(f"Fixed: {fixed} | Already OK: {already_ok} | No JSON: {no_json} | Errors: {errors}")
