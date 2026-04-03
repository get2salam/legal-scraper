#!/usr/bin/env python3
"""Generate readable HTML for 2023 cases."""

from pathlib import Path
import json
import html

DATA_DIR = Path(__file__).parent / 'data_v2'
HTML_DIR = DATA_DIR / 'html'

TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{citation} | Qanoon</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px; line-height: 1.6; }}
        .header {{ border-bottom: 2px solid #1a365d; padding-bottom: 20px; margin-bottom: 30px; }}
        .citation {{ font-size: 1.5em; font-weight: bold; color: #1a365d; }}
        .meta {{ color: #666; margin: 10px 0; }}
        .judgment {{ text-align: justify; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="citation">{citation}</div>
        <div class="meta">{court} | {date}</div>
        <div class="meta">Judges: {judges}</div>
    </div>
    <div class="judgment">{judgment}</div>
</body>
</html>'''

def main():
    generated = 0
    reporters = ['SCMR', 'PLD', 'CLC', 'MLD', 'PCrLJ', 'PTD', 'YLR', 'PLC', 'CLD', 'GBLR']
    
    for reporter in reporters:
        json_dir = DATA_DIR / reporter / '2023'
        html_out = HTML_DIR / reporter / '2023'
        
        if not json_dir.exists():
            continue
        
        html_out.mkdir(parents=True, exist_ok=True)
        
        for jf in json_dir.glob('*.json'):
            html_file = html_out / f'{jf.stem}.html'
            if html_file.exists():
                continue
            
            try:
                data = json.load(open(jf, 'r', encoding='utf-8'))
                
                citation = data.get('citation', jf.stem.replace('_', ' '))
                court = data.get('court', 'N/A')
                date = data.get('date', 'N/A')
                judges = ', '.join(data.get('judges', [])) if data.get('judges') else 'N/A'
                judgment = data.get('judgment', data.get('judgment_raw', ''))
                
                if not judgment:
                    continue
                
                content = TEMPLATE.format(
                    citation=html.escape(citation),
                    court=html.escape(str(court)),
                    date=html.escape(str(date)),
                    judges=html.escape(judges),
                    judgment=judgment
                )
                
                html_file.write_text(content, encoding='utf-8')
                generated += 1
                
                if generated % 100 == 0:
                    print(f'Generated {generated} files...')
                
            except Exception as e:
                print(f'Error {jf.name}: {e}')
    
    print(f'Done! Generated {generated} HTML files for 2023')

if __name__ == '__main__':
    main()
