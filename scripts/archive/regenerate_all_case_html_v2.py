#!/usr/bin/env python3
"""Regenerate all case law readable HTML with proper HTML decoding v2."""

import json
import html
import re
from pathlib import Path

DATA_DIR = Path(__file__).parent / 'data_v2'
HTML_DIR = DATA_DIR / 'html'

TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{citation} | Qanoon</title>
    <style>
        body {{ font-family: Georgia, serif; line-height: 1.8; max-width: 900px; margin: 0 auto; padding: 20px; color: #333; }}
        header {{ border-bottom: 2px solid #2c5282; margin-bottom: 30px; padding-bottom: 20px; }}
        h1 {{ color: #2c5282; margin-bottom: 10px; }}
        .metadata {{ margin: 20px 0; padding: 15px; background: #f7fafc; border-radius: 8px; }}
        .metadata div {{ margin: 8px 0; }}
        .label {{ font-weight: bold; color: #4a5568; display: inline-block; width: 100px; }}
        .judgment {{ margin-top: 30px; }}
        .judgment h2 {{ color: #2c5282; border-bottom: 1px solid #e2e8f0; padding-bottom: 10px; }}
        .judgment-text {{ text-align: justify; }}
        .judgment-text p {{ margin-bottom: 15px; }}
        footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #718096; font-size: 0.85em; }}
        a {{ color: #2b6cb0; }}
        a.back {{ display: inline-block; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <a href="../../index.html" class="back">← Back to Index</a>
    <article>
        <header>
            <h1>{citation}</h1>
        </header>
        
        <div class="metadata">
            <div><span class="label">Court:</span> {court}</div>
            <div><span class="label">Date:</span> {date}</div>
            <div><span class="label">Judges:</span> {judges}</div>
        </div>
        
        <div class="judgment">
            <h2>Judgment</h2>
            <div class="judgment-text">
                {judgment}
            </div>
        </div>
        
        <footer>
            <p>Qanoon Legal Research Platform</p>
        </footer>
    </article>
</body>
</html>'''


def decode_double_escaped(text: str) -> str:
    """Decode double-escaped HTML content."""
    if not text:
        return ""
    
    # Remove outer quotes if present
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    
    # Replace double-escaped unicode sequences
    text = text.replace('\\u003c', '<')
    text = text.replace('\\u003e', '>')
    text = text.replace('\\u0026', '&')
    text = text.replace('\\u0027', "'")
    text = text.replace('\\u0022', '"')
    text = text.replace('\\r\\n', '\n')
    text = text.replace('\\n', '\n')
    text = text.replace('\\t', '\t')
    text = text.replace('\\"', '"')
    text = text.replace("\\'", "'")
    
    return text


def clean_judgment(raw_html: str) -> str:
    """Clean up judgment HTML for display."""
    content = decode_double_escaped(raw_html)
    
    # Extract body content if present
    body_match = re.search(r'<body[^>]*>(.*?)</body>', content, re.DOTALL | re.IGNORECASE)
    if body_match:
        content = body_match.group(1)
    
    # Remove MS Word cruft
    content = re.sub(r'<!\[if[^\]]*\]>', '', content)
    content = re.sub(r'<!\[endif\]>', '', content)
    content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
    content = re.sub(r'<o:p>.*?</o:p>', '', content)
    content = re.sub(r'<xml>.*?</xml>', '', content, flags=re.DOTALL)
    
    # Clean up mso styles but keep structure
    content = re.sub(r'mso-[^;"\']+[;"\']?', '', content)
    content = re.sub(r'style\s*=\s*["\'][^"\']*["\']', '', content)
    content = re.sub(r'class\s*=\s*["\'][^"\']*["\']', '', content)
    
    # Remove empty tags
    content = re.sub(r'<div[^>]*>\s*</div>', '', content)
    content = re.sub(r'<span[^>]*>\s*</span>', '', content)
    
    return content.strip()


def generate_case_html(json_path: Path) -> bool:
    """Generate readable HTML for a single case."""
    try:
        data = json.load(open(json_path, 'r', encoding='utf-8'))
        
        citation = data.get('citation', json_path.stem.replace('_', ' '))
        
        # Validate metadata - if corrupted (too long), use N/A
        court = data.get('court', 'N/A')
        if len(str(court)) > 100:  # Court name shouldn't be this long
            court = 'N/A'
        
        date = data.get('date', 'N/A')
        if len(str(date)) > 50:
            date = 'N/A'
            
        judges = ', '.join(data.get('judges', [])) if data.get('judges') else 'N/A'
        
        # Get judgment - try multiple fields
        judgment_raw = data.get('judgment', data.get('judgment_raw', ''))
        if not judgment_raw:
            return False
        
        judgment = clean_judgment(judgment_raw)
        
        # Generate HTML
        content = TEMPLATE.format(
            citation=html.escape(str(citation)),
            court=html.escape(str(court)),
            date=html.escape(str(date)),
            judges=html.escape(str(judges)),
            judgment=judgment
        )
        
        # Determine output path
        parts = json_path.stem.split('_')  # 2024_SCMR_1 -> [2024, SCMR, 1]
        if len(parts) >= 3:
            year, reporter = parts[0], parts[1]
        else:
            year = data.get('year', '0000')
            reporter = data.get('reporter', 'UNKNOWN')
        
        output_dir = HTML_DIR / reporter / str(year)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{json_path.stem}.html"
        output_file.write_text(content, encoding='utf-8')
        
        return True
    except Exception as e:
        print(f"Error {json_path.name}: {e}")
        return False


def main():
    reporters = ['SCMR', 'PLD', 'CLC', 'MLD', 'PCrLJ', 'PTD', 'YLR', 'PLC', 'CLD', 'GBLR']
    years = ['2023', '2024', '2025']
    
    total = 0
    generated = 0
    
    for reporter in reporters:
        for year in years:
            json_dir = DATA_DIR / reporter / year
            if not json_dir.exists():
                continue
            
            json_files = list(json_dir.glob('*.json'))
            for jf in json_files:
                total += 1
                if generate_case_html(jf):
                    generated += 1
                
                if generated % 500 == 0 and generated > 0:
                    print(f"Generated {generated} files...")
    
    print(f"Done! Generated {generated}/{total} case HTML files")


if __name__ == '__main__':
    main()
