#!/usr/bin/env python3
"""Generate readable HTML for specific cases."""

import json
import html
import re
from pathlib import Path
from datetime import datetime

base = Path("data_v2")

# Cases to generate HTML for
cases = [
    ("2000", "PLD", "225"),
    ("1992", "PLD", "1"),
]

for year, reporter, page in cases:
    json_path = base / reporter / year / f"{year}_{reporter}_{page}.json"
    html_dir = base / "html" / reporter / year
    html_path = html_dir / f"{year}_{reporter}_{page}.html"
    
    if not json_path.exists():
        print(f"{year} {reporter} {page}: JSON not found")
        continue
        
    html_dir.mkdir(parents=True, exist_ok=True)
    
    with open(json_path, "r", encoding="utf-8") as f:
        case = json.load(f)
    
    citation = case.get("citation", f"{year} {reporter} {page}")
    case_id = case.get("case_name", "")
    court = case.get("court", "")
    judgment = case.get("judgment", "")
    
    if not judgment or judgment == "-1":
        judgment = case.get("judgment_raw", "")
    
    if judgment:
        judgment = html.unescape(judgment)
        judgment = re.sub(r"<p[^>]*>", "<p>", judgment)
    else:
        judgment = "<p><em>Judgment text not available.</em></p>"
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{citation}</title>
    <style>
        body {{ font-family: Georgia, serif; line-height: 1.8; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #2c5282; }}
        .judgment {{ text-align: justify; }}
    </style>
</head>
<body>
    <h1>{citation}</h1>
    <p><strong>Court:</strong> {court}</p>
    <div class="judgment">{judgment}</div>
    <footer><p>Generated: {datetime.now().strftime('%Y-%m-%d')}</p></footer>
</body>
</html>"""
    
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"{year} {reporter} {page}: HTML generated ({html_path.stat().st_size:,} bytes)")
