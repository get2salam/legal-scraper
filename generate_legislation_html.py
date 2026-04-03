#!/usr/bin/env python3
"""
Generate Legislation HTML - Creates readable HTML from statute JSON files
==========================================================================
Generates PLS-like HTML pages for statutes with:
- Table of contents (sections)
- Each section with its text
- Clickable case citations that link to our scraped cases

Usage:
    python generate_legislation_html.py              # Generate all
    python generate_legislation_html.py --letter A   # Only letter A
    python generate_legislation_html.py --statute "Arbitration Act"  # Single statute
"""

import json
import os
import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
OUTPUT_DIR = DATA_DIR / "legislation" / "html"


# HTML Template
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} | Qanoon Legal Research</title>
    <style>
        :root {{
            --primary-color: #1a365d;
            --secondary-color: #2c5282;
            --accent-color: #c53030;
            --text-color: #1a202c;
            --light-text: #4a5568;
            --border-color: #e2e8f0;
            --bg-light: #f7fafc;
            --link-color: #2b6cb0;
        }}
        
        * {{ box-sizing: border-box; }}
        
        body {{
            font-family: 'Georgia', 'Times New Roman', serif;
            line-height: 1.75;
            color: var(--text-color);
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
            background: #fff;
        }}
        
        .statute-header {{
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 25px;
            margin-bottom: 30px;
        }}
        
        .statute-title {{
            font-size: 1.8rem;
            font-weight: bold;
            color: var(--primary-color);
            margin: 0 0 10px 0;
        }}
        
        .statute-meta {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            background: var(--bg-light);
            padding: 15px;
            border-radius: 4px;
            margin-top: 15px;
        }}
        
        .meta-item {{
            font-size: 0.9rem;
        }}
        
        .meta-label {{
            font-weight: bold;
            color: var(--light-text);
        }}
        
        .toc {{
            background: var(--bg-light);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
        }}
        
        .toc h2 {{
            margin: 0 0 15px 0;
            font-size: 1.2rem;
            color: var(--primary-color);
        }}
        
        .toc ul {{
            list-style: none;
            padding: 0;
            margin: 0;
            columns: 2;
            column-gap: 30px;
        }}
        
        .toc li {{
            padding: 5px 0;
            break-inside: avoid;
        }}
        
        .toc a {{
            color: var(--link-color);
            text-decoration: none;
        }}
        
        .toc a:hover {{
            text-decoration: underline;
        }}
        
        .section {{
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .section-header {{
            display: flex;
            gap: 15px;
            align-items: baseline;
            margin-bottom: 15px;
        }}
        
        .section-number {{
            font-weight: bold;
            color: var(--primary-color);
            font-size: 1.1rem;
            white-space: nowrap;
        }}
        
        .section-title {{
            font-weight: bold;
            color: var(--secondary-color);
        }}
        
        .section-content {{
            text-align: justify;
        }}
        
        .section-content p {{
            margin: 0.5em 0;
        }}
        
        .case-links {{
            margin-top: 15px;
            padding: 10px 15px;
            background: #fffbeb;
            border-left: 4px solid #f59e0b;
            border-radius: 0 4px 4px 0;
        }}
        
        .case-links-title {{
            font-weight: bold;
            font-size: 0.85rem;
            color: #92400e;
            margin-bottom: 8px;
        }}
        
        .case-link {{
            display: inline-block;
            margin: 3px 5px 3px 0;
            padding: 2px 8px;
            background: #fef3c7;
            border-radius: 3px;
            font-size: 0.85rem;
            color: var(--link-color);
            text-decoration: none;
        }}
        
        .case-link:hover {{
            background: #fde68a;
            text-decoration: underline;
        }}
        
        .case-link.unavailable {{
            color: #9ca3af;
            background: #f3f4f6;
            cursor: not-allowed;
        }}
        
        .back-link {{
            display: inline-block;
            margin-bottom: 20px;
            color: var(--link-color);
            text-decoration: none;
        }}
        
        .back-link:hover {{
            text-decoration: underline;
        }}
        
        @media (max-width: 600px) {{
            .toc ul {{
                columns: 1;
            }}
            .section-header {{
                flex-direction: column;
                gap: 5px;
            }}
        }}
    </style>
</head>
<body>
    <a href="../index.html" class="back-link">&larr; Back to Index</a>
    
    <div class="statute-header">
        <h1 class="statute-title">{title}</h1>
        <div class="statute-meta">
            <div class="meta-item"><span class="meta-label">Year:</span> {year}</div>
            <div class="meta-item"><span class="meta-label">Status:</span> {status}</div>
            <div class="meta-item"><span class="meta-label">Sections:</span> {section_count}</div>
        </div>
    </div>
    
    <div class="toc">
        <h2>Table of Contents</h2>
        <ul>
{toc_items}
        </ul>
    </div>
    
    <div class="sections">
{sections_html}
    </div>
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid var(--border-color); font-size: 0.85rem; color: var(--light-text);">
        <p>Generated by Qanoon Legal Research Platform</p>
        <p>Source: Pakistan Law Site | Generated: {generated_date}</p>
    </footer>
</body>
</html>
'''

INDEX_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Legislation Index | Qanoon Legal Research</title>
    <style>
        :root {{
            --primary-color: #1a365d;
            --link-color: #2b6cb0;
        }}
        body {{
            font-family: 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            max-width: 1000px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        h1 {{
            color: var(--primary-color);
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 15px;
        }}
        .stats {{
            background: #f7fafc;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            display: flex;
            gap: 30px;
        }}
        .stat-item {{
            text-align: center;
        }}
        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary-color);
        }}
        .stat-label {{
            color: #718096;
            font-size: 0.9rem;
        }}
        .letter-section {{
            margin-bottom: 30px;
        }}
        .letter-section h2 {{
            color: var(--primary-color);
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 10px;
        }}
        .statute-list {{
            list-style: none;
            padding: 0;
        }}
        .statute-list li {{
            padding: 8px 0;
            border-bottom: 1px solid #f0f0f0;
        }}
        .statute-list a {{
            color: var(--link-color);
            text-decoration: none;
        }}
        .statute-list a:hover {{
            text-decoration: underline;
        }}
        .statute-year {{
            color: #718096;
            font-size: 0.9rem;
            margin-left: 10px;
        }}
    </style>
</head>
<body>
    <h1>Legislation Index</h1>
    <div class="stats">
        <div class="stat-item">
            <div class="stat-value">{total_statutes}</div>
            <div class="stat-label">Statutes</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{total_sections}</div>
            <div class="stat-label">Sections</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{letters_covered}</div>
            <div class="stat-label">Letters (A-Z)</div>
        </div>
    </div>
    
{letter_sections}
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #718096; font-size: 0.85rem;">
        <p>Qanoon Legal Research Platform | Generated: {generated_date}</p>
    </footer>
</body>
</html>
'''


def clean_html_content(raw_html: str) -> str:
    """Clean up the raw HTML content from PLS."""
    import codecs
    
    if not raw_html:
        return ""
    
    # Remove the outer quotes if present
    if raw_html.startswith('"') and raw_html.endswith('"'):
        raw_html = raw_html[1:-1]
    
    # Try to decode unicode escapes (handles \u003c -> <)
    try:
        raw_html = codecs.decode(raw_html, 'unicode_escape')
    except:
        pass
    
    # Also handle literal backslash escapes
    raw_html = raw_html.replace('\\r\\n', '\n')
    raw_html = raw_html.replace('\\n', '\n')
    raw_html = raw_html.replace('\\t', '\t')
    raw_html = raw_html.replace('\\"', '"')
    raw_html = raw_html.replace('\\u003c', '<')
    raw_html = raw_html.replace('\\u003e', '>')
    raw_html = raw_html.replace('\\u0026', '&')
    
    # Extract just the body content
    body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    if body_match:
        content = body_match.group(1)
    else:
        content = raw_html
    
    # Remove div wrappers but keep content
    content = re.sub(r'<div[^>]*>', '', content)
    content = re.sub(r'</div>', '', content)
    
    # Clean up MS Word cruft
    content = re.sub(r'<!\[if[^\]]*\]>', '', content)
    content = re.sub(r'<!\[endif\]>', '', content)
    content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
    content = re.sub(r'<o:p>.*?</o:p>', '', content)
    content = re.sub(r'mso-[^;"]+;?', '', content)
    content = re.sub(r'style=\'[^\']*\'', '', content)
    content = re.sub(r'style="[^"]*"', '', content)
    content = re.sub(r'class="[^"]*"', '', content)
    content = re.sub(r'class=\'[^\']*\'', '', content)
    
    return content.strip()


def get_case_link_path(citation: str) -> Optional[str]:
    """Get the path to a case file if it exists."""
    if not citation:
        return None
    
    # Parse citation: "1986 PLD 29" -> 1986, PLD, 29
    # Also handle "1986  PLD  29" (multiple spaces)
    match = re.match(r'(\d{4})\s+([A-Z]+)\s+(\d+)', citation.strip())
    if not match:
        return None
    
    year, reporter, page = match.groups()
    
    # Check if HTML file exists (primary) or JSON file exists (fallback)
    html_file = DATA_DIR / "html" / reporter / year / f"{year}_{reporter}_{page}.html"
    json_file = DATA_DIR / reporter / year / f"{year}_{reporter}_{page}.json"
    
    if html_file.exists():
        # Return relative path from legislation/html/X/ to case html
        return f"../../../html/{reporter}/{year}/{year}_{reporter}_{page}.html"
    elif json_file.exists():
        # JSON exists, HTML will be generated - return the expected path
        return f"../../../html/{reporter}/{year}/{year}_{reporter}_{page}.html"
    return None


def generate_statute_html(json_path: Path) -> Optional[Path]:
    """Generate HTML for a single statute."""
    try:
        data = json.loads(json_path.read_text(encoding='utf-8'))
        
        title = data.get('title', json_path.stem.replace('_', ' '))
        year = data.get('enactment_date', 'N/A')
        status = data.get('status', 'in_force').replace('_', ' ').title()
        sections = data.get('sections', [])
        letter = data.get('alphabet', json_path.parent.name)
        
        # Build TOC
        toc_items = []
        for section in sections:
            sec_num = section.get('number', '?')
            sec_title = section.get('title', '')
            sec_id = f"section-{sec_num}".replace(' ', '-').lower()
            toc_items.append(f'            <li><a href="#{sec_id}">Section {sec_num}: {html.escape(sec_title)}</a></li>')
        
        # Build sections
        sections_html = []
        for section in sections:
            sec_num = section.get('number', '?')
            sec_title = section.get('title', '')
            sec_id = f"section-{sec_num}".replace(' ', '-').lower()
            
            # Clean content
            raw_content = section.get('text', '')
            content = clean_html_content(raw_content)
            
            # Build case links
            case_links_html = ''
            case_links = section.get('case_links', [])
            if case_links:
                links = []
                for link in case_links:
                    citation = link.get('citation', '')
                    if citation:
                        path = get_case_link_path(citation)
                        if path:
                            links.append(f'<a href="{path}" class="case-link">{html.escape(citation)}</a>')
                        else:
                            links.append(f'<span class="case-link unavailable" title="Case not yet scraped">{html.escape(citation)}</span>')
                
                if links:
                    case_links_html = f'''
                <div class="case-links">
                    <div class="case-links-title">Related Cases:</div>
                    {''.join(links)}
                </div>'''
            
            sections_html.append(f'''
        <div class="section" id="{sec_id}">
            <div class="section-header">
                <span class="section-number">Section {html.escape(str(sec_num))}</span>
                <span class="section-title">{html.escape(sec_title)}</span>
            </div>
            <div class="section-content">
                {content}
            </div>{case_links_html}
        </div>''')
        
        # Generate HTML
        html_content = HTML_TEMPLATE.format(
            title=html.escape(title),
            year=html.escape(str(year)),
            status=html.escape(status),
            section_count=len(sections),
            toc_items='\n'.join(toc_items),
            sections_html='\n'.join(sections_html),
            generated_date=datetime.now().strftime('%Y-%m-%d %H:%M')
        )
        
        # Save
        output_dir = OUTPUT_DIR / letter
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{json_path.stem}.html"
        output_file.write_text(html_content, encoding='utf-8')
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error generating HTML for {json_path}: {e}")
        return None


def generate_index() -> Path:
    """Generate the main legislation index."""
    letter_sections = []
    total_statutes = 0
    total_sections = 0
    letters = []
    
    for letter_dir in sorted(OUTPUT_DIR.iterdir()):
        if not letter_dir.is_dir():
            continue
        
        letter = letter_dir.name
        if not letter.isalpha() or len(letter) != 1:
            continue
        
        letters.append(letter)
        html_files = list(letter_dir.glob('*.html'))
        
        if not html_files:
            continue
        
        statutes = []
        for html_file in sorted(html_files):
            # Try to get title from corresponding JSON
            json_file = LEGISLATION_DIR / letter / f"{html_file.stem}.json"
            if json_file.exists():
                try:
                    data = json.loads(json_file.read_text(encoding='utf-8'))
                    title = data.get('title', html_file.stem.replace('_', ' '))
                    year = data.get('enactment_date', '')
                    section_count = len(data.get('sections', []))
                    total_sections += section_count
                except:
                    title = html_file.stem.replace('_', ' ')
                    year = ''
            else:
                title = html_file.stem.replace('_', ' ')
                year = ''
            
            total_statutes += 1
            year_span = f'<span class="statute-year">({year})</span>' if year else ''
            statutes.append(f'            <li><a href="{letter}/{html_file.name}">{html.escape(title)}</a>{year_span}</li>')
        
        letter_sections.append(f'''
    <div class="letter-section">
        <h2>{letter} ({len(html_files)} statutes)</h2>
        <ul class="statute-list">
{chr(10).join(statutes)}
        </ul>
    </div>''')
    
    index_html = INDEX_TEMPLATE.format(
        total_statutes=total_statutes,
        total_sections=total_sections,
        letters_covered=len(letters),
        letter_sections='\n'.join(letter_sections),
        generated_date=datetime.now().strftime('%Y-%m-%d %H:%M')
    )
    
    index_file = OUTPUT_DIR / 'index.html'
    index_file.write_text(index_html, encoding='utf-8')
    return index_file


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generate legislation HTML')
    parser.add_argument('--letter', '-l', help='Only generate for this letter')
    parser.add_argument('--statute', '-s', help='Only generate for statute containing this name')
    args = parser.parse_args()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    generated = 0
    failed = 0
    
    for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
        if not letter_dir.is_dir() or letter_dir.name == 'html':
            continue
        
        letter = letter_dir.name
        
        if args.letter and letter.upper() != args.letter.upper():
            continue
        
        logger.info(f"Processing letter {letter}...")
        
        for json_file in sorted(letter_dir.glob('*.json')):
            if args.statute and args.statute.lower() not in json_file.stem.lower():
                continue
            
            result = generate_statute_html(json_file)
            if result:
                generated += 1
                logger.info(f"  Generated: {result.name}")
            else:
                failed += 1
    
    # Generate index
    if not args.statute:
        index = generate_index()
        logger.info(f"Generated index: {index}")
    
    print(f"\n{'='*50}")
    print(f"HTML GENERATION COMPLETE")
    print(f"{'='*50}")
    print(f"Generated: {generated}")
    print(f"Failed: {failed}")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()
