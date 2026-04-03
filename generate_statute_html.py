#!/usr/bin/env python3
"""
Statute HTML Generator
======================
Generates PLS-style HTML pages for statutes with interactive CaseLaw buttons.
Each section shows the text with a "CaseLaw" button that reveals linked cases.
"""

import json
import html
import re
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("data_v2")
LEGISLATION_DIR = DATA_DIR / "legislation"
HTML_OUTPUT_DIR = DATA_DIR / "html" / "statutes"

# HTML Template
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #006400, #228B22);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 1.8em;
        }}
        .header .meta {{
            opacity: 0.9;
            font-size: 0.95em;
        }}
        .section {{
            background: white;
            border-radius: 8px;
            margin-bottom: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            overflow: hidden;
        }}
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #e9ecef;
        }}
        .section-title {{
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        .section-number {{
            background: #006400;
            color: white;
            padding: 5px 12px;
            border-radius: 4px;
            font-weight: bold;
            min-width: 60px;
            text-align: center;
        }}
        .section-name {{
            font-weight: 600;
            color: #333;
        }}
        .btn-caselaw {{
            background: #28a745;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 0.9em;
            display: flex;
            align-items: center;
            gap: 5px;
            transition: background 0.2s;
        }}
        .btn-caselaw:hover {{
            background: #218838;
        }}
        .btn-caselaw.no-cases {{
            background: #6c757d;
            cursor: default;
        }}
        .btn-caselaw .count {{
            background: rgba(255,255,255,0.2);
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.85em;
        }}
        .section-content {{
            padding: 20px;
            color: #444;
        }}
        .section-content p {{
            margin: 0 0 15px 0;
        }}
        .case-links {{
            display: none;
            background: #f0fff0;
            border-top: 2px solid #28a745;
            padding: 20px;
        }}
        .case-links.show {{
            display: block;
        }}
        .case-links h4 {{
            margin: 0 0 15px 0;
            color: #006400;
        }}
        .case-card {{
            background: white;
            border: 1px solid #dee2e6;
            border-radius: 6px;
            padding: 15px;
            margin-bottom: 10px;
            transition: box-shadow 0.2s;
        }}
        .case-card:hover {{
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .case-citation {{
            font-weight: bold;
            color: #006400;
            font-size: 1.1em;
            margin-bottom: 5px;
        }}
        .case-citation a {{
            color: #006400;
            text-decoration: none;
        }}
        .case-citation a:hover {{
            text-decoration: underline;
        }}
        .case-meta {{
            color: #666;
            font-size: 0.9em;
        }}
        .back-link {{
            display: inline-block;
            margin-bottom: 20px;
            color: #006400;
            text-decoration: none;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
        .stats {{
            display: flex;
            gap: 20px;
            margin-top: 10px;
        }}
        .stat {{
            background: rgba(255,255,255,0.2);
            padding: 5px 15px;
            border-radius: 20px;
        }}
        .toc {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
        }}
        .toc h3 {{
            margin-top: 0;
        }}
        .toc-list {{
            list-style: none;
            padding: 0;
            columns: 2;
            column-gap: 40px;
        }}
        .toc-list li {{
            margin-bottom: 8px;
        }}
        .toc-list a {{
            color: #006400;
            text-decoration: none;
        }}
        .toc-list a:hover {{
            text-decoration: underline;
        }}
        @media (max-width: 768px) {{
            .section-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 10px;
            }}
            .toc-list {{
                columns: 1;
            }}
        }}
    </style>
</head>
<body>
    <a href="../index.html" class="back-link">← Back to Statutes</a>
    
    <div class="header">
        <h1>{title}</h1>
        <div class="meta">
            {jurisdiction_badge}
            {year_badge}
        </div>
        <div class="stats">
            <span class="stat">📄 {section_count} Sections</span>
            <span class="stat">⚖️ {case_link_count} Case Citations</span>
        </div>
    </div>
    
    <div class="toc">
        <h3>Table of Contents</h3>
        <ul class="toc-list">
            {toc_items}
        </ul>
    </div>
    
    {sections_html}
    
    <script>
        function toggleCases(btn, sectionId) {{
            const casesDiv = document.getElementById('cases-' + sectionId);
            if (casesDiv) {{
                casesDiv.classList.toggle('show');
                if (casesDiv.classList.contains('show')) {{
                    btn.innerHTML = btn.innerHTML.replace('Show', 'Hide');
                }} else {{
                    btn.innerHTML = btn.innerHTML.replace('Hide', 'Show');
                }}
            }}
        }}
    </script>
</body>
</html>
'''

SECTION_TEMPLATE = '''
<div class="section" id="section-{section_id}">
    <div class="section-header">
        <div class="section-title">
            <span class="section-number">{section_number}</span>
            <span class="section-name">{section_name}</span>
        </div>
        {caselaw_button}
    </div>
    <div class="section-content">
        {content}
    </div>
    {case_links_html}
</div>
'''


def safe_id(text: str) -> str:
    """Convert text to safe HTML ID."""
    return re.sub(r'[^a-zA-Z0-9]', '_', str(text))


def generate_statute_html(statute_data: Dict) -> str:
    """Generate HTML page for a statute."""
    title = statute_data.get("title", "Unknown Statute")
    sections = statute_data.get("sections", [])
    case_links = statute_data.get("case_links", [])
    
    # Group case links by section
    cases_by_section = {}
    for cl in case_links:
        section = cl.get("section", "")
        if section not in cases_by_section:
            cases_by_section[section] = []
        cases_by_section[section].append(cl)
    
    # Generate TOC
    toc_items = []
    for sec in sections:
        sec_num = sec.get("number", "")
        sec_name = sec.get("title", sec.get("name", ""))
        sec_id = safe_id(sec_num or sec_name)
        toc_items.append(f'<li><a href="#section-{sec_id}">{sec_num}. {sec_name}</a></li>')
    
    # Generate sections HTML
    sections_html = []
    for sec in sections:
        sec_num = sec.get("number", "")
        sec_name = sec.get("title", sec.get("name", ""))
        sec_id = safe_id(sec_num or sec_name)
        content = sec.get("text", sec.get("content", "<p>Content not available</p>"))
        
        # Decode content - it may be double-escaped JSON string
        if content.startswith('"') and content.endswith('"'):
            # Remove outer quotes and decode unicode escapes
            try:
                import codecs
                content = codecs.decode(content[1:-1], 'unicode_escape')
            except:
                content = content[1:-1]
        
        # Extract just the body content from full HTML
        if '<body' in content.lower():
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                body = soup.find('body')
                if body:
                    # Remove MSO-specific classes and clean up
                    for tag in body.find_all(True):
                        # Remove Word-specific attributes
                        for attr in ['class', 'style', 'lang']:
                            if attr in tag.attrs:
                                del tag.attrs[attr]
                    content = str(body.decode_contents())
            except:
                pass
        
        # Clean content - if still doesn't look like HTML, escape it
        if not content.strip().startswith("<"):
            content = f"<p>{html.escape(content)}</p>"
        
        # Get case links for this section
        section_cases = cases_by_section.get(sec_num, [])
        case_count = len(section_cases)
        
        # Generate CaseLaw button
        if case_count > 0:
            caselaw_button = f'''
                <button class="btn-caselaw" onclick="toggleCases(this, '{sec_id}')">
                    ⚖️ Show CaseLaw <span class="count">{case_count}</span>
                </button>
            '''
            
            # Generate case links HTML
            case_cards = []
            for cl in section_cases:
                citation = cl.get("citation", "")
                year = cl.get("year", "")
                reporter = cl.get("reporter", "")
                page = cl.get("page", "")
                
                # Link to case file if exists
                case_file = f"../../{reporter}/{year}/{year}_{reporter}_{page}.html"
                
                case_cards.append(f'''
                    <div class="case-card">
                        <div class="case-citation">
                            <a href="{case_file}">{citation}</a>
                        </div>
                        <div class="case-meta">{reporter} • {year}</div>
                    </div>
                ''')
            
            case_links_html = f'''
                <div class="case-links" id="cases-{sec_id}">
                    <h4>📚 Cases citing this section ({case_count})</h4>
                    {''.join(case_cards)}
                </div>
            '''
        else:
            caselaw_button = '<button class="btn-caselaw no-cases">No CaseLaw</button>'
            case_links_html = ''
        
        sections_html.append(SECTION_TEMPLATE.format(
            section_id=sec_id,
            section_number=sec_num,
            section_name=sec_name,
            content=content,
            caselaw_button=caselaw_button,
            case_links_html=case_links_html
        ))
    
    # Metadata
    jurisdiction = statute_data.get("jurisdiction", "Federal")
    year = statute_data.get("enactment_date", "")
    
    jurisdiction_badge = f'<span class="badge">📍 {jurisdiction}</span>' if jurisdiction else ''
    year_badge = f'<span class="badge">📅 {year}</span>' if year else ''
    
    return HTML_TEMPLATE.format(
        title=html.escape(title),
        jurisdiction_badge=jurisdiction_badge,
        year_badge=year_badge,
        section_count=len(sections),
        case_link_count=len(case_links),
        toc_items='\n'.join(toc_items),
        sections_html='\n'.join(sections_html)
    )


def generate_index_html(statutes: List[Dict]) -> str:
    """Generate index page listing all statutes."""
    rows = []
    for stat in sorted(statutes, key=lambda x: x.get("title", "")):
        title = stat.get("title", "")
        filename = stat.get("filename", "")
        sections = stat.get("section_count", 0)
        cases = stat.get("case_count", 0)
        
        rows.append(f'''
            <tr>
                <td><a href="{filename}">{html.escape(title)}</a></td>
                <td>{sections}</td>
                <td>{cases}</td>
            </tr>
        ''')
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pakistani Statutes</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        h1 {{
            color: #006400;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #006400;
            color: white;
        }}
        tr:hover {{
            background: #f0fff0;
        }}
        a {{
            color: #006400;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .search {{
            margin-bottom: 20px;
            padding: 10px;
            width: 100%;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
    </style>
</head>
<body>
    <h1>📚 Pakistani Statutes</h1>
    <input type="text" class="search" placeholder="Search statutes..." onkeyup="filterTable(this.value)">
    <table id="statutesTable">
        <thead>
            <tr>
                <th>Statute Name</th>
                <th>Sections</th>
                <th>Case Citations</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows)}
        </tbody>
    </table>
    <script>
        function filterTable(query) {{
            const rows = document.querySelectorAll('#statutesTable tbody tr');
            query = query.toLowerCase();
            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(query) ? '' : 'none';
            }});
        }}
    </script>
</body>
</html>
'''


def process_all_statutes():
    """Process all statute JSON files and generate HTML."""
    HTML_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    statutes_for_index = []
    total_processed = 0
    
    # Process each alphabet directory
    for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
        if not letter_dir.is_dir() or letter_dir.name in ['original', 'html']:
            continue
        
        letter = letter_dir.name
        output_dir = HTML_OUTPUT_DIR / letter
        output_dir.mkdir(exist_ok=True)
        
        logger.info(f"Processing letter {letter}...")
        
        for json_file in letter_dir.glob("*.json"):
            if json_file.name in ["progress.json", "legislation_index.json"]:
                continue
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    statute_data = json.load(f)
                
                # Generate HTML
                html_content = generate_statute_html(statute_data)
                
                # Save HTML
                html_filename = json_file.stem + ".html"
                html_path = output_dir / html_filename
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                # Track for index
                statutes_for_index.append({
                    "title": statute_data.get("title", json_file.stem),
                    "filename": f"{letter}/{html_filename}",
                    "section_count": len(statute_data.get("sections", [])),
                    "case_count": len(statute_data.get("case_links", []))
                })
                
                total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing {json_file.name}: {e}")
    
    # Generate index
    index_html = generate_index_html(statutes_for_index)
    with open(HTML_OUTPUT_DIR / "index.html", 'w', encoding='utf-8') as f:
        f.write(index_html)
    
    logger.info(f"Generated HTML for {total_processed} statutes")
    logger.info(f"Output directory: {HTML_OUTPUT_DIR}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate HTML pages for statutes")
    parser.add_argument("--statute", help="Process single statute file")
    args = parser.parse_args()
    
    if args.statute:
        with open(args.statute, 'r', encoding='utf-8') as f:
            data = json.load(f)
        html_content = generate_statute_html(data)
        print(html_content)
    else:
        process_all_statutes()


if __name__ == "__main__":
    main()
