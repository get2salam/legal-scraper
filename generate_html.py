#!/usr/bin/env python3
"""
HTML Generator for Qanoon Legal Research Platform
===================================================
Generates beautiful standalone HTML files from case JSON data.
"""

import json
import re
import html
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2"
HTML_DIR = DATA_DIR / "html"

# Professional legal document CSS styling
CSS_STYLES = """
:root {
    --primary-color: #1a365d;
    --secondary-color: #2c5282;
    --accent-color: #c53030;
    --text-color: #1a202c;
    --light-text: #4a5568;
    --border-color: #e2e8f0;
    --bg-light: #f7fafc;
    --bg-accent: #ebf4ff;
}

* {
    box-sizing: border-box;
}

html {
    font-size: 16px;
}

body {
    font-family: 'Georgia', 'Times New Roman', serif;
    line-height: 1.75;
    color: var(--text-color);
    max-width: 900px;
    margin: 0 auto;
    padding: 40px 20px;
    background: #fff;
}

/* Header Section */
.case-header {
    border-bottom: 3px solid var(--primary-color);
    padding-bottom: 25px;
    margin-bottom: 30px;
}

.citation {
    font-size: 1.5rem;
    font-weight: bold;
    color: var(--primary-color);
    margin: 0 0 10px 0;
    letter-spacing: 0.5px;
}

.case-title {
    font-size: 1.2rem;
    font-style: italic;
    color: var(--secondary-color);
    margin: 0 0 20px 0;
}

.meta-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 15px;
    background: var(--bg-light);
    padding: 20px;
    border-radius: 4px;
    border-left: 4px solid var(--primary-color);
}

.meta-item {
    margin: 0;
}

.meta-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--light-text);
    font-family: 'Helvetica Neue', Arial, sans-serif;
    margin-bottom: 4px;
}

.meta-value {
    font-size: 1rem;
    color: var(--text-color);
}

/* Headnotes Section */
.headnotes-section {
    background: var(--bg-accent);
    border: 1px solid var(--border-color);
    border-radius: 4px;
    padding: 25px;
    margin: 30px 0;
}

.section-title {
    font-family: 'Helvetica Neue', Arial, sans-serif;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--secondary-color);
    margin: 0 0 15px 0;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border-color);
}

.headnotes-content {
    font-size: 0.95rem;
    line-height: 1.8;
    white-space: pre-wrap;
}

/* Judgment Section */
.judgment-section {
    margin: 40px 0;
}

.judgment-content {
    text-align: justify;
    hyphens: auto;
}

.judgment-content p {
    margin: 1em 0;
    text-indent: 2em;
}

.judgment-content p:first-child {
    text-indent: 0;
}

/* Tables in judgment */
.judgment-content table {
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
    font-size: 0.9rem;
}

.judgment-content th,
.judgment-content td {
    border: 1px solid var(--border-color);
    padding: 10px 12px;
    text-align: left;
}

.judgment-content th {
    background: var(--bg-light);
    font-weight: bold;
}

/* Cited Section */
.cited-section {
    background: var(--bg-light);
    border: 1px solid var(--border-color);
    border-radius: 4px;
    padding: 25px;
    margin: 40px 0;
}

.cited-list {
    list-style: none;
    padding: 0;
    margin: 0;
}

.cited-list li {
    padding: 8px 0;
    border-bottom: 1px dotted var(--border-color);
    font-size: 0.95rem;
}

.cited-list li:last-child {
    border-bottom: none;
}

.cited-subsection {
    margin-top: 20px;
}

.cited-subsection:first-child {
    margin-top: 0;
}

.cited-subtitle {
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--light-text);
    margin-bottom: 10px;
    font-family: 'Helvetica Neue', Arial, sans-serif;
}

/* Footer */
.case-footer {
    margin-top: 50px;
    padding-top: 20px;
    border-top: 1px solid var(--border-color);
    text-align: center;
    color: var(--light-text);
    font-size: 0.85rem;
    font-family: 'Helvetica Neue', Arial, sans-serif;
}

.qanoon-branding {
    color: var(--primary-color);
    font-weight: bold;
}

/* Print Styles */
@media print {
    body {
        max-width: none;
        padding: 0;
        font-size: 11pt;
        line-height: 1.5;
    }
    
    .case-header {
        page-break-after: avoid;
    }
    
    .headnotes-section,
    .cited-section {
        page-break-inside: avoid;
        background: none;
        border: 1px solid #000;
    }
    
    .meta-grid {
        background: none;
        border: 1px solid #ccc;
    }
    
    .judgment-content {
        page-break-before: always;
    }
    
    @page {
        margin: 2cm;
    }
}

/* Responsive */
@media (max-width: 600px) {
    body {
        padding: 20px 15px;
    }
    
    .citation {
        font-size: 1.25rem;
    }
    
    .meta-grid {
        grid-template-columns: 1fr;
    }
}
"""


def sanitize_filename(citation: str) -> str:
    """Convert citation to safe filename."""
    # Replace spaces with underscores, remove special characters
    filename = citation.replace(" ", "_")
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    return filename + ".html"


def escape_html(text: str) -> str:
    """Safely escape HTML entities."""
    if not text:
        return ""
    return html.escape(str(text))


def format_judges(judges: List[str]) -> str:
    """Format judges list for display."""
    if not judges:
        return "—"
    if len(judges) == 1:
        return judges[0]
    if len(judges) == 2:
        return f"{judges[0]} and {judges[1]}"
    return ", ".join(judges[:-1]) + f", and {judges[-1]}"


def clean_judgment_html(judgment_html: str) -> str:
    """Clean and prepare judgment HTML for display."""
    if not judgment_html:
        return "<p><em>Judgment text not available.</em></p>"
    
    # Unescape unicode escapes if present
    try:
        if '\\u' in judgment_html:
            judgment_html = judgment_html.encode().decode('unicode_escape')
    except:
        pass
    
    # Unescape HTML entities
    judgment_html = html.unescape(judgment_html)
    
    # Remove MS Office artifacts
    judgment_html = re.sub(r'<!\[if[^\]]*\]>.*?<!\[endif\]>', '', judgment_html, flags=re.DOTALL)
    judgment_html = re.sub(r'<!--\[if.*?\]>.*?<!\[endif\]-->', '', judgment_html, flags=re.DOTALL)
    judgment_html = re.sub(r'<o:p>.*?</o:p>', '', judgment_html, flags=re.DOTALL)
    judgment_html = re.sub(r'</?o:[^>]+>', '', judgment_html)
    
    # Remove inline styles that might conflict
    judgment_html = re.sub(r'style="[^"]*mso-[^"]*"', '', judgment_html)
    
    # Clean up empty paragraphs
    judgment_html = re.sub(r'<p[^>]*>\s*(&nbsp;)?\s*</p>', '', judgment_html)
    
    return judgment_html


def format_headnotes(headnotes: str) -> str:
    """Format headnotes for display."""
    if not headnotes:
        return ""
    
    # Escape HTML but preserve line breaks
    escaped = escape_html(headnotes)
    
    # Convert line breaks to <br> for HTML display
    escaped = escaped.replace('\r\n', '\n').replace('\r', '\n')
    
    return escaped


def generate_case_html(case: Dict) -> str:
    """Generate complete HTML for a single case."""
    
    citation = case.get('citation', 'Unknown Citation')
    title = case.get('title', case.get('case_name', ''))
    court = case.get('court', '')
    date = case.get('date', '')
    judges = case.get('judges', [])
    headnotes = case.get('headnotes', '')
    judgment_html = case.get('judgment_html', case.get('judgment_clean', ''))
    statutes_cited = case.get('statutes_cited', [])
    cases_cited = case.get('cases_cited', [])
    
    # Build the HTML
    html_parts = [f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Legal case: {escape_html(citation)} - {escape_html(title)}">
    <meta name="keywords" content="Pakistan law, legal case, {escape_html(citation)}, {escape_html(court)}">
    <title>{escape_html(citation)} | Qanoon Legal Research</title>
    <style>
{CSS_STYLES}
    </style>
</head>
<body>
    <article class="case-document">
        <header class="case-header">
            <h1 class="citation">{escape_html(citation)}</h1>''']
    
    if title:
        html_parts.append(f'''
            <p class="case-title">{escape_html(title)}</p>''')
    
    html_parts.append('''
            <div class="meta-grid">''')
    
    if court:
        html_parts.append(f'''
                <div class="meta-item">
                    <div class="meta-label">Court</div>
                    <div class="meta-value">{escape_html(court)}</div>
                </div>''')
    
    if date:
        html_parts.append(f'''
                <div class="meta-item">
                    <div class="meta-label">Date</div>
                    <div class="meta-value">{escape_html(date)}</div>
                </div>''')
    
    if judges:
        html_parts.append(f'''
                <div class="meta-item">
                    <div class="meta-label">Judge{"s" if len(judges) > 1 else ""}</div>
                    <div class="meta-value">{escape_html(format_judges(judges))}</div>
                </div>''')
    
    html_parts.append('''
            </div>
        </header>''')
    
    # Headnotes section
    if headnotes and headnotes.strip():
        formatted_headnotes = format_headnotes(headnotes)
        html_parts.append(f'''
        
        <section class="headnotes-section">
            <h2 class="section-title">Headnotes</h2>
            <div class="headnotes-content">{formatted_headnotes}</div>
        </section>''')
    
    # Judgment section
    cleaned_judgment = clean_judgment_html(judgment_html)
    html_parts.append(f'''
        
        <section class="judgment-section">
            <h2 class="section-title">Judgment</h2>
            <div class="judgment-content">
                {cleaned_judgment}
            </div>
        </section>''')
    
    # Cited references section
    if statutes_cited or cases_cited:
        html_parts.append('''
        
        <section class="cited-section">
            <h2 class="section-title">Citations</h2>''')
        
        if statutes_cited:
            html_parts.append('''
            <div class="cited-subsection">
                <div class="cited-subtitle">Statutes Cited</div>
                <ul class="cited-list">''')
            for statute in statutes_cited:
                html_parts.append(f'''
                    <li>{escape_html(statute)}</li>''')
            html_parts.append('''
                </ul>
            </div>''')
        
        if cases_cited:
            html_parts.append('''
            <div class="cited-subsection">
                <div class="cited-subtitle">Cases Cited</div>
                <ul class="cited-list">''')
            for case_ref in cases_cited:
                html_parts.append(f'''
                    <li>{escape_html(case_ref)}</li>''')
            html_parts.append('''
                </ul>
            </div>''')
        
        html_parts.append('''
        </section>''')
    
    # Footer
    generated_date = datetime.now().strftime("%B %d, %Y")
    html_parts.append(f'''
        
        <footer class="case-footer">
            <p>Generated on {generated_date} by <span class="qanoon-branding">Qanoon Legal Research Platform</span></p>
            <p>This document is for informational purposes. Always verify with official sources.</p>
        </footer>
    </article>
</body>
</html>''')
    
    return ''.join(html_parts)


def generate_html_for_case(case: Dict, output_dir: Path) -> Optional[Path]:
    """Generate HTML file for a single case."""
    citation = case.get('citation', '')
    if not citation:
        return None
    
    # Determine output path based on citation
    # e.g., "2024 SCMR 1" -> html/SCMR/2024/2024_SCMR_1.html
    parts = citation.split()
    if len(parts) >= 2:
        year = parts[0]
        reporter = parts[1]
    else:
        year = "unknown"
        reporter = "unknown"
    
    output_path = output_dir / reporter / year
    output_path.mkdir(parents=True, exist_ok=True)
    
    filename = sanitize_filename(citation)
    filepath = output_path / filename
    
    try:
        html_content = generate_case_html(case)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return filepath
    except Exception as e:
        logger.error(f"Error generating HTML for {citation}: {e}")
        return None


def generate_all_html(data_dir: Path = DATA_DIR, output_dir: Path = HTML_DIR):
    """Generate HTML files for all cases in data directory."""
    logger.info(f"Scanning for JSON files in {data_dir}")
    
    # Find all JSON files (excluding progress.json and files in html directory)
    json_files = []
    for f in data_dir.rglob("*.json"):
        if f.name != 'progress.json' and 'html' not in f.parts:
            json_files.append(f)
    
    logger.info(f"Found {len(json_files)} case files")
    
    success = 0
    failed = 0
    
    for i, filepath in enumerate(json_files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                case = json.load(f)
            
            result = generate_html_for_case(case, output_dir)
            if result:
                success += 1
            else:
                failed += 1
            
            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{len(json_files)} ({success} generated, {failed} failed)")
        
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            failed += 1
    
    logger.info(f"Complete: {success} HTML files generated, {failed} failed")
    return success, failed


def generate_index(output_dir: Path = HTML_DIR):
    """Generate index.html listing all cases by reporter/year."""
    logger.info("Generating index.html...")
    
    # Collect all cases by reporter and year
    cases_by_reporter: Dict[str, Dict[str, List[Dict]]] = {}
    
    for html_file in output_dir.rglob("*.html"):
        if html_file.name == 'index.html':
            continue
        
        # Extract info from path: html/REPORTER/YEAR/filename.html
        parts = html_file.relative_to(output_dir).parts
        if len(parts) >= 3:
            reporter = parts[0]
            year = parts[1]
            filename = parts[2]
            
            # Extract citation from filename
            citation = filename.replace('.html', '').replace('_', ' ')
            
            if reporter not in cases_by_reporter:
                cases_by_reporter[reporter] = {}
            if year not in cases_by_reporter[reporter]:
                cases_by_reporter[reporter][year] = []
            
            cases_by_reporter[reporter][year].append({
                'citation': citation,
                'path': str(html_file.relative_to(output_dir)),
                'filename': filename
            })
    
    # Sort reporters and years
    sorted_reporters = sorted(cases_by_reporter.keys())
    
    # Generate index HTML
    index_html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Case Index | Qanoon Legal Research Platform</title>
    <style>
        :root {
            --primary-color: #1a365d;
            --secondary-color: #2c5282;
            --text-color: #1a202c;
            --light-text: #4a5568;
            --border-color: #e2e8f0;
            --bg-light: #f7fafc;
        }
        
        * { box-sizing: border-box; }
        
        body {
            font-family: 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: var(--text-color);
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 20px;
            background: #fff;
        }
        
        h1 {
            color: var(--primary-color);
            font-size: 2rem;
            margin-bottom: 10px;
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 15px;
        }
        
        .subtitle {
            color: var(--light-text);
            margin-bottom: 30px;
        }
        
        .stats {
            background: var(--bg-light);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 40px;
            display: flex;
            gap: 30px;
            flex-wrap: wrap;
        }
        
        .stat-item {
            text-align: center;
        }
        
        .stat-number {
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary-color);
        }
        
        .stat-label {
            font-size: 0.85rem;
            color: var(--light-text);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .reporter-section {
            margin-bottom: 40px;
        }
        
        .reporter-header {
            background: var(--primary-color);
            color: white;
            padding: 15px 20px;
            border-radius: 8px 8px 0 0;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .reporter-header:hover {
            background: var(--secondary-color);
        }
        
        .reporter-name {
            font-size: 1.25rem;
            font-weight: bold;
            margin: 0;
        }
        
        .reporter-count {
            background: rgba(255,255,255,0.2);
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.9rem;
        }
        
        .reporter-content {
            border: 1px solid var(--border-color);
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 20px;
        }
        
        .year-section {
            margin-bottom: 20px;
        }
        
        .year-header {
            font-size: 1.1rem;
            font-weight: bold;
            color: var(--secondary-color);
            margin-bottom: 10px;
            padding-bottom: 5px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .case-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 10px;
        }
        
        .case-link {
            display: block;
            padding: 10px 15px;
            background: var(--bg-light);
            border-radius: 4px;
            text-decoration: none;
            color: var(--text-color);
            transition: all 0.2s;
            font-size: 0.95rem;
        }
        
        .case-link:hover {
            background: var(--primary-color);
            color: white;
        }
        
        .footer {
            margin-top: 60px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
            text-align: center;
            color: var(--light-text);
            font-size: 0.9rem;
        }
        
        @media (max-width: 600px) {
            .stats { flex-direction: column; gap: 15px; }
            .case-list { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <h1>Qanoon Legal Research Platform</h1>
    <p class="subtitle">Pakistan Case Law Database</p>
    
    <div class="stats">
'''
    
    # Calculate stats
    total_cases = sum(
        len(cases) 
        for years in cases_by_reporter.values() 
        for cases in years.values()
    )
    total_reporters = len(cases_by_reporter)
    
    index_html += f'''
        <div class="stat-item">
            <div class="stat-number">{total_cases:,}</div>
            <div class="stat-label">Total Cases</div>
        </div>
        <div class="stat-item">
            <div class="stat-number">{total_reporters}</div>
            <div class="stat-label">Reporters</div>
        </div>
    </div>
'''
    
    # Generate reporter sections
    for reporter in sorted_reporters:
        years = cases_by_reporter[reporter]
        reporter_case_count = sum(len(cases) for cases in years.values())
        
        index_html += f'''
    <div class="reporter-section">
        <div class="reporter-header">
            <h2 class="reporter-name">{reporter}</h2>
            <span class="reporter-count">{reporter_case_count} cases</span>
        </div>
        <div class="reporter-content">
'''
        
        for year in sorted(years.keys(), reverse=True):
            cases = sorted(years[year], key=lambda x: x['citation'])
            
            index_html += f'''
            <div class="year-section">
                <h3 class="year-header">{year} ({len(cases)} cases)</h3>
                <div class="case-list">
'''
            
            for case in cases:
                index_html += f'''
                    <a href="{case['path']}" class="case-link">{case['citation']}</a>
'''
            
            index_html += '''
                </div>
            </div>
'''
        
        index_html += '''
        </div>
    </div>
'''
    
    generated_date = datetime.now().strftime("%B %d, %Y at %H:%M")
    index_html += f'''
    <footer class="footer">
        <p>Generated on {generated_date}</p>
        <p>© Qanoon Legal Research Platform</p>
    </footer>
</body>
</html>
'''
    
    index_path = output_dir / 'index.html'
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(index_html)
    
    logger.info(f"Index generated: {index_path}")
    return index_path


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate HTML files for case data")
    parser.add_argument("command", choices=["generate", "generate-one", "index", "all"],
                        help="Command to run")
    parser.add_argument("--file", "-f", help="Single JSON file to process")
    parser.add_argument("--output", "-o", help="Output directory", default=str(HTML_DIR))
    
    args = parser.parse_args()
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.command == "generate":
        generate_all_html(DATA_DIR, output_dir)
    
    elif args.command == "generate-one":
        if not args.file:
            print("Please specify --file")
            return
        with open(args.file, 'r', encoding='utf-8') as f:
            case = json.load(f)
        result = generate_html_for_case(case, output_dir)
        if result:
            print(f"Generated: {result}")
        else:
            print("Failed to generate HTML")
    
    elif args.command == "index":
        generate_index(output_dir)
    
    elif args.command == "all":
        generate_all_html(DATA_DIR, output_dir)
        generate_index(output_dir)
        print("\nAll HTML files and index generated successfully!")


if __name__ == "__main__":
    main()
