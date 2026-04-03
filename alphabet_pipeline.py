#!/usr/bin/env python3
"""
Alphabet Pipeline - Complete workflow per letter
1. Scrape legislation for alphabet
2. Extract cited cases  
3. Scrape missing cases
4. Generate legislation HTML
5. Generate case HTML (fixed)
6. Verify all links work
7. Mark alphabet complete

Usage:
    python alphabet_pipeline.py A          # Run full pipeline for letter A
    python alphabet_pipeline.py A --step 3 # Run from step 3 onwards
    python alphabet_pipeline.py A --verify # Only verify links
"""

import argparse
import json
import os
import re
import sys
import time
import html
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Tuple

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from legislation_scraper import LegislationScraper
from pls_scraper_v2 import PLSScraperV2 as PLSScraper


class AlphabetPipeline:
    def __init__(self, letter: str, base_dir: str = "data_v2"):
        self.letter = letter.upper()
        self.base_dir = Path(base_dir)
        self.legislation_dir = self.base_dir / "legislation" / self.letter
        self.html_dir = self.base_dir / "legislation" / "html" / self.letter
        self.pipeline_dir = self.base_dir / "pipeline"
        self.pipeline_dir.mkdir(parents=True, exist_ok=True)
        
        # Status file for this alphabet
        self.status_file = self.pipeline_dir / f"alphabet_{self.letter}_status.json"
        
    def log(self, msg: str):
        """Log with timestamp"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{self.letter}] {msg}")
        
    def load_status(self) -> Dict:
        """Load pipeline status"""
        if self.status_file.exists():
            with open(self.status_file, 'r') as f:
                return json.load(f)
        return {
            "letter": self.letter,
            "steps_completed": [],
            "legislation_count": 0,
            "cases_needed": 0,
            "cases_scraped": 0,
            "links_verified": False,
            "last_updated": None
        }
        
    def save_status(self, status: Dict):
        """Save pipeline status"""
        status["last_updated"] = datetime.now().isoformat()
        with open(self.status_file, 'w') as f:
            json.dump(status, f, indent=2)
            
    # ==================== STEP 1: Scrape Legislation ====================
    
    def step1_scrape_legislation(self) -> int:
        """Scrape all legislation for this alphabet"""
        self.log("STEP 1: Scraping legislation...")
        
        scraper = LegislationScraper()
        
        # Get statutes for this letter
        statutes = scraper.get_statutes_by_letter(self.letter)
        self.log(f"Found {len(statutes)} statutes for letter {self.letter}")
        
        scraped = 0
        for i, statute in enumerate(statutes):
            title = statute.get('title', 'Unknown')
            self.log(f"  [{i+1}/{len(statutes)}] {title[:50]}...")
            
            try:
                result = scraper.scrape_statute(statute)
                if result:
                    scraped += 1
            except Exception as e:
                self.log(f"    Error: {e}")
                
            # Human-like delay
            time.sleep(2 + (i % 3))
            
        self.log(f"Scraped {scraped}/{len(statutes)} statutes")
        return scraped
        
    # ==================== STEP 2: Extract Cited Cases ====================
    
    def step2_extract_citations(self) -> List[Dict]:
        """Extract all case citations from legislation"""
        self.log("STEP 2: Extracting case citations...")
        
        citations = []
        
        if not self.legislation_dir.exists():
            self.log(f"No legislation directory: {self.legislation_dir}")
            return []
            
        json_files = list(self.legislation_dir.glob("*.json"))
        self.log(f"Scanning {len(json_files)} statute files...")
        
        seen = set()
        for jf in json_files:
            try:
                with open(jf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Get case_links from top level (support both formats)
                case_links = data.get('case_links', []) or data.get('cases_cited', [])
                
                for link in case_links:
                    year = link.get('year', '')
                    reporter = link.get('reporter', '')
                    page = link.get('page', '')
                    
                    if not year or not reporter or not page:
                        continue
                        
                    # Normalize
                    year = int(year) if isinstance(year, str) else year
                    page = int(page) if isinstance(page, str) else page
                    reporter = reporter.upper().strip()
                    
                    citation = f"{year} {reporter} {page}"
                    if citation not in seen:
                        seen.add(citation)
                        citations.append({
                            "year": year,
                            "reporter": reporter,
                            "page": page,
                            "citation": citation
                        })
            except Exception as e:
                self.log(f"  Error reading {jf.name}: {e}")
                
        self.log(f"Found {len(citations)} unique citations")
        
        # Save to pipeline directory
        output_file = self.pipeline_dir / f"citations_{self.letter}.json"
        with open(output_file, 'w') as f:
            json.dump(citations, f, indent=2)
            
        return citations
        
    # ==================== STEP 3: Find Missing Cases ====================
    
    def step3_find_missing_cases(self, citations: List[Dict]) -> List[Dict]:
        """Check which cited cases we don't have"""
        self.log("STEP 3: Finding missing cases...")
        
        missing = []
        for cite in citations:
            year = cite['year']
            reporter = cite['reporter']
            page = cite['page']
            
            # Check if we have this case
            case_path = self.base_dir / reporter / str(year) / f"{year}_{reporter}_{page}.json"
            html_path = self.base_dir / "html" / reporter / str(year) / f"{year}_{reporter}_{page}.html"
            
            if not case_path.exists():
                missing.append(cite)
                
        self.log(f"Missing {len(missing)} of {len(citations)} cited cases")
        
        # Save missing list
        output_file = self.pipeline_dir / f"missing_{self.letter}.json"
        with open(output_file, 'w') as f:
            json.dump(missing, f, indent=2)
            
        return missing
        
    # ==================== STEP 4: Scrape Missing Cases ====================
    
    def step4_scrape_missing_cases(self, missing: List[Dict]) -> int:
        """Scrape all missing cases"""
        self.log(f"STEP 4: Scraping {len(missing)} missing cases...")
        
        if not missing:
            self.log("No missing cases to scrape")
            return 0
            
        scraper = PLSScraper()
        scraped = 0
        
        for i, case in enumerate(missing):
            year = case['year']
            reporter = case['reporter']
            page = case['page']
            citation = case['citation']
            
            self.log(f"  [{i+1}/{len(missing)}] {citation}")
            
            try:
                # Build case_id (e.g., "1986K29" for "1986 PLD 29")
                # Format: YYYY + reporter_code + page
                reporter_codes = {
                    'PLD': 'K', 'SCMR': 'S', 'CLC': 'C', 'MLD': 'M',
                    'PCrLJ': 'P', 'PTD': 'D', 'PLC': 'L', 'YLR': 'Y',
                    'CLD': 'A', 'GBLR': 'G'
                }
                code = reporter_codes.get(reporter.upper(), 'X')
                case_id = f"{year}{code}{page}"
                
                result = scraper.fetch_case(case_id, citation)
                if result:
                    # Save the case in all formats (JSON, Original HTML, JSONL)
                    scraper._save_case(result)
                    scraped += 1
                    self.log(f"    [OK] Scraped & Saved")
                else:
                    self.log(f"    [MISS] Not found on PLS")
            except Exception as e:
                self.log(f"    [ERR] {e}")
                
            # Human-like delay
            time.sleep(3 + (i % 4))
            
        self.log(f"Scraped {scraped}/{len(missing)} cases")
        return scraped
        
    # ==================== STEP 5: Generate Legislation HTML ====================
    
    def step5_generate_legislation_html(self) -> int:
        """Generate HTML for all legislation"""
        self.log("STEP 5: Generating legislation HTML...")
        
        self.html_dir.mkdir(parents=True, exist_ok=True)
        
        json_files = list(self.legislation_dir.glob("*.json"))
        generated = 0
        
        for jf in json_files:
            try:
                with open(jf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                html_content = self._generate_statute_html(data)
                
                output_file = self.html_dir / f"{jf.stem}.html"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                    
                generated += 1
            except Exception as e:
                self.log(f"  Error generating HTML for {jf.name}: {e}")
                
        self.log(f"Generated {generated} legislation HTML files")
        
        # Generate index
        self._generate_legislation_index()
        
        return generated
        
    def _generate_statute_html(self, data: Dict) -> str:
        """Generate HTML for a single statute with clickable case links"""
        title = data.get('title', 'Unknown Statute')
        year = data.get('year', '')
        status = data.get('status', 'Unknown')
        sections = data.get('sections', [])
        
        # Citation pattern for linking
        citation_pattern = re.compile(
            r'(\d{4})\s+(PLD|SCMR|CLC|MLD|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d+)',
            re.IGNORECASE
        )
        
        def make_case_link(match):
            year = match.group(1)
            reporter = match.group(2).upper()
            page = match.group(3)
            citation = f"{year} {reporter} {page}"
            
            # Check if case exists
            case_html_path = self.base_dir / "html" / reporter / year / f"{year}_{reporter}_{page}.html"
            rel_path = f"../../../html/{reporter}/{year}/{year}_{reporter}_{page}.html"
            
            if case_html_path.exists():
                return f'<a href="{rel_path}" class="case-link">{citation}</a>'
            else:
                return f'<span class="case-missing" title="Case not yet scraped">{citation}</span>'
        
        # Build sections HTML
        sections_html = ""
        toc_html = ""
        
        for section in sections:
            sec_num = section.get('number', '')
            sec_title = section.get('title', '')
            content = section.get('text', '')
            # Support both case_links and cases_cited formats
            cases = section.get('case_links', []) or section.get('cases_cited', [])
            
            # Escape HTML in content but preserve structure
            content_safe = html.escape(content) if content else ''
            content_safe = content_safe.replace('\n', '<br>')
            
            # Create anchor ID
            anchor_id = f"section-{sec_num.lower().replace(' ', '-')}"
            
            # TOC entry
            toc_html += f'<li><a href="#{anchor_id}">Section {sec_num}: {sec_title}</a></li>\n'
            
            # Cases HTML
            cases_html = ""
            if cases:
                case_links_html = []
                for case in cases:
                    cite = case.get('citation', '').strip().rstrip(',')
                    year = case.get('year', '')
                    reporter = case.get('reporter', '')
                    page = case.get('page', '')
                    # Support both 'available' and 'exists_locally' fields
                    available = case.get('available', case.get('exists_locally', False))
                    
                    # Also check if file actually exists
                    case_html_path = self.base_dir / "html" / reporter / str(year) / f"{year}_{reporter}_{page}.html"
                    case_exists = case_html_path.exists()
                    
                    if available or case_exists:
                        url = f"../../../html/{reporter}/{year}/{year}_{reporter}_{page}.html"
                        case_links_html.append(f'<a href="{url}" class="case-link">{cite}</a>')
                    else:
                        case_links_html.append(f'<span class="case-missing" title="Case not yet scraped">{cite}</span>')
                        
                cases_html = f'''
                <div class="related-cases">
                    <div class="cases-label">Related Cases:</div>
                    {" ".join(case_links_html)}
                </div>
                '''
            
            sections_html += f'''
            <div class="section" id="{anchor_id}">
                <div class="section-header">
                    <div class="section-number">Section {sec_num}</div>
                    <div class="section-title">{sec_title}</div>
                </div>
                <div class="section-content">
                    <p>{content_safe}</p>
                </div>
                {cases_html}
            </div>
            '''
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: Georgia, serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; padding: 20px; }}
        header {{ margin-bottom: 30px; border-bottom: 2px solid #2c5282; padding-bottom: 20px; }}
        h1 {{ color: #2c5282; font-size: 1.8em; margin-bottom: 10px; }}
        .metadata {{ display: flex; gap: 20px; color: #666; font-size: 0.9em; }}
        .metadata div {{ background: #f7fafc; padding: 5px 10px; border-radius: 4px; }}
        .toc {{ background: #f7fafc; padding: 20px; margin-bottom: 30px; border-radius: 8px; }}
        .toc h2 {{ color: #2c5282; margin-bottom: 15px; }}
        .toc ul {{ list-style: none; }}
        .toc li {{ margin: 8px 0; }}
        .toc a {{ color: #2b6cb0; text-decoration: none; }}
        .toc a:hover {{ text-decoration: underline; }}
        .section {{ margin-bottom: 30px; padding: 20px; background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; }}
        .section-header {{ margin-bottom: 15px; }}
        .section-number {{ font-weight: bold; color: #2c5282; }}
        .section-title {{ color: #4a5568; font-style: italic; }}
        .section-content {{ margin-bottom: 15px; }}
        .section-content p {{ margin-bottom: 10px; text-align: justify; }}
        .related-cases {{ background: #fffbeb; padding: 10px; border-radius: 4px; border-left: 3px solid #d69e2e; }}
        .cases-label {{ font-weight: bold; color: #744210; margin-bottom: 5px; }}
        .case-link {{ color: #2b6cb0; text-decoration: none; margin-right: 8px; }}
        .case-link:hover {{ text-decoration: underline; }}
        .case-missing {{ color: #a0aec0; margin-right: 8px; cursor: help; }}
        footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #718096; font-size: 0.85em; text-align: center; }}
        a.back-link {{ display: inline-block; margin-bottom: 20px; color: #2b6cb0; }}
    </style>
</head>
<body>
    <a href="../index.html" class="back-link">← Back to Index</a>
    <header>
        <h1>{title}</h1>
        <div class="metadata">
            <div>Year: {year}</div>
            <div>Status: {status}</div>
            <div>Sections: {len(sections)}</div>
        </div>
    </header>
    
    <nav class="toc">
        <h2>Table of Contents</h2>
        <ul>
            {toc_html}
        </ul>
    </nav>
    
    <main>
        {sections_html}
    </main>
    
    <footer>
        <p>Generated by Qanoon Legal Research Platform</p>
        <p>Source: Pakistan Law Site | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
    </footer>
</body>
</html>'''
        
    def _generate_legislation_index(self):
        """Generate index page for legislation"""
        index_file = self.base_dir / "legislation" / "html" / "index.html"
        
        # Collect all statutes
        statutes = []
        for letter_dir in sorted((self.base_dir / "legislation" / "html").iterdir()):
            if letter_dir.is_dir() and len(letter_dir.name) == 1:
                for html_file in sorted(letter_dir.glob("*.html")):
                    # Extract title from filename
                    title = html_file.stem.replace('_', ' ')
                    # Try to get year from title
                    year_match = re.search(r'(\d{4})', title)
                    year = year_match.group(1) if year_match else ''
                    
                    statutes.append({
                        'title': title,
                        'year': year,
                        'letter': letter_dir.name,
                        'path': f"{letter_dir.name}/{html_file.name}"
                    })
        
        # Generate index HTML
        rows = ""
        for s in statutes:
            rows += f'<tr><td><a href="{s["path"]}">{s["title"]}</a></td><td>{s["year"]}</td></tr>\n'
            
        index_html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Pakistan Legislation Index</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 1000px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #2c5282; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #2c5282; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        a {{ color: #2b6cb0; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .stats {{ margin-bottom: 20px; color: #666; }}
    </style>
</head>
<body>
    <h1>Pakistan Legislation Index</h1>
    <div class="stats">Total Statutes: {len(statutes)}</div>
    <table>
        <thead><tr><th>Statute Title</th><th>Year</th></tr></thead>
        <tbody>{rows}</tbody>
    </table>
    <footer style="margin-top: 40px; color: #888; font-size: 0.9em;">
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    </footer>
</body>
</html>'''
        
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write(index_html)
            
        self.log(f"Generated index with {len(statutes)} statutes")
        
    # ==================== STEP 6: Generate Case HTML (Fixed) ====================
    
    def step6_generate_case_html(self) -> int:
        """Generate/fix HTML for all cases referenced by this alphabet's legislation"""
        self.log("STEP 6: Generating case HTML...")
        
        # Load citations for this alphabet
        citations_file = self.pipeline_dir / f"citations_{self.letter}.json"
        if not citations_file.exists():
            self.log("No citations file found - run step 2 first")
            return 0
            
        with open(citations_file, 'r') as f:
            citations = json.load(f)
            
        generated = 0
        for cite in citations:
            year = cite['year']
            reporter = cite['reporter']
            page = cite['page']
            
            # Check if case JSON exists
            case_json = self.base_dir / reporter / str(year) / f"{year}_{reporter}_{page}.json"
            if not case_json.exists():
                continue
                
            try:
                with open(case_json, 'r', encoding='utf-8') as f:
                    case_data = json.load(f)
                    
                html_content = self._generate_case_html(case_data)
                
                # Output path
                html_dir = self.base_dir / "html" / reporter / str(year)
                html_dir.mkdir(parents=True, exist_ok=True)
                html_file = html_dir / f"{year}_{reporter}_{page}.html"
                
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                    
                generated += 1
            except Exception as e:
                self.log(f"  Error generating HTML for {cite['citation']}: {e}")
                
        self.log(f"Generated {generated} case HTML files")
        return generated
        
    def _generate_case_html(self, case_data: Dict) -> str:
        """Generate clean HTML for a case - fixes the encoding issues"""
        citation = case_data.get('citation', 'Unknown')
        case_id = case_data.get('case_id', '')
        court = case_data.get('court', '')
        
        # Get judgment text - try clean version first, then raw
        judgment = case_data.get('judgment_clean', '')
        if not judgment or judgment == '-1':
            judgment = case_data.get('judgment_raw', '')
            
        # Clean up the judgment text
        if judgment:
            # Decode HTML entities
            judgment = html.unescape(judgment)
            
            # Remove Word/Office XML cruft
            judgment = re.sub(r'<!\[if[^>]*>.*?<!\[endif\]>', '', judgment, flags=re.DOTALL)
            judgment = re.sub(r'<o:p>.*?</o:p>', '', judgment, flags=re.DOTALL)
            judgment = re.sub(r'<span[^>]*style=[^>]*mso-[^>]*>', '', judgment)
            judgment = re.sub(r'style="[^"]*mso-[^"]*"', '', judgment)
            
            # Simplify to basic HTML
            # Keep paragraphs and basic formatting
            judgment = re.sub(r'<p[^>]*>', '<p>', judgment)
            judgment = re.sub(r'class="[^"]*"', '', judgment)
            
            # If it's still messy, extract just text
            if '<![' in judgment or 'mso-' in judgment:
                # Fall back to plain text extraction
                judgment = re.sub(r'<[^>]+>', ' ', judgment)
                judgment = re.sub(r'\s+', ' ', judgment).strip()
                judgment = f'<p>{judgment}</p>'
        else:
            judgment = '<p><em>Judgment text not available.</em></p>'
            
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{citation}</title>
    <style>
        body {{ font-family: Georgia, serif; line-height: 1.8; max-width: 800px; margin: 0 auto; padding: 20px; color: #333; }}
        header {{ border-bottom: 2px solid #2c5282; margin-bottom: 30px; padding-bottom: 20px; }}
        h1 {{ color: #2c5282; margin-bottom: 10px; }}
        .case-id {{ color: #718096; font-size: 0.9em; }}
        .metadata {{ margin: 20px 0; padding: 15px; background: #f7fafc; border-radius: 8px; }}
        .metadata div {{ margin: 5px 0; }}
        .label {{ font-weight: bold; color: #4a5568; }}
        .judgment {{ margin-top: 30px; }}
        .judgment h2 {{ color: #2c5282; border-bottom: 1px solid #e2e8f0; padding-bottom: 10px; }}
        .judgment-text {{ text-align: justify; }}
        .judgment-text p {{ margin-bottom: 15px; }}
        footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #718096; font-size: 0.85em; }}
        a {{ color: #2b6cb0; }}
    </style>
</head>
<body>
    <article>
        <header>
            <h1>{citation}</h1>
            <div class="case-id">{case_id}</div>
        </header>
        
        <div class="metadata">
            <div><span class="label">Court</span> {court}</div>
        </div>
        
        <section class="judgment">
            <h2>Judgment</h2>
            <div class="judgment-text">
                {judgment}
            </div>
        </section>
    </article>
    
    <footer>
        <p>Generated on {datetime.now().strftime('%B %d, %Y')} by Qanoon Legal Research Platform</p>
        <p>This document is for informational purposes. Always verify with official sources.</p>
    </footer>
</body>
</html>'''
        
    # ==================== STEP 7: Verify Links ====================
    
    def step7_verify_links(self) -> Tuple[int, int, List[str]]:
        """Verify all case links in legislation HTML work"""
        self.log("STEP 7: Verifying all links...")
        
        working = 0
        broken = 0
        broken_links = []
        
        # Check each legislation HTML
        for html_file in self.html_dir.glob("*.html"):
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Find all case links
            link_pattern = re.compile(r'href="([^"]*html/[^"]+\.html)"')
            matches = link_pattern.findall(content)
            
            for link in matches:
                # Resolve relative path
                full_path = (html_file.parent / link).resolve()
                
                if full_path.exists():
                    working += 1
                else:
                    broken += 1
                    broken_links.append(f"{html_file.name} -> {link}")
                    
        self.log(f"Links: {working} working, {broken} broken")
        
        if broken_links:
            self.log("Broken links:")
            for bl in broken_links[:10]:  # Show first 10
                self.log(f"  {bl}")
            if len(broken_links) > 10:
                self.log(f"  ... and {len(broken_links) - 10} more")
                
        # Save broken links
        broken_file = self.pipeline_dir / f"broken_links_{self.letter}.json"
        with open(broken_file, 'w') as f:
            json.dump(broken_links, f, indent=2)
            
        return working, broken, broken_links
        
    # ==================== Main Pipeline ====================
    
    def run(self, start_step: int = 1, verify_only: bool = False):
        """Run the full pipeline"""
        self.log(f"=" * 50)
        self.log(f"ALPHABET PIPELINE: {self.letter}")
        self.log(f"=" * 50)
        
        status = self.load_status()
        
        if verify_only:
            working, broken, _ = self.step7_verify_links()
            status["links_verified"] = broken == 0
            self.save_status(status)
            return
            
        # Step 1: Scrape Legislation
        if start_step <= 1:
            count = self.step1_scrape_legislation()
            status["legislation_count"] = count
            status["steps_completed"].append(1)
            self.save_status(status)
            
        # Step 2: Extract Citations
        if start_step <= 2:
            citations = self.step2_extract_citations()
            status["steps_completed"].append(2)
            self.save_status(status)
        else:
            # Load existing citations
            citations_file = self.pipeline_dir / f"citations_{self.letter}.json"
            if citations_file.exists():
                with open(citations_file, 'r') as f:
                    citations = json.load(f)
            else:
                citations = []
                
        # Step 3: Find Missing Cases
        if start_step <= 3:
            missing = self.step3_find_missing_cases(citations)
            status["cases_needed"] = len(missing)
            status["steps_completed"].append(3)
            self.save_status(status)
        else:
            missing_file = self.pipeline_dir / f"missing_{self.letter}.json"
            if missing_file.exists():
                with open(missing_file, 'r') as f:
                    missing = json.load(f)
            else:
                missing = []
                
        # Step 4: Scrape Missing Cases
        if start_step <= 4 and missing:
            scraped = self.step4_scrape_missing_cases(missing)
            status["cases_scraped"] = scraped
            status["steps_completed"].append(4)
            self.save_status(status)
            
        # Step 5: Generate Legislation HTML
        if start_step <= 5:
            self.step5_generate_legislation_html()
            status["steps_completed"].append(5)
            self.save_status(status)
            
        # Step 6: Generate Case HTML
        if start_step <= 6:
            self.step6_generate_case_html()
            status["steps_completed"].append(6)
            self.save_status(status)
            
        # Step 7: Verify Links
        working, broken, broken_links = self.step7_verify_links()
        status["links_verified"] = broken == 0
        status["working_links"] = working
        status["broken_links"] = broken
        status["steps_completed"].append(7)
        self.save_status(status)
        
        # Summary
        self.log(f"=" * 50)
        self.log(f"PIPELINE COMPLETE FOR LETTER {self.letter}")
        self.log(f"  Legislation: {status.get('legislation_count', 0)}")
        self.log(f"  Cases needed: {status.get('cases_needed', 0)}")
        self.log(f"  Cases scraped: {status.get('cases_scraped', 0)}")
        self.log(f"  Links: {working} working, {broken} broken")
        self.log(f"  Ready for production: {'YES' if broken == 0 else 'NO'}")
        self.log(f"=" * 50)
        
        return broken == 0


def main():
    parser = argparse.ArgumentParser(description="Alphabet Pipeline for Legislation")
    parser.add_argument("letter", help="Alphabet letter (A-Z)")
    parser.add_argument("--step", type=int, default=1, help="Start from step N")
    parser.add_argument("--verify", action="store_true", help="Only verify links")
    args = parser.parse_args()
    
    pipeline = AlphabetPipeline(args.letter)
    success = pipeline.run(start_step=args.step, verify_only=args.verify)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
