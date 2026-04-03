"""
Case Scraper
============

Scrapes case law from Pakistan Law Site by reporter and year.
Uses Chrome TLS fingerprint for undetectable requests.

Features:
- Citation-based search by reporter and year
- Full case content extraction
- Resumable progress tracking
- JSON + JSONL + HTML output

Example:
    from qanoon.scrapers import CaseScraper
    
    scraper = CaseScraper()
    if scraper.login():
        scraper.scrape_reporter_year("SCMR", 2024)
"""

import re
import json
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

from bs4 import BeautifulSoup

from .base import BaseScraper, BASE_URL

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent.parent.parent / "data_v2"
PROGRESS_FILE = DATA_DIR / "progress.json"

# Reporters to scrape
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
START_YEAR = 1947
END_YEAR = 2025


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Case:
    """Represents a scraped legal case."""
    citation: str
    case_name: str
    title: str = ""
    court: str = ""
    date: str = ""
    judges: List[str] = None
    headnotes: str = ""
    judgment: str = ""
    statutes_cited: List[str] = None
    cases_cited: List[str] = None
    fetched_at: str = ""
    
    def __post_init__(self):
        if self.judges is None:
            self.judges = []
        if self.statutes_cited is None:
            self.statutes_cited = []
        if self.cases_cited is None:
            self.cases_cited = []
        if not self.fetched_at:
            self.fetched_at = datetime.now().isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class CaseScraper(BaseScraper):
    """
    Pakistan Law Site Case Scraper.
    
    Scrapes case law by reporter (SCMR, PLD, etc.) and year.
    Inherits session management, delays, and operating hours from BaseScraper.
    
    Args:
        ignore_hours: Skip PLS operating hours check
        use_proxy: Use Bright Data proxy for Pakistan IPs
        data_dir: Output directory for scraped data
    """
    
    # Case scraper runs during day hours (7 AM - 9 PM PKT)
    DEFAULT_OPEN_HOUR = 7
    DEFAULT_CLOSE_HOUR = 21
    DEFAULT_NIGHT_MODE = False
    
    def __init__(
        self,
        ignore_hours: bool = False,
        use_proxy: bool = False,
        data_dir: Path = None
    ):
        super().__init__(
            ignore_hours=ignore_hours,
            use_proxy=use_proxy,
        )
        
        self.data_dir = data_dir or DATA_DIR
        self.progress_file = self.data_dir / "progress.json"
        self.progress = self._load_progress()
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_progress(self) -> Dict:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                return json.loads(self.progress_file.read_text(encoding='utf-8'))
            except:
                pass
        return {
            "completed_searches": [],
            "cases_fetched": [],
            "total_cases": 0,
            "last_updated": None
        }
    
    def _save_progress(self):
        """Save progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        self.progress_file.write_text(
            json.dumps(self.progress, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
    
    def _save_case(self, case: Case):
        """Save case to JSON file, JSONL, and original HTML."""
        # Create directory structure: data_v2/REPORTER/YEAR/
        reporter = case.citation.split()[1] if len(case.citation.split()) > 1 else "UNKNOWN"
        year = case.citation.split()[0] if case.citation else "0000"
        
        case_dir = self.data_dir / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)
        
        case_dict = asdict(case)
        safe_citation = re.sub(r'[^\w\-]', '_', case.citation)
        
        # 1. Save individual JSON file
        json_filepath = case_dir / f"{safe_citation}.json"
        json_filepath.write_text(
            json.dumps(case_dict, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        
        # 2. Save original HTML file
        original_dir = case_dir / "original"
        original_dir.mkdir(parents=True, exist_ok=True)
        html_filepath = original_dir / f"{safe_citation}.html"
        
        original_html = case.judgment
        if original_html:
            try:
                if original_html.startswith('"') or '\\u' in original_html:
                    import html as html_lib
                    original_html = original_html.encode().decode('unicode_escape')
                    original_html = html_lib.unescape(original_html)
                html_filepath.write_text(original_html, encoding='utf-8')
            except Exception as e:
                logger.warning(f"Could not save original HTML for {case.citation}: {e}")
        
        # 3. Append to JSONL file
        jsonl_filepath = self.data_dir / f"{reporter}_{year}.jsonl"
        with open(jsonl_filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
        
        # 4. Append to master JSONL
        master_jsonl = self.data_dir / "all_cases.jsonl"
        with open(master_jsonl, "a", encoding="utf-8") as f:
            f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
        
        logger.info(f"Saved: {case.citation}")
    
    # ══════════════════════════════════════════════════════════════════════════
    # Search Methods
    # ══════════════════════════════════════════════════════════════════════════
    
    def citation_search(self, year: int, reporter: str) -> List[Dict]:
        """Search cases by year and reporter."""
        if not self.ensure_logged_in():
            return []
        
        logger.info(f"Searching: {year} {reporter}")
        
        resp = self.request("POST", f"{BASE_URL}/Login/CitationSearch", data={
            "year": year,
            "book": reporter,
            "code": "",
            "court": "",
            "judge": "",
            "lawyer": "",
            "party": "",
        })
        
        if not resp:
            return []
        
        cases = self._parse_search_results(resp.text)
        logger.info(f"  Found {len(cases)} cases")
        
        return cases
    
    def _parse_search_results(self, html: str) -> List[Dict]:
        """Parse case listings from search results."""
        cases = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Format 1: Table rows with class="caseType"
        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""
                
                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    cases.append({
                        "citation": citation,
                        "case_name": case_id,
                    })
        
        # Format 2: caseLawTable format
        for table in soup.find_all('table', class_='caseLawTable'):
            onclick = table.get('onclick', '')
            case_name_match = re.search(r"'([^']+)'", onclick)
            case_name = case_name_match.group(1) if case_name_match else ""
            
            citation_match = re.search(r'\d{4}\s+[A-Z]+\s+\d+', table.get_text())
            if citation_match:
                cases.append({
                    "citation": citation_match.group(0),
                    "case_name": case_name,
                })
        
        # Format 3: Regex fallback
        if not cases:
            citations = re.findall(
                r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', 
                html
            )
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)
            
            for i, citation in enumerate(citations):
                case_id = case_ids[i] if i < len(case_ids) else ""
                cases.append({
                    "citation": citation,
                    "case_name": case_id,
                })
        
        # Deduplicate
        seen = set()
        unique = []
        for c in cases:
            if c["citation"] not in seen:
                seen.add(c["citation"])
                unique.append(c)
        
        return unique
    
    # ══════════════════════════════════════════════════════════════════════════
    # Case Fetching
    # ══════════════════════════════════════════════════════════════════════════
    
    def fetch_case(self, case_id: str, citation: str = "") -> Optional[Case]:
        """Fetch full case content using casetypeid."""
        if not self.ensure_logged_in():
            return None
        
        logger.info(f"Fetching: {citation or case_id}")
        
        resp = self.request("POST", f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_id,
            "headNotes": 0,
        })
        
        if not resp or resp.text.strip() in ["1", '"1"', ""] or len(resp.text) < 100:
            logger.warning(f"  Failed to fetch case content")
            return None
        
        # Simulate reading
        self.human_delay(reading=True)
        
        case = self._parse_case_content(resp.text, citation, case_id)
        return case
    
    def _parse_case_content(self, html: str, citation: str, case_name: str) -> Case:
        """Parse case content from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract title
        title = ""
        title_elem = soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'title|heading', re.I))
        if title_elem:
            title = title_elem.get_text(strip=True)
        
        # Extract court
        court = ""
        court_match = re.search(r'(Supreme Court|High Court|Federal Shariat|Tribunal)[^<]*', html, re.I)
        if court_match:
            court = court_match.group(0).strip()
        
        # Extract date
        date = ""
        date_match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})', html)
        if date_match:
            date = date_match.group(1)
        
        # Extract judges
        judges = []
        judge_section = soup.find(string=re.compile(r'Before|Coram|JUDGE', re.I))
        if judge_section:
            parent = judge_section.find_parent()
            if parent:
                judge_text = parent.get_text()
                judges = re.findall(
                    r'(?:Mr\.|Mrs\.|Justice|J\.)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', 
                    judge_text
                )
        
        # Extract headnotes
        headnotes = ""
        hn_section = soup.find(['div', 'p'], class_=re.compile(r'headnote', re.I))
        if hn_section:
            headnotes = hn_section.get_text(strip=True)
        
        # Extract judgment text
        judgment = ""
        for selector in ['.judgment', '.caseText', '#caseContent', 'div[class*="case"]']:
            elem = soup.select_one(selector)
            if elem:
                judgment = elem.get_text(separator='\n', strip=True)
                break
        
        if not judgment:
            judgment = soup.get_text(separator='\n', strip=True)
        
        # Extract cited statutes
        statutes = re.findall(r'(?:Act|Ordinance|Code|Rules?),?\s+\d{4}', html)
        statutes = list(set(statutes))
        
        # Extract cited cases
        cited_cases = re.findall(
            r'\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR)\s+\d+', 
            html
        )
        cited_cases = list(set(cited_cases))
        
        return Case(
            citation=citation,
            case_name=case_name,
            title=title,
            court=court,
            date=date,
            judges=judges,
            headnotes=headnotes,
            judgment=judgment,
            statutes_cited=statutes,
            cases_cited=cited_cases,
        )
    
    # ══════════════════════════════════════════════════════════════════════════
    # Main Scraping Methods
    # ══════════════════════════════════════════════════════════════════════════
    
    def scrape_reporter_year(self, reporter: str, year: int) -> int:
        """Scrape all cases for a reporter/year combination."""
        search_key = f"{year}-{reporter}"
        
        if search_key in self.progress["completed_searches"]:
            logger.info(f"Skipping {search_key} (already completed)")
            return 0
        
        # Search for cases
        self.human_delay()
        cases = self.citation_search(year, reporter)
        
        if not cases:
            logger.info(f"No cases found for {year} {reporter}")
            self.progress["completed_searches"].append(search_key)
            self._save_progress()
            return 0
        
        fetched = 0
        total_cases = len(cases)
        
        for case_info in cases:
            citation = case_info["citation"]
            case_name = case_info["case_name"]
            
            if citation in self.progress["cases_fetched"]:
                logger.debug(f"Skipping {citation} (already fetched)")
                continue
            
            self.human_delay()
            case = self.fetch_case(case_name, citation)
            
            if case:
                self._save_case(case)
                self.progress["cases_fetched"].append(citation)
                self.progress["total_cases"] += 1
                fetched += 1
                
                if fetched % 10 == 0:
                    self._save_progress()
        
        self.progress["completed_searches"].append(search_key)
        self._save_progress()
        
        logger.info(f"Completed {search_key}: {fetched} cases fetched")
        return fetched
    
    def scrape(
        self, 
        reporters: List[str] = None, 
        start_year: int = None, 
        end_year: int = None
    ):
        """
        Main scraping method - scrape all reporters and years.
        
        Args:
            reporters: List of reporters to scrape (default: all)
            start_year: Start year (default: 1947)
            end_year: End year (default: current year)
        """
        reporters = reporters or REPORTERS
        start_year = start_year or START_YEAR
        end_year = end_year or END_YEAR
        
        logger.info(f"Starting scrape: {reporters} from {start_year} to {end_year}")
        logger.info(f"Operating hours: {self.open_hour}:00 - {self.close_hour}:00 PKT")
        
        if not self.login():
            logger.error("Failed to login. Aborting.")
            return
        
        total_fetched = 0
        
        # Go year by year, newest first
        for year in range(end_year, start_year - 1, -1):
            if not self.is_open():
                self.wait_for_open()
                self.logged_in = False
                if not self.login():
                    logger.error("Failed to re-login after waiting. Aborting.")
                    break
            
            for reporter in reporters:
                try:
                    fetched = self.scrape_reporter_year(reporter, year)
                    total_fetched += fetched
                except KeyboardInterrupt:
                    logger.info("Interrupted by user. Saving progress...")
                    self._save_progress()
                    raise
                except Exception as e:
                    logger.error(f"Error scraping {year} {reporter}: {e}")
                    self._save_progress()
                    self.logged_in = False
        
        logger.info(f"Scraping complete! Total cases: {total_fetched}")
        self._save_progress()
    
    def get_status(self) -> Dict:
        """Get current scraping status."""
        return {
            "data_dir": str(self.data_dir),
            "completed_searches": len(self.progress["completed_searches"]),
            "cases_fetched": len(self.progress["cases_fetched"]),
            "total_cases": self.progress.get("total_cases", 0),
            "last_updated": self.progress.get("last_updated", "Never"),
        }
