"""
Linked Cases Scraper
====================

Scrapes case content for cases referenced in statute-case links.
Reads from statute_case_links.jsonl and fetches cases that don't exist yet.

Example:
    from qanoon.scrapers import LinkedCasesScraper
    
    scraper = LinkedCasesScraper()
    scraper.run(limit=100)
"""

import re
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

from bs4 import BeautifulSoup

from .base import BaseScraper, BASE_URL

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent.parent.parent / "data_v2"
LINKS_FILE = DATA_DIR / "legislation" / "statute_case_links.jsonl"


class LinkedCasesScraper(BaseScraper):
    """
    Scrapes case content for cases linked from statutes.
    
    Reads statute_case_links.jsonl and fetches each case that doesn't
    exist locally. Runs during night hours like the legislation scraper.
    
    Args:
        ignore_hours: Skip operating hours check
        data_dir: Data directory containing case files
    """
    
    # Night hours like legislation scraper
    DEFAULT_OPEN_HOUR = 22
    DEFAULT_CLOSE_HOUR = 5
    DEFAULT_NIGHT_MODE = True
    
    def __init__(
        self,
        ignore_hours: bool = False,
        data_dir: Path = None
    ):
        super().__init__(
            ignore_hours=ignore_hours,
            night_mode=True,
            open_hour=22,
            close_hour=5,
        )
        
        self.data_dir = data_dir or DATA_DIR
        self.links_file = self.data_dir / "legislation" / "statute_case_links.jsonl"
        
        # Stats
        self.cases_scraped = 0
        self.cases_skipped = 0
        self.cases_failed = 0
    
    def parse_citation(self, citation: str) -> Optional[Dict]:
        """Parse citation like '1986 PLD 29' into components."""
        citation = citation.strip().rstrip(',')
        parts = citation.split()
        
        if len(parts) < 3:
            return None
        
        year = parts[0]
        reporter = parts[1]
        page = parts[2]
        
        if not year.isdigit() or len(year) != 4:
            return None
        if not page.isdigit():
            return None
        
        return {
            "year": year,
            "reporter": reporter,
            "page": page,
            "citation": f"{year} {reporter} {page}"
        }
    
    def case_exists(self, year: str, reporter: str, page: str) -> bool:
        """Check if case already exists locally."""
        case_file = self.data_dir / reporter / year / f"{year}_{reporter}_{page}.json"
        return case_file.exists()
    
    def get_unique_citations(self) -> List[Dict]:
        """Get unique citations from statute links that we don't have yet."""
        if not self.links_file.exists():
            logger.error(f"Links file not found: {self.links_file}")
            return []
        
        seen = set()
        citations = []
        
        with open(self.links_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    link = json.loads(line)
                    citation = link.get("citation", "")
                    parsed = self.parse_citation(citation)
                    
                    if parsed:
                        key = f"{parsed['year']}_{parsed['reporter']}_{parsed['page']}"
                        if key not in seen:
                            seen.add(key)
                            if not self.case_exists(parsed['year'], parsed['reporter'], parsed['page']):
                                citations.append(parsed)
                except:
                    continue
        
        return citations
    
    def scrape_case(self, year: str, reporter: str, page: str) -> Optional[Dict]:
        """Scrape a single case by citation."""
        citation = f"{year} {reporter} {page}"
        
        if not self.is_open():
            logger.info("Outside operating hours, waiting...")
            return None
        
        if not self.ensure_logged_in():
            return None
        
        self.human_delay()
        
        try:
            # Search for the case to get casetypeid
            search_resp = self.request(
                "POST",
                f"{BASE_URL}/Login/CitationSearch",
                data={
                    "year": year,
                    "book": reporter,
                    "code": page,
                    "court": "",
                    "judge": "",
                }
            )
            
            if not search_resp or search_resp.status_code != 200:
                logger.warning(f"Search failed for {citation}")
                return None
            
            # Find casetypeid
            case_id = None
            soup = BeautifulSoup(search_resp.text, 'html.parser')
            
            btn = soup.find('input', attrs={'casetypeid': True})
            if btn:
                case_id = btn.get('casetypeid')
            else:
                match = re.search(r'casetypeid="([^"]+)"', search_resp.text)
                if match:
                    case_id = match.group(1)
            
            if not case_id:
                logger.warning(f"  No casetypeid found for {citation}")
                return None
            
            self.human_delay(1, 2)
            
            # Fetch full case content
            case_resp = self.request(
                "POST",
                f"{BASE_URL}/Login/GetCaseFile",
                data={
                    "caseName": case_id,
                    "headNotes": 0,
                }
            )
            
            if not case_resp or case_resp.status_code != 200 or len(case_resp.text) < 100:
                logger.warning(f"  Failed to fetch case content for {citation}")
                return None
            
            # Parse case content
            case_data = {
                "citation": citation,
                "year": int(year),
                "reporter": reporter,
                "page": int(page),
                "case_name": case_id,
                "scraped_at": datetime.now().isoformat(),
                "source": "linked_cases_scraper",
                "judgment_raw": case_resp.text
            }
            
            soup = BeautifulSoup(case_resp.text, 'html.parser')
            
            title_elem = soup.find(['h1', 'h2', 'h3'])
            if title_elem:
                case_data["title"] = title_elem.get_text(strip=True)
            
            court_match = re.search(
                r'(Supreme Court|High Court|Tribunal|Court)[^<]*', 
                case_resp.text, 
                re.I
            )
            if court_match:
                case_data["court"] = court_match.group(0).strip()
            
            case_data["judgment_text"] = soup.get_text(separator='\n', strip=True)
            
            return case_data
            
        except Exception as e:
            logger.error(f"Error scraping {citation}: {e}")
            return None
    
    def save_case(self, case_data: Dict):
        """Save case to JSON file."""
        year = str(case_data["year"])
        reporter = case_data["reporter"]
        page = str(case_data["page"])
        
        case_dir = self.data_dir / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)
        
        case_file = case_dir / f"{year}_{reporter}_{page}.json"
        with open(case_file, 'w', encoding='utf-8') as f:
            json.dump(case_data, f, ensure_ascii=False, indent=2)
        
        if "judgment_raw" in case_data:
            orig_dir = case_dir / "original"
            orig_dir.mkdir(exist_ok=True)
            html_file = orig_dir / f"{year}_{reporter}_{page}.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(case_data["judgment_raw"])
        
        logger.info(f"  Saved: {case_file.name}")
    
    def scrape(self, limit: int = None):
        """
        Main scraping method - scrape linked cases.
        
        Args:
            limit: Maximum number of cases to scrape
        """
        logger.info("=" * 60)
        logger.info("Linked Cases Scraper")
        logger.info("=" * 60)
        
        citations = self.get_unique_citations()
        total = len(citations)
        
        logger.info(f"Found {total} unique cases to scrape")
        
        if limit:
            citations = citations[:limit]
            logger.info(f"Limiting to {limit} cases")
        
        for i, cit in enumerate(citations):
            if not self.is_open():
                logger.info("Outside operating hours. Stopping.")
                break
            
            year, reporter, page = cit["year"], cit["reporter"], cit["page"]
            logger.info(f"[{i+1}/{len(citations)}] Scraping {cit['citation']}...")
            
            case_data = self.scrape_case(year, reporter, page)
            
            if case_data:
                self.save_case(case_data)
                self.cases_scraped += 1
            else:
                self.cases_failed += 1
            
            if (i + 1) % 10 == 0:
                logger.info(
                    f"  Progress: {i+1}/{len(citations)} | "
                    f"Scraped: {self.cases_scraped} | Failed: {self.cases_failed}"
                )
        
        logger.info("=" * 60)
        logger.info(f"Completed: {self.cases_scraped} scraped, {self.cases_failed} failed")
        logger.info("=" * 60)
    
    def get_status(self) -> Dict:
        """Get current status."""
        citations = self.get_unique_citations()
        return {
            "pending_citations": len(citations),
            "cases_scraped": self.cases_scraped,
            "cases_failed": self.cases_failed,
        }
