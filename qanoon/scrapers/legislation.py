"""
Legislation Scraper
===================

Scrapes statutes and legislation from Pakistan Law Site alphabetically.
Runs during night hours to complement the case scraper.

Features:
- Alphabetical navigation (A-Z)
- Section-by-section extraction
- Case law links extraction
- Resumable progress tracking

Example:
    from qanoon.scrapers import LegislationScraper
    
    scraper = LegislationScraper()
    if scraper.login():
        scraper.scrape_alphabet("A")
"""

import re
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict

from bs4 import BeautifulSoup

from .base import BaseScraper, BASE_URL

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent.parent.parent / "data_v2" / "legislation"
PROGRESS_FILE = DATA_DIR / "progress.json"
LINKS_FILE = DATA_DIR / "statute_case_links.jsonl"
STATUTES_JSONL = DATA_DIR / "all_statutes.jsonl"

ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Section:
    """Represents a section of a statute."""
    section_id: str
    number: str
    title: str = ""
    text: str = ""
    case_links: List[Dict] = field(default_factory=list)


@dataclass
class Statute:
    """Represents a complete statute."""
    id: str
    title: str
    short_title: str = ""
    alphabet: str = ""
    enactment_date: str = ""
    jurisdiction: str = ""
    status: str = "in_force"
    sections: List[Section] = field(default_factory=list)
    case_links: List[Dict] = field(default_factory=list)
    full_text: str = ""
    amendments: List[str] = field(default_factory=list)
    scraped_at: str = ""
    source_url: str = ""
    
    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
        if not self.id:
            self.id = hashlib.md5(self.title.encode()).hexdigest()[:12]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "title": self.title,
            "short_title": self.short_title,
            "alphabet": self.alphabet,
            "enactment_date": self.enactment_date,
            "jurisdiction": self.jurisdiction,
            "status": self.status,
            "sections": [asdict(s) if isinstance(s, Section) else s for s in self.sections],
            "case_links": self.case_links,
            "full_text": self.full_text,
            "amendments": self.amendments,
            "scraped_at": self.scraped_at,
            "source_url": self.source_url,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class LegislationScraper(BaseScraper):
    """
    Pakistan Law Site Legislation Scraper.
    
    Scrapes statutes alphabetically (A-Z). Runs during night hours
    (10 PM - 5 AM PKT) to complement the case scraper.
    
    Args:
        ignore_hours: Skip operating hours check
        data_dir: Output directory for scraped data
    """
    
    # Legislation scraper runs during night hours
    DEFAULT_OPEN_HOUR = 22   # 10 PM PKT
    DEFAULT_CLOSE_HOUR = 5   # 5 AM PKT
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
        self.progress_file = self.data_dir / "progress.json"
        self.links_file = self.data_dir / "statute_case_links.jsonl"
        self.statutes_jsonl = self.data_dir / "all_statutes.jsonl"
        self.progress = self._load_progress()
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for letter in ALPHABETS:
            (self.data_dir / letter).mkdir(exist_ok=True)
            (self.data_dir / letter / "original").mkdir(exist_ok=True)
    
    def _load_progress(self) -> Dict:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                return json.loads(self.progress_file.read_text(encoding='utf-8'))
            except:
                pass
        return {
            "completed_alphabets": [],
            "statutes_scraped": [],
            "current_alphabet": None,
            "total_statutes": 0,
            "last_updated": None
        }
    
    def _save_progress(self):
        """Save progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        self.progress_file.write_text(
            json.dumps(self.progress, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
    
    def _save_statute(self, statute: Statute, html_content: str = ""):
        """Save statute to JSON and HTML files."""
        safe_name = re.sub(r'[^\w\-]', '_', statute.title)[:100]
        statute_dir = self.data_dir / statute.alphabet
        
        # Save JSON
        json_path = statute_dir / f"{safe_name}.json"
        json_path.write_text(
            json.dumps(statute.to_dict(), indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        
        # Save original HTML
        if html_content:
            html_path = statute_dir / "original" / f"{safe_name}.html"
            html_path.write_text(html_content, encoding='utf-8')
        
        # Append to main JSONL
        with open(self.statutes_jsonl, 'a', encoding='utf-8') as f:
            f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
        
        # Append to case links file
        if statute.case_links:
            with open(self.links_file, 'a', encoding='utf-8') as f:
                for link in statute.case_links:
                    entry = {
                        "statute_id": statute.id,
                        "statute_title": statute.title,
                        **link
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved: {statute.title[:60]}")
    
    # ══════════════════════════════════════════════════════════════════════════
    # Statute List
    # ══════════════════════════════════════════════════════════════════════════
    
    def get_statutes_by_letter(self, letter: str) -> List[Dict]:
        """Get all statutes starting with a letter."""
        if not self.ensure_logged_in():
            return []
        
        logger.info(f"Fetching statutes starting with '{letter}'...")
        
        resp = self.request(
            "GET", 
            f"{BASE_URL}/Login/StatuecharSearch",
            params={"character": letter}
        )
        if not resp:
            return []
        
        self.human_delay(reading=True)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        statutes = []
        rows = soup.find_all('tr', class_='caseType')
        
        for row in rows:
            caseid = row.get('casetypeid', '')
            if caseid:
                statutes.append({
                    "name": caseid.strip(),
                    "alphabet": letter
                })
        
        logger.info(f"  Found {len(statutes)} statutes for '{letter}'")
        return statutes
    
    # ══════════════════════════════════════════════════════════════════════════
    # Statute Sections
    # ══════════════════════════════════════════════════════════════════════════
    
    def get_statute_sections(self, statute_name: str) -> List[Dict]:
        """Get all sections of a statute."""
        if not self.ensure_logged_in():
            return []
        
        resp = self.request(
            "GET", 
            f"{BASE_URL}/Login/GetStatuesSearch",
            params={"caseName": statute_name}
        )
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        sections = []
        rows = soup.find_all('tr', class_='table_row_hover')
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                read_cell = cells[0]
                section_num = cells[1].get_text(strip=True)
                act_name = cells[2].get_text(strip=True)
                definition = cells[3].get_text(strip=True)
                
                section_id = read_cell.get('casetypeid', '')
                if not section_id:
                    link = read_cell.find(class_='readCaseLaw')
                    if link:
                        section_id = link.get('casetypeid', '')
                
                case_cell = cells[4] if len(cells) > 4 else None
                case_id = case_cell.get('casetypeid', '') if case_cell else ''
                statute_section = case_cell.get('statutename', '') if case_cell else ''
                
                sections.append({
                    "section_id": section_id,
                    "number": section_num,
                    "act_name": act_name,
                    "definition": definition,
                    "case_type_id": case_id,
                    "statute_section": statute_section,
                })
        
        return sections
    
    def get_section_content(self, section_id: str) -> str:
        """Get the full text content of a section."""
        if not section_id:
            return ""
        
        resp = self.request(
            "POST", 
            f"{BASE_URL}/Login/SearchStatueFile",
            data={"caseTypeId": section_id}
        )
        
        if not resp or resp.text == "-1":
            return ""
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        return soup.get_text(separator='\n', strip=True)
    
    def get_section_case_links(self, case_type_id: str) -> List[Dict]:
        """Get case law citations for a section."""
        if not case_type_id:
            return []
        
        resp = self.request(
            "POST", 
            f"{BASE_URL}/Login/GetStatuteCaseLaw",
            data={"caseTypeId": case_type_id, "subTopic": ""}
        )
        
        if not resp or len(resp.text) < 50:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        case_links = []
        citation_pattern = r'(\d{4})\s+(PLD|SCMR|CLC|PCrLJ|MLD|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)'
        
        for link in soup.find_all('a'):
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            citation_match = re.search(citation_pattern, text)
            if citation_match:
                case_links.append({
                    "citation": text,
                    "year": citation_match.group(1),
                    "reporter": citation_match.group(2),
                    "page": citation_match.group(3),
                    "url": href,
                })
        
        for row in soup.find_all('tr'):
            text = row.get_text(strip=True)
            citation_match = re.search(citation_pattern, text)
            if citation_match:
                case_links.append({
                    "citation": citation_match.group(0),
                    "year": citation_match.group(1),
                    "reporter": citation_match.group(2),
                    "page": citation_match.group(3),
                    "url": "",
                })
        
        # Deduplicate
        seen = set()
        unique = []
        for cl in case_links:
            key = cl["citation"]
            if key not in seen:
                seen.add(key)
                unique.append(cl)
        
        return unique
    
    # ══════════════════════════════════════════════════════════════════════════
    # Full Statute Scraping
    # ══════════════════════════════════════════════════════════════════════════
    
    def scrape_statute(self, statute_info: Dict) -> Optional[Statute]:
        """Scrape a complete statute with all sections and case links."""
        statute_name = statute_info["name"]
        alphabet = statute_info["alphabet"]
        
        if statute_name in self.progress["statutes_scraped"]:
            logger.debug(f"Skipping {statute_name} (already scraped)")
            return None
        
        logger.info(f"Scraping: {statute_name[:60]}")
        
        self.human_delay(1, 2)
        sections = self.get_statute_sections(statute_name)
        
        if not sections:
            logger.warning(f"  No sections found for {statute_name}")
            self.progress["statutes_scraped"].append(statute_name)
            return None
        
        statute = Statute(
            id="",
            title=statute_name,
            alphabet=alphabet,
            source_url=f"{BASE_URL}/Login/GetStatuesSearch?caseName={statute_name}",
        )
        
        # Extract metadata from title
        year_match = re.search(r'(\d{4})$', statute_name)
        if year_match:
            statute.enactment_date = year_match.group(1)
            statute.short_title = statute_name[:statute_name.rfind(year_match.group(1))].strip()
        else:
            statute.short_title = statute_name
        
        # Determine jurisdiction
        jurisdiction_keywords = {
            "Federal": ["Federal", "Pakistan", "National"],
            "Punjab": ["Punjab"],
            "Sindh": ["Sindh", "Karachi"],
            "KPK": ["KPK", "Khyber", "Pakhtunkhwa", "NWFP"],
            "Balochistan": ["Balochistan", "Baluchistan"],
            "AJK": ["Azad Jammu", "Kashmir", "AJK"],
            "Gilgit-Baltistan": ["Gilgit", "Baltistan", "GBLR"],
        }
        for jurisdiction, keywords in jurisdiction_keywords.items():
            if any(kw.lower() in statute_name.lower() for kw in keywords):
                statute.jurisdiction = jurisdiction
                break
        
        all_case_links = []
        full_text_parts = []
        
        for i, section_info in enumerate(sections):
            if i > 0 and i % 5 == 0:
                self.human_delay(4, 8)
            else:
                self.human_delay(1.5, 3)
            
            section_id = section_info.get("section_id", "")
            case_type_id = section_info.get("case_type_id", "")
            
            section_text = ""
            if section_id:
                section_text = self.get_section_content(section_id)
                if section_text:
                    self.human_delay(reading=True)
            
            case_links = []
            if case_type_id:
                self.human_delay(1, 2)
                case_links = self.get_section_case_links(case_type_id)
                all_case_links.extend([
                    {**cl, "section": section_info.get("number", "")}
                    for cl in case_links
                ])
            
            section = Section(
                section_id=section_id,
                number=section_info.get("number", ""),
                title=section_info.get("definition", ""),
                text=section_text,
                case_links=case_links,
            )
            statute.sections.append(asdict(section))
            
            if section_text:
                full_text_parts.append(f"[Section {section.number}]\n{section_text}")
        
        statute.full_text = "\n\n".join(full_text_parts)
        statute.case_links = all_case_links
        
        self.progress["statutes_scraped"].append(statute_name)
        self.progress["total_statutes"] += 1
        
        return statute
    
    def scrape_alphabet(self, letter: str, limit: int = None) -> int:
        """Scrape all statutes for a given alphabet letter."""
        if letter in self.progress["completed_alphabets"]:
            logger.info(f"Skipping '{letter}' (already completed)")
            return 0
        
        logger.info(f"=== Scraping alphabet '{letter}' ===")
        self.progress["current_alphabet"] = letter
        self._save_progress()
        
        statutes = self.get_statutes_by_letter(letter)
        if not statutes:
            logger.warning(f"No statutes found for '{letter}'")
            self._save_progress()
            return 0
        
        if limit:
            statutes = statutes[:limit]
        
        scraped_count = 0
        total = len(statutes)
        
        for i, statute_info in enumerate(statutes):
            try:
                if not self.is_open():
                    self.wait_for_open()
                    self.logged_in = False
                
                self.human_delay()
                statute = self.scrape_statute(statute_info)
                
                if statute:
                    self._save_statute(statute)
                    scraped_count += 1
                
                self._save_progress()
                
                if (i + 1) % 10 == 0:
                    logger.info(f"  Progress: {i + 1}/{total} for '{letter}'")
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user. Saving progress...")
                self._save_progress()
                raise
            except Exception as e:
                logger.error(f"Error scraping {statute_info['name']}: {e}")
                self._save_progress()
                self.logged_in = False
        
        self.progress["completed_alphabets"].append(letter)
        self.progress["current_alphabet"] = None
        self._save_progress()
        
        logger.info(f"Completed '{letter}': {scraped_count} statutes scraped")
        return scraped_count
    
    def scrape(self, start_letter: str = "A", limit_per_letter: int = None):
        """
        Main scraping method - scrape all alphabets.
        
        Args:
            start_letter: Letter to start from (default: A)
            limit_per_letter: Limit statutes per letter (for testing)
        """
        logger.info(f"Starting full scrape from '{start_letter}'")
        logger.info(f"Operating hours: {self.open_hour}:00 - {self.close_hour}:00 PKT")
        
        if not self.login():
            logger.error("Failed to login. Aborting.")
            return
        
        start_index = ALPHABETS.index(start_letter) if start_letter in ALPHABETS else 0
        total_scraped = 0
        
        for letter in ALPHABETS[start_index:]:
            try:
                if not self.is_open():
                    self.wait_for_open()
                    self.logged_in = False
                    if not self.login():
                        logger.error("Failed to re-login. Aborting.")
                        break
                
                count = self.scrape_alphabet(letter, limit=limit_per_letter)
                total_scraped += count
                
                if letter != ALPHABETS[-1]:
                    import random
                    break_time = random.uniform(60, 120)
                    logger.info(f"Break between alphabets: {break_time:.0f}s")
                    import time
                    time.sleep(break_time)
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user.")
                break
        
        logger.info(f"Scraping complete! Total statutes: {total_scraped}")
        self._save_progress()
    
    def get_status(self) -> Dict:
        """Get current scraping status."""
        return {
            "data_dir": str(self.data_dir),
            "completed_alphabets": self.progress["completed_alphabets"],
            "current_alphabet": self.progress["current_alphabet"],
            "statutes_scraped": len(self.progress["statutes_scraped"]),
            "total_statutes": self.progress.get("total_statutes", 0),
            "last_updated": self.progress.get("last_updated", "Never"),
        }
