#!/usr/bin/env python3
"""
rescrape_empty_legislation.py — Re-scrape legislation files that have no body text.

Finds all JSON files in data_v2/legislation/ with empty/missing body content,
then re-fetches the content from PLS for each one.

Usage:
    python rescrape_empty_legislation.py                # Re-scrape all empty
    python rescrape_empty_legislation.py --letter P     # Only letter P
    python rescrape_empty_legislation.py --limit 100    # First 100 empty
    python rescrape_empty_legislation.py --dry-run      # Count only, don't scrape
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S", stream=sys.stdout)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\legislation-rescrape")

PLS_USER = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
PLS_PASS = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
BASE_URL = "https://www.pakistanlawsite.com"
REQUEST_DELAY = 2.0

try:
    from curl_cffi import requests as cffi_requests
    SESSION_CLASS = cffi_requests.Session
    USE_CFFI = True
except ImportError:
    import requests
    SESSION_CLASS = requests.Session
    USE_CFFI = False

from bs4 import BeautifulSoup


class PLSLegislation:
    """PLS session for legislation scraping."""

    def __init__(self):
        self.session = SESSION_CLASS()
        if USE_CFFI:
            self.session.impersonate = "chrome"
        self.logged_in = False

    def login(self):
        log.info("Logging in to PLS...")
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/ClearLoginHistory",
                data={"Login.UserName": PLS_USER, "Login.Password": PLS_PASS},
                timeout=30,
            )
            self.logged_in = resp.status_code == 200
            if self.logged_in:
                log.info("Login OK")
            return self.logged_in
        except Exception as e:
            log.error(f"Login failed: {e}")
            return False

    def get_statute_sections(self, statute_name):
        """Get the list of sections for a statute from PLS."""
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/GetStatuteSections",
                data={"statuteName": statute_name},
                timeout=30,
            )
            if resp.status_code != 200:
                return []
            
            try:
                data = resp.json()
            except:
                data = resp.text
            
            if isinstance(data, list):
                return data
            
            # Try parsing HTML
            soup = BeautifulSoup(resp.text, "html.parser")
            sections = []
            for row in soup.find_all("tr"):
                section_id = ""
                case_type_id = ""
                number = ""
                definition = ""
                
                for elem in row.find_all(attrs={"sectionid": True}):
                    section_id = elem.get("sectionid", "")
                for elem in row.find_all(attrs={"casetypeid": True}):
                    case_type_id = elem.get("casetypeid", "")
                
                tds = row.find_all("td")
                if len(tds) >= 2:
                    number = tds[0].get_text(strip=True)
                    definition = tds[1].get_text(strip=True)
                
                if section_id or number:
                    sections.append({
                        "section_id": section_id,
                        "case_type_id": case_type_id,
                        "number": number,
                        "definition": definition,
                    })
            
            return sections
        except Exception as e:
            log.error(f"GetStatuteSections error: {e}")
            return []

    def get_section_content(self, section_id):
        """Get the content of a specific section."""
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/GetSectionContent",
                data={"sectionId": section_id},
                timeout=30,
            )
            if resp.status_code != 200:
                return "", ""
            
            html = resp.text
            try:
                html = json.loads(html)
            except:
                pass
            
            if isinstance(html, str) and len(html) > 20:
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
                return html, text
            
            return "", ""
        except Exception as e:
            log.error(f"GetSectionContent error for {section_id}: {e}")
            return "", ""


def find_empty_legislation(letter=None):
    """Find all legislation files with no body text."""
    empty = []
    
    for subdir in sorted(DATA_DIR.iterdir()):
        if not subdir.is_dir():
            continue
        if letter and subdir.name.upper() != letter.upper():
            continue
        
        for f in subdir.glob("*.json"):
            try:
                data = json.load(open(f, encoding="utf-8"))
                body = (data.get("full_text", "") or data.get("body", "") or 
                       data.get("content", "") or data.get("text", "") or "")
                
                if not body or len(str(body).strip()) < 50:
                    statute_name = data.get("title", "") or data.get("short_title", "") or data.get("name", "")
                    pls_id = data.get("pls_id", "") or data.get("statute_id", "") or data.get("id", "")
                    empty.append({
                        "path": str(f),
                        "name": statute_name,
                        "pls_id": pls_id,
                        "letter": subdir.name,
                        "current_body_len": len(str(body)),
                    })
            except Exception:
                empty.append({"path": str(f), "name": f.stem, "pls_id": "", "letter": subdir.name, "current_body_len": 0})
    
    return empty


def rescrape(pls, empty_list, limit=None):
    """Re-scrape body text for empty legislation files."""
    total = len(empty_list)
    if limit:
        empty_list = empty_list[:limit]
    
    log.info(f"Re-scraping {len(empty_list)} / {total} empty statutes")
    
    filled = 0
    still_empty = 0
    errors = 0
    
    for i, entry in enumerate(empty_list):
        statute_name = entry["name"]
        file_path = Path(entry["path"])
        
        if not statute_name:
            log.info(f"  [{i+1}/{len(empty_list)}] SKIP: no statute name")
            still_empty += 1
            continue
        
        log.info(f"  [{i+1}/{len(empty_list)}] {statute_name[:60]}...")
        
        # Get sections
        time.sleep(REQUEST_DELAY)
        sections = pls.get_statute_sections(statute_name)
        
        if not sections:
            log.info(f"    No sections found on PLS")
            still_empty += 1
            continue
        
        # Fetch content for each section
        full_text_parts = []
        full_html_parts = []
        
        for sec in sections:
            section_id = sec.get("section_id", "")
            if not section_id:
                continue
            
            time.sleep(REQUEST_DELAY * 0.5)
            html, text = pls.get_section_content(section_id)
            
            if text:
                number = sec.get("number", "")
                full_text_parts.append(f"[Section {number}]\n{text}")
                full_html_parts.append(f"<!-- Section {number} -->\n{html}")
        
        if full_text_parts:
            # Update the JSON file with body text
            try:
                data = json.load(open(file_path, encoding="utf-8"))
                data["full_text"] = "\n\n".join(full_html_parts)
                data["body_text"] = "\n\n".join(full_text_parts)
                data["rescrape_date"] = datetime.now().isoformat()
                data["section_count"] = len(sections)
                
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                filled += 1
                log.info(f"    FILLED: {len(full_text_parts)} sections, {sum(len(p) for p in full_text_parts)} chars")
            except Exception as e:
                log.error(f"    Save error: {e}")
                errors += 1
        else:
            still_empty += 1
            log.info(f"    Still empty (PLS has no content)")
        
        # Session health check every 50 statutes
        if (i + 1) % 50 == 0:
            time.sleep(5)
            log.info(f"  Progress: {filled} filled, {still_empty} empty, {errors} errors")
    
    return filled, still_empty, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--letter", type=str, help="Only this letter")
    parser.add_argument("--limit", type=int, help="Max statutes to process")
    parser.add_argument("--dry-run", action="store_true", help="Count only")
    args = parser.parse_args()
    
    log.info("=" * 50)
    log.info("LEGISLATION BODY RE-SCRAPER")
    log.info("=" * 50)
    
    empty = find_empty_legislation(args.letter)
    log.info(f"Found {len(empty)} empty legislation files")
    
    if args.dry_run:
        # Show breakdown by letter
        from collections import Counter
        by_letter = Counter(e["letter"] for e in empty)
        for letter, count in sorted(by_letter.items()):
            log.info(f"  {letter}: {count} empty")
        return
    
    if not empty:
        log.info("Nothing to re-scrape!")
        return
    
    pls = PLSLegislation()
    if not pls.login():
        log.error("Login failed")
        sys.exit(2)
    
    filled, still_empty, errors = rescrape(pls, empty, args.limit)
    
    log.info(f"\n{'=' * 50}")
    log.info(f"RESULTS: Filled {filled} | Still empty {still_empty} | Errors {errors}")
    log.info(f"{'=' * 50}")
    
    # Save results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.json", "w", encoding="utf-8") as f:
        json.dump({"filled": filled, "still_empty": still_empty, "errors": errors, "total_checked": len(empty)}, f)


if __name__ == "__main__":
    main()
