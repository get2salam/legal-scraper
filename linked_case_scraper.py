#!/usr/bin/env python3
"""
PLS Linked Case Scraper v1.0
=============================
Reads case citations from legislation statute_case_links.jsonl,
fetches the full case content from PLS, saves it, and maintains
a link mapping index.

Runs ALONGSIDE the existing scrapers as a separate process with
its own session, cookies, and slightly longer timing to avoid
overlapping with the main case scraper.

Features:
- Chrome 120 TLS fingerprint via curl_cffi (own session)
- Deduplicates citations from statute_case_links.jsonl
- Groups by (year, reporter) for efficient batch searching
- Checks existing scraped data before fetching
- Saves in all 4 formats: JSON, original HTML, readable HTML, JSONL
- Maintains linked_cases_index.json mapping
- Resumable progress tracking (saves after each case)
- Human-like delays with jitter (slightly longer than main scraper)
- Random breaks, reading pauses

Usage:
    python linked_case_scraper.py run              # Start/resume scraping
    python linked_case_scraper.py status            # Show progress stats
    python linked_case_scraper.py rebuild-index     # Rebuild link index from data
"""

import os
import re
import json
import time
import random
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any, Set, Tuple
from collections import defaultdict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
LINKS_FILE = LEGISLATION_DIR / "statute_case_links.jsonl"
INDEX_FILE = LEGISLATION_DIR / "linked_cases_index.json"
PROGRESS_FILE = DATA_DIR / "linked_cases_progress.json"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing — slightly longer than the main scraper to avoid overlap
MIN_DELAY = 2.0          # Minimum seconds between requests (main uses 1.5)
MAX_DELAY = 4.0          # Maximum seconds between requests (main uses 3.0)
LOGIN_DELAY = 6.0        # Delay after login (main uses 5.0)
RATE_LIMIT_BACKOFF = 90  # Seconds to wait if rate limited (main uses 60)
READING_DELAY_MIN = 3.0  # Minimum "reading" delay (main uses 2.0)
READING_DELAY_MAX = 7.0  # Maximum "reading" delay (main uses 6.0)

# Break simulation — offset from main scraper
REQUESTS_BEFORE_BREAK = 70   # Take a break every N requests (main uses 100)
BREAK_MIN = 45               # Minimum break seconds (main uses 30)
BREAK_MAX = 120              # Maximum break seconds (main uses 90)

# Known reporters
REPORTERS = {"SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | [linked] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("linked_case_scraper")


# ══════════════════════════════════════════════════════════════════════════════
# Data Class — same as pls_scraper_v2 for format compatibility
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Case:
    citation: str
    case_name: str
    title: str = ""
    court: str = ""
    date: str = ""
    judges: List[str] = field(default_factory=list)
    headnotes: str = ""
    judgment: str = ""
    judgment_raw: str = ""
    statutes_cited: List[str] = field(default_factory=list)
    cases_cited: List[str] = field(default_factory=list)
    fetched_at: str = ""

    def __post_init__(self):
        if not self.fetched_at:
            self.fetched_at = datetime.now().isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# Citation Utilities
# ══════════════════════════════════════════════════════════════════════════════

_CITATION_RE = re.compile(
    r"(\d{4})\s+(PLD|SCMR|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR)\s+(\d+)"
)


def clean_citation(raw: str) -> Optional[str]:
    """Normalise a citation string.  Returns canonical 'YEAR REPORTER PAGE' or None."""
    if not raw:
        return None
    m = _CITATION_RE.search(raw.strip())
    if not m:
        return None
    return f"{m.group(1)} {m.group(2)} {m.group(3)}"


def citation_parts(citation: str) -> Tuple[str, str, str]:
    """Split a clean citation into (year, reporter, page)."""
    parts = citation.split()
    return parts[0], parts[1], parts[2]


def safe_filename(citation: str) -> str:
    """Convert citation to safe filename — matches pls_scraper_v2 convention."""
    return re.sub(r"[^\w\-]", "_", citation)


# ══════════════════════════════════════════════════════════════════════════════
# Linked Case Scraper
# ══════════════════════════════════════════════════════════════════════════════

class LinkedCaseScraper:
    """Fetches case-law cited in legislation from PLS."""

    def __init__(self):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0.0
        self.requests_since_break = 0

        # JSONL duplicate sets (lazy-loaded)
        self._jsonl_sets: Dict[str, Set[str]] = {}
        self._master_set: Optional[Set[str]] = None

        # Ensure dirs exist
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        LEGISLATION_DIR.mkdir(parents=True, exist_ok=True)

    # ── Session / HTTP ────────────────────────────────────────────────────────

    def _create_session(self) -> Session:
        """Create a fresh curl_cffi session with Chrome 120 TLS impersonation."""
        s = Session(impersonate=BrowserType.chrome120)
        s.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", '
                         '"Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        })
        return s

    def _human_delay(
        self,
        min_s: float = None,
        max_s: float = None,
        reading: bool = False,
    ):
        """Wait a random human-like delay with Gaussian jitter."""
        if reading:
            min_s = min_s or READING_DELAY_MIN
            max_s = max_s or READING_DELAY_MAX
        else:
            min_s = min_s or MIN_DELAY
            max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s) + random.gauss(0, 0.6)
        delay = max(1.2, delay)
        time.sleep(delay)

    def _maybe_take_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            dur = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking a {dur:.0f}s break (human sim)…")
            time.sleep(dur)
            self.requests_since_break = 0

    def _request(
        self,
        method: str,
        url: str,
        retries: int = 3,
        **kwargs,
    ) -> Optional[Any]:
        """HTTP request with rate-limiting, retries, exponential backoff."""
        self._maybe_take_break()

        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)

        last_err = None
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)

                self.last_request_time = time.time()
                self.request_count += 1

                if resp.status_code in (403, 429, 500):
                    backoff = RATE_LIMIT_BACKOFF * (2 ** attempt)
                    logger.warning(
                        f"HTTP {resp.status_code} — backoff {backoff}s "
                        f"(attempt {attempt + 1}/{retries})"
                    )
                    time.sleep(backoff)
                    continue

                if resp.status_code != 200:
                    logger.warning(f"Unexpected HTTP {resp.status_code} for {url}")
                    return None

                return resp

            except Exception as e:
                last_err = e
                backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                logger.error(
                    f"Request error (attempt {attempt + 1}/{retries}): {e}"
                )
                time.sleep(backoff)

        logger.error(f"All {retries} attempts failed for {url}: {last_err}")
        return None

    # ── Login ─────────────────────────────────────────────────────────────────

    def login(self) -> bool:
        """Login to PLS with a fresh session (independent of other scrapers)."""
        logger.info("Logging in to PLS…")
        self.session = self._create_session()

        try:
            resp = self.session.get(f"{BASE_URL}/", timeout=30)
            if resp.status_code != 200:
                logger.error(f"Homepage load failed: HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Homepage load failed: {e}")
            return False

        self._human_delay(reading=True)

        csrf_m = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text,
        )
        if not csrf_m:
            logger.error("CSRF token not found on homepage")
            return False

        self._human_delay(2.5, 5)

        login_resp = self._request(
            "POST",
            f"{BASE_URL}/Login/Login",
            data={
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
                "__RequestVerificationToken": csrf_m.group(1),
            },
        )
        if not login_resp:
            logger.error("Login POST failed")
            return False

        self._human_delay(2, 3)

        check = self._request("GET", f"{BASE_URL}/Login/Check")
        if not check or "Logout" not in check.text:
            logger.error("Login verification failed")
            return False

        self.logged_in = True
        self.requests_since_break = 0
        logger.info("✓ Login successful!")
        self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 4)
        return True

    def _ensure_logged_in(self) -> bool:
        if not self.logged_in:
            return self.login()
        return True

    # ── Citation Search ───────────────────────────────────────────────────────

    def citation_search(self, year: int, reporter: str) -> List[Dict]:
        """Search PLS for all cases in a year+reporter. Returns [{citation, case_name}, …]."""
        if not self._ensure_logged_in():
            return []

        logger.info(f"CitationSearch: {year} {reporter}")
        resp = self._request(
            "POST",
            f"{BASE_URL}/Login/CitationSearch",
            data={
                "year": year,
                "book": reporter,
                "code": "",
                "court": "",
                "judge": "",
                "lawyer": "",
                "party": "",
            },
        )
        if not resp:
            return []

        results = self._parse_search_results(resp.text)
        logger.info(f"  Found {len(results)} cases for {year} {reporter}")
        return results

    def _parse_search_results(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        cases: List[Dict] = []

        # Format 1: table rows with class="caseType"
        for row in soup.find_all("tr", class_="caseType"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                citation_text = cells[1].get_text(strip=True)
                btn = row.find("input", attrs={"casetypeid": True})
                case_id = btn.get("casetypeid", "") if btn else ""
                cit = clean_citation(citation_text)
                if cit:
                    cases.append({"citation": cit, "case_name": case_id})

        # Format 2: caseLawTable
        for table in soup.find_all("table", class_="caseLawTable"):
            onclick = table.get("onclick", "")
            cn_m = re.search(r"'([^']+)'", onclick)
            case_name = cn_m.group(1) if cn_m else ""
            cit = clean_citation(table.get_text())
            if cit:
                cases.append({"citation": cit, "case_name": case_name})

        # Format 3: regex fallback
        if not cases:
            citations = re.findall(
                r"(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)",
                html,
            )
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)
            for i, raw in enumerate(citations):
                cit = clean_citation(raw)
                if cit:
                    cases.append({
                        "citation": cit,
                        "case_name": case_ids[i] if i < len(case_ids) else "",
                    })

        # deduplicate
        seen: Set[str] = set()
        unique: List[Dict] = []
        for c in cases:
            if c["citation"] not in seen:
                seen.add(c["citation"])
                unique.append(c)
        return unique

    # ── Case Fetching ─────────────────────────────────────────────────────────

    def fetch_case(self, case_id: str, citation: str = "") -> Optional[Case]:
        """Fetch full case via GetCaseFile. Returns Case or None."""
        if not self._ensure_logged_in():
            return None

        logger.info(f"  Fetching: {citation or case_id}")
        resp = self._request(
            "POST",
            f"{BASE_URL}/Login/GetCaseFile",
            data={"caseName": case_id, "headNotes": 0},
        )

        if (
            not resp
            or resp.text.strip() in ("1", '"1"', "")
            or len(resp.text) < 100
        ):
            logger.warning(f"  Empty/invalid content for {citation or case_id}")
            return None

        self._human_delay(reading=True)
        return self._parse_case_content(resp.text, citation, case_id)

    def _parse_case_content(
        self, html: str, citation: str, case_name: str
    ) -> Case:
        soup = BeautifulSoup(html, "html.parser")

        title = ""
        t_elem = soup.find(
            ["h1", "h2", "h3"], class_=re.compile(r"title|heading", re.I)
        )
        if t_elem:
            title = t_elem.get_text(strip=True)

        court = ""
        cm = re.search(
            r"(Supreme Court|High Court|Federal Shariat|Tribunal)[^<]*",
            html,
            re.I,
        )
        if cm:
            court = cm.group(0).strip()

        date = ""
        dm = re.search(r"(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})", html)
        if dm:
            date = dm.group(1)

        judges: List[str] = []
        js = soup.find(string=re.compile(r"Before|Coram|JUDGE", re.I))
        if js:
            parent = js.find_parent()
            if parent:
                judges = re.findall(
                    r"(?:Mr\.|Mrs\.|Justice|J\.)\s*"
                    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
                    parent.get_text(),
                )

        headnotes = ""
        hn = soup.find(["div", "p"], class_=re.compile(r"headnote", re.I))
        if hn:
            headnotes = hn.get_text(strip=True)

        judgment = ""
        for sel in [".judgment", ".caseText", "#caseContent", 'div[class*="case"]']:
            elem = soup.select_one(sel)
            if elem:
                judgment = elem.get_text(separator="\n", strip=True)
                break
        if not judgment:
            judgment = soup.get_text(separator="\n", strip=True)

        statutes = list(set(re.findall(r"(?:Act|Ordinance|Code|Rules?),?\s+\d{4}", html)))
        cited = list(
            set(
                re.findall(
                    r"\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR)\s+\d+", html
                )
            )
        )

        return Case(
            citation=citation,
            case_name=case_name,
            title=title,
            court=court,
            date=date,
            judges=judges,
            headnotes=headnotes,
            judgment=judgment,
            judgment_raw=html,
            statutes_cited=statutes,
            cases_cited=cited,
        )

    # ── Save (all 4 formats — identical to pls_scraper_v2) ───────────────────

    def _get_jsonl_set(self, path: Path) -> Set[str]:
        key = str(path)
        if key not in self._jsonl_sets:
            s: Set[str] = set()
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                obj = json.loads(line)
                                if "citation" in obj:
                                    s.add(obj["citation"])
                            except json.JSONDecodeError:
                                m = re.search(r'"citation":\s*"([^"]+)"', line)
                                if m:
                                    s.add(m.group(1))
                except Exception as e:
                    logger.warning(f"Could not load JSONL set from {path}: {e}")
            self._jsonl_sets[key] = s
        return self._jsonl_sets[key]

    def _get_master_set(self) -> Set[str]:
        if self._master_set is None:
            self._master_set = self._get_jsonl_set(DATA_DIR / "all_cases.jsonl")
        return self._master_set

    def _save_case(self, case: Case):
        """Save case in all 4 formats (JSON, original HTML, readable HTML, JSONL)."""
        year, reporter, page = citation_parts(case.citation)
        case_dir = DATA_DIR / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)

        case_dict = asdict(case)
        sf = safe_filename(case.citation)

        # 1. Individual JSON
        json_path = case_dir / f"{sf}.json"
        json_path.write_text(
            json.dumps(case_dict, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        # 2. Original HTML
        orig_dir = case_dir / "original"
        orig_dir.mkdir(parents=True, exist_ok=True)
        html_path = orig_dir / f"{sf}.html"
        raw = case.judgment_raw or case.judgment or ""
        if raw:
            try:
                html_path.write_text(raw, encoding="utf-8")
            except Exception as e:
                logger.warning(f"Could not save original HTML for {case.citation}: {e}")

        # 3. Reporter JSONL
        jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
        jset = self._get_jsonl_set(jsonl_path)
        if case.citation not in jset:
            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
            jset.add(case.citation)

        # 4. Master JSONL
        mset = self._get_master_set()
        if case.citation not in mset:
            with open(DATA_DIR / "all_cases.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
            mset.add(case.citation)

        # 5. Readable HTML
        self._save_readable_html(case, reporter, year, sf)

        logger.info(f"  Saved: {case.citation}")

    def _save_readable_html(
        self, case: Case, reporter: str, year: str, sf: str
    ):
        try:
            rd = DATA_DIR / "html" / reporter / year
            rd.mkdir(parents=True, exist_ok=True)
            path = rd / f"{sf}.html"

            judgment = case.judgment or ""
            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{case.citation} - {case.case_name or "Case"}</title>
<style>
body {{ font-family: Georgia, serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; background: #fafafa; }}
.header {{ background: #1a365d; color: white; padding: 20px; margin: -20px -20px 20px; }}
.citation {{ font-size: 1.4em; font-weight: bold; }}
.case-name {{ font-style: italic; margin-top: 10px; }}
.meta {{ background: #e2e8f0; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
.meta-item {{ margin: 5px 0; }}
.meta-label {{ font-weight: bold; color: #2d3748; }}
.judgment {{ background: white; padding: 20px; border: 1px solid #e2e8f0; text-align: justify; }}
.footer {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #e2e8f0; font-size: 0.9em; color: #718096; }}
</style>
</head>
<body>
<div class="header">
  <div class="citation">{case.citation}</div>
  <div class="case-name">{case.case_name or ""}</div>
</div>
<div class="meta">
  <div class="meta-item"><span class="meta-label">Court:</span> {case.court or "N/A"}</div>
  <div class="meta-item"><span class="meta-label">Judge:</span> {', '.join(case.judges) if case.judges else "N/A"}</div>
  <div class="meta-item"><span class="meta-label">Date:</span> {case.date or "N/A"}</div>
</div>
<div class="judgment">
{judgment}
</div>
<div class="footer">
Source: Pakistan Law Site | Scraped: {case.fetched_at or "N/A"} | Via: linked_case_scraper
</div>
</body>
</html>"""
            path.write_text(html, encoding="utf-8")
        except Exception as e:
            logger.warning(f"Could not save readable HTML for {case.citation}: {e}")

    # ── Progress ──────────────────────────────────────────────────────────────

    @staticmethod
    def _load_progress() -> Dict:
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "fetched": [],          # citations successfully fetched
            "not_found": [],        # citations not on PLS (no search match)
            "errors": [],           # citations that errored during fetch
            "searched_groups": [],   # "YEAR-REPORTER" groups already searched
            "last_updated": None,
        }

    @staticmethod
    def _save_progress(progress: Dict):
        progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(
            json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ── Load & Deduplicate Citations ──────────────────────────────────────────

    @staticmethod
    def load_citations() -> Tuple[
        List[str],
        Dict[str, List[Dict]],
    ]:
        """Load citations from statute_case_links.jsonl.

        Returns:
            unique_citations: deduplicated list of clean citation strings
            statute_map: {citation: [{statute_id, statute_title, section}, …]}
        """
        raw_entries: List[Dict] = []
        if not LINKS_FILE.exists():
            logger.error(f"Links file not found: {LINKS_FILE}")
            return [], {}

        with open(LINKS_FILE, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw_entries.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Bad JSON on line {lineno} of {LINKS_FILE}")

        logger.info(f"Loaded {len(raw_entries)} raw entries from statute_case_links.jsonl")

        # Deduplicate by clean citation
        statute_map: Dict[str, List[Dict]] = defaultdict(list)
        seen: Set[str] = set()
        unique: List[str] = []

        for entry in raw_entries:
            cit = clean_citation(entry.get("citation", ""))
            if not cit:
                continue
            statute_map[cit].append({
                "statute_id": entry.get("statute_id", ""),
                "statute_title": entry.get("statute_title", ""),
                "section": entry.get("section", ""),
            })
            if cit not in seen:
                seen.add(cit)
                unique.append(cit)

        logger.info(f"Unique citations: {len(unique)}")
        return unique, dict(statute_map)

    # ── Check Already Scraped ─────────────────────────────────────────────────

    @staticmethod
    def find_existing(citations: List[str]) -> Dict[str, str]:
        """Check which citations already have JSON files on disk.

        Returns {citation: filepath} for those that exist.
        """
        existing: Dict[str, str] = {}
        for cit in citations:
            year, reporter, page = citation_parts(cit)
            sf = safe_filename(cit)
            p = DATA_DIR / reporter / year / f"{sf}.json"
            if p.exists():
                existing[cit] = str(p)
        return existing

    # ── Link Index ────────────────────────────────────────────────────────────

    @staticmethod
    def build_index(
        statute_map: Dict[str, List[Dict]],
        existing: Dict[str, str],
        progress: Dict,
    ) -> Dict:
        """Build the linked_cases_index.json structure.

        {
          statute_id: {
            title: "…",
            linked_cases: {
              section: [{citation, status, path}, …]
            }
          }
        }
        """
        fetched_set = set(progress.get("fetched", []))
        not_found_set = set(progress.get("not_found", []))
        error_set = set(progress.get("errors", []))

        index: Dict[str, Dict] = {}

        for cit, entries in statute_map.items():
            # Determine status
            if cit in existing or cit in fetched_set:
                status = "scraped"
                year, reporter, page = citation_parts(cit)
                sf = safe_filename(cit)
                path = f"data_v2/{reporter}/{year}/{sf}.json"
            elif cit in not_found_set:
                status = "not_found"
                path = ""
            elif cit in error_set:
                status = "error"
                path = ""
            else:
                status = "pending"
                path = ""

            for e in entries:
                sid = e["statute_id"]
                stitle = e["statute_title"]
                section = e["section"]

                if sid not in index:
                    index[sid] = {"title": stitle, "linked_cases": {}}

                if section not in index[sid]["linked_cases"]:
                    index[sid]["linked_cases"][section] = []

                # Deduplicate within a section
                existing_cits = {
                    e["citation"] for e in index[sid]["linked_cases"][section]
                }
                if cit not in existing_cits:
                    index[sid]["linked_cases"][section].append({
                        "citation": cit,
                        "status": status,
                        "path": path,
                    })

        return index

    @staticmethod
    def save_index(index: Dict):
        INDEX_FILE.write_text(
            json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        logger.info(f"Saved link index → {INDEX_FILE}  ({len(index)} statutes)")

    # ══════════════════════════════════════════════════════════════════════════
    # Main Workflows
    # ══════════════════════════════════════════════════════════════════════════

    def cmd_status(self):
        """Show progress stats."""
        citations, statute_map = self.load_citations()
        existing = self.find_existing(citations)
        progress = self._load_progress()

        fetched_set = set(progress.get("fetched", []))
        not_found_set = set(progress.get("not_found", []))
        error_set = set(progress.get("errors", []))
        searched = set(progress.get("searched_groups", []))

        all_done = fetched_set | set(existing.keys()) | not_found_set | error_set
        pending = [c for c in citations if c not in all_done]

        # Group pending by reporter
        by_reporter: Dict[str, int] = defaultdict(int)
        for c in pending:
            _, rp, _ = citation_parts(c)
            by_reporter[rp] += 1

        print("=" * 60)
        print("  Linked Case Scraper — Status")
        print("=" * 60)
        print(f"  Total unique citations : {len(citations)}")
        print(f"  Already on disk        : {len(existing)}")
        print(f"  Fetched (this scraper) : {len(fetched_set)}")
        print(f"  Not found on PLS       : {len(not_found_set)}")
        print(f"  Errors                 : {len(error_set)}")
        print(f"  Pending                : {len(pending)}")
        print(f"  Searched groups done   : {len(searched)}")
        print(f"  Last updated           : {progress.get('last_updated', 'Never')}")
        if by_reporter:
            print(f"  Pending by reporter:")
            for rp in sorted(by_reporter):
                print(f"    {rp:>8}: {by_reporter[rp]}")
        print("=" * 60)

    def cmd_rebuild_index(self):
        """Rebuild the link index from existing data + progress."""
        citations, statute_map = self.load_citations()
        existing = self.find_existing(citations)
        progress = self._load_progress()
        index = self.build_index(statute_map, existing, progress)
        self.save_index(index)
        print(f"Index rebuilt with {len(index)} statutes.")

    def cmd_run(self):
        """Main scrape loop: fetch missing linked cases."""
        # 1. Load citations
        citations, statute_map = self.load_citations()
        if not citations:
            logger.error("No citations to process.")
            return

        # 2. Check existing
        existing = self.find_existing(citations)
        logger.info(f"Already on disk: {len(existing)} / {len(citations)}")

        # 3. Load progress
        progress = self._load_progress()
        fetched_set = set(progress.get("fetched", []))
        not_found_set = set(progress.get("not_found", []))
        error_set = set(progress.get("errors", []))
        searched_groups = set(progress.get("searched_groups", []))

        all_done = fetched_set | set(existing.keys()) | not_found_set | error_set
        pending = [c for c in citations if c not in all_done]
        logger.info(f"Pending citations: {len(pending)}")

        if not pending:
            logger.info("Nothing to do — all citations processed!")
            # Rebuild index and exit
            index = self.build_index(statute_map, existing, progress)
            self.save_index(index)
            return

        # 4. Group pending by (year, reporter)
        groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        for cit in pending:
            year, reporter, _ = citation_parts(cit)
            groups[(year, reporter)].append(cit)

        # Sort groups: newest first, then alphabetically by reporter
        sorted_groups = sorted(groups.keys(), key=lambda g: (-int(g[0]), g[1]))
        logger.info(f"Groups to search: {len(sorted_groups)}")

        # 5. Login
        if not self.login():
            logger.error("Login failed. Aborting.")
            return

        total_fetched = 0
        total_not_found = 0
        total_errors = 0
        session_start = time.time()

        try:
            for gidx, (year, reporter) in enumerate(sorted_groups):
                group_key = f"{year}-{reporter}"
                wanted = set(groups[(year, reporter)])

                # If we already searched this group and none were found, skip
                # But only skip if all wanted in this group are already accounted for
                remaining_wanted = wanted - all_done
                if not remaining_wanted:
                    logger.debug(f"Skipping group {group_key} — all citations done")
                    continue

                logger.info(
                    f"\n{'─' * 50}\n"
                    f"Group {gidx + 1}/{len(sorted_groups)}: "
                    f"{year} {reporter} — {len(remaining_wanted)} wanted\n"
                    f"{'─' * 50}"
                )

                # CitationSearch for this year+reporter
                self._human_delay()
                search_results = self.citation_search(int(year), reporter)

                # Build lookup: citation → case_name (casetypeid)
                result_map: Dict[str, str] = {}
                for sr in search_results:
                    result_map[sr["citation"]] = sr["case_name"]

                # Process each wanted citation
                for cit in sorted(remaining_wanted):
                    if cit in result_map:
                        case_id = result_map[cit]
                        if not case_id:
                            logger.warning(f"  No case_id for {cit}, marking not_found")
                            progress.setdefault("not_found", []).append(cit)
                            not_found_set.add(cit)
                            all_done.add(cit)
                            total_not_found += 1
                            self._save_progress(progress)
                            continue

                        # Fetch the case
                        self._human_delay()
                        case = self.fetch_case(case_id, cit)

                        if case:
                            self._save_case(case)
                            progress.setdefault("fetched", []).append(cit)
                            fetched_set.add(cit)
                            all_done.add(cit)
                            total_fetched += 1
                        else:
                            logger.warning(f"  Fetch failed for {cit}")
                            progress.setdefault("errors", []).append(cit)
                            error_set.add(cit)
                            all_done.add(cit)
                            total_errors += 1
                    else:
                        # Citation not in search results
                        logger.info(f"  Not found in search: {cit}")
                        progress.setdefault("not_found", []).append(cit)
                        not_found_set.add(cit)
                        all_done.add(cit)
                        total_not_found += 1

                    # Save progress after EACH citation
                    self._save_progress(progress)

                # Mark group as searched
                if group_key not in searched_groups:
                    progress.setdefault("searched_groups", []).append(group_key)
                    searched_groups.add(group_key)
                    self._save_progress(progress)

                # Periodic summary
                if (gidx + 1) % 10 == 0:
                    elapsed = time.time() - session_start
                    rate = total_fetched / (elapsed / 3600) if elapsed > 0 else 0
                    logger.info(
                        f"  ── Progress: {gidx + 1}/{len(sorted_groups)} groups | "
                        f"fetched={total_fetched} not_found={total_not_found} "
                        f"errors={total_errors} | {rate:.1f} cases/hr"
                    )

        except KeyboardInterrupt:
            logger.info("\nInterrupted by user. Saving progress…")
            self._save_progress(progress)

        # Final summary
        elapsed = time.time() - session_start
        elapsed_min = elapsed / 60
        rate = total_fetched / (elapsed / 3600) if elapsed > 0 else 0

        logger.info(f"\n{'═' * 60}")
        logger.info(f"  Session Complete")
        logger.info(f"  Fetched    : {total_fetched}")
        logger.info(f"  Not found  : {total_not_found}")
        logger.info(f"  Errors     : {total_errors}")
        logger.info(f"  Time       : {elapsed_min:.1f} min")
        logger.info(f"  Rate       : {rate:.1f} cases/hr")
        logger.info(f"{'═' * 60}")

        # Rebuild index
        logger.info("Rebuilding link index…")
        existing = self.find_existing(citations)
        index = self.build_index(statute_map, existing, progress)
        self.save_index(index)


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="PLS Linked Case Scraper — fetch cases cited in legislation"
    )
    parser.add_argument(
        "command",
        choices=["run", "status", "rebuild-index"],
        help="run: scrape missing cases | status: show stats | rebuild-index: rebuild link mapping",
    )
    args = parser.parse_args()

    scraper = LinkedCaseScraper()

    if args.command == "status":
        scraper.cmd_status()
    elif args.command == "rebuild-index":
        scraper.cmd_rebuild_index()
    elif args.command == "run":
        scraper.cmd_run()


if __name__ == "__main__":
    main()
