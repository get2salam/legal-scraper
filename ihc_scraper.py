#!/usr/bin/env python3
"""
IHC Scraper — Islamabad High Court Judgments
=============================================
Scrapes judgments from https://mis.ihc.gov.pk via its ASMX web service API.

API Endpoints:
  - ihc.asmx/Juges_GA         — list judges (sitting/retired)
  - ihc.asmx/srchDecisionClms — search judgments by judge/year
  - ihc.asmx/FillYear         — available years (1980–2026)
  - ihc.asmx/GetLatestJgmntsNew — latest published judgments

PDFs at: mis.ihc.gov.pk/attachments/judgements/{CASECODE}/1/{filename}.pdf

INTERNAL USE ONLY — never push to public GitHub.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
import random

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi required. Install: pip install curl_cffi")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    print("WARNING: PyMuPDF not installed. PDF text extraction disabled.")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://mis.ihc.gov.pk"
ASMX_URL = f"{BASE_URL}/ihc.asmx"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
CASES_DIR = DATA_ROOT / "court_cases" / "IHC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "IHC"
JSONL_DIR = DATA_ROOT / "court_cases"
PROGRESS_FILE = DATA_ROOT / "court_cases" / "ihc_progress.json"
LOG_DIR = PROJECT_ROOT / "logs"

MIN_DELAY = 5
MAX_DELAY = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 15

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ihc_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ihc_scraper")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

class IHCSession:
    """Wrapper around curl_cffi for IHC ASMX API requests."""

    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self._last_request_time = 0

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        delay = MIN_DELAY + random.random() * (MAX_DELAY - MIN_DELAY)
        remaining = delay - elapsed
        if remaining > 0:
            log.debug(f"Throttling {remaining:.1f}s")
            time.sleep(remaining)
        self._last_request_time = time.time()

    def _post_asmx(self, method: str, params: dict, timeout: int = 60) -> Any:
        """POST to ihc.asmx/{method} with JSON params."""
        url = f"{ASMX_URL}/{method}"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                r = self.session.post(url, headers=headers, data=json.dumps(params), timeout=timeout)
                r.raise_for_status()
                data = r.json()
                if "d" in data:
                    parsed = json.loads(data["d"])
                    return parsed
                return data
            except Exception as e:
                log.warning(f"{method} attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
        return None

    def get_judges(self, retired: bool = False) -> List[Dict]:
        """Get list of judges (sitting or retired)."""
        result = self._post_asmx("Juges_GA", {
            "_params": {"PISRETIRED": 1 if retired else 0, "CUR_REC": None}
        })
        if isinstance(result, list):
            return result
        return []

    def get_years(self) -> List[str]:
        """Get available case years."""
        result = self._post_asmx("FillYear", {})
        if isinstance(result, list):
            return [y["Code"] for y in result]
        return []

    def search_judgments(self, judge_id: int = 0, year: int = 0,
                         case_no: int = 0, party: str = "",
                         landmark: int = 0, afr: str = "0") -> List[Dict]:
        """
        Search judgments via srchDecisionClms.
        
        Args:
            judge_id: Judge ID (0 = all)
            year: Case year (0 = all)
            case_no: Case number (0 = all)
            party: Party name filter
            landmark: 0=all, 1=landmark only
            afr: '0'=all, '1'=reported only
        """
        result = self._post_asmx("srchDecisionClms", {
            "PCASENO": str(case_no),
            "PJUG": str(judge_id),
            "PADV": "0",
            "PYEAR": str(year),
            "pPrty": party,
            "PDDATE": "01/01/1900",
            "PLANDMARK": landmark,
            "PAFR": afr,
        })
        if isinstance(result, list):
            return result
        if result == "empty":
            return []
        return []

    def get_latest_judgments(self) -> List[Dict]:
        """Get latest published judgments."""
        result = self._post_asmx("GetLatestJgmntsNew", {})
        if isinstance(result, list):
            return result
        return []

    def download_pdf(self, attachment_path: str, dest_path: Path) -> bool:
        """Download a judgment PDF from the attachment path."""
        if not attachment_path or attachment_path.strip() in ("", "-"):
            return False

        url = f"{BASE_URL}{attachment_path}" if attachment_path.startswith("/") else attachment_path
        self._throttle()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code == 404:
                    log.warning(f"PDF not found: {attachment_path}")
                    return False
                r.raise_for_status()
                if len(r.content) < 100:
                    log.warning(f"PDF too small ({len(r.content)} bytes)")
                    return False
                if not r.content[:4] == b"%PDF":
                    log.warning(f"Not a PDF file: {attachment_path}")
                    return False
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(r.content)
                log.debug(f"Downloaded PDF ({len(r.content)} bytes): {dest_path.name}")
                return True
            except Exception as e:
                log.warning(f"PDF attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
        return False


# ---------------------------------------------------------------------------
# Data Processing
# ---------------------------------------------------------------------------

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF."""
    if fitz is None:
        return ""
    try:
        doc = fitz.open(str(pdf_path))
        parts = [page.get_text() for page in doc]
        doc.close()
        return "\n".join(parts).strip()
    except Exception as e:
        log.warning(f"PDF extraction failed for {pdf_path.name}: {e}")
        return ""


def normalize_date(date_str: str) -> str:
    """Convert IHC date formats to YYYY-MM-DD."""
    if not date_str or date_str.strip() in ("", "-", "N/A"):
        return ""
    date_str = date_str.strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_year_from_case(case_no: str) -> Optional[int]:
    """Extract year from case number like 'Writ Petition-2795-2025'."""
    m = re.search(r"[-/](\d{4})$", case_no.strip())
    if m:
        y = int(m.group(1))
        if 1950 <= y <= 2100:
            return y
    return None


def make_safe_filename(text: str) -> str:
    """Convert to filesystem-safe filename."""
    s = re.sub(r"[./\\:*?\"<>|]", "_", text.strip())
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s[:120] if s else hashlib.md5(text.encode()).hexdigest()[:12]


def build_case_json(record: Dict, pdf_text: str = "") -> Dict:
    """Build standardized JSON from an IHC API record."""
    case_no = (record.get("CASENO") or "").strip()
    parties = (record.get("PARTIES") or "").strip()
    ddate = normalize_date(record.get("DDATE", ""))
    attachments = (record.get("ATTACHMENTS") or "").strip()

    # Parse party names
    party_parts = re.split(r"\s+(?:VS|vs|Vs|v\.)\s+", parties, maxsplit=1)
    petitioner = party_parts[0].strip() if len(party_parts) > 0 else ""
    respondent = party_parts[1].strip() if len(party_parts) > 1 else ""

    # Citation
    citation = (record.get("O_CITATION") or "").strip()
    if citation in ("Citation Awaited", "-", ""):
        citation_imp = (record.get("O_CITATION_IMP") or "").strip()
        if citation_imp and citation_imp != "-":
            citation = citation_imp

    pdf_url = f"{BASE_URL}{attachments}" if attachments and attachments.startswith("/") else ""

    return {
        "source": "IHC",
        "court": "Islamabad High Court",
        "case_number": case_no,
        "o_id": record.get("O_ID"),
        "case_code": record.get("CASECODE"),
        "parent_case_code": record.get("PRNTCASECODE"),
        "parties": {
            "petitioner": petitioner,
            "respondent": respondent,
            "full": parties,
        },
        "case_subject": (record.get("O_SUBJECT") or "").strip() or None,
        "bench": (record.get("BENCHNAME") or "").strip() or None,
        "author_judge": (record.get("AUTHOR_JUDGES") or "").strip() or None,
        "judge_name": (record.get("JUDGENAME") or "").strip() or None,
        "judgment_date": ddate,
        "citation": citation if citation and citation != "-" else None,
        "headnote": (record.get("O_IHC_HEADNOTE") or "").strip() or None,
        "discussed_laws": (record.get("O_UNDERSECTION") or "").strip() or None,
        "description": (record.get("O_REMARKS") or "").strip() or None,
        "is_landmark": record.get("ISLANDMARK") == 1 or record.get("ISLANDMARK") == "1",
        "approved_for_reporting": record.get("O_AFR") == 1 or record.get("O_AFR") == "1",
        "report_type": record.get("RPTTYPE"),
        "sc_appeal": {
            "afr": record.get("O_SC_AFR"),
            "citation": record.get("O_SC_CITATION"),
            "result": record.get("O_SC_RESULT"),
            "status": record.get("O_SC_STATUS"),
        } if record.get("O_SC_AFR") else None,
        "judgment_text": pdf_text if pdf_text else None,
        "pdf_url": pdf_url if pdf_url else None,
        "attachment_path": attachments if attachments and attachments != "-" else None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_html(case_data: Dict) -> str:
    """Generate readable HTML from case data."""
    title = case_data["parties"].get("full") or case_data.get("case_number") or "Unknown"
    text = case_data.get("judgment_text") or "(No text extracted)"
    paragraphs = text.split("\n\n") if "\n\n" in text else text.split("\n")
    body_html = "\n".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())

    headnote = case_data.get("headnote") or ""
    headnote_html = f"<div class='headnote'><h3>Head Notes</h3><p>{headnote}</p></div>" if headnote and headnote != "-" else ""

    laws = case_data.get("discussed_laws") or ""
    laws_html = f"<div class='laws'><h3>Discussed Laws</h3><p>{laws}</p></div>" if laws and laws != "-" else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Islamabad High Court</title>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.8; color: #333; }}
        .header {{ border-bottom: 2px solid #1a5276; margin-bottom: 20px; padding-bottom: 10px; }}
        .meta dt {{ font-weight: bold; float: left; clear: left; width: 180px; }}
        .meta dd {{ margin-left: 190px; margin-bottom: 5px; }}
        .headnote, .laws {{ background: #f0f8ff; padding: 10px; border-left: 4px solid #1a5276; margin: 15px 0; }}
        .body {{ text-align: justify; }}
        p {{ margin-bottom: 1em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <h2>{case_data.get('case_number', '')}</h2>
    </div>
    <dl class="meta">
        <dt>Court:</dt><dd>Islamabad High Court</dd>
        <dt>Case Subject:</dt><dd>{case_data.get('case_subject') or 'N/A'}</dd>
        <dt>Author Judge:</dt><dd>{case_data.get('author_judge') or 'N/A'}</dd>
        <dt>Bench:</dt><dd>{case_data.get('bench') or 'N/A'}</dd>
        <dt>Judgment Date:</dt><dd>{case_data.get('judgment_date') or 'N/A'}</dd>
        <dt>Citation:</dt><dd>{case_data.get('citation') or 'N/A'}</dd>
        <dt>Approved for Reporting:</dt><dd>{'Yes' if case_data.get('approved_for_reporting') else 'No'}</dd>
    </dl>
    {headnote_html}
    {laws_html}
    <div class="body">
        {body_html}
    </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Progress Tracking
# ---------------------------------------------------------------------------

class ProgressTracker:
    def __init__(self, path: Path = PROGRESS_FILE):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "court": "IHC",
            "started_at": None,
            "last_updated": None,
            "years_completed": [],
            "years_in_progress": {},
            "judges_completed": [],
            "total_cases_found": 0,
            "total_cases_scraped": 0,
            "total_pdfs_downloaded": 0,
            "total_pdfs_failed": 0,
            "errors": [],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_year_start(self, year: int, total: int):
        if not self.data["started_at"]:
            self.data["started_at"] = datetime.now(timezone.utc).isoformat()
        self.data["years_in_progress"][str(year)] = {
            "total": total, "scraped": 0, "pdfs_ok": 0, "pdfs_fail": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save()

    def mark_case_done(self, year: int, pdf_ok: bool):
        ykey = str(year)
        if ykey in self.data["years_in_progress"]:
            self.data["years_in_progress"][ykey]["scraped"] += 1
            if pdf_ok:
                self.data["years_in_progress"][ykey]["pdfs_ok"] += 1
            else:
                self.data["years_in_progress"][ykey]["pdfs_fail"] += 1
        self.data["total_cases_scraped"] += 1
        if pdf_ok:
            self.data["total_pdfs_downloaded"] += 1
        else:
            self.data["total_pdfs_failed"] += 1

    def mark_year_done(self, year: int):
        ykey = str(year)
        self.data["years_in_progress"].pop(ykey, None)
        if year not in self.data["years_completed"]:
            self.data["years_completed"].append(year)
            self.data["years_completed"].sort()
        self.save()

    def is_year_done(self, year: int) -> bool:
        return year in self.data["years_completed"]

    def is_case_scraped(self, year: int, case_id: str) -> bool:
        """Check if a specific case JSON already exists."""
        safe = make_safe_filename(case_id)
        json_path = CASES_DIR / str(year) / f"{safe}.json"
        return json_path.exists()

    def add_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:200],
        })
        self.data["errors"] = self.data["errors"][-100:]

    def print_status(self):
        print("\n" + "=" * 60)
        print("  Islamabad High Court Scraper — Progress Report")
        print("=" * 60)
        print(f"  Started:          {self.data.get('started_at', 'Never')}")
        print(f"  Last updated:     {self.data.get('last_updated', 'Never')}")
        print(f"  Years completed:  {len(self.data['years_completed'])}")
        print(f"  Cases scraped:    {self.data['total_cases_scraped']}")
        print(f"  PDFs downloaded:  {self.data['total_pdfs_downloaded']}")
        print(f"  PDFs failed:      {self.data['total_pdfs_failed']}")
        print()
        if self.data["years_completed"]:
            print(f"  Completed: {', '.join(str(y) for y in sorted(self.data['years_completed']))}")
        if self.data["years_in_progress"]:
            print("  In progress:")
            for ykey, info in sorted(self.data["years_in_progress"].items()):
                print(f"    {ykey}: {info['scraped']}/{info['total']} "
                      f"(PDFs: {info['pdfs_ok']} ok, {info['pdfs_fail']} fail)")
        if self.data["errors"]:
            print(f"\n  Recent errors ({len(self.data['errors'])}):")
            for err in self.data["errors"][-5:]:
                print(f"    [{err['time'][:19]}] {err['error'][:80]}")
        print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# JSONL
# ---------------------------------------------------------------------------

def append_jsonl(filepath: Path, record: dict):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    compact = {k: v for k, v in record.items() if k not in ("judgment_text",)}
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(compact, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Scraping Logic
# ---------------------------------------------------------------------------

def scrape_year(session: IHCSession, year: int, progress: ProgressTracker,
                skip_existing: bool = True):
    """Scrape all judgments for a given year."""
    if progress.is_year_done(year) and skip_existing:
        log.info(f"Year {year} already completed, skipping")
        return

    log.info(f"{'=' * 50}")
    log.info(f"Scraping IHC year {year}")
    log.info(f"{'=' * 50}")

    # Search all judgments for this year (all judges, all types)
    records = session.search_judgments(year=year, landmark=0, afr="0")
    if not records:
        log.info(f"No records for year {year}")
        progress.mark_year_done(year)
        return

    log.info(f"Found {len(records)} records for year {year}")
    progress.data["total_cases_found"] += len(records)
    progress.mark_year_start(year, len(records))

    year_dir = CASES_DIR / str(year)
    html_year_dir = HTML_DIR / str(year)
    pdf_dir = year_dir / "pdf"
    jsonl_path = JSONL_DIR / f"IHC_{year}.jsonl"

    year_dir.mkdir(parents=True, exist_ok=True)
    html_year_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        case_no = (record.get("CASENO") or "").strip()
        o_id = record.get("O_ID", "")
        safe_name = make_safe_filename(case_no) if case_no else f"oid_{o_id}"
        json_path = year_dir / f"{safe_name}.json"

        if skip_existing and json_path.exists():
            log.debug(f"  [{i+1}/{len(records)}] Already exists: {safe_name}")
            progress.mark_case_done(year, True)
            continue

        log.info(f"  [{i+1}/{len(records)}] {case_no}")

        # Download PDF
        attachment = (record.get("ATTACHMENTS") or "").strip()
        pdf_ok = False
        pdf_text = ""
        if attachment and attachment != "-":
            pdf_path = pdf_dir / f"{safe_name}.pdf"
            if pdf_path.exists():
                pdf_ok = True
                pdf_text = extract_pdf_text(pdf_path)
            else:
                pdf_ok = session.download_pdf(attachment, pdf_path)
                if pdf_ok:
                    pdf_text = extract_pdf_text(pdf_path)

        # Build case JSON
        case_data = build_case_json(record, pdf_text)

        # Save JSON
        json_path.write_text(json.dumps(case_data, indent=2, ensure_ascii=False), encoding="utf-8")

        # Save readable HTML
        html_path = html_year_dir / f"{safe_name}.html"
        html_path.write_text(generate_html(case_data), encoding="utf-8")

        # Append to JSONL
        append_jsonl(jsonl_path, case_data)
        append_jsonl(JSONL_DIR / "IHC_master.jsonl", case_data)

        progress.mark_case_done(year, pdf_ok)

        # Save progress every 10 cases
        if (i + 1) % 10 == 0:
            progress.save()
            log.info(f"  Progress saved: {i+1}/{len(records)}")

    progress.mark_year_done(year)
    log.info(f"Year {year} complete: {len(records)} records processed")


def discover_judges(session: IHCSession):
    """Print all judges and their judgment counts."""
    print("\n=== IHC Judges Discovery ===\n")

    for label, retired in [("Sitting Justices", False), ("Former Justices", True)]:
        judges = session.get_judges(retired=retired)
        print(f"\n{label} ({len(judges)}):")
        print("-" * 70)
        for j in judges:
            jid = j["JUDGE_ID"]
            name = j["JUG_REALNAME"]
            # Quick count
            records = session.search_judgments(judge_id=jid, landmark=0, afr="0")
            count = len(records) if records else 0
            is_retired = "RETIRED" if j["ISRETIRED"] else "ACTIVE"
            print(f"  [{jid:3d}] {name:<45s} {is_retired:8s} Judgments: {count}")
    print()


def discover_years(session: IHCSession):
    """Print judgment counts per year."""
    print("\n=== IHC Year Discovery ===\n")
    years = session.get_years()
    total = 0
    for year in years:
        records = session.search_judgments(year=int(year), landmark=0, afr="0")
        count = len(records) if records else 0
        total += count
        bar = "█" * min(count, 50)
        print(f"  {year}: {count:4d}  {bar}")
    print(f"\n  Total: {total}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="IHC Judgment Scraper")
    parser.add_argument("--year", type=int, help="Scrape specific year")
    parser.add_argument("--all-years", action="store_true", help="Scrape all years (2026 down to 1980)")
    parser.add_argument("--status", action="store_true", help="Show progress report")
    parser.add_argument("--discover", action="store_true", help="Discover available judgments per year")
    parser.add_argument("--discover-judges", action="store_true", help="Discover judges and counts")
    parser.add_argument("--no-skip", action="store_true", help="Re-scrape even if already done")
    parser.add_argument("--limit", type=int, help="Limit records per year")
    args = parser.parse_args()

    progress = ProgressTracker()

    if args.status:
        progress.print_status()
        return

    session = IHCSession()

    if args.discover:
        discover_years(session)
        return

    if args.discover_judges:
        discover_judges(session)
        return

    skip = not args.no_skip

    if args.year:
        scrape_year(session, args.year, progress, skip_existing=skip)
    elif args.all_years:
        years = session.get_years()
        log.info(f"Scraping {len(years)} years: {years[0]} down to {years[-1]}")
        for year_str in years:
            try:
                scrape_year(session, int(year_str), progress, skip_existing=skip)
            except KeyboardInterrupt:
                log.info("Interrupted by user")
                progress.save()
                break
            except Exception as e:
                log.error(f"Error scraping year {year_str}: {e}")
                progress.add_error(f"Year {year_str}: {e}")
                progress.save()
    else:
        # Default: scrape current year
        current_year = datetime.now().year
        scrape_year(session, current_year, progress, skip_existing=skip)

    progress.save()
    progress.print_status()


if __name__ == "__main__":
    main()
