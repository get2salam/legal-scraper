#!/usr/bin/env python3
"""
scrape_new_reporters.py — Scrape newly discovered PLS reporters

Handles reporters found by reporter_discovery.py that aren't in the main
scraper's REPORTERS list. Uses the same PLS API + login flow.

New reporters discovered (Mar 18, 2026):
    - PLC(CS)   — Civil Service cases (~800+ confirmed)
    - CLCN      — CLC Notes variant
    - PCRLJN    — PCrLJ Notes variant  
    - PLC(CS)N  — PLC(CS) Notes variant
    - YLRN      — YLR Notes variant

Usage:
    python scrape_new_reporters.py                          # Scrape all new reporters
    python scrape_new_reporters.py --reporter "PLC(CS)"     # Single reporter
    python scrape_new_reporters.py --year 2024              # Single year
    python scrape_new_reporters.py --probe                  # Just count cases, don't download
    python scrape_new_reporters.py --start-year 2020 --end-year 2026  # Year range
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

# Add scraper directory to path
SCRAPER_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper")
sys.path.insert(0, str(SCRAPER_DIR))

from dotenv import load_dotenv

load_dotenv(SCRAPER_DIR / ".env")

# ── Config ──────────────────────────────────────────────────────────────────

DATA_DIR = SCRAPER_DIR / "data_v2"
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\new-reporter-results")

# New reporters to scrape (discovered Mar 18, 2026)
NEW_REPORTERS = ["PLC(CS)", "CLCN", "PCRLJN", "PLC(CS)N", "YLRN"]

# Priority order: PLC(CS) first (confirmed active), then Notes variants
PRIORITY_ORDER = ["PLC(CS)", "PLC(CS)N", "CLCN", "PCRLJN", "YLRN"]

YEAR_MIN = 1947
YEAR_MAX = 2026
REQUEST_DELAY = 3.0  # Seconds between PLS requests (be respectful)

PLS_USER = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
PLS_PASS = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
BASE_URL = "https://www.pakistanlawsite.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── PLS Session ─────────────────────────────────────────────────────────────

try:
    from curl_cffi import requests as cffi_requests
    SESSION_CLASS = cffi_requests.Session
    USE_CFFI = True
except ImportError:
    import requests
    SESSION_CLASS = requests.Session
    USE_CFFI = False


class PLSSession:
    """Manages PLS login and API requests."""

    def __init__(self):
        self.session = SESSION_CLASS()
        if USE_CFFI:
            self.session.impersonate = "chrome"
        self.logged_in = False

    def login(self):
        """Login to PLS using ClearLoginHistory flow."""
        log.info("Logging in to PLS...")
        try:
            # Clear any existing session
            resp = self.session.post(
                f"{BASE_URL}/Login/ClearLoginHistory",
                data={"Login.UserName": PLS_USER, "Login.Password": PLS_PASS},
                timeout=30,
            )
            if resp.status_code == 200:
                self.logged_in = True
                log.info("Login successful")
                return True
            else:
                log.error(f"Login failed: HTTP {resp.status_code}")
                return False
        except Exception as e:
            log.error(f"Login error: {e}")
            return False

    def citation_search(self, year, reporter):
        """Search PLS for cases by year and reporter."""
        try:
            resp = self.session.post(
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
                timeout=30,
            )
            if resp.status_code != 200:
                return []

            # Parse case IDs from response
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            cases = []

            # Method 1: rows with class="caseType" — used by ALL reporters
            # Structure: <tr class="caseType"><td>num</td><td>citation</td><td>parties+judge</td><td>court</td><td><input casetypeid="ID"></td></tr>
            for row in soup.find_all("tr", class_="caseType"):
                tds = row.find_all("td")
                case_id = ""
                citation = ""
                court = ""
                judge = ""
                parties = ""

                # Extract citation from 2nd td
                if len(tds) >= 2:
                    citation = tds[1].get_text(strip=True)

                # Extract court from 4th td
                if len(tds) >= 4:
                    court = tds[3].get_text(strip=True)

                # Extract judge from darkred span
                judge_elem = row.find("span", style=lambda s: s and "darkred" in s)
                if judge_elem:
                    judge = judge_elem.get_text(strip=True)

                # Extract parties from 3rd td (before spans)
                if len(tds) >= 3:
                    parts = tds[2].get_text(strip=True).split("Versus")
                    if len(parts) == 2:
                        parties = f"{parts[0].strip()} v. {parts[1].strip().split(chr(10))[0].strip()}"

                # Extract case ID from input[casetypeid] — PLS internal ID
                input_elem = row.find("input", attrs={"casetypeid": True})
                if input_elem:
                    case_id = input_elem.get("casetypeid", "")

                # Fallback: try casename/caseName attributes
                if not case_id:
                    for attr in ["casename", "caseName"]:
                        elem = row.find(attrs={attr: True})
                        if elem:
                            case_id = elem.get(attr, "")
                            break

                if citation:
                    cases.append({
                        "case_id": case_id or citation.replace(" ", ""),
                        "citation": citation,
                        "court": court,
                        "judge": judge,
                        "parties": parties,
                    })

            # Method 2: anchor tags with caseName (older format)
            if not cases:
                for link in soup.find_all("a", attrs={"casename": True}):
                    case_id = link.get("casename", "")
                    citation = link.get_text(strip=True)
                    if case_id:
                        cases.append({"case_id": case_id, "citation": citation})

            return cases
        except Exception as e:
            log.error(f"Search error for {year} {reporter}: {e}")
            return []

    def get_case(self, case_id):
        """Download a case by its internal ID."""
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/GetCaseFile",
                data={"caseName": case_id, "headNotes": 0},
                timeout=60,
            )
            if resp.status_code == 200 and len(resp.text) > 100:
                # Decode JSON-wrapped HTML
                try:
                    content = json.loads(resp.text)
                except (json.JSONDecodeError, ValueError):
                    content = resp.text
                return content
            return None
        except Exception as e:
            log.error(f"GetCaseFile error for {case_id}: {e}")
            return None


# ── Scraper Logic ───────────────────────────────────────────────────────────

def safe_filename(citation):
    """Convert citation to safe filename."""
    return citation.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")


def save_case(reporter, year, citation, case_id, content, case_meta=None):
    """Save case in all required formats."""
    reporter_clean = reporter.replace("(", "").replace(")", "")
    out_dir = DATA_DIR / reporter_clean / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = safe_filename(citation)

    # JSON format
    case_data = {
        "citation": citation,
        "reporter": reporter,
        "year": year,
        "case_name": case_id,
        "court": case_meta.get("court", "") if case_meta else "",
        "judges": case_meta.get("judge", "") if case_meta else "",
        "parties": case_meta.get("parties", "") if case_meta else "",
        "judgment": content if isinstance(content, str) else json.dumps(content),
        "scraped_at": datetime.now().isoformat(),
    }

    json_path = out_dir / f"{filename}.json"
    if json_path.exists():
        return False  # Already scraped

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(case_data, f, ensure_ascii=False, indent=2)

    # Original HTML
    html_dir = out_dir / "original_html"
    html_dir.mkdir(exist_ok=True)
    html_content = content if isinstance(content, str) else json.dumps(content)
    with open(html_dir / f"{filename}.html", "w", encoding="utf-8") as f:
        f.write(html_content)

    # Readable HTML — decode unicode escapes + clean up
    readable_dir = out_dir / "readable_html"
    readable_dir.mkdir(exist_ok=True)
    try:
        readable = html_content
        # Decode JSON-wrapped content
        if readable.startswith('"') and readable.endswith('"'):
            try:
                readable = json.loads(readable)
            except (json.JSONDecodeError, ValueError):
                readable = readable[1:-1]
        # Decode unicode escapes
        import re as _re
        def _decode_esc(m):
            try:
                return chr(int(m.group(1), 16))
            except (ValueError, OverflowError):
                return m.group(0)
        readable = _re.sub(r'\\u([0-9a-fA-F]{4})', _decode_esc, readable)
        readable = readable.replace('\\r\\n', '\n').replace('\\n', '\n')
        readable = readable.replace('\\r', '\r').replace('\\t', '\t')
        readable = readable.replace('\\"', '"').replace('\\\\', '\\')
        with open(readable_dir / f"{filename}.html", "w", encoding="utf-8") as f:
            f.write(readable)
    except Exception:
        pass  # Non-critical, don't fail the whole case

    # JSONL — append to year-level JSONL file
    jsonl_path = out_dir / f"{year}_{reporter_clean}.jsonl"
    try:
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(case_data, ensure_ascii=False) + "\n")
    except Exception:
        pass

    return True


def probe_reporter(pls, reporter, sample_years=None):
    """Quick probe to count cases for a reporter without downloading."""
    if sample_years is None:
        sample_years = [2024, 2023, 2022, 2020, 2015, 2010, 2005, 2000, 1990, 1980, 1970]

    log.info(f"\nProbing {reporter}...")
    total = 0
    results = {}

    for year in sample_years:
        cases = pls.citation_search(year, reporter)
        count = len(cases)
        if count > 0:
            results[year] = count
            total += count
            log.info(f"  {year}: {count} cases")
        time.sleep(REQUEST_DELAY)

    log.info(f"  Total in sampled years: {total}")
    return results, total


def scrape_reporter(pls, reporter, start_year=None, end_year=None):
    """Scrape all cases for a reporter across years."""
    start_year = start_year or YEAR_MAX
    end_year = end_year or YEAR_MIN

    log.info(f"\nScraping {reporter}: {start_year} down to {end_year}")

    total_scraped = 0
    total_skipped = 0
    total_errors = 0
    empty_years = 0

    # Scrape newest first
    for year in range(start_year, end_year - 1, -1):
        cases = pls.citation_search(year, reporter)

        if not cases:
            empty_years += 1
            if empty_years >= 5:
                log.info(f"  5 consecutive empty years at {year}, stopping")
                break
            continue
        else:
            empty_years = 0

        log.info(f"  {year} {reporter}: {len(cases)} cases found")

        for case in cases:
            time.sleep(REQUEST_DELAY)

            content = pls.get_case(case["case_id"])
            if content:
                citation = case.get("citation", f"{year} {reporter} {case['case_id']}")
                saved = save_case(reporter, year, citation, case["case_id"], content, case)
                if saved:
                    total_scraped += 1
                else:
                    total_skipped += 1
            else:
                total_errors += 1

        time.sleep(REQUEST_DELAY)

    return total_scraped, total_skipped, total_errors


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape new PLS reporters")
    parser.add_argument("--reporter", type=str, help="Single reporter to scrape")
    parser.add_argument("--year", type=int, help="Single year")
    parser.add_argument("--start-year", type=int, default=YEAR_MAX, help="Start year (newest)")
    parser.add_argument("--end-year", type=int, default=YEAR_MIN, help="End year (oldest)")
    parser.add_argument("--probe", action="store_true", help="Probe mode: count cases only")
    args = parser.parse_args()

    reporters = [args.reporter] if args.reporter else PRIORITY_ORDER

    if not PLS_USER or not PLS_PASS:
        log.error("PLS credentials not found in .env")
        sys.exit(2)

    pls = PLSSession()
    if not pls.login():
        log.error("PLS login failed")
        sys.exit(2)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")

    if args.probe:
        # Probe mode
        log.info("=" * 50)
        log.info("NEW REPORTER PROBE")
        log.info("=" * 50)

        all_results = {}
        for reporter in reporters:
            results, total = probe_reporter(pls, reporter)
            all_results[reporter] = {"years": results, "total_sampled": total}

        # Save results
        with open(RESULTS_DIR / f"probe-{today}.json", "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2)

        log.info("\n" + "=" * 50)
        log.info("PROBE RESULTS")
        log.info("=" * 50)
        for rep, data in all_results.items():
            log.info(f"  {rep}: {data['total_sampled']} cases in sampled years")
            for y, c in sorted(data.get("years", {}).items(), reverse=True):
                log.info(f"    {y}: {c}")

    else:
        # Scrape mode
        log.info("=" * 50)
        log.info("NEW REPORTER SCRAPER")
        log.info("=" * 50)

        for reporter in reporters:
            if args.year:
                scraped, skipped, errors = scrape_reporter(
                    pls, reporter, start_year=args.year, end_year=args.year
                )
            else:
                scraped, skipped, errors = scrape_reporter(
                    pls, reporter,
                    start_year=args.start_year,
                    end_year=args.end_year,
                )

            log.info(f"\n{reporter}: Scraped {scraped} | Skipped {skipped} | Errors {errors}")

            # Save progress
            with open(RESULTS_DIR / f"scrape-{reporter.replace('(','').replace(')','')}-{today}.json", "w", encoding="utf-8") as f:
                json.dump({
                    "reporter": reporter,
                    "date": today,
                    "scraped": scraped,
                    "skipped": skipped,
                    "errors": errors,
                }, f, indent=2)


if __name__ == "__main__":
    main()
