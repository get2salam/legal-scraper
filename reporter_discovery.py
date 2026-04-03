#!/usr/bin/env python3
"""
Reporter Discovery Agent
=========================
Discovers ALL available reporters on PLS by:
1. Scraping the search form dropdowns for reporter options
2. Brute-force testing known Pakistani legal reporter abbreviations
3. Comparing against our known list
4. Estimating case counts for any new reporters found
5. Optionally auto-triggering scraping for new reporters

Designed to run as a weekly cron job for bulletproof coverage.

Usage:
    python reporter_discovery.py                    # Full discovery scan
    python reporter_discovery.py --quick            # Quick check (form scrape only)
    python reporter_discovery.py --scrape-new       # Auto-scrape any new reporters found
    python reporter_discovery.py --deep             # Deep scan with year-by-year estimates
"""

import os
import re
import json
import time
import random
import logging
import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Set, Tuple

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
DISCOVERY_DIR = DATA_DIR / "discovery"
REPORT_FILE = DISCOVERY_DIR / "reporter_discovery.json"
HISTORY_FILE = DISCOVERY_DIR / "discovery_history.json"

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Known reporters we're already scraping
KNOWN_REPORTERS = {
    "SCMR": "Supreme Court Monthly Review",
    "PLD": "Pakistan Legal Decisions",
    "MLD": "Monthly Law Digest",
    "CLC": "Civil Law Cases",
    "PCrLJ": "Pakistan Criminal Law Journal",
    "PTD": "Pakistan Tax Decisions",
    "PLC": "Pakistan Law Cases",
    "YLR": "Yearly Law Reports",
    "CLD": "Corporate Law Decisions",
    "GBLR": "Gilgit-Baltistan Law Reports",
}

# Candidate reporters to test (comprehensive list of Pakistani legal reporters)
# These are known to exist in Pakistani legal publishing — we test each against PLS
CANDIDATE_REPORTERS = [
    # Major reporters we might be missing
    "PLJ",      # Pakistan Law Journal
    "NLR",      # National Law Reports
    "PTCL",     # Pakistan Tax Cases Library
    "PSC",      # Pakistan Supreme Court Reports
    "PLR",      # Punjab Law Reporter
    "KLR",      # Karachi Law Reports
    "FTR",      # Federal Tax Reports
    "STR",      # Sales Tax Reports
    "SLR",      # Sindh Law Reports
    "BLR",      # Balochistan Law Reports
    "KPLR",     # KPK/NWFP Law Reporter
    "DLR",      # Dhaka Law Reports (pre-1971)
    "AIR",      # All India Reporter (pre-1947)
    "ILR",      # Indian Law Reports (pre-1947)
    "LHC",      # Lahore High Court
    "SHC",      # Sindh High Court
    "PHC",      # Peshawar High Court
    "BHC",      # Balochistan High Court
    "IHC",      # Islamabad High Court
    "FSC",      # Federal Shariat Court
    "ITAT",     # Income Tax Appellate Tribunal
    "CCI",      # Competition Commission
    "SECP",     # Securities & Exchange Commission
    "SBLR",     # Sindh Bar Law Reports
    "PBLR",     # Punjab Bar Law Reports
    "LAW",      # Law Reports (generic)
    "TAX",      # Tax Reports (generic)
    "CLR",      # Criminal Law Reports
    "LLR",      # Labour Law Reports
    "LNotes",   # Law Notes
    "ALD",      # Allahabad Law Decisions
    "NLJ",      # National Law Journal
    "FLR",      # Federal Law Reports
    "PLT",      # Pakistan Law Times
    "PCTLR",    # Pakistan Customs & Tariff Law Reports
    "PCBLR",    # Pakistan Corporate & Banking Law Reports
    "TTR",      # Tax Tribunal Reports
    "CLCN",     # Civil Law Cases Notes
    "MLD(S)",   # MLD Supplement
    "SCMR(S)",  # SCMR Supplement
    "PLD(S)",   # PLD Supplement
    "PCrLJ(N)", # PCrLJ Notes
    "PLC(CS)",  # PLC Company Supplement
    "PLC(S)",   # PLC Supplement
    "PTD(Trib)",# PTD Tribunal
    # Try common abbreviation patterns
    "NLR",      # National Law Reports
    "SBR",      # Supreme Bar Review
    "PLJC",     # PLJ Criminal
    "PLJS",     # PLJ Supreme
    "PLJL",     # PLJ Lahore
    "PLJK",     # PLJ Karachi
]

# Remove duplicates and already-known reporters
CANDIDATE_REPORTERS = list(set(c for c in CANDIDATE_REPORTERS if c not in KNOWN_REPORTERS))

# Timing (very gentle — this is just discovery, not bulk scraping)
MIN_DELAY = 3.0
MAX_DELAY = 6.0
LOGIN_DELAY = 5.0
SEARCH_DELAY = 4.0

# Fix Windows encoding
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Logging — force UTF-8 on all handlers
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)

_stream_handler = logging.StreamHandler()
_stream_handler.setStream(open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False))

_file_handler = logging.FileHandler(log_dir / "reporter_discovery.log", encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_stream_handler, _file_handler],
)
logger = logging.getLogger("reporter_discovery")


class ReporterDiscovery:
    """Discovers and catalogs ALL reporters available on PLS."""

    def __init__(self):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        DISCOVERY_DIR.mkdir(parents=True, exist_ok=True)
        (Path(__file__).parent / "logs").mkdir(parents=True, exist_ok=True)

    def _create_session(self) -> Session:
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": BASE_URL,
        })
        return session

    def _delay(self, min_s=None, max_s=None):
        min_s = min_s or MIN_DELAY
        max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s)
        time.sleep(delay)

    def _request(self, method: str, url: str, **kwargs) -> Optional[object]:
        try:
            if method == "GET":
                resp = self.session.get(url, timeout=30, **kwargs)
            else:
                resp = self.session.post(url, timeout=30, **kwargs)
            self.request_count += 1
            return resp if resp.status_code == 200 else None
        except Exception as e:
            logger.warning(f"Request failed: {url} — {e}")
            return None

    def login(self) -> bool:
        """Login to PLS."""
        logger.info("Logging in to PLS...")
        self.session = self._create_session()

        resp = self._request("GET", f"{BASE_URL}/")
        if not resp:
            logger.error("Failed to load homepage")
            return False

        self._delay(2, 4)

        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False

        login_resp = self._request("POST", f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": csrf_match.group(1),
        })
        if not login_resp:
            logger.error("Login failed")
            return False

        # Handle multi-login protection modal
        if login_resp and "ClearLoginHistory" in login_resp.text:
            logger.info("Multi-login modal detected — clearing stale session...")
            self._delay(2, 3)
            clear_resp = self._request("POST", f"{BASE_URL}/Login/ClearLoginHistory", data={
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
            })
            if clear_resp and "/Login/Check" in clear_resp.text:
                logger.info("Stale session cleared — now logged in")
                self.logged_in = True
                logger.info("Login successful")
                self._delay(LOGIN_DELAY, LOGIN_DELAY + 3)
                return True
            else:
                logger.error("ClearLoginHistory failed")
                return False

        self._delay(2, 3)

        check_resp = self._request("GET", f"{BASE_URL}/Login/Check")
        if not check_resp or "Logout" not in check_resp.text:
            logger.error("Login verification failed")
            return False

        self.logged_in = True
        logger.info("✓ Login successful")
        self._delay(LOGIN_DELAY, LOGIN_DELAY + 3)
        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 1: Scrape the search form to find all reporter options
    # ──────────────────────────────────────────────────────────────────────────

    def discover_from_search_form(self) -> Dict[str, str]:
        """Scrape the PLS search/citation page to extract reporter dropdown options."""
        logger.info("═" * 60)
        logger.info("Phase 1: Scraping search form for reporter options...")

        discovered = {}

        # Try multiple pages that might have reporter dropdowns
        pages_to_check = [
            f"{BASE_URL}/Login/Check",                # Main logged-in page
            f"{BASE_URL}/Login/CitationSearch",        # Citation search
            f"{BASE_URL}/Login/AdvanceSearch",         # Advanced search
            f"{BASE_URL}/Login/CaseSearch",            # Case search
            f"{BASE_URL}/Login/Search",                # Generic search
            f"{BASE_URL}/Login/SubjectSearch",         # Subject search
        ]

        for url in pages_to_check:
            self._delay(2, 4)
            resp = self._request("GET", url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Find all select/dropdown elements
            for select in soup.find_all('select'):
                select_id = select.get('id', '') or select.get('name', '')
                options = select.find_all('option')

                for opt in options:
                    value = opt.get('value', '').strip()
                    text = opt.get_text(strip=True)

                    # Skip empty/placeholder options
                    if not value or value in ('', '0', '-1', '--', 'Select'):
                        continue

                    # Check if this looks like a reporter abbreviation
                    # Reporter codes are typically 2-6 uppercase letters
                    if re.match(r'^[A-Za-z()]{2,10}$', value):
                        discovered[value] = text
                        logger.info(f"  Found in dropdown ({select_id}): {value} = {text}")

            # Also search for reporter patterns in JavaScript/hidden data
            # PLS sometimes embeds reporter lists in JS
            js_reporters = re.findall(
                r'["\']((?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR|[A-Z]{2,6}(?:\([A-Z]+\))?))["\']',
                resp.text
            )
            for jr in js_reporters:
                if jr not in discovered and re.match(r'^[A-Z]{2,6}(?:\([A-Z]+\))?$', jr):
                    discovered[jr] = f"(found in JS on {url.split('/')[-1]})"

            # Look for radio buttons or checkboxes with reporter values
            for inp in soup.find_all('input', attrs={'type': ['radio', 'checkbox']}):
                name = inp.get('name', '')
                value = inp.get('value', '').strip()
                if name.lower() in ('book', 'reporter', 'journal') and value:
                    if re.match(r'^[A-Za-z()]{2,10}$', value):
                        discovered[value] = f"(radio/checkbox: {name})"

            logger.info(f"  Checked {url.split('/')[-1]} — found {len(discovered)} reporters so far")

        logger.info(f"Phase 1 complete: {len(discovered)} reporters from search forms")
        return discovered

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2: Brute-force test candidate reporters via CitationSearch
    # ──────────────────────────────────────────────────────────────────────────

    def test_candidate_reporters(self, candidates: List[str] = None) -> Dict[str, Dict]:
        """Test each candidate reporter by searching for it on PLS."""
        candidates = candidates or CANDIDATE_REPORTERS
        logger.info("═" * 60)
        logger.info(f"Phase 2: Testing {len(candidates)} candidate reporters...")

        results = {}
        test_years = [2024, 2023, 2020, 2015, 2010, 2005, 2000]

        for i, reporter in enumerate(candidates):
            logger.info(f"  [{i+1}/{len(candidates)}] Testing: {reporter}")

            total_found = 0
            years_with_data = []

            for year in test_years:
                self._delay(SEARCH_DELAY, SEARCH_DELAY + 2)

                resp = self._request("POST", f"{BASE_URL}/Login/CitationSearch", data={
                    "year": year,
                    "book": reporter,
                    "code": "",
                    "court": "",
                    "judge": "",
                    "lawyer": "",
                    "party": "",
                })

                if not resp:
                    continue

                # Count cases in response
                cases = self._count_cases_in_response(resp.text)
                if cases > 0:
                    total_found += cases
                    years_with_data.append({"year": year, "cases": cases})
                    logger.info(f"    ✅ {reporter} {year}: {cases} cases!")

            if total_found > 0:
                results[reporter] = {
                    "total_sample": total_found,
                    "years_tested": test_years,
                    "years_with_data": years_with_data,
                    "estimated_total": self._estimate_total(years_with_data, test_years),
                    "status": "NEW_REPORTER" if reporter not in KNOWN_REPORTERS else "KNOWN",
                }
                logger.info(f"  🆕 {reporter}: {total_found} cases in sample years!")
            else:
                logger.info(f"    ❌ {reporter}: no cases found")

            # Random longer break every 10 reporters
            if (i + 1) % 10 == 0:
                logger.info("  Taking a break...")
                self._delay(15, 30)

        logger.info(f"Phase 2 complete: {len(results)} reporters with data")
        return results

    def _count_cases_in_response(self, html: str) -> int:
        """Count cases returned in a search response."""
        soup = BeautifulSoup(html, 'html.parser')

        # Count table rows with caseType class
        case_rows = soup.find_all('tr', class_='caseType')
        if case_rows:
            return len(case_rows)

        # Count caseLawTable elements
        case_tables = soup.find_all('table', class_='caseLawTable')
        if case_tables:
            return len(case_tables)

        # Regex fallback — count citation patterns
        citations = re.findall(r'\d{4}\s+[A-Za-z()]+\s+\d+', html)
        return len(set(citations))

    def _estimate_total(self, years_with_data: List[Dict], test_years: List[int]) -> str:
        """Rough estimate of total cases based on sample years."""
        if not years_with_data:
            return "0"

        avg_per_year = sum(y["cases"] for y in years_with_data) / len(years_with_data)
        # Assume ~30 years of coverage on PLS (rough)
        estimated = int(avg_per_year * 30)
        return f"~{estimated} (rough estimate)"

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 3: Deep year-by-year scan for new reporters
    # ──────────────────────────────────────────────────────────────────────────

    def deep_scan_reporter(self, reporter: str, start_year: int = 1980, end_year: int = 2026) -> Dict:
        """Deep scan: check every year for a specific reporter."""
        logger.info(f"Deep scanning {reporter} from {start_year} to {end_year}...")

        year_data = {}
        total = 0

        for year in range(end_year, start_year - 1, -1):
            self._delay(SEARCH_DELAY, SEARCH_DELAY + 2)

            resp = self._request("POST", f"{BASE_URL}/Login/CitationSearch", data={
                "year": year,
                "book": reporter,
                "code": "",
                "court": "",
                "judge": "",
                "lawyer": "",
                "party": "",
            })

            if not resp:
                year_data[str(year)] = {"cases": 0, "error": "request_failed"}
                continue

            cases = self._count_cases_in_response(resp.text)
            year_data[str(year)] = {"cases": cases}
            total += cases

            if cases > 0:
                logger.info(f"  {reporter} {year}: {cases} cases")

        return {
            "reporter": reporter,
            "total": total,
            "years": year_data,
            "coverage": f"{start_year}-{end_year}",
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 4: Compare and report
    # ──────────────────────────────────────────────────────────────────────────

    def generate_report(self, form_reporters: Dict, tested_reporters: Dict) -> Dict:
        """Generate comprehensive discovery report."""
        logger.info("═" * 60)
        logger.info("Generating discovery report...")

        # Merge all discovered reporters
        all_discovered = set()
        all_discovered.update(form_reporters.keys())
        all_discovered.update(tested_reporters.keys())
        all_discovered.update(KNOWN_REPORTERS.keys())

        new_reporters = {}
        known_confirmed = {}
        empty_candidates = []

        for reporter in sorted(all_discovered):
            is_known = reporter in KNOWN_REPORTERS
            has_data = reporter in tested_reporters and tested_reporters[reporter]["total_sample"] > 0
            in_form = reporter in form_reporters

            if is_known:
                known_confirmed[reporter] = {
                    "description": KNOWN_REPORTERS[reporter],
                    "in_form": in_form,
                    "tested": reporter in tested_reporters,
                }
            elif has_data:
                new_reporters[reporter] = {
                    "source": "form" if in_form else "brute_force",
                    "form_label": form_reporters.get(reporter, ""),
                    **tested_reporters[reporter],
                }
                logger.info(f"  🆕 NEW REPORTER: {reporter} — {tested_reporters[reporter]['total_sample']} cases in sample!")
            else:
                empty_candidates.append(reporter)

        report = {
            "scan_time": datetime.now(timezone.utc).isoformat(),
            "pls_url": BASE_URL,
            "requests_made": self.request_count,
            "known_reporters": known_confirmed,
            "new_reporters_found": new_reporters,
            "empty_candidates_tested": len(empty_candidates),
            "total_reporters_on_pls": len(known_confirmed) + len(new_reporters),
            "form_reporters_raw": form_reporters,
            "summary": {
                "known": len(known_confirmed),
                "new": len(new_reporters),
                "empty": len(empty_candidates),
            }
        }

        # Save report
        REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Report saved: {REPORT_FILE}")

        # Update history
        self._update_history(report)

        return report

    def _update_history(self, report: Dict):
        """Append to discovery history for tracking changes over time."""
        history = []
        if HISTORY_FILE.exists():
            try:
                history = json.loads(HISTORY_FILE.read_text())
            except:
                pass

        history.append({
            "date": report["scan_time"],
            "known": report["summary"]["known"],
            "new": report["summary"]["new"],
            "new_reporters": list(report["new_reporters_found"].keys()),
        })

        # Keep last 52 entries (1 year of weekly scans)
        history = history[-52:]

        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2)

    # ──────────────────────────────────────────────────────────────────────────
    # Auto-scrape new reporters
    # ──────────────────────────────────────────────────────────────────────────

    def auto_scrape_new(self, new_reporters: Dict):
        """Trigger scraping for newly discovered reporters."""
        if not new_reporters:
            logger.info("No new reporters to scrape")
            return

        for reporter, info in new_reporters.items():
            logger.info(f"🚀 Auto-scraping new reporter: {reporter}")
            logger.info(f"   Estimated cases: {info.get('estimated_total', 'unknown')}")

            # Add to KNOWN_REPORTERS in config
            # (For now, just log — actual scraping requires adding to pls_scraper_v2.py)
            logger.warning(
                f"   ⚠️ ACTION REQUIRED: Add '{reporter}' to REPORTERS list in "
                f"pls_scraper_v2.py and historical_scraper.py, then run scraper."
            )

            # Could auto-trigger:
            # subprocess.Popen([
            #     sys.executable, "pls_scraper_v2.py",
            #     "--reporter", reporter, "--from-year", "2020", "--to-year", "2026"
            # ])

    # ──────────────────────────────────────────────────────────────────────────
    # Main orchestration
    # ──────────────────────────────────────────────────────────────────────────

    def run(self, quick: bool = False, deep: bool = False, scrape_new: bool = False) -> Dict:
        """Run the full discovery pipeline."""
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║       PLS Reporter Discovery Agent v1.0                ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")
        logger.info(f"Known reporters: {len(KNOWN_REPORTERS)}")
        logger.info(f"Candidates to test: {len(CANDIDATE_REPORTERS)}")

        # Login
        if not self.login():
            logger.error("Login failed — aborting")
            # Try 2 more times
            for attempt in range(2):
                logger.info(f"Retry {attempt + 2}/3...")
                time.sleep(10)
                if self.login():
                    break
            else:
                return {"error": "login_failed"}

        # Phase 1: Scrape search forms
        form_reporters = self.discover_from_search_form()

        if quick:
            # Quick mode: just check forms, don't brute-force
            report = self.generate_report(form_reporters, {})
            self._print_summary(report)
            return report

        # Phase 2: Test candidates
        # Include any form-discovered reporters that aren't in our known list
        extra_candidates = [r for r in form_reporters if r not in KNOWN_REPORTERS and r not in CANDIDATE_REPORTERS]
        all_candidates = CANDIDATE_REPORTERS + extra_candidates
        tested = self.test_candidate_reporters(all_candidates)

        # Phase 3 (optional): Deep scan new reporters
        if deep and tested:
            for reporter in list(tested.keys()):
                if reporter not in KNOWN_REPORTERS:
                    deep_data = self.deep_scan_reporter(reporter)
                    tested[reporter]["deep_scan"] = deep_data

        # Generate report
        report = self.generate_report(form_reporters, tested)
        self._print_summary(report)

        # Phase 4 (optional): Auto-scrape
        if scrape_new and report["new_reporters_found"]:
            self.auto_scrape_new(report["new_reporters_found"])

        return report

    def _print_summary(self, report: Dict):
        """Print human-readable summary."""
        print("\n" + "=" * 60)
        print("  REPORTER DISCOVERY SUMMARY")
        print("=" * 60)
        print(f"  Known reporters (confirmed): {report['summary']['known']}")
        print(f"  NEW reporters found:         {report['summary']['new']}")
        print(f"  Empty candidates tested:     {report['summary']['empty']}")
        print(f"  Total PLS reporters:         {report['total_reporters_on_pls']}")
        print(f"  PLS requests made:           {report['requests_made']}")

        if report["new_reporters_found"]:
            print("\n  🆕 NEW REPORTERS DETECTED:")
            for reporter, info in report["new_reporters_found"].items():
                print(f"    • {reporter}: {info['total_sample']} cases (sample)")
                print(f"      Estimated total: {info['estimated_total']}")
                if info.get("years_with_data"):
                    years_str = ", ".join(f"{y['year']}({y['cases']})" for y in info["years_with_data"])
                    print(f"      Years with data: {years_str}")
        else:
            print("\n  ✅ No new reporters found — our list is complete!")

        print("\n  Known reporters:")
        for reporter, desc in sorted(KNOWN_REPORTERS.items()):
            local_count = self._count_local(reporter)
            print(f"    • {reporter:8s} — {local_count:>6,} cases — {desc}")

        print("=" * 60)

    def _count_local(self, reporter: str) -> int:
        """Count local JSON files for a reporter."""
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            return 0
        return sum(1 for _ in reporter_dir.rglob("*.json"))


def main():
    parser = argparse.ArgumentParser(description="PLS Reporter Discovery Agent")
    parser.add_argument("--quick", action="store_true", help="Quick scan (form scrape only)")
    parser.add_argument("--deep", action="store_true", help="Deep year-by-year scan for new reporters")
    parser.add_argument("--scrape-new", action="store_true", help="Auto-trigger scraping for new reporters")
    args = parser.parse_args()

    if not PLS_USER or not PLS_PASS:
        print("ERROR: PLS_USER and PLS_PASS must be set in .env")
        sys.exit(1)

    agent = ReporterDiscovery()
    report = agent.run(
        quick=args.quick,
        deep=args.deep,
        scrape_new=args.scrape_new,
    )

    if report.get("new_reporters_found"):
        print(f"\n⚠️  {len(report['new_reporters_found'])} NEW REPORTER(S) FOUND — ACTION NEEDED!")
        sys.exit(2)  # Non-zero exit for cron alerting
    else:
        print("\n✅ All clear — no new reporters")
        sys.exit(0)


if __name__ == "__main__":
    main()
