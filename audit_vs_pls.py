#!/usr/bin/env python3
"""
PLS Case Audit — Verify local cases against pakistanlawsite.com
================================================================
Picks random cases from our local data and compares them against the live
PLS source to ensure data integrity.

Usage:
    python audit_vs_pls.py              # Check 10 random cases (default)
    python audit_vs_pls.py --count 20   # Check 20 random cases
"""

import os
import re
import sys
import json
import time
import random
import argparse
import difflib
import subprocess
import logging
from pathlib import Path
from datetime import datetime, date

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("audit_vs_pls")

# ── Constants ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
BASE_URL = "https://www.pakistanlawsite.com"
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
SCRAPER_PROCESSES = ["historical_scraper", "pls_scraper", "scraper_chain"]

REQUEST_TIMEOUT = 15
FETCH_DELAY_MIN = 3.0
FETCH_DELAY_MAX = 5.0


# ── HTTP client ──────────────────────────────────────────────────────────────
# Prefer curl_cffi (matches our scraper's TLS fingerprint) but fall back to
# requests if it isn't installed.
try:
    from curl_cffi.requests import Session as CurlSession, BrowserType
    USE_CURL_CFFI = True
    logger.info("Using curl_cffi (Chrome TLS fingerprint)")
except ImportError:
    import requests as _requests_mod  # noqa: F401
    USE_CURL_CFFI = False
    logger.info("curl_cffi not available — falling back to requests")


# ── Helpers ──────────────────────────────────────────────────────────────────

def strip_html(html: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", "", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def similarity(a: str, b: str) -> float:
    """Return 0-100 similarity between two strings using SequenceMatcher."""
    if not a and not b:
        return 100.0
    if not a or not b:
        return 0.0
    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    return round(ratio * 100, 2)


def fmt_bytes(n: int) -> str:
    """Format byte count with commas."""
    return f"{n:,}"


# ── Scraper-active check ────────────────────────────────────────────────────

def is_scraper_running() -> bool:
    """Check (via PowerShell) whether any of our scrapers are active."""
    for proc_name in SCRAPER_PROCESSES:
        try:
            result = subprocess.run(
                [
                    "powershell", "-NoProfile", "-Command",
                    f"Get-Process python* -ErrorAction SilentlyContinue | "
                    f"Where-Object {{ $_.CommandLine -match '{proc_name}' }} | "
                    f"Measure-Object | Select-Object -ExpandProperty Count",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            count = int(result.stdout.strip() or "0")
            if count > 0:
                logger.info(f"Active scraper detected: {proc_name} ({count} process(es))")
                return True
        except Exception as exc:
            logger.warning(f"Could not check for {proc_name}: {exc}")
    return False


# ── Case picker ──────────────────────────────────────────────────────────────

def discover_cases() -> dict:
    """Build a mapping  {year: [(reporter, json_path), ...]}  from data_v2."""
    year_map: dict[int, list[tuple[str, Path]]] = {}
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.is_dir():
            continue
        for year_dir in reporter_dir.iterdir():
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            json_files = list(year_dir.glob("*.json"))
            if json_files:
                year_map.setdefault(year, [])
                for jf in json_files:
                    year_map[year].append((reporter, jf))
    return year_map


def pick_cases(count: int) -> list[dict]:
    """Pick *count* random cases — at most 1 per year where possible."""
    year_map = discover_cases()
    if not year_map:
        logger.error("No case files found under data_v2/")
        return []

    available_years = list(year_map.keys())
    random.shuffle(available_years)

    # Try to spread across years; if count > available years, allow repeats
    selected_years = available_years[: count]
    if len(selected_years) < count:
        extra = random.choices(available_years, k=count - len(selected_years))
        selected_years.extend(extra)

    picks: list[dict] = []
    for yr in selected_years:
        reporter, json_path = random.choice(year_map[yr])
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            picks.append({
                "year": yr,
                "reporter": reporter,
                "path": str(json_path),
                "citation": data.get("citation", ""),
                "case_name": data.get("case_name", ""),
                "judgment_raw": data.get("judgment_raw", "") or data.get("judgment_html", ""),
            })
        except Exception as exc:
            logger.warning(f"Failed to read {json_path}: {exc}")
    return picks


# ── PLS session ──────────────────────────────────────────────────────────────

class PLSSession:
    """Lightweight wrapper around a PLS-authenticated HTTP session."""

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.logged_in = False

        if USE_CURL_CFFI:
            self.session = CurlSession(impersonate=BrowserType.chrome120)
            self.session.headers.update({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            })
        else:
            import requests as _req
            self.session = _req.Session()
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            })

    # ── internal request wrappers ────────────────────────────────────────
    def _get(self, url: str, **kw):
        return self.session.get(url, timeout=REQUEST_TIMEOUT, **kw)

    def _post(self, url: str, **kw):
        return self.session.post(url, timeout=REQUEST_TIMEOUT, **kw)

    # ── login ────────────────────────────────────────────────────────────
    def login(self) -> bool:
        """Authenticate once; reuse the session cookie for all fetches."""
        try:
            logger.info("Logging in to PLS …")
            # Use ClearLoginHistory to clear any existing sessions and auto-login
            login_resp = self._post(
                f"{BASE_URL}/Login/ClearLoginHistory",
                data={
                    "Login.UserName": self.username,
                    "Login.Password": self.password,
                },
            )
            if not login_resp or login_resp.status_code != 200:
                logger.error(f"ClearLoginHistory returned {getattr(login_resp, 'status_code', '?')}")
                return False

            time.sleep(random.uniform(1.0, 2.0))

            # Verify login
            check = self._get(f"{BASE_URL}/Login/Check")
            if not check or "Logout" not in check.text:
                logger.error("Login verification failed (no 'Logout' in /Login/Check)")
                return False

            self.logged_in = True
            logger.info("✓ PLS login successful")
            return True

        except Exception as exc:
            logger.error(f"Login error: {exc}")
            return False

    # ── fetch case ───────────────────────────────────────────────────────
    def fetch_case(self, case_name: str) -> str | None:
        """Fetch a single case's HTML from PLS. Returns HTML string or None."""
        if not self.logged_in:
            logger.warning("Not logged in — cannot fetch case")
            return None
        try:
            url = f"{BASE_URL}/Login/GetCaseFile"
            resp = self._post(url, data={'caseName': case_name, 'headNotes': 0})
            if resp.status_code != 200:
                logger.warning(f"GetCaseFile returned {resp.status_code} for {case_name}")
                return None
            html = resp.text
            # PLS returns HTML wrapped as JSON string — decode envelope
            if html.startswith('"'):
                try:
                    html = json.loads(html)
                except (json.JSONDecodeError, ValueError):
                    pass
            # Basic validity: must be longer than a trivial error page
            if not html or len(html) < 100:
                logger.warning(f"GetCaseFile returned near-empty response for {case_name}")
                return None
            return html
        except Exception as exc:
            logger.warning(f"Fetch failed for {case_name}: {exc}")
            return None


# ── Audit engine ─────────────────────────────────────────────────────────────

def run_audit(count: int = 10) -> dict:
    """Run the full audit and return a report dict."""
    today = date.today().isoformat()

    # 0. Check for active scrapers
    if is_scraper_running():
        print("Scraper active, audit postponed")
        sys.exit(0)

    # 1. Load credentials
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE_DIR / ".env")
    except ImportError:
        logger.warning("python-dotenv not installed — trying os.environ directly")

    pls_user = os.getenv("PLS_USER")
    pls_pass = os.getenv("PLS_PASS")
    if not pls_user or not pls_pass:
        logger.error("PLS_USER / PLS_PASS not set in .env — aborting")
        sys.exit(1)

    # 2. Pick cases
    cases = pick_cases(count)
    if not cases:
        logger.error("No cases selected — nothing to audit")
        sys.exit(1)
    logger.info(f"Selected {len(cases)} cases for audit")

    # 3. Login (once)
    pls = PLSSession(pls_user, pls_pass)
    if not pls.login():
        logger.error("PLS login failed — aborting audit")
        sys.exit(1)

    # 4. Compare each case
    results: list[dict] = []
    for idx, case in enumerate(cases, 1):
        citation = case["citation"]
        case_name = case["case_name"]
        local_html = case["judgment_raw"]
        reporter = case["reporter"]
        year = case["year"]

        logger.info(f"[{idx}/{len(cases)}] Auditing {citation} ({case_name}) …")

        # Human-like delay between requests
        if idx > 1:
            delay = random.uniform(FETCH_DELAY_MIN, FETCH_DELAY_MAX)
            time.sleep(delay)

        # Fetch from PLS
        pls_html = pls.fetch_case(case_name)

        if pls_html is None:
            results.append({
                "citation": citation,
                "case_name": case_name,
                "year": year,
                "reporter": reporter,
                "local_html_size": len(local_html),
                "pls_html_size": 0,
                "similarity_pct": 0.0,
                "status": "FETCH_FAILED",
                "snippet_match": False,
            })
            continue

        # Strip both to plain text for comparison
        local_text = strip_html(local_html)
        pls_text = strip_html(pls_html)

        # Similarity (on full text)
        sim_pct = similarity(local_text, pls_text)

        # Snippet match — first 500 chars of body text
        snippet_local = local_text[:500]
        snippet_pls = pls_text[:500]
        snippet_ok = similarity(snippet_local, snippet_pls) > 90.0

        # Status
        if sim_pct >= 95.0:
            status = "MATCH"
        else:
            status = "MISMATCH"

        results.append({
            "citation": citation,
            "case_name": case_name,
            "year": year,
            "reporter": reporter,
            "local_html_size": len(local_html),
            "pls_html_size": len(pls_html),
            "similarity_pct": sim_pct,
            "status": status,
            "snippet_match": snippet_ok,
        })

    # 5. Build report
    matches = sum(1 for r in results if r["status"] == "MATCH")
    mismatches = sum(1 for r in results if r["status"] == "MISMATCH")
    failures = sum(1 for r in results if r["status"] == "FETCH_FAILED")

    report = {
        "date": today,
        "cases_checked": len(results),
        "matches": matches,
        "mismatches": mismatches,
        "fetch_failures": failures,
        "results": results,
    }
    return report


def save_report(report: dict) -> Path:
    """Persist the audit report as JSON under data_v2/audit/."""
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = AUDIT_DIR / f"daily_audit_{report['date']}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Report saved → {out_path}")
    return out_path


def print_summary(report: dict):
    """Pretty-print the audit summary to the console."""
    d = report["date"]
    total = report["cases_checked"]
    m = report["matches"]
    mm = report["mismatches"]
    ff = report["fetch_failures"]

    print()
    print(f"=== PLS AUDIT REPORT — {d} ===")
    print(f"Cases checked: {total}")
    print(f"✅ Matches: {m}")
    print(f"❌ Mismatches: {mm}")
    print(f"⚠️  Fetch failures: {ff}")
    print()
    print("Details:")

    for r in report["results"]:
        cit = r["citation"]
        sim = r["similarity_pct"]
        local_sz = r["local_html_size"]
        pls_sz = r["pls_html_size"]

        if r["status"] == "MATCH":
            print(f"  ✅ {cit} — {sim}% match ({fmt_bytes(local_sz)} bytes)")
        elif r["status"] == "MISMATCH":
            print(
                f"  ❌ {cit} — {sim}% match "
                f"(local: {fmt_bytes(local_sz)} vs PLS: {fmt_bytes(pls_sz)})"
            )
        else:
            print(f"  ⚠️  {cit} — fetch failed")

    print()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Audit local PLS cases against the live source")
    parser.add_argument("--count", type=int, default=10, help="Number of cases to check (default 10)")
    args = parser.parse_args()

    try:
        report = run_audit(count=args.count)
        save_report(report)
        print_summary(report)
    except KeyboardInterrupt:
        print("\nAudit interrupted.")
        sys.exit(130)
    except Exception as exc:
        logger.error(f"Unexpected error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
