#!/usr/bin/env python3
"""
refetch_failed.py - Re-fetch 699 failed PLS original HTML files.

Loads data_v2/audit/refetch_list.json and re-fetches each case using the
GetCaseFile API, saving in all 4 formats: original HTML, JSON, readable HTML, JSONL.

Usage:
    python refetch_failed.py
"""

import os
import re
import sys
import json
import time
import random
import logging
import psutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from curl_cffi.requests import Session

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL       = "https://www.pakistanlawsite.com"
SCRIPT_DIR     = Path(__file__).parent
DATA_DIR       = SCRIPT_DIR / "data_v2"
REFETCH_LIST   = DATA_DIR / "audit" / "refetch_list.json"
PROGRESS_FILE  = SCRIPT_DIR / "refetch_progress.json"
LOG_FILE       = SCRIPT_DIR / "refetch_failed.log"

DELAY_MIN      = 2.0   # seconds between requests
DELAY_MAX      = 4.0
HEALTH_EVERY   = 50    # check session every N requests
LOGIN_RETRIES  = 3

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── Readable HTML template ───────────────────────────────────────────────────

READABLE_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{citation}</title>
<style>
body {{ background: #1a1a2e; color: #e0e0e0; font-family: Georgia, serif; max-width: 900px; margin: 0 auto; padding: 40px 20px; line-height: 1.8; }}
h1, h2, h3 {{ color: #00d4ff; }}
a {{ color: #4fc3f7; }}
.citation {{ color: #ffd700; font-weight: bold; }}
.metadata {{ background: #16213e; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
</style>
</head>
<body>
<div class="metadata">
<h1 class="citation">{citation}</h1>
</div>
{judgment_html}
</body>
</html>
"""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def citation_to_casename(citation: str) -> str:
    """Convert 'YYYY Reporter NNNN' → 'YYYY_Reporter_NNNN' for API."""
    return citation.replace(" ", "_")


def is_error_page(html: str) -> bool:
    """Return True if html looks like a PLS error / login redirect page."""
    if not html or len(html.strip()) < 200:
        return True
    error_markers = [
        "Object moved",
        "Login/Check",
        "pakistanlawsite.com/Login",
        "The resource you are looking for",
        "404",
        "An error occurred",
        "Server Error",
        "Access Denied",
    ]
    for marker in error_markers:
        if marker in html:
            return True
    return False


def check_concurrent_scrapers() -> None:
    """Warn if another instance of a scraper script is already running."""
    current_pid = os.getpid()
    parent_pid = os.getppid()
    scraper_scripts = {"historical_scraper.py", "pls_scraper_v2.py", "fill_round3.py", "fill_audit_gaps.py", "scraper_chain.py", "fill_all_gaps.py"}
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if proc.pid in (current_pid, parent_pid):
                continue
            if "python" not in (proc.info.get("name") or "").lower():
                continue
            cmdline = proc.info.get("cmdline") or []
            for arg in cmdline:
                if any(s in arg for s in scraper_scripts):
                    log.warning(
                        "⚠️  ANOTHER SCRAPER MAY BE RUNNING: PID %d → %s",
                        proc.pid, " ".join(cmdline),
                    )
                    log.warning(
                        "⚠️  PLS only supports ONE active session. "
                        "Running two scrapers simultaneously will invalidate each other's sessions."
                    )
                    print(
                        f"\n⚠️  WARNING: Another scraper process detected (PID {proc.pid}).\n"
                        f"   PLS only supports ONE active session at a time.\n"
                        f"   Running two scrapers will break both sessions.\n"
                        f"   Kill the other process first, then retry.\n",
                        file=sys.stderr,
                    )
                    answer = input("Continue anyway? [y/N] ").strip().lower()
                    if answer != "y":
                        sys.exit(1)
                    return
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass


def load_progress() -> set:
    """Load set of already-completed citations from progress file."""
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            done = set(data.get("completed", []))
            log.info("Resuming: %d entries already completed.", len(done))
            return done
        except Exception as exc:
            log.warning("Could not read progress file: %s", exc)
    return set()


def save_progress(completed: set, failed: list) -> None:
    """Persist progress so the script can resume after interruption."""
    data = {
        "completed": sorted(completed),
        "failed": failed,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    PROGRESS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ─── Login ────────────────────────────────────────────────────────────────────

def login(session: Session, username: str, password: str) -> bool:
    """Login to PLS using the ClearLoginHistory flow. Returns True on success."""
    for attempt in range(1, LOGIN_RETRIES + 1):
        try:
            log.info("Login attempt %d/%d …", attempt, LOGIN_RETRIES)

            # 1. GET homepage → extract CSRF token
            resp = session.get(f"{BASE_URL}/", timeout=30)
            resp.raise_for_status()
            csrf_match = re.search(
                r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text
            )
            if not csrf_match:
                log.warning("CSRF token not found on homepage.")
                time.sleep(3)
                continue
            csrf_token = csrf_match.group(1)

            # 2. POST ClearLoginHistory (clears old sessions AND logs in)
            clear_resp = session.post(
                f"{BASE_URL}/Login/ClearLoginHistory",
                data={
                    "UserName": username,
                    "Password": password,
                    "__RequestVerificationToken": csrf_token,
                },
                timeout=30,
            )
            log.debug("ClearLoginHistory status: %d", clear_resp.status_code)

            # 3. Verify with Login/Check
            check_resp = session.get(f"{BASE_URL}/Login/Check", timeout=15)
            result_text = check_resp.text.strip().lower()
            log.debug("Login/Check response: %r", result_text[:200])

            if "true" in result_text or check_resp.status_code == 200:
                log.info("✅ Logged in successfully.")
                return True
            else:
                log.warning("Login/Check returned unexpected response: %r", result_text[:100])

        except Exception as exc:
            log.error("Login attempt %d failed: %s", attempt, exc)
            if attempt < LOGIN_RETRIES:
                time.sleep(5 * attempt)

    log.error("❌ All %d login attempts failed.", LOGIN_RETRIES)
    return False


def check_session(session: Session) -> bool:
    """Verify the session is still alive via Login/Check."""
    try:
        resp = session.get(f"{BASE_URL}/Login/Check", timeout=15)
        result = resp.text.strip().lower()
        alive = "true" in result
        if not alive:
            log.warning("Session health check failed: %r", resp.text[:100])
        return alive
    except Exception as exc:
        log.warning("Session health check error: %s", exc)
        return False


# ─── Fetch one case ───────────────────────────────────────────────────────────

def fetch_case(session: Session, citation: str) -> str | None:
    """
    Fetch a case via GetCaseFile. Returns decoded HTML string or None on failure.
    citation format: "2016 SCMR 2081" → caseName: "2016_SCMR_2081"
    """
    case_name = citation_to_casename(citation)
    try:
        resp = session.post(
            f"{BASE_URL}/Login/GetCaseFile",
            data={"caseName": case_name, "headNotes": 0},
            timeout=30,
        )
        resp.raise_for_status()
        html = resp.text

        # Response is HTML wrapped as a JSON string — decode it
        if html and html.strip().startswith('"'):
            try:
                html = json.loads(html)
            except json.JSONDecodeError:
                pass  # use raw text as-is

        if is_error_page(html):
            log.warning("Error/empty page received for %s", citation)
            return None

        return html

    except Exception as exc:
        log.error("Failed to fetch %s: %s", citation, exc)
        return None


# ─── Save helpers ─────────────────────────────────────────────────────────────

def save_original_html(html: str, reporter: str, year: str, citation: str) -> None:
    """Save raw HTML to data_v2/{reporter}/{year}/original/{citation}.html"""
    path = DATA_DIR / reporter / year / "original" / f"{citation}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    log.debug("Saved original HTML: %s", path)


def save_readable_html(html: str, reporter: str, year: str, citation: str) -> None:
    """Save dark-themed readable HTML to data_v2/html/{reporter}/{year}/{citation}.html"""
    path = DATA_DIR / "html" / reporter / year / f"{citation}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    readable = READABLE_HTML_TEMPLATE.format(citation=citation, judgment_html=html)
    path.write_text(readable, encoding="utf-8")
    log.debug("Saved readable HTML: %s", path)


def update_json(html: str, reporter: str, year: str, citation: str) -> None:
    """Load existing JSON record, update judgment_raw, save back."""
    json_path = DATA_DIR / reporter / year / f"{citation}.json"
    if not json_path.exists():
        log.warning("JSON not found, skipping JSON update: %s", json_path)
        return
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        data["judgment_raw"] = html
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        log.debug("Updated JSON: %s", json_path)
    except Exception as exc:
        log.error("Failed to update JSON for %s: %s", citation, exc)


def update_jsonl(html: str, reporter: str, year: str, citation: str) -> None:
    """
    Update the JSONL file for this reporter+year.
    Rewrites the matching line's judgment_raw field in data_v2/{reporter}_{year}.jsonl
    If the citation is not found, appends a minimal record.
    """
    jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
    if not jsonl_path.exists():
        log.warning("JSONL not found, skipping JSONL update: %s", jsonl_path)
        return

    updated = False
    lines_out = []
    try:
        with jsonl_path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                raw_line = raw_line.rstrip("\n")
                if not raw_line.strip():
                    lines_out.append(raw_line)
                    continue
                try:
                    record = json.loads(raw_line)
                    if record.get("citation") == citation:
                        record["judgment_raw"] = html
                        lines_out.append(json.dumps(record, ensure_ascii=False))
                        updated = True
                    else:
                        lines_out.append(raw_line)
                except json.JSONDecodeError:
                    lines_out.append(raw_line)

        if updated:
            jsonl_path.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
            log.debug("Updated JSONL record: %s in %s", citation, jsonl_path.name)
        else:
            log.warning("Citation %s not found in %s, appending minimal record.", citation, jsonl_path.name)
            with jsonl_path.open("a", encoding="utf-8") as fh:
                minimal = {
                    "citation": citation,
                    "reporter": reporter,
                    "year": year,
                    "judgment_raw": html,
                    "refetched_at": datetime.utcnow().isoformat() + "Z",
                }
                fh.write(json.dumps(minimal, ensure_ascii=False) + "\n")

    except Exception as exc:
        log.error("Failed to update JSONL for %s: %s", citation, exc)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("refetch_failed.py — PLS re-fetch for 699 failed entries")
    log.info("=" * 60)

    # ── Check for concurrent scrapers ──
    check_concurrent_scrapers()

    # ── Load credentials ──
    load_dotenv()
    username = os.getenv("PLS_USER")
    password = os.getenv("PLS_PASS")
    if not username or not password:
        log.error("PLS_USER and PLS_PASS must be set in .env file.")
        sys.exit(1)

    # ── Load refetch list ──
    if not REFETCH_LIST.exists():
        log.error("Refetch list not found: %s", REFETCH_LIST)
        sys.exit(1)

    entries = json.loads(REFETCH_LIST.read_text(encoding="utf-8"))
    log.info("Loaded %d entries from refetch list.", len(entries))

    # ── Load progress ──
    completed = load_progress()
    remaining = [e for e in entries if e["citation"] not in completed]
    log.info("%d entries remaining to process.", len(remaining))

    if not remaining:
        log.info("Nothing to do — all entries already completed!")
        return

    # ── Create session and login ──
    session = Session(impersonate="chrome")
    if not login(session, username, password):
        log.error("Cannot proceed without a valid session.")
        sys.exit(1)

    # ── Main loop ──
    succeeded = 0
    failed_entries = []
    request_count = 0

    for idx, entry in enumerate(remaining, start=1):
        citation = entry["citation"]
        reporter = entry["reporter"]
        year     = str(entry["year"])
        entry_type = entry.get("type", "unknown")

        log.info(
            "[%d/%d] Fetching %s (was: %s) …",
            idx, len(remaining), citation, entry_type,
        )

        # ── Session health check every HEALTH_EVERY requests ──
        if request_count > 0 and request_count % HEALTH_EVERY == 0:
            log.info("Session health check at request #%d …", request_count)
            if not check_session(session):
                log.warning("Session died — re-logging in …")
                session = Session(impersonate="chrome")
                if not login(session, username, password):
                    log.error("Re-login failed. Aborting.")
                    break

        # ── Fetch ──
        html = fetch_case(session, citation)
        request_count += 1

        if html:
            # Save all 4 formats
            try:
                save_original_html(html, reporter, year, citation)
                save_readable_html(html, reporter, year, citation)
                update_json(html, reporter, year, citation)
                update_jsonl(html, reporter, year, citation)
                succeeded += 1
                completed.add(citation)
                log.info("✅ %s — saved OK", citation)
            except Exception as exc:
                log.error("❌ %s — save error: %s", citation, exc)
                failed_entries.append({**entry, "error": f"save_error: {exc}"})
        else:
            log.warning("❌ %s — fetch returned nothing", citation)
            failed_entries.append({**entry, "error": "fetch_failed"})

        # ── Save progress ──
        save_progress(completed, failed_entries)

        # ── Human-like delay ──
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        # Occasional longer break (every ~30 requests, 5% chance)
        if request_count % 30 == 0:
            extra = random.uniform(5, 15)
            log.info("Taking a short break (%.1fs) …", delay + extra)
            delay += extra
        time.sleep(delay)

    # ── Summary ──
    log.info("=" * 60)
    log.info("DONE — Summary")
    log.info("  Total entries:   %d", len(entries))
    log.info("  Already done:    %d", len(entries) - len(remaining))
    log.info("  Processed now:   %d", len(remaining))
    log.info("  Succeeded:       %d", succeeded)
    log.info("  Still failed:    %d", len(failed_entries))
    log.info("  Log file:        %s", LOG_FILE)
    log.info("  Progress file:   %s", PROGRESS_FILE)
    log.info("=" * 60)

    print(f"\n{'='*60}")
    print(f"  Succeeded: {succeeded}")
    print(f"  Still failed: {len(failed_entries)}")
    print(f"{'='*60}\n")

    # Write final failed list for inspection
    if failed_entries:
        failed_out = SCRIPT_DIR / "refetch_still_failed.json"
        failed_out.write_text(
            json.dumps(failed_entries, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        log.info("Still-failed entries written to: %s", failed_out)


if __name__ == "__main__":
    main()
