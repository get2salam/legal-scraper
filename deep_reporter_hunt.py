#!/usr/bin/env python3
"""
deep_reporter_hunt.py — Creative multi-technique reporter & case discovery

Goes beyond simple dropdown scraping. Uses 7 different techniques to find
reporters and cases that might be hiding on PLS.

Techniques:
    1. DROPDOWN_SCAN      — Scrape ALL form dropdowns/radio/checkboxes
    2. BRUTE_FORCE_CODES  — Test 200+ known Pakistani legal abbreviations
    3. WILDCARD_YEARS     — For known reporters, check years we might have skipped
    4. ADJACENT_PAGES     — Check if page numbers extend beyond what we scraped
    5. CITATION_CROSS_REF — Extract reporter codes from cases_cited in existing data
    6. URL_PATTERN_PROBE  — Try URL patterns PLS might use for hidden reporters
    7. COMPARE_COUNTS     — Compare our counts vs PLS dropdown counts per year

Usage:
    python deep_reporter_hunt.py                    # Run all techniques
    python deep_reporter_hunt.py --technique 5      # Only citation cross-ref
    python deep_reporter_hunt.py --quick             # Fast mode (fewer probes)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S", stream=sys.stdout)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2"
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\reporter-hunt")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

PLS_USER = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
PLS_PASS = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
BASE_URL = "https://www.pakistanlawsite.com"
DELAY = 2.5

KNOWN_REPORTERS = [
    "SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR",
    "PLC(CS)", "CLCN", "PCRLJN", "PLC(CS)N", "YLRN", "PLCCS",
]

try:
    from curl_cffi import requests as cffi_requests
    session = cffi_requests.Session()
    session.impersonate = "chrome"
except ImportError:
    import requests
    session = requests.Session()

from bs4 import BeautifulSoup


def pls_login():
    resp = session.post(f"{BASE_URL}/Login/ClearLoginHistory",
        data={"Login.UserName": PLS_USER, "Login.Password": PLS_PASS}, timeout=30)
    return resp.status_code == 200


PLS_BASELINE = 14  # PLS page template has 14 "caseType" strings even with zero results

def citation_search(year, reporter):
    """Search PLS and return REAL case count (subtracting page baseline)."""
    try:
        resp = session.post(f"{BASE_URL}/Login/CitationSearch",
            data={"year": year, "book": reporter, "code": "", "court": "", "judge": "", "lawyer": "", "party": ""},
            timeout=30)
        raw = resp.text.count("caseType")
        return max(0, raw - PLS_BASELINE)
    except Exception:
        return 0


# ── Technique 1: Dropdown Scan ──────────────────────────────────────────────

def technique_dropdown_scan():
    """Scrape ALL form elements from PLS search pages."""
    log.info("=== TECHNIQUE 1: Dropdown Scan ===")
    
    reporters = set()
    pages = [
        f"{BASE_URL}/Login/Check",
        f"{BASE_URL}/Login/CitationSearch",
        f"{BASE_URL}/Login/AdvanceSearch",
    ]
    
    for url in pages:
        try:
            time.sleep(DELAY)
            resp = session.get(url, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Select/option elements
            for select in soup.find_all("select"):
                for option in select.find_all("option"):
                    val = option.get("value", "").strip()
                    if val and len(val) <= 15 and val.upper() == val:
                        reporters.add(val)
            
            # Radio buttons
            for radio in soup.find_all("input", type="radio"):
                val = radio.get("value", "").strip()
                if val and len(val) <= 15:
                    reporters.add(val)
            
            # Hidden inputs that might contain reporter lists
            for hidden in soup.find_all("input", type="hidden"):
                val = hidden.get("value", "")
                if "," in val:
                    for part in val.split(","):
                        part = part.strip()
                        if len(part) <= 15 and part.isalpha():
                            reporters.add(part)
            
            log.info(f"  {url.split('/')[-1]}: found {len(reporters)} so far")
        except Exception as e:
            log.error(f"  Error scanning {url}: {e}")
    
    new = reporters - set(KNOWN_REPORTERS)
    log.info(f"  Total: {len(reporters)} | New: {len(new)}")
    return {"all": sorted(reporters), "new": sorted(new)}


# ── Technique 2: Brute Force Codes ──────────────────────────────────────────

def technique_brute_force(quick=False):
    """Test 200+ known Pakistani legal abbreviations."""
    log.info("=== TECHNIQUE 2: Brute Force Reporter Codes ===")
    
    # Comprehensive list of Pakistani legal abbreviations
    candidates = [
        # Known variants
        "SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR",
        "PLCCS", "CLCN", "PCRLJN", "YLRN",
        # Possible reporters
        "PLJ", "NLR", "PTCL", "PSC", "KLR", "FTR", "STR", "SLR", "BLR", "KPLR",
        "PLR", "CLR", "DLR", "ALR", "ILR", "TLR", "MLR", "NLJ",
        "ALD", "LAW", "TAX", "CCI", "AIR", "SCC", "SCR", "ACR",
        "PCBLR", "PCTLR", "FSJ", "LHR", "IHCR", "SHCR", "BHCR",
        "MPLR", "FSCR", "ASSR", "LNotes", "PLDNotes",
        # High court reporter variants
        "LHC", "SHC", "IHC", "PHC", "BHC", "FSC", "SST", "FCC",
        # Tax reporters
        "PTDA", "PTDB", "TAD", "ITR", "CTR", "STD", "TTR",
        # Labour reporters
        "PLCS", "PLCN", "LLR", "ILT", "ILTD",
        # Criminal
        "PCrLJN", "CrLJ", "CrR", "CrA",
        # Civil
        "CLCA", "CLCB", "CPC",
        # Constitutional
        "PLC(S)", "PLJ(S)", "SCMRN",
        # Shariat
        "ISJ", "SLR", "FSCJ", "ISR",
        # Special
        "SECP", "SBP", "NAB", "FIA", "ITAT",
        # Regional
        "PLJ(AJK)", "AJK", "AJKLR", "NWFP", "KPK",
    ]
    
    # Deduplicate and remove known
    candidates = sorted(set(c for c in candidates if c not in KNOWN_REPORTERS))
    
    if quick:
        candidates = candidates[:30]
    
    log.info(f"  Testing {len(candidates)} candidates...")
    
    found = {}
    test_years = [2024, 2020, 2015, 2010, 2000]
    
    for i, code in enumerate(candidates):
        if (i + 1) % 20 == 0:
            log.info(f"  Progress: {i+1}/{len(candidates)}")
        
        total = 0
        for year in test_years:
            time.sleep(DELAY)
            count = citation_search(year, code)
            total += count
            if count > 0:
                break  # Found something, no need to check more years
        
        if total > 0:
            found[code] = total
            log.info(f"  FOUND: {code} has {total} cases!")
    
    log.info(f"  Results: {len(found)} new reporters found")
    return found


# ── Technique 3: Year Gap Check ─────────────────────────────────────────────

def technique_year_gaps(quick=False):
    """Check if any years were skipped for known reporters."""
    log.info("=== TECHNIQUE 3: Year Gap Check ===")
    
    gaps_found = {}
    reporters_to_check = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
    
    for reporter in reporters_to_check:
        rep_dir = DATA_DIR / reporter
        if not rep_dir.exists():
            continue
        
        # Get years we have
        our_years = set()
        for d in rep_dir.iterdir():
            if d.is_dir():
                try:
                    our_years.add(int(d.name))
                except ValueError:
                    pass
        
        if not our_years:
            continue
        
        min_year = min(our_years)
        max_year = max(our_years)
        
        # Check missing years in our range
        missing = []
        years_to_check = sorted(set(range(min_year, max_year + 1)) - our_years)
        
        if quick:
            years_to_check = years_to_check[:5]
        
        for year in years_to_check:
            time.sleep(DELAY)
            count = citation_search(year, reporter)
            if count > 0:
                missing.append({"year": year, "cases": count})
                log.info(f"  GAP: {reporter} {year} has {count} cases on PLS but we have 0!")
        
        if missing:
            gaps_found[reporter] = missing
    
    log.info(f"  Found gaps in {len(gaps_found)} reporters")
    return gaps_found


# ── Technique 4: Page Range Check ───────────────────────────────────────────

def technique_page_range():
    """For recent years, check if PLS has more cases than we scraped."""
    log.info("=== TECHNIQUE 4: Count Comparison (Our Count vs PLS) ===")
    
    mismatches = {}
    years_to_check = [2026, 2025, 2024]
    reporters = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD"]
    
    for reporter in reporters:
        for year in years_to_check:
            # Our count
            our_dir = DATA_DIR / reporter / str(year)
            our_count = len(list(our_dir.glob("*.json"))) if our_dir.exists() else 0
            
            # PLS count
            time.sleep(DELAY)
            pls_count = citation_search(year, reporter)
            
            diff = pls_count - our_count
            if diff > 5:  # More than 5 cases difference
                mismatches[f"{year} {reporter}"] = {
                    "our_count": our_count,
                    "pls_count": pls_count,
                    "missing": diff,
                }
                log.info(f"  MISMATCH: {year} {reporter}: we have {our_count}, PLS has {pls_count} (missing {diff})")
    
    log.info(f"  Found {len(mismatches)} mismatches")
    return mismatches


# ── Technique 5: Citation Cross-Reference ───────────────────────────────────

def technique_citation_crossref():
    """Extract reporter codes from cases_cited in existing data."""
    log.info("=== TECHNIQUE 5: Citation Cross-Reference ===")
    
    cited_reporters = Counter()
    sample_count = 0
    max_samples = 5000
    
    # Sample JSON files and extract citations
    for reporter_dir in DATA_DIR.iterdir():
        if not reporter_dir.is_dir() or reporter_dir.name in ("legislation", "federal_laws", "court_cases"):
            continue
        
        for year_dir in reporter_dir.iterdir():
            if not year_dir.is_dir():
                continue
            
            for json_file in year_dir.glob("*.json"):
                if sample_count >= max_samples:
                    break
                
                try:
                    data = json.load(open(json_file, encoding="utf-8"))
                    
                    # Check cases_cited, citations, headnotes for reporter codes
                    for field in ["cases_cited", "citations", "headnotes", "judgment"]:
                        text = str(data.get(field, ""))
                        if text:
                            # Find patterns like "2024 XYZ 123" 
                            matches = re.findall(r'\d{4}\s+([A-Z][A-Za-z()]+)\s+\d+', text)
                            for m in matches:
                                if len(m) <= 15:
                                    cited_reporters[m] += 1
                    
                    sample_count += 1
                except Exception:
                    continue
            
            if sample_count >= max_samples:
                break
        if sample_count >= max_samples:
            break
    
    log.info(f"  Scanned {sample_count} cases")
    
    # Find reporters cited but not in our collection
    unknown = {}
    for code, count in cited_reporters.most_common(50):
        if code not in KNOWN_REPORTERS and code not in ("Versus", "ORDER", "PETITION", "APPEAL"):
            if count >= 3:  # At least 3 citations to be meaningful
                unknown[code] = count
    
    if unknown:
        log.info(f"  Unknown reporters cited in case text:")
        for code, count in sorted(unknown.items(), key=lambda x: -x[1])[:20]:
            log.info(f"    {code}: cited {count} times")
    
    log.info(f"  Found {len(unknown)} potentially unknown reporter codes")
    return unknown


# ── Technique 6: URL Pattern Probe ──────────────────────────────────────────

def technique_url_probe():
    """Try URL patterns PLS might use for hidden content."""
    log.info("=== TECHNIQUE 6: URL Pattern Probe ===")
    
    findings = []
    
    # Try different PLS API endpoints
    endpoints = [
        "/Login/GetReporterList",
        "/Login/GetBookList",
        "/Login/GetAllBooks",
        "/Login/BookList",
        "/Login/GetCategories",
        "/api/reporters",
        "/api/books",
    ]
    
    for endpoint in endpoints:
        try:
            time.sleep(DELAY)
            resp = session.get(f"{BASE_URL}{endpoint}", timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                findings.append({
                    "endpoint": endpoint,
                    "status": resp.status_code,
                    "length": len(resp.text),
                    "preview": resp.text[:200],
                })
                log.info(f"  HIT: {endpoint} returned {len(resp.text)} bytes")
        except Exception:
            pass
    
    # Try POST variants
    post_endpoints = [
        "/Login/GetReporterList",
        "/Login/GetBookList",
    ]
    
    for endpoint in post_endpoints:
        try:
            time.sleep(DELAY)
            resp = session.post(f"{BASE_URL}{endpoint}", data={}, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                findings.append({
                    "endpoint": f"POST {endpoint}",
                    "status": resp.status_code,
                    "length": len(resp.text),
                    "preview": resp.text[:200],
                })
                log.info(f"  HIT: POST {endpoint} returned {len(resp.text)} bytes")
        except Exception:
            pass
    
    log.info(f"  Found {len(findings)} responsive endpoints")
    return findings


# ── Technique 7: 2026 Fresh Cases ───────────────────────────────────────────

def technique_2026_check():
    """Check all reporters for new 2026 cases we might have missed."""
    log.info("=== TECHNIQUE 7: 2026 Fresh Case Check ===")
    
    all_reporters = KNOWN_REPORTERS + ["PLC(CS)", "CLCN", "PCRLJN", "PLC(CS)N", "YLRN"]
    all_reporters = sorted(set(all_reporters))
    
    fresh = {}
    for reporter in all_reporters:
        time.sleep(DELAY)
        pls_count = citation_search(2026, reporter)
        
        # Our count
        rep_clean = reporter.replace("(", "").replace(")", "")
        our_dir = DATA_DIR / rep_clean / "2026"
        our_count = len(list(our_dir.glob("*.json"))) if our_dir.exists() else 0
        
        diff = pls_count - our_count
        if diff > 0:
            fresh[reporter] = {"pls": pls_count, "ours": our_count, "new": diff}
            log.info(f"  {reporter} 2026: PLS has {pls_count}, we have {our_count} (+{diff} new)")
    
    log.info(f"  Found {len(fresh)} reporters with new 2026 cases")
    return fresh


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--technique", type=int, help="Run only this technique (1-7)")
    parser.add_argument("--quick", action="store_true", help="Fast mode")
    args = parser.parse_args()
    
    log.info("=" * 60)
    log.info("DEEP REPORTER HUNT")
    log.info("=" * 60)
    
    if not pls_login():
        log.error("PLS login failed!")
        sys.exit(2)
    log.info("Logged in to PLS\n")
    
    results = {}
    
    techniques = {
        1: ("Dropdown Scan", lambda: technique_dropdown_scan()),
        2: ("Brute Force", lambda: technique_brute_force(args.quick)),
        3: ("Year Gaps", lambda: technique_year_gaps(args.quick)),
        4: ("Count Comparison", lambda: technique_page_range()),
        5: ("Citation Cross-Ref", lambda: technique_citation_crossref()),
        6: ("URL Probe", lambda: technique_url_probe()),
        7: ("2026 Fresh Cases", lambda: technique_2026_check()),
    }
    
    if args.technique:
        to_run = {args.technique: techniques[args.technique]}
    else:
        to_run = techniques
    
    for num, (name, fn) in to_run.items():
        try:
            results[name] = fn()
        except Exception as e:
            log.error(f"Technique {num} ({name}) failed: {e}")
            results[name] = {"error": str(e)}
    
    # Save results
    today = datetime.now().strftime("%Y-%m-%d")
    with open(RESULTS_DIR / f"{today}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    log.info(f"\n{'=' * 60}")
    log.info("HUNT COMPLETE")
    log.info(f"Results saved: {RESULTS_DIR / f'{today}.json'}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
