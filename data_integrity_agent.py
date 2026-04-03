#!/usr/bin/env python3
"""
Data Integrity Agent
====================
A robust agent that monitors data integrity across the entire database.
Runs under the orchestrator to find and report gaps automatically.

Checks:
1. Reporter parity (2024 vs 2025 and future years)
2. JSONL file existence for all scraped reporters
3. Case count verification against PLS
4. Four format consistency (JSON, Original HTML, Readable HTML, JSONL)
5. Missing cases within reporters
6. Statute-case link integrity

Auto-fixes:
- Generates missing JSONL files
- Generates missing readable HTML
- Reports unfixable gaps for manual scraping
"""

import os
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path("data_v2")
REPORTS_DIR = DATA_DIR / "integrity_reports"
REPORTS_DIR.mkdir(exist_ok=True)

# All known reporters
ALL_REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Years to check (will be dynamic based on what exists)
CURRENT_YEAR = datetime.now().year

# Full scrape years (these should have complete data)
FULL_SCRAPE_YEARS = [2024, 2025]

# Minimum cases for a "full" year (ignore sparse historical data)
MIN_CASES_FOR_PARITY_CHECK = 50

# Credentials for PLS verification
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class GapReport:
    """Represents a gap in the data."""
    gap_type: str  # missing_jsonl, missing_html, incomplete_scrape, format_mismatch, etc.
    reporter: str
    year: int
    details: str
    severity: str  # critical, warning, info
    auto_fixable: bool
    fix_action: str = ""
    fixed: bool = False


@dataclass 
class IntegrityReport:
    """Full integrity report."""
    timestamp: str
    total_gaps: int = 0
    critical_gaps: int = 0
    warning_gaps: int = 0
    auto_fixed: int = 0
    gaps: List[Dict] = field(default_factory=list)
    summary: Dict = field(default_factory=dict)
    
    def add_gap(self, gap: GapReport):
        self.gaps.append(asdict(gap))
        self.total_gaps += 1
        if gap.severity == "critical":
            self.critical_gaps += 1
        elif gap.severity == "warning":
            self.warning_gaps += 1
        if gap.fixed:
            self.auto_fixed += 1


class DataIntegrityAgent:
    """Agent that monitors and fixes data integrity issues."""
    
    def __init__(self, verify_with_pls: bool = False):
        self.verify_with_pls = verify_with_pls
        self.session = None
        self.logged_in = False
        self.report = IntegrityReport(timestamp=datetime.now().isoformat())
        
    def _login(self) -> bool:
        """Login to PLS for verification."""
        if not self.verify_with_pls:
            return True
        if self.logged_in:
            return True
            
        try:
            self.session = Session(impersonate=BrowserType.chrome120)
            self.session.get(f"{BASE_URL}/")
            
            resp = self.session.post(
                f"{BASE_URL}/Login/Login",
                data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                },
                allow_redirects=True
            )
            
            check = self.session.get(f"{BASE_URL}/Login/Check")
            if "Logout" in check.text:
                self.logged_in = True
                logger.info("[OK] PLS login successful")
                return True
        except Exception as e:
            logger.error(f"PLS login failed: {e}")
        return False
    
    def get_pls_case_count(self, reporter: str, year: int) -> Optional[int]:
        """Get case count from PLS for verification."""
        if not self._login():
            return None
            
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/SearchCaseLaw",
                data={
                    "keyword": "",
                    "year": str(year),
                    "book": reporter,
                    "court": "",
                    "judge": ""
                }
            )
            
            # Count casetypeid attributes
            matches = re.findall(r'casetypeid="[^"]+"', resp.text)
            return len(matches)
        except:
            return None
    
    def get_local_years(self) -> Set[int]:
        """Get all years that have local data."""
        years = set()
        for reporter_dir in DATA_DIR.iterdir():
            if reporter_dir.is_dir() and reporter_dir.name in ALL_REPORTERS:
                for year_dir in reporter_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        years.add(int(year_dir.name))
        return years
    
    def get_local_reporters_for_year(self, year: int) -> Dict[str, int]:
        """Get reporters and their case counts for a year."""
        reporters = {}
        for reporter in ALL_REPORTERS:
            reporter_dir = DATA_DIR / reporter / str(year)
            if reporter_dir.exists():
                count = len(list(reporter_dir.glob("*.json")))
                if count > 0:
                    reporters[reporter] = count
        return reporters
    
    def check_jsonl_existence(self, reporter: str, year: int) -> bool:
        """Check if JSONL file exists for reporter/year."""
        jsonl_file = DATA_DIR / f"{reporter}_{year}.jsonl"
        return jsonl_file.exists()
    
    def check_format_consistency(self, reporter: str, year: int) -> Dict[str, int]:
        """Check all 4 formats for a reporter/year."""
        base_dir = DATA_DIR / reporter / str(year)
        html_dir = DATA_DIR / "html" / reporter / str(year)
        
        counts = {
            "json": 0,
            "original_html": 0,
            "readable_html": 0,
        }
        
        if base_dir.exists():
            counts["json"] = len(list(base_dir.glob("*.json")))
            orig_dir = base_dir / "original"
            if orig_dir.exists():
                counts["original_html"] = len(list(orig_dir.glob("*.html")))
        
        if html_dir.exists():
            counts["readable_html"] = len(list(html_dir.glob("*.html")))
        
        return counts
    
    def generate_missing_jsonl(self, reporter: str, year: int) -> bool:
        """Generate JSONL from JSON files."""
        json_dir = DATA_DIR / reporter / str(year)
        jsonl_file = DATA_DIR / f"{reporter}_{year}.jsonl"
        
        if not json_dir.exists():
            return False
        
        try:
            json_files = list(json_dir.glob("*.json"))
            if not json_files:
                return False
            
            with open(jsonl_file, 'w', encoding='utf-8') as out:
                for jf in sorted(json_files):
                    with open(jf, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        out.write(json.dumps(data, ensure_ascii=False) + '\n')
            
            logger.info(f"  [FIXED] Generated {jsonl_file.name} ({len(json_files)} cases)")
            return True
        except Exception as e:
            logger.error(f"  Failed to generate JSONL: {e}")
            return False
    
    def generate_missing_readable_html(self, reporter: str, year: int) -> int:
        """Generate readable HTML for missing cases."""
        from generate_html import generate_case_html  # Import the HTML generator
        
        json_dir = DATA_DIR / reporter / str(year)
        html_dir = DATA_DIR / "html" / reporter / str(year)
        html_dir.mkdir(parents=True, exist_ok=True)
        
        generated = 0
        for json_file in json_dir.glob("*.json"):
            html_file = html_dir / f"{json_file.stem}.html"
            if not html_file.exists():
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        case_data = json.load(f)
                    html_content = generate_case_html(case_data)
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    generated += 1
                except:
                    pass
        
        if generated > 0:
            logger.info(f"  [FIXED] Generated {generated} readable HTML files for {reporter} {year}")
        return generated
    
    def check_year_parity(self, year1: int, year2: int) -> List[GapReport]:
        """Check if two years have the same reporters.
        
        Only flags issues for years with substantial data (>50 cases).
        Historical years with sparse linked-case data are ignored.
        """
        gaps = []
        
        reporters1 = self.get_local_reporters_for_year(year1)
        reporters2 = self.get_local_reporters_for_year(year2)
        
        # Only compare if both years are "full scrape" years or have substantial data
        year1_is_full = year1 in FULL_SCRAPE_YEARS or sum(reporters1.values()) > MIN_CASES_FOR_PARITY_CHECK * len(reporters1)
        year2_is_full = year2 in FULL_SCRAPE_YEARS or sum(reporters2.values()) > MIN_CASES_FOR_PARITY_CHECK * len(reporters2)
        
        if not (year1_is_full and year2_is_full):
            return gaps  # Skip parity check for sparse historical data
        
        # Check reporters in year2 but not year1
        for reporter in reporters2:
            if reporter not in reporters1 and reporters2[reporter] >= MIN_CASES_FOR_PARITY_CHECK:
                gaps.append(GapReport(
                    gap_type="missing_reporter",
                    reporter=reporter,
                    year=year1,
                    details=f"{reporter} exists in {year2} ({reporters2[reporter]} cases) but not in {year1}",
                    severity="critical",
                    auto_fixable=False,
                    fix_action=f"Run: python pls_scraper_v2.py scrape --reporter {reporter} --year {year1}"
                ))
        
        # Check reporters in year1 but not year2 (only if year1 has substantial cases)
        for reporter in reporters1:
            if reporter not in reporters2 and reporters1[reporter] >= MIN_CASES_FOR_PARITY_CHECK:
                gaps.append(GapReport(
                    gap_type="missing_reporter",
                    reporter=reporter,
                    year=year2,
                    details=f"{reporter} exists in {year1} ({reporters1[reporter]} cases) but not in {year2}",
                    severity="warning",  # year2 might still be in progress
                    auto_fixable=False,
                    fix_action=f"Run: python pls_scraper_v2.py scrape --reporter {reporter} --year {year2}"
                ))
        
        return gaps
    
    def check_incomplete_scrapes(self, reporter: str, year: int) -> Optional[GapReport]:
        """Check if a reporter/year scrape is incomplete by comparing with PLS."""
        if not self.verify_with_pls:
            return None
        
        local_count = len(list((DATA_DIR / reporter / str(year)).glob("*.json")))
        pls_count = self.get_pls_case_count(reporter, year)
        
        if pls_count is None:
            return None
        
        if local_count < pls_count * 0.9:  # More than 10% missing
            return GapReport(
                gap_type="incomplete_scrape",
                reporter=reporter,
                year=year,
                details=f"Local: {local_count}, PLS: {pls_count} ({pls_count - local_count} missing)",
                severity="critical",
                auto_fixable=False,
                fix_action=f"Run: python pls_scraper_v2.py scrape --reporter {reporter} --year {year}"
            )
        elif local_count < pls_count:
            return GapReport(
                gap_type="incomplete_scrape",
                reporter=reporter,
                year=year,
                details=f"Local: {local_count}, PLS: {pls_count} ({pls_count - local_count} missing)",
                severity="warning",
                auto_fixable=False,
                fix_action=f"Run: python pls_scraper_v2.py scrape --reporter {reporter} --year {year}"
            )
        
        return None
    
    def run_full_check(self, auto_fix: bool = True) -> IntegrityReport:
        """Run complete integrity check."""
        logger.info("=" * 70)
        logger.info("DATA INTEGRITY AGENT - Full Check")
        logger.info("=" * 70)
        
        years = sorted(self.get_local_years(), reverse=True)
        logger.info(f"Found data for years: {years}")
        
        # 1. Check year parity (compare consecutive years)
        logger.info("\n[1/5] Checking year parity...")
        for i in range(len(years) - 1):
            gaps = self.check_year_parity(years[i+1], years[i])
            for gap in gaps:
                self.report.add_gap(gap)
                logger.warning(f"  GAP: {gap.details}")
        
        # 2. Check JSONL existence
        logger.info("\n[2/5] Checking JSONL files...")
        for year in years:
            reporters = self.get_local_reporters_for_year(year)
            for reporter, count in reporters.items():
                if count > 0 and not self.check_jsonl_existence(reporter, year):
                    gap = GapReport(
                        gap_type="missing_jsonl",
                        reporter=reporter,
                        year=year,
                        details=f"{reporter}_{year}.jsonl missing ({count} JSON files exist)",
                        severity="warning",
                        auto_fixable=True,
                        fix_action="Generate JSONL from JSON files"
                    )
                    
                    if auto_fix:
                        if self.generate_missing_jsonl(reporter, year):
                            gap.fixed = True
                    
                    self.report.add_gap(gap)
        
        # 3. Check format consistency
        logger.info("\n[3/5] Checking format consistency...")
        for year in years:
            reporters = self.get_local_reporters_for_year(year)
            for reporter in reporters:
                counts = self.check_format_consistency(reporter, year)
                
                # Check original HTML
                if counts["json"] > 0 and counts["original_html"] < counts["json"]:
                    missing = counts["json"] - counts["original_html"]
                    self.report.add_gap(GapReport(
                        gap_type="missing_original_html",
                        reporter=reporter,
                        year=year,
                        details=f"{missing} original HTML files missing",
                        severity="info",
                        auto_fixable=False,
                        fix_action="Re-scrape with original HTML preservation"
                    ))
                
                # Check readable HTML
                if counts["json"] > 0 and counts["readable_html"] < counts["json"]:
                    missing = counts["json"] - counts["readable_html"]
                    gap = GapReport(
                        gap_type="missing_readable_html",
                        reporter=reporter,
                        year=year,
                        details=f"{missing} readable HTML files missing",
                        severity="warning",
                        auto_fixable=True,
                        fix_action="Generate from JSON files"
                    )
                    
                    if auto_fix:
                        try:
                            fixed = self.generate_missing_readable_html(reporter, year)
                            if fixed > 0:
                                gap.fixed = True
                                gap.details = f"Fixed {fixed}/{missing} readable HTML files"
                        except:
                            pass
                    
                    self.report.add_gap(gap)
        
        # 4. Check incomplete scrapes (if PLS verification enabled)
        if self.verify_with_pls:
            logger.info("\n[4/5] Verifying against PLS...")
            for year in years[-2:]:  # Only check recent years
                reporters = self.get_local_reporters_for_year(year)
                for reporter in reporters:
                    gap = self.check_incomplete_scrapes(reporter, year)
                    if gap:
                        self.report.add_gap(gap)
                        logger.warning(f"  GAP: {gap.details}")
        else:
            logger.info("\n[4/5] Skipping PLS verification (disabled)")
        
        # 5. Check statute-case links
        logger.info("\n[5/5] Checking statute-case link integrity...")
        links_file = DATA_DIR / "legislation" / "statute_case_links.jsonl"
        if links_file.exists():
            missing_linked = 0
            total_links = 0
            with open(links_file) as f:
                for line in f:
                    total_links += 1
                    link = json.loads(line)
                    citation = link.get("citation", "").strip().rstrip(',')
                    parts = citation.split()
                    if len(parts) >= 3:
                        year, reporter, page = parts[0], parts[1], parts[2]
                        case_file = DATA_DIR / reporter / year / f"{year}_{reporter}_{page}.json"
                        if not case_file.exists():
                            missing_linked += 1
            
            if missing_linked > 0:
                self.report.add_gap(GapReport(
                    gap_type="missing_linked_cases",
                    reporter="ALL",
                    year=0,
                    details=f"{missing_linked}/{total_links} statute-linked cases not yet scraped",
                    severity="info" if missing_linked < 50 else "warning",
                    auto_fixable=False,
                    fix_action="Run: python linked_cases_scraper.py"
                ))
        
        # Generate summary
        self.report.summary = {
            "years_checked": years,
            "reporters_checked": ALL_REPORTERS,
            "total_gaps": self.report.total_gaps,
            "critical": self.report.critical_gaps,
            "warnings": self.report.warning_gaps,
            "auto_fixed": self.report.auto_fixed
        }
        
        # Save report
        report_file = REPORTS_DIR / f"integrity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, ensure_ascii=False)
        
        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("INTEGRITY CHECK COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Total gaps found: {self.report.total_gaps}")
        logger.info(f"  Critical: {self.report.critical_gaps}")
        logger.info(f"  Warnings: {self.report.warning_gaps}")
        logger.info(f"  Auto-fixed: {self.report.auto_fixed}")
        logger.info(f"Report saved: {report_file}")
        
        return self.report


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Data Integrity Agent")
    parser.add_argument("--verify-pls", action="store_true", help="Verify against PLS (requires login)")
    parser.add_argument("--no-fix", action="store_true", help="Don't auto-fix issues")
    parser.add_argument("--report-only", action="store_true", help="Only generate report, no fixes")
    args = parser.parse_args()
    
    agent = DataIntegrityAgent(verify_with_pls=args.verify_pls)
    report = agent.run_full_check(auto_fix=not args.no_fix and not args.report_only)
    
    # Exit with error code if critical gaps found
    if report.critical_gaps > 0:
        exit(1)


if __name__ == "__main__":
    main()
