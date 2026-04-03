#!/usr/bin/env python3
"""
Fill Format Gaps
================
Scans all data_v2/ directories and ensures every case has all 4 formats:
  1. JSON       — data_v2/REPORTER/YEAR/CITATION.json
  2. Original   — data_v2/REPORTER/YEAR/original/CITATION.html
  3. Readable   — data_v2/html/REPORTER/YEAR/CITATION.html
  4. JSONL      — data_v2/REPORTER_YEAR.jsonl + data_v2/all_cases.jsonl

Usage:
    python fill_format_gaps.py --dry-run                 # Preview gaps
    python fill_format_gaps.py                            # Fix all gaps
    python fill_format_gaps.py --reporter SCMR            # Only SCMR
    python fill_format_gaps.py --year 2014                # Only 2014
    python fill_format_gaps.py --reporter PLD --year 2011 # Specific combo
"""

import os
import re
import json
import sys
import argparse
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, Optional, List

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"

REPORTERS = {"SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"}

# Reconfigure stdout for Windows compatibility
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Readable HTML Template
# ══════════════════════════════════════════════════════════════════════════════

def generate_readable_html(case_data: dict) -> str:
    """Generate styled readable HTML from case JSON data."""
    citation = case_data.get("citation", "Unknown")
    court = case_data.get("court", "")
    judges = case_data.get("judges", "")
    date_decided = case_data.get("date_decided", case_data.get("date", case_data.get("fetched_at", "")))
    title = case_data.get("title", case_data.get("case_name", ""))
    headnotes = case_data.get("headnotes", "")
    judgment_raw = case_data.get("judgment_raw", "")
    reporter = case_data.get("reporter", "")
    year = case_data.get("year", "")
    page = case_data.get("page", "")

    # Build metadata lines
    meta_lines = []
    if court:
        meta_lines.append(f"<b>Court:</b> {court}")
    if judges:
        meta_lines.append(f"<b>Judges:</b> {judges}")
    if date_decided:
        meta_lines.append(f"<b>Date:</b> {date_decided}")
    if reporter:
        meta_lines.append(f"<b>Reporter:</b> {reporter}")
    if year:
        meta_lines.append(f"<b>Year:</b> {year}")
    if page:
        meta_lines.append(f"<b>Page:</b> {page}")

    meta_html = "<br>\n".join(meta_lines) if meta_lines else "<i>No metadata available</i>"

    title_html = f"<h2>{title}</h2>" if title and title != citation else ""

    headnotes_html = ""
    if headnotes:
        headnotes_html = f'<div class="headnotes"><h3>Headnotes</h3>{headnotes}</div>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{citation}</title>
<style>body{{font-family:Georgia,serif;max-width:800px;margin:40px auto;padding:20px;line-height:1.6;color:#333}}
h1{{font-size:1.4rem;border-bottom:2px solid #333;padding-bottom:10px}}
h2{{font-size:1.1rem;color:#555;margin-top:5px}}
.meta{{background:#f5f5f5;padding:15px;border-radius:5px;margin:15px 0}}
.headnotes{{border-left:3px solid #666;padding-left:15px;margin:20px 0;color:#555}}
.judgment{{margin-top:20px}}</style></head>
<body><h1>{citation}</h1>
{title_html}
<div class="meta">{meta_html}</div>
{headnotes_html}
<div class="judgment">{judgment_raw}</div></body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
# JSONL Helpers
# ══════════════════════════════════════════════════════════════════════════════

def load_jsonl_citations(jsonl_path: Path) -> Set[str]:
    """Load all citations from a JSONL file into a set.
    Uses fast regex scanning on raw bytes for speed on large files."""
    citations = set()
    if not jsonl_path.exists():
        return citations

    file_size = jsonl_path.stat().st_size
    if file_size > 500_000_000:  # > 500MB — skip scanning, too slow
        logger.warning(f"  Skipping JSONL scan for {jsonl_path.name} ({file_size/1e9:.1f}GB — too large)")
        return citations

    try:
        # Fast: read raw and regex extract citations
        with open(jsonl_path, 'rb') as f:
            for line in f:
                m = re.search(rb'"citation":\s*"([^"]+)"', line)
                if m:
                    citations.add(m.group(1).decode('utf-8', errors='replace'))
    except Exception as e:
        logger.warning(f"Error reading {jsonl_path}: {e}")
    return citations


def append_to_jsonl(jsonl_path: Path, case_data: dict):
    """Append a case record to a JSONL file."""
    with open(jsonl_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(case_data, ensure_ascii=False) + '\n')


# ══════════════════════════════════════════════════════════════════════════════
# Gap Scanner & Filler
# ══════════════════════════════════════════════════════════════════════════════

class GapFiller:
    def __init__(self, dry_run: bool = False, target_reporter: str = None, target_year: int = None):
        self.dry_run = dry_run
        self.target_reporter = target_reporter
        self.target_year = target_year

        # Stats
        self.stats = {
            "json_found": 0,
            "original_missing": 0,
            "original_fixed": 0,
            "readable_missing": 0,
            "readable_fixed": 0,
            "jsonl_missing": 0,
            "jsonl_fixed": 0,
            "errors": 0,
        }

        # JSONL caches: {jsonl_path_str: set_of_citations}
        self._jsonl_caches: Dict[str, Set[str]] = {}
        # Track what we've appended to master this run (avoid scanning the huge file)
        self._master_appended: Set[str] = set()

    def _get_jsonl_cache(self, jsonl_path: Path) -> Set[str]:
        """Get or load the citation set for a JSONL file."""
        key = str(jsonl_path)
        if key not in self._jsonl_caches:
            self._jsonl_caches[key] = load_jsonl_citations(jsonl_path)
        return self._jsonl_caches[key]

    def _discover_reporter_years(self) -> List[tuple]:
        """Discover all (reporter, year) combos with JSON files."""
        combos = []
        for reporter_dir in sorted(DATA_DIR.iterdir()):
            if not reporter_dir.is_dir():
                continue
            reporter = reporter_dir.name
            if reporter not in REPORTERS:
                continue
            if self.target_reporter and reporter != self.target_reporter:
                continue
            for year_dir in sorted(reporter_dir.iterdir()):
                if not year_dir.is_dir():
                    continue
                if not re.match(r'^\d{4}$', year_dir.name):
                    continue
                year = int(year_dir.name)
                if self.target_year and year != self.target_year:
                    continue
                combos.append((reporter, year, year_dir))
        return combos

    def scan_and_fill(self):
        """Main entry: scan all data and fill gaps."""
        combos = self._discover_reporter_years()
        logger.info(f"Found {len(combos)} reporter/year combinations to check")
        if self.dry_run:
            logger.info("DRY RUN — no files will be written")

        for i, (reporter, year, year_dir) in enumerate(combos):
            if (i + 1) % 20 == 0 or i == 0:
                logger.info(f"Progress: {i+1}/{len(combos)} — {reporter}/{year}")
            self._process_year_dir(reporter, year, year_dir)

        self._print_stats()

    def _process_year_dir(self, reporter: str, year: int, year_dir: Path):
        """Process all JSON files in a single reporter/year directory."""
        json_files = list(year_dir.glob("*.json"))
        if not json_files:
            return

        # Pre-load JSONL cache for this reporter/year
        jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
        jsonl_citations = self._get_jsonl_cache(jsonl_path)
        master_path = DATA_DIR / "all_cases.jsonl"

        gaps_in_dir = {"original": 0, "readable": 0, "jsonl": 0}

        for json_file in json_files:
            self.stats["json_found"] += 1
            stem = json_file.stem  # e.g. "2024_SCMR_1"

            try:
                case_data = json.loads(json_file.read_text(encoding='utf-8'))
            except Exception as e:
                logger.warning(f"  Cannot read {json_file}: {e}")
                self.stats["errors"] += 1
                continue

            citation = case_data.get("citation", "")
            if not citation:
                # Try to reconstruct from filename
                citation = stem.replace("_", " ", 2)
                case_data["citation"] = citation

            # --- Check Original HTML ---
            orig_path = year_dir / "original" / f"{stem}.html"
            if not orig_path.exists():
                self.stats["original_missing"] += 1
                gaps_in_dir["original"] += 1
                if not self.dry_run:
                    judgment_raw = case_data.get("judgment_raw", "")
                    if judgment_raw:
                        orig_path.parent.mkdir(exist_ok=True)
                        orig_path.write_text(judgment_raw, encoding='utf-8')
                        self.stats["original_fixed"] += 1
                    else:
                        # Try judgment_html or judgment as fallback
                        fallback = case_data.get("judgment_html", case_data.get("judgment", ""))
                        if fallback:
                            orig_path.parent.mkdir(exist_ok=True)
                            orig_path.write_text(fallback, encoding='utf-8')
                            self.stats["original_fixed"] += 1

            # --- Check Readable HTML ---
            readable_path = DATA_DIR / "html" / reporter / str(year) / f"{stem}.html"
            if not readable_path.exists():
                self.stats["readable_missing"] += 1
                gaps_in_dir["readable"] += 1
                if not self.dry_run:
                    readable_html = generate_readable_html(case_data)
                    readable_path.parent.mkdir(parents=True, exist_ok=True)
                    readable_path.write_text(readable_html, encoding='utf-8')
                    self.stats["readable_fixed"] += 1

            # --- Check Reporter JSONL ---
            if citation and citation not in jsonl_citations:
                self.stats["jsonl_missing"] += 1
                gaps_in_dir["jsonl"] += 1
                if not self.dry_run:
                    append_to_jsonl(jsonl_path, case_data)
                    jsonl_citations.add(citation)
                    self.stats["jsonl_fixed"] += 1

                    # Also append to master
                    if citation not in self._master_appended:
                        append_to_jsonl(master_path, case_data)
                        self._master_appended.add(citation)

        # Log if gaps found
        total_gaps = sum(gaps_in_dir.values())
        if total_gaps > 0:
            logger.info(f"  {reporter}/{year}: {len(json_files)} cases, gaps: "
                        f"orig={gaps_in_dir['original']} readable={gaps_in_dir['readable']} "
                        f"jsonl={gaps_in_dir['jsonl']}")

    def _print_stats(self):
        """Print final statistics."""
        s = self.stats
        mode = "DRY RUN" if self.dry_run else "APPLIED"
        print(f"\n{'='*60}")
        print(f"FORMAT GAP FILLER — {mode}")
        print(f"{'='*60}")
        print(f"JSON files scanned:     {s['json_found']:>8,}")
        print(f"")
        print(f"Original HTML missing:  {s['original_missing']:>8,}")
        print(f"Original HTML fixed:    {s['original_fixed']:>8,}")
        print(f"")
        print(f"Readable HTML missing:  {s['readable_missing']:>8,}")
        print(f"Readable HTML fixed:    {s['readable_fixed']:>8,}")
        print(f"")
        print(f"JSONL entries missing:  {s['jsonl_missing']:>8,}")
        print(f"JSONL entries fixed:    {s['jsonl_fixed']:>8,}")
        print(f"")
        print(f"Errors:                 {s['errors']:>8,}")
        print(f"{'='*60}")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Fill format gaps in case law data")
    parser.add_argument("--dry-run", action="store_true", help="Preview gaps without writing")
    parser.add_argument("--reporter", type=str, help="Only process this reporter (e.g. SCMR)")
    parser.add_argument("--year", type=int, help="Only process this year (e.g. 2014)")
    args = parser.parse_args()

    filler = GapFiller(
        dry_run=args.dry_run,
        target_reporter=args.reporter,
        target_year=args.year,
    )
    filler.scan_and_fill()


if __name__ == "__main__":
    main()
