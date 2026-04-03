#!/usr/bin/env python3
"""
Migrate Legislation Format
===========================
Converts existing legislation JSON files to the new v2 format.

This script:
1. Reads existing JSON files
2. Converts section text from raw HTML to clean text
3. Preserves raw HTML in text_raw field
4. Updates the JSON schema to match case law format
5. Generates missing JSONL files (per letter and master)
6. Generates clean HTML files

Usage:
    python migrate_legislation_format.py --letter A      # Migrate specific letter
    python migrate_legislation_format.py --all           # Migrate all letters
    python migrate_legislation_format.py --dry-run       # Preview without writing
"""

import json
import re
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from html_cleaner import (
    strip_html_to_text,
    extract_preamble,
    normalize_citation,
    generate_statute_slug
)
from generate_legislation_html import generate_statute_html
from case_link_enricher import enrich_case_links, enrich_statute_case_links

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
HTML_DIR = DATA_DIR / "html" / "statutes"
ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def migrate_statute(json_file: Path, dry_run: bool = False) -> Dict:
    """Migrate a single statute JSON file to new format."""
    
    try:
        content = json_file.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        # Try with latin-1 fallback
        content = json_file.read_text(encoding='latin-1')
    
    data = json.loads(content)
    
    sections = data.get("sections", [])
    
    # Check if already has the new format with enriched case links
    # If full_text_raw is present AND cases_cited contains dicts, skip full migration
    cases = data.get("cases_cited", [])
    has_enriched_cases = cases and isinstance(cases[0], dict) and "exists_locally" in cases[0]
    
    if data.get("full_text_raw") and data.get("preamble_raw") is not None and has_enriched_cases:
        logger.debug(f"Already fully migrated: {json_file.name}")
        return data
    
    # Check if sections already have text_raw (partial migration)
    already_has_text_raw = sections and isinstance(sections[0], dict) and "text_raw" in sections[0]
    
    # Migrate sections
    migrated_sections = []
    for sec in sections:
        section_num = sec.get("number", "")
        section_ref = f"Section {section_num}" if section_num else ""
        
        if already_has_text_raw:
            # Already partially migrated - sections have text (clean) and text_raw (raw HTML)
            raw_html = sec.get("text_raw", "")
            clean_text = sec.get("text", "")
            # Handle cases_cited which might already be a list of strings or dicts
            cases = sec.get("cases_cited", [])
            if cases and isinstance(cases[0], dict):
                # Already enriched - extract citation strings for re-enrichment
                case_citations = [c.get("citation", "") for c in cases if c.get("citation")]
            elif cases and isinstance(cases[0], str):
                case_citations = [normalize_citation(c) for c in cases if c]
            else:
                case_citations = []
        else:
            # Old format - "text" contains raw HTML
            raw_html = sec.get("text", "")
            clean_text = strip_html_to_text(raw_html)
            # case_links is the old field name
            case_citations = [normalize_citation(c.get("citation", "")) 
                             for c in sec.get("case_links", []) if c.get("citation")]
        
        # Enrich case links with full details
        enriched_cases = enrich_case_links(case_citations, section_ref)
        
        new_sec = {
            "number": section_num,
            "title": sec.get("title", sec.get("definition", "")),
            "text": clean_text,
            "text_raw": raw_html,
            "cases_cited": enriched_cases,
            "section_id": sec.get("section_id", ""),
        }
        migrated_sections.append(new_sec)
    
    # Build full_text_raw from all section raw HTML
    full_text_raw = "\n\n<!-- SECTION BREAK -->\n\n".join(
        s['text_raw'] for s in migrated_sections if s.get('text_raw')
    )
    
    # Find preamble raw
    preamble_raw = ""
    for s in migrated_sections:
        if s['number'].upper() == 'PREAMBLE' or 'preamble' in s['number'].lower():
            preamble_raw = s.get('text_raw', '')
            break
    
    # Collect all unique enriched cases from sections
    all_cases = []
    seen_citations = set()
    for s in migrated_sections:
        for case in s.get("cases_cited", []):
            if isinstance(case, dict):
                citation = case.get("citation", "")
                if citation and citation not in seen_citations:
                    seen_citations.add(citation)
                    all_cases.append(case.copy())
    
    # Update the main document
    migrated = {
        "citation": data.get("title", ""),
        "statute_id": data.get("id", data.get("statute_id", "")),
        "title": data.get("title", ""),
        "short_title": data.get("short_title", ""),
        "alphabet": data.get("alphabet", ""),
        "jurisdiction": data.get("jurisdiction", "Federal"),
        "enactment_date": data.get("enactment_date", ""),
        "status": data.get("status", "in_force"),
        "preamble": extract_preamble(migrated_sections) or "",
        "preamble_raw": preamble_raw,
        "full_text": "\n\n".join(
            f"[Section {s['number']}]\n{s['text']}" 
            for s in migrated_sections if s['text']
        ),
        "full_text_raw": full_text_raw,
        "sections": migrated_sections,
        "statutes_cited": data.get("statutes_cited", []),
        "cases_cited": all_cases,
        "fetched_at": data.get("scraped_at", datetime.now().isoformat()),
        "source_url": data.get("source_url", ""),
    }
    
    return migrated


def migrate_letter(letter: str, dry_run: bool = False) -> tuple:
    """Migrate all statutes for a letter. Returns (success_count, fail_count)."""
    
    letter_dir = LEGISLATION_DIR / letter
    html_letter_dir = HTML_DIR / letter
    original_dir = letter_dir / "original"
    
    if not letter_dir.exists():
        logger.info(f"No data for letter {letter}")
        return 0, 0
    
    # Ensure directories exist
    if not dry_run:
        html_letter_dir.mkdir(parents=True, exist_ok=True)
        original_dir.mkdir(exist_ok=True)
    
    json_files = list(letter_dir.glob("*.json"))
    if not json_files:
        return 0, 0
    
    logger.info(f"Migrating {len(json_files)} statutes for letter {letter}...")
    
    success = 0
    failed = 0
    all_statutes = []
    
    for json_file in json_files:
        try:
            migrated = migrate_statute(json_file, dry_run)
            
            if not dry_run:
                # Save updated JSON
                json_file.write_text(
                    json.dumps(migrated, indent=2, ensure_ascii=False),
                    encoding='utf-8'
                )
                
                # Generate and save clean HTML
                slug = json_file.stem
                html_content = generate_statute_html(migrated)
                html_path = html_letter_dir / f"{slug}.html"
                html_path.write_text(html_content, encoding='utf-8')
            
            all_statutes.append(migrated)
            success += 1
            
        except Exception as e:
            logger.error(f"Failed to migrate {json_file.name}: {e}")
            failed += 1
    
    # Generate letter JSONL
    if not dry_run and all_statutes:
        jsonl_path = LEGISLATION_DIR / f"{letter}.jsonl"
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for statute in all_statutes:
                f.write(json.dumps(statute, ensure_ascii=False) + '\n')
        logger.info(f"  Generated {jsonl_path.name}")
    
    return success, failed


def migrate_all(dry_run: bool = False):
    """Migrate all letters."""
    
    total_success = 0
    total_failed = 0
    all_statutes = []
    
    for letter in ALPHABETS:
        letter_dir = LEGISLATION_DIR / letter
        if letter_dir.exists():
            success, failed = migrate_letter(letter, dry_run)
            total_success += success
            total_failed += failed
            
            # Collect for master JSONL
            if not dry_run:
                jsonl_path = LEGISLATION_DIR / f"{letter}.jsonl"
                if jsonl_path.exists():
                    with open(jsonl_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                all_statutes.append(json.loads(line))
    
    # Generate master JSONL
    if not dry_run and all_statutes:
        master_jsonl = LEGISLATION_DIR / "all_statutes.jsonl"
        # Clear and rewrite (to remove duplicates from old appends)
        with open(master_jsonl, 'w', encoding='utf-8') as f:
            for statute in all_statutes:
                f.write(json.dumps(statute, ensure_ascii=False) + '\n')
        logger.info(f"Generated master JSONL: {len(all_statutes)} statutes")
    
    logger.info(f"Migration complete: {total_success} success, {total_failed} failed")
    
    return total_success, total_failed


def main():
    parser = argparse.ArgumentParser(description="Migrate legislation to new format")
    parser.add_argument("--letter", "-l", help="Migrate specific letter")
    parser.add_argument("--all", action="store_true", help="Migrate all letters")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    
    args = parser.parse_args()
    
    if args.letter:
        success, failed = migrate_letter(args.letter.upper(), args.dry_run)
        print(f"Letter {args.letter}: {success} success, {failed} failed")
    elif args.all:
        migrate_all(args.dry_run)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
