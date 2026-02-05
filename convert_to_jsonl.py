#!/usr/bin/env python3
"""
Convert existing individual JSON case files into JSONL format.
Reads from data/pakistanlawsite/cases/*.json and headnotes/*.json,
groups by book+year, and writes to data/pakistanlawsite/jsonl/cases_{BOOK}_{YEAR}.jsonl
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("data/pakistanlawsite")
CASES_DIR = DATA_DIR / "cases"
HEADNOTES_DIR = DATA_DIR / "headnotes"
JSONL_DIR = DATA_DIR / "jsonl"
PROGRESS_FILE = DATA_DIR / "pls_progress.json"


def load_progress():
    """Load progress file to map case names to book_year keys."""
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        return json.load(f)


def build_case_to_book_year_map(progress: dict) -> dict:
    """Build a mapping from caseName -> (book, year)."""
    mapping = {}
    for key, case_names in progress.get("enumerated", {}).items():
        parts = key.rsplit("_", 1)
        if len(parts) == 2:
            book, year_str = parts
            year = int(year_str)
            for cn in case_names:
                mapping[cn] = (book, year)
    return mapping


def main():
    JSONL_DIR.mkdir(parents=True, exist_ok=True)

    progress = load_progress()
    case_map = build_case_to_book_year_map(progress)

    # Find all case JSON files
    case_files = sorted(CASES_DIR.glob("*.json"))
    print(f"Found {len(case_files)} case JSON files")
    print(f"Found {len(case_map)} case->book_year mappings from progress.json")

    # Group records by book_year
    grouped = {}  # key -> list of dicts
    unmapped = []

    for case_file in case_files:
        case_name = case_file.stem
        try:
            with open(case_file, encoding="utf-8") as f:
                case_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  WARNING: Could not read {case_file.name}: {e}")
            continue

        # Look up book and year
        if case_name not in case_map:
            unmapped.append(case_name)
            # Try to infer from the first 4 chars as year
            year_str = case_name[:4]
            try:
                year = int(year_str)
            except ValueError:
                print(f"  SKIP: {case_name} — not in enumerated data and can't parse year")
                continue
            # We don't know the book, skip
            print(f"  SKIP: {case_name} — not in enumerated data (year={year} but book unknown)")
            continue

        book, year = case_map[case_name]
        key = f"{book}_{year}"

        # Load headnotes if available
        headnotes_file = HEADNOTES_DIR / f"{case_name}_headnotes.json"
        headnotes_text = ""
        if headnotes_file.exists():
            try:
                with open(headnotes_file, encoding="utf-8") as f:
                    hn_data = json.load(f)
                headnotes_text = hn_data.get("text", "")
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        # Build JSONL record
        record = {
            "id": case_name,
            "book": book,
            "year": year,
            "court": case_data.get("court", ""),
            "parties": case_data.get("parties", ""),
            "judges": case_data.get("judges", ""),
            "date": case_data.get("date", ""),
            "case_number": case_data.get("case_number", ""),
            "title": case_data.get("title", ""),
            "headnotes": headnotes_text,
            "judgment": case_data.get("text", ""),
            "text": case_data.get("text", ""),
            "scraped_at": case_data.get("fetched_at", ""),
        }

        grouped.setdefault(key, []).append(record)

    # Write JSONL files
    total_written = 0
    print(f"\nWriting JSONL files to {JSONL_DIR}/")

    for key in sorted(grouped.keys()):
        records = grouped[key]
        parts = key.rsplit("_", 1)
        book, year = parts[0], parts[1]
        jsonl_path = JSONL_DIR / f"cases_{book}_{year}.jsonl"

        with open(jsonl_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        total_written += len(records)
        print(f"  {jsonl_path.name}: {len(records)} cases")

    # Summary
    print(f"\n{'='*50}")
    print(f"CONVERSION SUMMARY")
    print(f"{'='*50}")
    print(f"  Case files found:    {len(case_files)}")
    print(f"  Successfully mapped: {total_written}")
    print(f"  Unmapped (skipped):  {len(unmapped)}")
    if unmapped:
        print(f"  Unmapped cases:      {unmapped[:20]}")
    print(f"  JSONL files created: {len(grouped)}")
    print(f"  Output directory:    {JSONL_DIR}")


if __name__ == "__main__":
    main()
