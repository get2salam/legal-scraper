#!/usr/bin/env python3
"""
gap_registry.py — Legislation Gap Analysis Registry

Scans all legislation JSON files in data_v2/legislation/ and identifies
sections with unavailable or missing content. Produces:
  - data_v2/legislation_gaps.json       (machine-readable gap report)
  - data_v2/legislation_gaps_summary.txt (human-readable summary)

A section is considered "unavailable" if:
  - text is an empty string ""
  - text is exactly "-1" or '"-1"' (placeholder values)
  - text length < 20 characters (too short to be real content)
  - text contains "[Content not available on source]"
  - text is a redirect script (e.g. "window.location = '/'")

Usage:
  python gap_registry.py --all              # Scan all letters
  python gap_registry.py --letter A         # Scan only letter A
  python gap_registry.py --letter A B C     # Scan multiple letters
"""

import argparse
import json
import os
import re
import string
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_v2" / "legislation"
OUTPUT_JSON = BASE_DIR / "data_v2" / "legislation_gaps.json"
OUTPUT_TXT = BASE_DIR / "data_v2" / "legislation_gaps_summary.txt"

# Regex for JavaScript redirect stubs the scraper left behind
REDIRECT_PATTERN = re.compile(r"^\s*window\.location\s*=", re.IGNORECASE)

# Unavailability marker strings (case-insensitive check)
UNAVAILABLE_MARKERS = [
    "[content not available on source]",
    "[content not available]",
]

# ---------------------------------------------------------------------------
# Alt-source heuristics
# ---------------------------------------------------------------------------

ALT_SOURCE_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"income\s*tax|sales\s*tax|customs", re.I), "fbr.gov.pk"),
    (re.compile(r"compan(?:y|ies)|securities", re.I),       "secp.gov.pk"),
    (re.compile(r"punjab", re.I),                           "punjablaws.gov.pk"),
    (re.compile(r"sindh", re.I),                            "sindhassembly.gov.pk"),
    (re.compile(r"balochistan|baluchistan", re.I),          "pabalochistan.gov.pk"),
    (re.compile(r"kpk|khyber|pakhtunkhwa", re.I),          "kpassembly.gov.pk"),
]

FALLBACK_SOURCES = ["gazette.gov.pk", "na.gov.pk"]


def get_alt_sources(statute_title: str) -> list[str]:
    """Return potential alternative sources based on statute title keywords."""
    sources: list[str] = []
    for pattern, source in ALT_SOURCE_RULES:
        if pattern.search(statute_title):
            sources.append(source)
    # Always append the two fallback sources
    sources.extend(FALLBACK_SOURCES)
    return sources


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

def is_section_unavailable(text: str | None) -> bool:
    """
    Return True if a section's text should be considered unavailable.

    Criteria:
      1. None / empty string
      2. Exact "-1" or '"-1"' (scraper placeholder)
      3. Length < 20 characters (too short to be real legislation text)
      4. Contains known "not available" markers
      5. JavaScript redirect stub (window.location = '/')
    """
    if text is None:
        return True

    stripped = text.strip()

    # Empty
    if not stripped:
        return True

    # Placeholder values
    if stripped in ("-1", '"-1"', "'-1'"):
        return True

    # Known markers (case-insensitive)
    lower = stripped.lower()
    for marker in UNAVAILABLE_MARKERS:
        if marker in lower:
            return True

    # JavaScript redirect stub
    if REDIRECT_PATTERN.match(stripped):
        return True

    # Too short to be real content
    if len(stripped) < 20:
        return True

    return False


# ---------------------------------------------------------------------------
# Scanning logic
# ---------------------------------------------------------------------------

def scan_letter(letter: str) -> dict:
    """
    Scan all statute JSON files under data_v2/legislation/<letter>/.

    Returns a dict with:
      - statutes: int
      - sections: int
      - available: int
      - unavailable: int
      - unavailable_sections: list[dict]
      - fully_unavailable_statutes: list[dict]
      - fully_available_count: int
      - partially_available_count: int
      - fully_unavailable_count: int
    """
    letter_dir = DATA_DIR / letter.upper()
    result = {
        "statutes": 0,
        "sections": 0,
        "available": 0,
        "unavailable": 0,
        "unavailable_sections": [],
        "fully_unavailable_statutes": [],
        "fully_available_count": 0,
        "partially_available_count": 0,
        "fully_unavailable_count": 0,
    }

    if not letter_dir.is_dir():
        return result

    json_files = sorted(letter_dir.glob("*.json"))

    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            print(f"  ⚠ Skipping malformed file: {filepath.name} ({exc})")
            continue

        statute_title = data.get("title", filepath.stem.replace("_", " "))
        sections = data.get("sections", [])

        if not sections:
            continue

        result["statutes"] += 1
        statute_available = 0
        statute_unavailable = 0
        statute_unavailable_details: list[dict] = []

        alt_sources = get_alt_sources(statute_title)

        for section in sections:
            result["sections"] += 1
            text = section.get("text", "")

            if is_section_unavailable(text):
                result["unavailable"] += 1
                statute_unavailable += 1
                statute_unavailable_details.append({
                    "letter": letter.upper(),
                    "statute_file": filepath.name,
                    "statute_title": statute_title,
                    "section_id": section.get("section_id", ""),
                    "section_number": section.get("number", ""),
                    "section_title": section.get("title", ""),
                    "source": "PLS",
                    "potential_alt_sources": alt_sources,
                })
            else:
                result["available"] += 1
                statute_available += 1

        # Classify the statute
        if statute_unavailable == 0:
            result["fully_available_count"] += 1
        elif statute_available == 0:
            result["fully_unavailable_count"] += 1
            result["fully_unavailable_statutes"].append({
                "letter": letter.upper(),
                "statute_file": filepath.name,
                "statute_title": statute_title,
                "total_sections": len(sections),
                "note": "No sections available on PLS - likely not digitized",
            })
        else:
            result["partially_available_count"] += 1

        # Accumulate unavailable section details
        result["unavailable_sections"].extend(statute_unavailable_details)

    return result


def build_report(letters: list[str]) -> dict:
    """Scan requested letters and build the full gap report."""
    by_letter: dict[str, dict] = {}
    all_unavailable_sections: list[dict] = []
    all_fully_unavailable: list[dict] = []

    total_statutes = 0
    total_sections = 0
    total_available = 0
    total_unavailable = 0
    fully_available = 0
    partially_available = 0
    fully_unavailable_count = 0

    for letter in sorted(letters):
        print(f"  Scanning {letter}...", end=" ", flush=True)
        result = scan_letter(letter)

        by_letter[letter] = {
            "statutes": result["statutes"],
            "sections": result["sections"],
            "available": result["available"],
            "unavailable": result["unavailable"],
        }

        total_statutes += result["statutes"]
        total_sections += result["sections"]
        total_available += result["available"]
        total_unavailable += result["unavailable"]
        fully_available += result["fully_available_count"]
        partially_available += result["partially_available_count"]
        fully_unavailable_count += result["fully_unavailable_count"]

        all_unavailable_sections.extend(result["unavailable_sections"])
        all_fully_unavailable.extend(result["fully_unavailable_statutes"])

        avail_str = f"{result['available']}/{result['sections']}" if result["sections"] else "0/0"
        print(f"{result['statutes']} statutes, {avail_str} sections available")

    # Compute availability rate
    if total_sections > 0:
        rate = (total_available / total_sections) * 100
        availability_rate = f"{rate:.1f}%"
    else:
        availability_rate = "N/A"

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_statutes": total_statutes,
            "total_sections": total_sections,
            "available_sections": total_available,
            "unavailable_sections": total_unavailable,
            "availability_rate": availability_rate,
            "statutes_fully_available": fully_available,
            "statutes_partially_available": partially_available,
            "statutes_fully_unavailable": fully_unavailable_count,
        },
        "by_letter": by_letter,
        "unavailable_sections": all_unavailable_sections,
        "fully_unavailable_statutes": all_fully_unavailable,
    }

    return report


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_json_report(report: dict, path: Path) -> None:
    """Write the full report as formatted JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"\n📄 JSON report: {path}")


def write_text_summary(report: dict, path: Path) -> None:
    """Write a human-readable summary text file."""
    s = report["summary"]
    lines: list[str] = []
    sep = "=" * 70

    lines.append(sep)
    lines.append("  PAKISTAN LEGISLATION SCRAPER — GAP ANALYSIS REPORT")
    lines.append(f"  Generated: {report['generated_at']}")
    lines.append(sep)
    lines.append("")

    # Overall summary
    lines.append("OVERALL SUMMARY")
    lines.append("-" * 40)
    lines.append(f"  Total statutes scanned:      {s['total_statutes']:,}")
    lines.append(f"  Total sections:              {s['total_sections']:,}")
    lines.append(f"  Available sections:          {s['available_sections']:,}")
    lines.append(f"  Unavailable sections:        {s['unavailable_sections']:,}")
    lines.append(f"  Availability rate:           {s['availability_rate']}")
    lines.append("")
    lines.append(f"  Statutes fully available:    {s['statutes_fully_available']:,}")
    lines.append(f"  Statutes partially avail.:   {s['statutes_partially_available']:,}")
    lines.append(f"  Statutes fully unavailable:  {s['statutes_fully_unavailable']:,}")
    lines.append("")

    # Per-letter breakdown
    lines.append("PER-LETTER BREAKDOWN")
    lines.append("-" * 40)
    lines.append(f"  {'Letter':<8} {'Statutes':>10} {'Sections':>10} {'Available':>10} {'Unavail.':>10} {'Rate':>8}")
    lines.append(f"  {'------':<8} {'--------':>10} {'--------':>10} {'---------':>10} {'--------':>10} {'----':>8}")

    for letter in sorted(report["by_letter"].keys()):
        bl = report["by_letter"][letter]
        if bl["sections"] > 0:
            rate = f"{(bl['available'] / bl['sections']) * 100:.1f}%"
        else:
            rate = "N/A"
        lines.append(
            f"  {letter:<8} {bl['statutes']:>10,} {bl['sections']:>10,} "
            f"{bl['available']:>10,} {bl['unavailable']:>10,} {rate:>8}"
        )
    lines.append("")

    # Fully unavailable statutes (top 50)
    fu = report["fully_unavailable_statutes"]
    lines.append(f"FULLY UNAVAILABLE STATUTES ({len(fu)} total)")
    lines.append("-" * 40)
    if fu:
        for i, stat in enumerate(fu[:50], 1):
            lines.append(
                f"  {i:>3}. [{stat['letter']}] {stat['statute_title']} "
                f"({stat['total_sections']} sections)"
            )
        if len(fu) > 50:
            lines.append(f"  ... and {len(fu) - 50} more (see JSON report)")
    else:
        lines.append("  None — all statutes have at least some content.")
    lines.append("")

    # Unavailable section count by statute (top 30)
    unavail = report["unavailable_sections"]
    statute_counts: dict[str, int] = defaultdict(int)
    for u in unavail:
        key = f"[{u['letter']}] {u['statute_title']}"
        statute_counts[key] += 1

    top_gaps = sorted(statute_counts.items(), key=lambda x: -x[1])[:30]
    lines.append(f"TOP STATUTES BY UNAVAILABLE SECTIONS (of {len(statute_counts)} with gaps)")
    lines.append("-" * 40)
    for i, (name, count) in enumerate(top_gaps, 1):
        lines.append(f"  {i:>3}. {name}: {count} sections unavailable")
    lines.append("")

    lines.append(sep)
    lines.append("  Full details in: legislation_gaps.json")
    lines.append(sep)
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"📝 Text summary: {path}")


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_console_summary(report: dict) -> None:
    """Print a concise summary to stdout."""
    s = report["summary"]
    print()
    print("┌─────────────────────────────────────────┐")
    print("│        GAP ANALYSIS COMPLETE             │")
    print("├─────────────────────────────────────────┤")
    print(f"│  Statutes:    {s['total_statutes']:>6,}                      │")
    print(f"│  Sections:    {s['total_sections']:>6,}                      │")
    print(f"│  Available:   {s['available_sections']:>6,}  ({s['availability_rate']:>6})          │")
    print(f"│  Unavailable: {s['unavailable_sections']:>6,}                      │")
    print("├─────────────────────────────────────────┤")
    print(f"│  Fully available:    {s['statutes_fully_available']:>5,} statutes       │")
    print(f"│  Partially avail.:   {s['statutes_partially_available']:>5,} statutes       │")
    print(f"│  Fully unavailable:  {s['statutes_fully_unavailable']:>5,} statutes       │")
    print("└─────────────────────────────────────────┘")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Pakistan legislation data and report content gaps.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Scan all letters A-Z",
    )
    group.add_argument(
        "--letter",
        nargs="+",
        type=str,
        metavar="L",
        help="One or more letters to scan (e.g. --letter A B C)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory (default: data_v2/)",
    )
    return parser.parse_args()


def main() -> None:
    # Force UTF-8 output on Windows to handle emoji/unicode
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    args = parse_args()

    if args.all:
        letters = list(string.ascii_uppercase)
    else:
        letters = [l.upper() for l in args.letter]
        invalid = [l for l in letters if l not in string.ascii_uppercase]
        if invalid:
            print(f"Error: invalid letters: {', '.join(invalid)}", file=sys.stderr)
            sys.exit(1)

    # Resolve output paths
    if args.output_dir:
        out_dir = Path(args.output_dir)
        out_json = out_dir / "legislation_gaps.json"
        out_txt = out_dir / "legislation_gaps_summary.txt"
    else:
        out_json = OUTPUT_JSON
        out_txt = OUTPUT_TXT

    print(f"🔍 Scanning {len(letters)} letter(s): {', '.join(letters)}")
    print()

    report = build_report(letters)

    write_json_report(report, out_json)
    write_text_summary(report, out_txt)
    print_console_summary(report)


if __name__ == "__main__":
    main()
