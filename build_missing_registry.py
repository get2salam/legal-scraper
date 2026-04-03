#!/usr/bin/env python3
"""
build_missing_registry.py
=========================
Scans all legislation JSON files on disk and builds a comprehensive
missing-legislation registry (JSON + Markdown).

LOCAL-ONLY analysis - does NOT contact PLS.
"""

import sys
import os
import json
import re
import string
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Force UTF-8 on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_v2" / "legislation"
OUTPUT_JSON = BASE_DIR / "data_v2" / "missing_legislation_registry.json"
OUTPUT_MD = BASE_DIR / "data_v2" / "missing_legislation_registry.md"

# Known PLS counts from verification runs / progress data
# These come from the verification script results and progress tracking
KNOWN_PLS_COUNTS = {
    "A": 642,  # from verify_legislation runs
    "B": 343,  # matches local count - fully scraped
    "C": 968,  # task states 968 on PLS, 581 scraped, ~383 DNS failures
    "Z": 20,   # matches local count
}

REDIRECT_PATTERN = re.compile(r"^\s*window\.location\s*=", re.IGNORECASE)

UNAVAILABLE_MARKERS = [
    "[content not available on source]",
    "[content not available]",
]


def is_section_unavailable(text):
    """Check if section text is unavailable/empty."""
    if text is None:
        return True
    stripped = text.strip()
    if not stripped:
        return True
    if stripped in ("-1", '"-1"', "'-1'"):
        return True
    lower = stripped.lower()
    for marker in UNAVAILABLE_MARKERS:
        if marker in lower:
            return True
    if REDIRECT_PATTERN.match(stripped):
        return True
    if len(stripped) < 20:
        return True
    return False


def scan_letter(letter):
    """Scan all statute JSON files for a given letter."""
    letter_dir = DATA_DIR / letter.upper()
    result = {
        "statutes_scraped": 0,
        "statutes_with_sections": 0,
        "statutes_no_sections": 0,
        "sections_total": 0,
        "sections_available": 0,
        "sections_unavailable": 0,
        "fully_available_statutes": 0,
        "partially_available_statutes": 0,
        "fully_unavailable_statutes": 0,
        "unavailable_sections_detail": [],
        "fully_unavailable_statutes_detail": [],
        "statute_details": [],
    }

    if not letter_dir.is_dir():
        return result

    json_files = sorted(letter_dir.glob("*.json"))

    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            print(f"  [WARN] Skipping malformed file: {filepath.name} ({exc})")
            continue

        result["statutes_scraped"] += 1
        statute_title = data.get("title", filepath.stem.replace("_", " "))
        sections = data.get("sections", [])

        if not sections:
            result["statutes_no_sections"] += 1
            continue

        result["statutes_with_sections"] += 1
        statute_avail = 0
        statute_unavail = 0

        for section in sections:
            result["sections_total"] += 1
            text = section.get("text", "")

            if is_section_unavailable(text):
                result["sections_unavailable"] += 1
                statute_unavail += 1
                result["unavailable_sections_detail"].append({
                    "letter": letter.upper(),
                    "statute": statute_title,
                    "statute_file": filepath.name,
                    "section_id": section.get("section_id", ""),
                    "section_number": section.get("number", ""),
                    "section_title": section.get("title", ""),
                    "reason": "Content not available on PLS (-1 or empty)"
                })
            else:
                result["sections_available"] += 1
                statute_avail += 1

        # Classify statute
        if statute_unavail == 0:
            result["fully_available_statutes"] += 1
        elif statute_avail == 0:
            result["fully_unavailable_statutes"] += 1
            result["fully_unavailable_statutes_detail"].append({
                "letter": letter.upper(),
                "statute_title": statute_title,
                "statute_file": filepath.name,
                "total_sections": len(sections),
                "note": "All sections unavailable on PLS"
            })
        else:
            result["partially_available_statutes"] += 1

        result["statute_details"].append({
            "title": statute_title,
            "file": filepath.name,
            "total_sections": len(sections),
            "available": statute_avail,
            "unavailable": statute_unavail,
        })

    return result


def build_registry():
    """Build the complete missing legislation registry."""
    print("Building Missing Legislation Registry (LOCAL-ONLY analysis)")
    print("=" * 60)

    all_letters = list(string.ascii_uppercase)
    by_letter = {}
    all_unavailable_sections = []
    all_missing_statutes = []

    total_statutes_scraped = 0
    total_sections_scraped = 0
    total_sections_available = 0
    total_sections_unavailable = 0
    total_statutes_missing = 0

    letters_complete = []
    letters_partial = []
    letters_not_started = []

    for letter in all_letters:
        print(f"  Scanning {letter}...", end=" ", flush=True)
        result = scan_letter(letter)

        scraped = result["statutes_scraped"]
        pls_count = KNOWN_PLS_COUNTS.get(letter, None)

        if scraped == 0:
            letters_not_started.append(letter)
            by_letter[letter] = {
                "statutes_scraped": 0,
                "statutes_on_pls": pls_count if pls_count else "unknown",
                "sections_total": 0,
                "sections_available": 0,
                "sections_unavailable": 0,
                "status": "not_started"
            }
            print("no data")
            continue

        total_statutes_scraped += scraped
        total_sections_scraped += result["sections_total"]
        total_sections_available += result["sections_available"]
        total_sections_unavailable += result["sections_unavailable"]

        # Determine missing statutes for this letter
        missing_count = 0
        missing_reason = ""
        if pls_count and pls_count > scraped:
            missing_count = pls_count - scraped
            total_statutes_missing += missing_count

            if letter == "C":
                missing_reason = "DNS failures during scrape - need re-scrape"
            elif letter == "A":
                missing_reason = "Not scraped - may need verification against PLS"
            else:
                missing_reason = "Not yet scraped from PLS"

            for i in range(missing_count):
                all_missing_statutes.append({
                    "letter": letter,
                    "statute_name": "unknown (needs PLS query to identify)",
                    "reason": missing_reason
                })

        # Determine completion status
        if pls_count:
            if scraped >= pls_count:
                letters_complete.append(letter)
                status = "complete"
            else:
                letters_partial.append(letter)
                status = "partial"
        else:
            # No known PLS count - assume complete if we have data
            letters_complete.append(letter)
            status = "complete (PLS count unknown)"

        rate = ""
        if result["sections_total"] > 0:
            pct = (result["sections_available"] / result["sections_total"]) * 100
            rate = f"{pct:.1f}%"

        by_letter[letter] = {
            "statutes_scraped": scraped,
            "statutes_on_pls": pls_count if pls_count else "unknown",
            "statutes_missing": missing_count,
            "statutes_with_sections": result["statutes_with_sections"],
            "statutes_no_sections": result["statutes_no_sections"],
            "sections_total": result["sections_total"],
            "sections_available": result["sections_available"],
            "sections_unavailable": result["sections_unavailable"],
            "availability_rate": rate,
            "fully_available_statutes": result["fully_available_statutes"],
            "partially_available_statutes": result["partially_available_statutes"],
            "fully_unavailable_statutes": result["fully_unavailable_statutes"],
            "fully_unavailable_statutes_detail": result["fully_unavailable_statutes_detail"],
            "status": status,
        }

        if missing_count > 0 and letter == "C":
            by_letter[letter]["missing_reason"] = missing_reason
            by_letter[letter]["note"] = f"968 statutes on PLS, only {scraped} scraped. {missing_count} failed due to DNS errors."

        if missing_count > 0 and letter == "A":
            by_letter[letter]["missing_reason"] = missing_reason
            by_letter[letter]["note"] = f"642 statutes on PLS, {scraped} scraped. {missing_count} may need verification."

        all_unavailable_sections.extend(result["unavailable_sections_detail"])

        print(f"{scraped} statutes, {result['sections_total']} sections "
              f"({result['sections_available']} avail / {result['sections_unavailable']} unavail)"
              + (f" [{missing_count} statutes missing]" if missing_count > 0 else ""))

    # Build final registry
    registry = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "analysis_type": "LOCAL-ONLY (no PLS queries - case law scraper active)",
        "summary": {
            "letters_complete": letters_complete,
            "letters_partial": letters_partial,
            "letters_not_started": letters_not_started,
            "total_statutes_scraped": total_statutes_scraped,
            "total_sections_scraped": total_sections_scraped,
            "total_sections_available": total_sections_available,
            "total_sections_unavailable": total_sections_unavailable,
            "total_statutes_missing": total_statutes_missing,
            "section_availability_rate": f"{(total_sections_available / max(total_sections_scraped, 1)) * 100:.1f}%",
        },
        "by_letter": by_letter,
        "unavailable_sections_count": len(all_unavailable_sections),
        "unavailable_sections": all_unavailable_sections,
        "missing_statutes_count": len(all_missing_statutes),
        "missing_statutes": all_missing_statutes,
    }

    return registry


def write_json_registry(registry, path):
    """Write registry as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(registry, fh, indent=2, ensure_ascii=False)
    print(f"\n[OK] JSON registry: {path}")


def write_markdown_registry(registry, path):
    """Write human-readable Markdown summary."""
    s = registry["summary"]
    bl = registry["by_letter"]
    lines = []

    lines.append("# Missing Legislation Registry")
    lines.append("")
    lines.append(f"**Generated:** {registry['generated']}")
    lines.append(f"**Analysis Type:** {registry['analysis_type']}")
    lines.append("")

    # Summary
    lines.append("## Overall Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Letters complete | {', '.join(s['letters_complete'])} ({len(s['letters_complete'])}) |")
    lines.append(f"| Letters partial | {', '.join(s['letters_partial']) if s['letters_partial'] else 'None'} ({len(s['letters_partial'])}) |")
    lines.append(f"| Letters not started | {', '.join(s['letters_not_started'])} ({len(s['letters_not_started'])}) |")
    lines.append(f"| Total statutes scraped | {s['total_statutes_scraped']:,} |")
    lines.append(f"| Total sections scraped | {s['total_sections_scraped']:,} |")
    lines.append(f"| Sections with content | {s['total_sections_available']:,} |")
    lines.append(f"| Sections unavailable | {s['total_sections_unavailable']:,} |")
    lines.append(f"| Section availability rate | {s['section_availability_rate']} |")
    lines.append(f"| Statutes missing (not scraped) | {s['total_statutes_missing']:,} |")
    lines.append("")

    # Per-letter breakdown
    lines.append("## Per-Letter Breakdown")
    lines.append("")
    lines.append("| Letter | Statutes Scraped | On PLS | Missing | Sections | Available | Unavailable | Rate | Status |")
    lines.append("|--------|-----------------|--------|---------|----------|-----------|-------------|------|--------|")

    for letter in string.ascii_uppercase:
        if letter in bl:
            b = bl[letter]
            scraped = b["statutes_scraped"]
            on_pls = b.get("statutes_on_pls", "?")
            missing = b.get("statutes_missing", 0)
            sect = b.get("sections_total", 0)
            avail = b.get("sections_available", 0)
            unavail = b.get("sections_unavailable", 0)
            rate = b.get("availability_rate", "N/A")
            status = b.get("status", "?")

            lines.append(
                f"| {letter} | {scraped:,} | {on_pls} | {missing} | "
                f"{sect:,} | {avail:,} | {unavail:,} | {rate} | {status} |"
            )

    lines.append("")

    # Detailed notes per letter with data
    lines.append("## Detailed Letter Analysis")
    lines.append("")

    for letter in ["A", "B", "C", "Z"]:
        if letter not in bl or bl[letter]["statutes_scraped"] == 0:
            continue

        b = bl[letter]
        lines.append(f"### Letter {letter}")
        lines.append("")

        if b.get("note"):
            lines.append(f"> **Note:** {b['note']}")
            lines.append("")

        lines.append(f"- **Statutes scraped:** {b['statutes_scraped']}")
        lines.append(f"- **Statutes on PLS:** {b.get('statutes_on_pls', 'unknown')}")
        lines.append(f"- **Statutes missing:** {b.get('statutes_missing', 0)}")
        lines.append(f"- **Total sections:** {b['sections_total']}")
        lines.append(f"- **Available sections:** {b['sections_available']}")
        lines.append(f"- **Unavailable sections:** {b['sections_unavailable']}")
        lines.append(f"- **Fully available statutes:** {b['fully_available_statutes']}")
        lines.append(f"- **Partially available statutes:** {b['partially_available_statutes']}")
        lines.append(f"- **Fully unavailable statutes:** {b['fully_unavailable_statutes']}")
        lines.append("")

        if b.get("fully_unavailable_statutes_detail"):
            lines.append(f"#### Fully Unavailable Statutes ({letter})")
            lines.append("")
            lines.append("These statutes have NO sections with content on PLS:")
            lines.append("")
            for i, stat in enumerate(b["fully_unavailable_statutes_detail"][:50], 1):
                lines.append(f"{i}. **{stat['statute_title']}** ({stat['total_sections']} sections)")
            if len(b["fully_unavailable_statutes_detail"]) > 50:
                lines.append(f"... and {len(b['fully_unavailable_statutes_detail']) - 50} more (see JSON)")
            lines.append("")

    # Sections unavailable on PLS
    lines.append("## Unavailable Sections Summary")
    lines.append("")
    lines.append(f"Total unavailable sections across all letters: **{registry['unavailable_sections_count']:,}**")
    lines.append("")

    # Group by statute for top offenders
    statute_counts = defaultdict(int)
    for u in registry["unavailable_sections"]:
        key = f"[{u['letter']}] {u['statute']}"
        statute_counts[key] += 1

    top_gaps = sorted(statute_counts.items(), key=lambda x: -x[1])[:40]
    if top_gaps:
        lines.append("### Top Statutes by Unavailable Sections")
        lines.append("")
        lines.append("| # | Statute | Unavailable Sections |")
        lines.append("|---|---------|---------------------|")
        for i, (name, count) in enumerate(top_gaps, 1):
            lines.append(f"| {i} | {name} | {count} |")
        lines.append("")

    # Missing statutes section
    lines.append("## Missing Statutes (Not Scraped)")
    lines.append("")
    lines.append(f"Total statutes missing: **{registry['missing_statutes_count']:,}**")
    lines.append("")

    if registry["missing_statutes"]:
        # Group by letter
        by_l = defaultdict(list)
        for ms in registry["missing_statutes"]:
            by_l[ms["letter"]].append(ms)

        for letter in sorted(by_l.keys()):
            items = by_l[letter]
            lines.append(f"### Letter {letter}: {len(items)} missing statutes")
            lines.append(f"- **Reason:** {items[0]['reason']}")
            lines.append(f"- These statutes need to be identified via PLS query and re-scraped")
            lines.append("")

    # Letters not started
    lines.append("## Letters Not Yet Scraped (D-Y)")
    lines.append("")
    lines.append("The following letters have not been scraped yet:")
    lines.append("")
    not_started = s["letters_not_started"]
    lines.append(f"**{', '.join(not_started)}** ({len(not_started)} letters)")
    lines.append("")
    lines.append("These need to be scraped from PLS to achieve 100% coverage.")
    lines.append("Based on the average of ~350-640 statutes per letter for A-C,")
    lines.append(f"this could represent approximately **{len(not_started) * 400:,}+** additional statutes.")
    lines.append("")

    # Action items
    lines.append("## Action Items")
    lines.append("")
    lines.append("### Priority 1: Re-scrape Letter C DNS Failures")
    lines.append(f"- 387 statutes failed due to DNS errors during C scrape")
    lines.append("- These need to be re-scraped when PLS session is available")
    lines.append("")
    lines.append("### Priority 2: Verify Letter A Missing Statutes")
    lines.append(f"- 18 statutes possibly missing (642 on PLS vs 624 scraped)")
    lines.append("- Run verification script to identify which ones are missing")
    lines.append("")
    lines.append("### Priority 3: Scrape Letters D-Y")
    lines.append(f"- {len(not_started)} letters not yet started")
    lines.append("- Estimated ~8,800+ statutes remaining")
    lines.append("")
    lines.append("### Priority 4: Address Unavailable Content")
    lines.append(f"- {s['total_sections_unavailable']:,} sections have no content on PLS")
    lines.append("- These are likely not digitized on PLS itself")
    lines.append("- Consider alternative sources (gazette.gov.pk, provincial assembly sites)")
    lines.append("")

    # Notes
    lines.append("## Technical Notes")
    lines.append("")
    lines.append("- **Analysis type:** Local file scan only (PLS not queried)")
    lines.append("- **Reason:** Case law scraper (PID 38872) is running; PLS allows only one session")
    lines.append("- **Unavailability criteria:**")
    lines.append("  - Empty text or text shorter than 20 characters")
    lines.append("  - Text equal to `-1` (PLS placeholder)")
    lines.append("  - Text containing `[Content not available on source]`")
    lines.append("  - JavaScript redirect stubs")
    lines.append("- **PLS counts** for A (642) and C (968) from prior verification runs")
    lines.append("- **B and Z counts** match local data (assumed complete)")
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"[OK] Markdown registry: {path}")


def print_summary(registry):
    """Print a console summary."""
    s = registry["summary"]
    print()
    print("=" * 60)
    print("  MISSING LEGISLATION REGISTRY - SUMMARY")
    print("=" * 60)
    print()
    print(f"  Letters complete:       {len(s['letters_complete'])} ({', '.join(s['letters_complete'])})")
    print(f"  Letters partial:        {len(s['letters_partial'])} ({', '.join(s['letters_partial']) if s['letters_partial'] else 'none'})")
    print(f"  Letters not started:    {len(s['letters_not_started'])} ({', '.join(s['letters_not_started'])})")
    print()
    print(f"  Total statutes scraped: {s['total_statutes_scraped']:,}")
    print(f"  Total sections:         {s['total_sections_scraped']:,}")
    print(f"  Sections available:     {s['total_sections_available']:,}")
    print(f"  Sections unavailable:   {s['total_sections_unavailable']:,}")
    print(f"  Availability rate:      {s['section_availability_rate']}")
    print(f"  Statutes missing:       {s['total_statutes_missing']:,}")
    print()
    print("=" * 60)


def main():
    registry = build_registry()
    write_json_registry(registry, OUTPUT_JSON)
    write_markdown_registry(registry, OUTPUT_MD)
    print_summary(registry)


if __name__ == "__main__":
    main()
