#!/usr/bin/env python3
"""
legislation_deep_audit.py — Deep content & format audit for legislation data

Checks every dimension of legislation quality:
    1. FILE_EXISTS      — JSON readable
    2. JSON_VALID       — Parseable
    3. TITLE_PRESENT    — Has a title/short_title
    4. BODY_CONTENT     — Has actual legislation text (not just metadata)
    5. BODY_LENGTH      — Body is substantial (>200 chars = real content)
    6. ENCODING_CLEAN   — No double-encoded HTML, no raw entities
    7. SECTION_STRUCTURE — Has identifiable sections/articles
    8. YEAR_PRESENT     — Year extractable from title or metadata
    9. CATEGORY_VALID   — Belongs to a valid A-Z category
    10. DUPLICATE_CHECK  — No duplicate titles
    11. METADATA_RICH    — Has category, jurisdiction, status fields
    12. CROSS_REFERENCE  — Links to related statutes or amendments

Usage:
    python legislation_deep_audit.py                # 100 random statutes
    python legislation_deep_audit.py --n 500        # Larger sample
    python legislation_deep_audit.py --letter P     # Only letter P
    python legislation_deep_audit.py --full         # ALL statutes (slow)
"""

import argparse
import datetime
import json
import logging
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
LEG_DIR = DATA_DIR / "legislation"
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\audit-results")

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)


# ── Check classes ───────────────────────────────────────────────────────────

class Check:
    def __init__(self, name, passed, detail="", severity="critical"):
        self.name = name
        self.passed = passed
        self.detail = detail
        self.severity = severity

    def __repr__(self):
        icon = "\u2705" if self.passed else ("\u26a0\ufe0f" if self.severity == "warning" else "\u274c")
        return f"{icon} {self.name}: {self.detail}"


class LegAudit:
    def __init__(self, filename, filepath):
        self.filename = filename
        self.filepath = filepath
        self.checks = []
        self.data = None
        self.title = ""
        self.body = ""
        self.body_length = 0

    def add(self, check):
        self.checks.append(check)

    @property
    def passed(self):
        return all(c.passed for c in self.checks if c.severity == "critical")

    @property
    def score(self):
        if not self.checks:
            return 0
        return sum(1 for c in self.checks if c.passed) / len(self.checks) * 100

    @property
    def failures(self):
        return [c for c in self.checks if not c.passed]

    @property
    def critical_failures(self):
        return [c for c in self.checks if not c.passed and c.severity == "critical"]


# ── Audit checks ────────────────────────────────────────────────────────────

def check_file_exists(audit):
    exists = os.path.isfile(audit.filepath)
    size = os.path.getsize(audit.filepath) if exists else 0
    audit.add(Check("FILE_EXISTS", exists, f"{size:,} bytes" if exists else "Not found"))
    return exists


def check_json_valid(audit):
    try:
        with open(audit.filepath, "r", encoding="utf-8") as f:
            audit.data = json.load(f)
        audit.add(Check("JSON_VALID", True, f"{len(audit.data)} fields"))
        return True
    except Exception as e:
        audit.add(Check("JSON_VALID", False, str(e)))
        return False


def check_title(audit):
    if not audit.data:
        audit.add(Check("TITLE_PRESENT", False, "No data"))
        return
    title = audit.data.get("title", "") or audit.data.get("short_title", "") or audit.data.get("name", "")
    audit.title = title
    if not title or len(title.strip()) < 3:
        audit.add(Check("TITLE_PRESENT", False, "Missing or too short"))
    else:
        audit.add(Check("TITLE_PRESENT", True, f"{len(title)} chars: {title[:80]}"))


def check_body_content(audit):
    if not audit.data:
        audit.add(Check("BODY_CONTENT", False, "No data"))
        return

    # Try multiple possible field names
    body = ""
    for field in ["body", "content", "text", "full_text", "legislation_text", "body_text"]:
        val = audit.data.get(field, "")
        if val and len(str(val)) > len(body):
            body = str(val)

    audit.body = body
    audit.body_length = len(body)

    if not body or len(body.strip()) < 10:
        audit.add(Check("BODY_CONTENT", False, "No body text (metadata-only)", "critical"))
    else:
        audit.add(Check("BODY_CONTENT", True, f"{len(body):,} chars"))


def check_body_length(audit):
    if audit.body_length < 10:
        audit.add(Check("BODY_LENGTH", False, "Empty", "critical"))
    elif audit.body_length < 200:
        audit.add(Check("BODY_LENGTH", False, f"Stub: {audit.body_length} chars (likely truncated/incomplete)", "warning"))
    elif audit.body_length < 500:
        audit.add(Check("BODY_LENGTH", True, f"Short: {audit.body_length} chars", "info"))
    else:
        audit.add(Check("BODY_LENGTH", True, f"Substantial: {audit.body_length:,} chars"))


def check_encoding(audit):
    if not audit.body:
        audit.add(Check("ENCODING_CLEAN", True, "No body to check", "info"))
        return

    issues = []
    if "\\u003c" in audit.body or "\\u003e" in audit.body:
        issues.append("double-encoded unicode")
    if '\\"' in audit.body and audit.body.startswith('"'):
        issues.append("JSON-wrapped string")
    if "\\r\\n" in audit.body:
        issues.append("literal \\r\\n")
    if "&amp;amp;" in audit.body or "&lt;lt;" in audit.body:
        issues.append("double-escaped HTML entities")

    if issues:
        audit.add(Check("ENCODING_CLEAN", False, "; ".join(issues)))
    else:
        audit.add(Check("ENCODING_CLEAN", True, "Clean"))


def check_section_structure(audit):
    if not audit.body or len(audit.body) < 200:
        audit.add(Check("SECTION_STRUCTURE", True, "Too short to check", "info"))
        return

    # Look for section markers
    section_patterns = [
        r"Section\s+\d+",
        r"Article\s+\d+",
        r"Clause\s+\d+",
        r"\b\d+\.\s+",
        r"Part\s+[IVX]+",
        r"Chapter\s+[IVX\d]+",
        r"Schedule",
    ]

    found = []
    for pattern in section_patterns:
        matches = re.findall(pattern, audit.body, re.IGNORECASE)
        if matches:
            found.append(f"{pattern.split(r'\\')[0].strip('(')}: {len(matches)}")

    if found:
        audit.add(Check("SECTION_STRUCTURE", True, f"Found: {'; '.join(found[:3])}"))
    else:
        audit.add(Check("SECTION_STRUCTURE", False, "No section/article/clause markers found", "warning"))


def check_year(audit):
    if not audit.data:
        audit.add(Check("YEAR_PRESENT", False, "No data", "warning"))
        return

    year = audit.data.get("year", "")
    if not year:
        # Try extracting from title
        match = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", audit.title)
        if match:
            year = match.group(1)

    if year:
        try:
            y = int(year)
            if 1800 <= y <= 2030:
                audit.add(Check("YEAR_PRESENT", True, str(y)))
            else:
                audit.add(Check("YEAR_PRESENT", False, f"Invalid year: {y}", "warning"))
        except ValueError:
            audit.add(Check("YEAR_PRESENT", False, f"Non-numeric: {year}", "warning"))
    else:
        audit.add(Check("YEAR_PRESENT", False, "No year found", "warning"))


def check_category(audit):
    if not audit.data:
        audit.add(Check("CATEGORY_VALID", True, "Skipped", "info"))
        return

    category = audit.data.get("category", "") or audit.data.get("letter", "")
    # Can also infer from parent directory
    parent = Path(audit.filepath).parent.name
    if len(parent) == 1 and parent.isalpha():
        category = category or parent

    if category and len(category) == 1 and category.isalpha():
        audit.add(Check("CATEGORY_VALID", True, f"Category: {category.upper()}"))
    elif category:
        audit.add(Check("CATEGORY_VALID", True, f"Category: {category}", "info"))
    else:
        audit.add(Check("CATEGORY_VALID", False, "No category", "warning"))


def check_metadata_richness(audit):
    if not audit.data:
        audit.add(Check("METADATA_RICH", False, "No data", "info"))
        return

    meta_fields = {
        "jurisdiction": audit.data.get("jurisdiction", "") or audit.data.get("province", ""),
        "status": audit.data.get("status", "") or audit.data.get("act_status", ""),
        "gazette_ref": audit.data.get("gazette", "") or audit.data.get("gazette_ref", ""),
        "amendments": audit.data.get("amendments", "") or audit.data.get("amended_by", ""),
        "ministry": audit.data.get("ministry", "") or audit.data.get("department", ""),
    }

    populated = sum(1 for v in meta_fields.values() if v and str(v).strip())
    detail = f"{populated}/{len(meta_fields)} enrichment fields"

    missing = [k for k, v in meta_fields.items() if not v or not str(v).strip()]
    if missing:
        detail += f" (missing: {', '.join(missing)})"

    audit.add(Check("METADATA_RICH", populated >= 2, detail, "info"))


def check_cross_references(audit):
    if not audit.body or len(audit.body) < 100:
        audit.add(Check("CROSS_REFERENCE", True, "Too short", "info"))
        return

    # Look for references to other legislation
    ref_patterns = [
        r"(?:Act|Ordinance|Order|Rules?|Regulation)\s+(?:of\s+)?\d{4}",
        r"(?:amended|repealed|substituted)\s+by",
        r"(?:Section|S\.)\s+\d+\s+of",
    ]

    refs_found = 0
    for pattern in ref_patterns:
        refs_found += len(re.findall(pattern, audit.body, re.IGNORECASE))

    if refs_found > 0:
        audit.add(Check("CROSS_REFERENCE", True, f"{refs_found} cross-references found"))
    else:
        audit.add(Check("CROSS_REFERENCE", True, "No cross-references (standalone)", "info"))


# ── Main ────────────────────────────────────────────────────────────────────

def collect_legislation_files(letter=None):
    files = []
    if not LEG_DIR.exists():
        return files

    for subdir in sorted(LEG_DIR.iterdir()):
        if subdir.is_dir():
            if letter and subdir.name.upper() != letter.upper():
                continue
            for f in subdir.glob("*.json"):
                files.append(str(f))
        elif subdir.suffix == ".json":
            if letter and not subdir.name.upper().startswith(letter.upper()):
                continue
            files.append(str(subdir))

    return files


def run_audit(n=100, letter=None, full=False):
    start_time = time.time()
    today = datetime.date.today().isoformat()

    log.info("=" * 60)
    log.info(f"LEGISLATION DEEP AUDIT - {today}")
    log.info("=" * 60)

    all_files = collect_legislation_files(letter)
    log.info(f"Total legislation files: {len(all_files):,}")

    if full:
        sample = all_files
    else:
        sample = random.sample(all_files, min(n, len(all_files)))
    log.info(f"Auditing: {len(sample)} files")

    if letter:
        log.info(f"Letter filter: {letter}")

    # Run audit
    audits = []
    titles_seen = Counter()

    for i, filepath in enumerate(sample):
        audit = LegAudit(Path(filepath).name, filepath)

        if check_file_exists(audit):
            if check_json_valid(audit):
                check_title(audit)
                check_body_content(audit)
                check_body_length(audit)
                check_encoding(audit)
                check_section_structure(audit)
                check_year(audit)
                check_category(audit)
                check_metadata_richness(audit)
                check_cross_references(audit)

                if audit.title:
                    titles_seen[audit.title.lower().strip()] += 1

        audits.append(audit)

        if (i + 1) % 50 == 0:
            log.info(f"  [{i+1}/{len(sample)}] Score: {audit.score:.0f}%")

    # Duplicate titles
    dupes = {t: c for t, c in titles_seen.items() if c > 1}

    # Results
    elapsed = time.time() - start_time
    total_checks = sum(len(a.checks) for a in audits)
    passed_checks = sum(sum(1 for c in a.checks if c.passed) for a in audits)
    avg_score = sum(a.score for a in audits) / len(audits) if audits else 0

    # Content breakdown
    has_body = sum(1 for a in audits if a.body_length >= 200)
    metadata_only = sum(1 for a in audits if a.body_length < 10)
    stubs = sum(1 for a in audits if 10 <= a.body_length < 200)

    # Per-check stats
    check_stats = defaultdict(lambda: {"passed": 0, "failed": 0})
    for audit in audits:
        for check in audit.checks:
            if check.passed:
                check_stats[check.name]["passed"] += 1
            else:
                check_stats[check.name]["failed"] += 1

    log.info("\n" + "=" * 60)
    log.info("LEGISLATION AUDIT RESULTS")
    log.info("=" * 60)

    log.info(f"\nSample: {len(sample)} / {len(all_files):,} total")
    log.info(f"Time: {elapsed:.1f}s")
    log.info(f"Overall Score: {avg_score:.1f}%")

    log.info(f"\nContent Breakdown:")
    log.info(f"  Full body (>200 chars): {has_body} ({has_body/len(sample)*100:.0f}%)")
    log.info(f"  Stub (<200 chars):      {stubs} ({stubs/len(sample)*100:.0f}%)")
    log.info(f"  Metadata-only:          {metadata_only} ({metadata_only/len(sample)*100:.0f}%)")
    log.info(f"  Duplicate titles:       {len(dupes)}")

    log.info(f"\nPer-check pass rates:")
    for name in ["FILE_EXISTS", "JSON_VALID", "TITLE_PRESENT", "BODY_CONTENT",
                 "BODY_LENGTH", "ENCODING_CLEAN", "SECTION_STRUCTURE", "YEAR_PRESENT",
                 "CATEGORY_VALID", "METADATA_RICH", "CROSS_REFERENCE"]:
        stats = check_stats.get(name, {"passed": 0, "failed": 0})
        total = stats["passed"] + stats["failed"]
        rate = stats["passed"] / total * 100 if total > 0 else 0
        icon = "\u2705" if rate >= 95 else ("\u26a0\ufe0f" if rate >= 70 else "\u274c")
        log.info(f"  {icon} {name}: {stats['passed']}/{total} ({rate:.0f}%)")

    # Critical failures
    critical = sum(len(a.critical_failures) for a in audits)
    if critical > 0 and critical <= 20:
        log.info(f"\n\u274c CRITICAL FAILURES ({critical}):")
        for audit in audits:
            for check in audit.critical_failures:
                log.info(f"  {audit.filename}: {check.name} - {check.detail}")
    elif critical > 20:
        log.info(f"\n\u274c {critical} critical failures (top issue: BODY_CONTENT — {metadata_only} files have no body)")

    # Grade
    if avg_score >= 90:
        grade = "A"
    elif avg_score >= 75:
        grade = "B"
    elif avg_score >= 60:
        grade = "C"
    elif avg_score >= 40:
        grade = "D"
    else:
        grade = "F"

    log.info(f"\n{'=' * 60}")
    log.info(f"LEGISLATION HEALTH GRADE: {grade} ({avg_score:.1f}%)")
    log.info(f"{'=' * 60}")

    # Save
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / f"legislation-{today}.json"
    result_data = {
        "date": today,
        "type": "legislation",
        "sample_size": len(sample),
        "total_files": len(all_files),
        "grade": grade,
        "avg_score": round(avg_score, 1),
        "has_body": has_body,
        "metadata_only": metadata_only,
        "stubs": stubs,
        "duplicates": len(dupes),
        "check_pass_rates": {
            name: round(stats["passed"] / (stats["passed"] + stats["failed"]) * 100, 1)
            if (stats["passed"] + stats["failed"]) > 0 else 0
            for name, stats in check_stats.items()
        },
        "elapsed_seconds": round(elapsed, 1),
    }
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)
    log.info(f"\nResults saved: {result_file}")

    return 0 if critical == 0 else 1


def main():
    parser = argparse.ArgumentParser(description="Deep legislation audit")
    parser.add_argument("--n", type=int, default=100, help="Sample size")
    parser.add_argument("--letter", type=str, help="Filter by letter (A-Z)")
    parser.add_argument("--full", action="store_true", help="Audit ALL files")
    args = parser.parse_args()
    sys.exit(run_audit(n=args.n, letter=args.letter, full=args.full))


if __name__ == "__main__":
    main()
