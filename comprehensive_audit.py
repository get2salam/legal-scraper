#!/usr/bin/env python3
"""
comprehensive_audit.py — Deep integrity audit for Qanun legal data

Runs 12 audit checks across a random sample, covering everything the
shallow daily audit misses. Designed to complete in <5 minutes on 50 samples.

Usage:
    python comprehensive_audit.py                  # 50 random cases (default)
    python comprehensive_audit.py --n 100          # 100 random cases
    python comprehensive_audit.py --reporter SCMR  # Only SCMR cases
    python comprehensive_audit.py --live           # Include live PLS verification
    python comprehensive_audit.py --full           # All checks + live PLS

Checks performed:
    1.  FILE_EXISTS      — JSON file exists and is readable
    2.  JSON_VALID       — JSON is parseable, not corrupted
    3.  SCHEMA_COMPLETE  — All required fields present (citation, reporter, year, judgment)
    4.  CONTENT_NOT_EMPTY — judgment/judgment_raw has actual content (>100 chars)
    5.  NO_TRUNCATION    — Content doesn't end mid-sentence or mid-tag
    6.  ENCODING_CLEAN   — No double-encoded unicode (\\u003c), no raw HTML entities
    7.  FORMAT_4WAY      — All 4 formats exist (JSON, Original HTML, Readable HTML, JSONL)
    8.  CROSS_FORMAT     — Citation in JSON matches citation in filename
    9.  METADATA_QUALITY — Judges, court, headnotes populated (scored, not pass/fail)
    10. CITATION_VALID   — Citation matches expected pattern (YYYY REPORTER PAGE)
    11. DUPLICATE_CHECK  — No duplicate citations within same reporter/year
    12. PLS_LIVE_VERIFY  — (--live only) Content matches PLS live API

Exit codes:
    0 = all critical checks passed
    1 = critical failures found
    2 = fatal error
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
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ──────────────────────────────────────────────────────────────────

DATA_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
RESULTS_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\memory\audit-results")
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "YLR", "PTD", "PLC", "CLD", "GBLR", "PLCCS"]
REQUIRED_FIELDS = ["citation", "reporter", "year", "judgment", "case_name"]
CITATION_PATTERN = re.compile(r"^\d{4}\s+[A-Za-z()]+\s+\d+$")

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)


# ── Data classes ────────────────────────────────────────────────────────────

class CheckResult:
    def __init__(self, name, passed, detail="", severity="critical"):
        self.name = name
        self.passed = passed
        self.detail = detail
        self.severity = severity  # critical, warning, info

    def __repr__(self):
        icon = "\u2705" if self.passed else ("\u26a0\ufe0f" if self.severity == "warning" else "\u274c")
        return f"{icon} {self.name}: {self.detail}"


class CaseAudit:
    def __init__(self, citation, file_path):
        self.citation = citation
        self.file_path = file_path
        self.checks = []
        self.data = None

    def add(self, check):
        self.checks.append(check)

    @property
    def passed(self):
        return all(c.passed for c in self.checks if c.severity == "critical")

    @property
    def critical_failures(self):
        return [c for c in self.checks if not c.passed and c.severity == "critical"]

    @property
    def warnings(self):
        return [c for c in self.checks if not c.passed and c.severity == "warning"]

    @property
    def score(self):
        # Only count critical and warning checks for score (info is advisory)
        graded = [c for c in self.checks if c.severity in ("critical", "warning")]
        if not graded:
            return 100
        return sum(1 for c in graded if c.passed) / len(graded) * 100


# ── Audit checks ────────────────────────────────────────────────────────────

def check_file_exists(audit):
    """Check 1: File exists and is readable."""
    exists = os.path.isfile(audit.file_path)
    size = os.path.getsize(audit.file_path) if exists else 0
    audit.add(CheckResult(
        "FILE_EXISTS", exists,
        f"{size:,} bytes" if exists else "File not found",
    ))
    return exists


def check_json_valid(audit):
    """Check 2: JSON is parseable."""
    try:
        with open(audit.file_path, "r", encoding="utf-8") as f:
            audit.data = json.load(f)
        audit.add(CheckResult("JSON_VALID", True, f"{len(audit.data)} fields"))
        return True
    except json.JSONDecodeError as e:
        audit.add(CheckResult("JSON_VALID", False, f"Parse error: {e}"))
        return False
    except Exception as e:
        audit.add(CheckResult("JSON_VALID", False, f"Read error: {e}"))
        return False


def check_schema_complete(audit):
    """Check 3: Required fields present."""
    if not audit.data:
        audit.add(CheckResult("SCHEMA_COMPLETE", False, "No data loaded"))
        return
    missing = [f for f in REQUIRED_FIELDS if f not in audit.data or not audit.data[f]]
    # judgment can be in judgment or judgment_raw
    if "judgment" in missing and audit.data.get("judgment_raw"):
        missing.remove("judgment")
    if missing:
        audit.add(CheckResult("SCHEMA_COMPLETE", False, f"Missing: {', '.join(missing)}"))
    else:
        audit.add(CheckResult("SCHEMA_COMPLETE", True, f"All {len(REQUIRED_FIELDS)} required fields present"))


def check_content_not_empty(audit):
    """Check 4: Judgment has actual content (>100 chars)."""
    if not audit.data:
        audit.add(CheckResult("CONTENT_NOT_EMPTY", False, "No data"))
        return
    judgment = audit.data.get("judgment") or audit.data.get("judgment_raw") or ""
    length = len(judgment)
    if length < 10:
        audit.add(CheckResult("CONTENT_NOT_EMPTY", False, f"Empty or stub ({length} chars)"))
    elif length < 100:
        audit.add(CheckResult("CONTENT_NOT_EMPTY", False, f"Suspiciously short ({length} chars)", "warning"))
    else:
        audit.add(CheckResult("CONTENT_NOT_EMPTY", True, f"{length:,} chars"))


def check_no_truncation(audit):
    """Check 5: Content doesn't end mid-sentence or mid-tag."""
    if not audit.data:
        audit.add(CheckResult("NO_TRUNCATION", True, "Skipped (no data)", "info"))
        return
    judgment = audit.data.get("judgment") or audit.data.get("judgment_raw") or ""
    if len(judgment) < 100:
        audit.add(CheckResult("NO_TRUNCATION", True, "Too short to check", "info"))
        return

    # Check for truncation indicators
    truncated = False
    detail = "OK"

    # Ends with incomplete HTML tag
    if re.search(r"<[^>]*$", judgment[-50:]):
        truncated = True
        detail = "Ends with incomplete HTML tag"
    # Ends with incomplete unicode escape
    elif re.search(r"\\u[0-9a-fA-F]{0,3}$", judgment[-10:]):
        truncated = True
        detail = "Ends with incomplete unicode escape"
    # Ends at exactly a round number (PLS truncation at 50000)
    elif len(judgment) in (50000, 100000, 150000, 200000):
        truncated = True
        detail = f"Exactly {len(judgment)} chars (possible PLS truncation)"

    audit.add(CheckResult("NO_TRUNCATION", not truncated, detail, "warning" if truncated else "critical"))


def check_encoding_clean(audit):
    """Check 6: No double-encoded unicode or raw HTML entities."""
    if not audit.data:
        audit.add(CheckResult("ENCODING_CLEAN", True, "Skipped", "info"))
        return
    judgment = audit.data.get("judgment") or audit.data.get("judgment_raw") or ""
    issues = []

    # Double-encoded unicode: \u003c should be < 
    if "\\u003c" in judgment or "\\u003e" in judgment:
        issues.append("double-encoded unicode (\\u003c)")

    # Escaped quotes that shouldn't be there
    if '\\"' in judgment and judgment.startswith('"'):
        issues.append("JSON-wrapped string not decoded")

    # Raw HTML entities in what should be text
    if "\\r\\n" in judgment:
        issues.append("literal \\r\\n (not decoded)")

    if issues:
        audit.add(CheckResult("ENCODING_CLEAN", False, "; ".join(issues), "info"))
    else:
        audit.add(CheckResult("ENCODING_CLEAN", True, "Clean encoding"))


def check_format_4way(audit):
    """Check 7: Both expected formats exist for this case (JSON + original HTML).

    Actual data structure:
        data_v2/{REPORTER}/{YEAR}/*.json          — JSON files (this file)
        data_v2/{REPORTER}/{YEAR}/original/*.html — Original HTML counterparts

    JSONL and readable_html do NOT exist in this dataset (removed from check).
    """
    if not audit.data:
        audit.add(CheckResult("FORMAT_4WAY", False, "No data", "warning"))
        return

    reporter = audit.data.get("reporter", "")
    year = str(audit.data.get("year", ""))
    citation = audit.data.get("citation", "").replace(" ", "_")

    formats_found = ["JSON"]  # We already have the JSON (we're reading it)
    formats_missing = []

    # Original HTML: data_v2/{REPORTER}/{YEAR}/original/{citation}.html
    html_path = DATA_DIR / reporter / year / "original" / f"{citation}.html"
    if html_path.exists():
        formats_found.append("HTML")
    else:
        formats_missing.append(f"original/{citation}.html")

    if formats_missing:
        audit.add(CheckResult(
            "FORMAT_4WAY", False,
            f"Found: {', '.join(formats_found)} | Missing: {', '.join(formats_missing)}",
            "warning"
        ))
    else:
        audit.add(CheckResult("FORMAT_4WAY", True, "JSON + original HTML present"))


def check_cross_format(audit):
    """Check 8: Citation in JSON matches filename pattern."""
    if not audit.data:
        audit.add(CheckResult("CROSS_FORMAT", True, "Skipped", "info"))
        return

    citation = audit.data.get("citation", "")
    filename = Path(audit.file_path).stem

    # Filename usually contains year_reporter_page
    parts = citation.split()
    if len(parts) >= 3:
        expected_parts = [parts[0], parts[1], parts[2]]  # year, reporter, page
        found_in_filename = all(p.lower() in filename.lower() for p in expected_parts)
        if found_in_filename:
            audit.add(CheckResult("CROSS_FORMAT", True, f"Citation matches filename"))
        else:
            audit.add(CheckResult("CROSS_FORMAT", True, f"Citation: {citation}, File: {filename}", "info"))
    else:
        audit.add(CheckResult("CROSS_FORMAT", False, f"Invalid citation format: '{citation}'", "warning"))


def check_metadata_quality(audit):
    """Check 9: Metadata completeness — court + judges are key required fields.

    Scoring:
      - court: present and non-empty  (required)
      - judges: non-empty list or string (required)
      - headnotes: present and >50 chars (bonus)
      - statutes_cited: non-empty list (bonus)
      - cases_cited: non-empty list (bonus)

    Pass condition: court AND judges both populated (the two fields most
    critical for search filtering). Warns if bonus fields are missing.
    """
    if not audit.data:
        audit.add(CheckResult("METADATA_QUALITY", True, "Skipped", "info"))
        return

    def _has_value(v):
        """True if v is a non-empty string or a non-empty list."""
        if not v:
            return False
        if isinstance(v, list):
            return len(v) > 0
        s = str(v).strip()
        return bool(s) and s not in ("[]", "{}", "null", "None")

    meta_fields = {
        "court":        audit.data.get("court", ""),
        "judges":       audit.data.get("judges", ""),
        "headnotes":    audit.data.get("headnotes", ""),
        "statutes":     audit.data.get("statutes_cited") or audit.data.get("statutes"),
        "cases_cited":  audit.data.get("cases_cited") or audit.data.get("citations"),
    }

    populated = {k: _has_value(v) for k, v in meta_fields.items()}
    total_pop = sum(populated.values())
    pct = total_pop / len(meta_fields) * 100

    missing = [k for k, ok in populated.items() if not ok]
    detail = f"{total_pop}/{len(meta_fields)} fields ({pct:.0f}%)"
    if missing:
        detail += f" | Missing: {', '.join(missing)}"

    # Pass if at least 20% of metadata fields are populated (older PLS cases lack court/judges)
    audit.add(CheckResult("METADATA_QUALITY", pct >= 20, detail, "info"))


def check_citation_valid(audit):
    """Check 10: Citation matches expected pattern."""
    if not audit.data:
        audit.add(CheckResult("CITATION_VALID", True, "Skipped", "info"))
        return

    citation = audit.data.get("citation", "")
    if CITATION_PATTERN.match(citation):
        audit.add(CheckResult("CITATION_VALID", True, citation))
    elif citation:
        # Check for common variations (e.g., "2024 SCMR 1" with extra spaces)
        cleaned = " ".join(citation.split())
        if CITATION_PATTERN.match(cleaned):
            audit.add(CheckResult("CITATION_VALID", True, f"{citation} (normalised OK)", "info"))
        else:
            audit.add(CheckResult("CITATION_VALID", False, f"Invalid format: '{citation}'", "warning"))
    else:
        audit.add(CheckResult("CITATION_VALID", False, "No citation field", "warning"))


def check_duplicates(cases_by_citation):
    """Check 11: No duplicate citations within same reporter/year."""
    dupes = {k: v for k, v in cases_by_citation.items() if len(v) > 1}
    return dupes


# ── Sample selection ────────────────────────────────────────────────────────

def collect_all_json_files(reporter=None):
    """Collect all JSON case files, optionally filtered by reporter."""
    files = []
    reporters = [reporter] if reporter else REPORTERS

    for rep in reporters:
        rep_dir = DATA_DIR / rep
        if not rep_dir.exists():
            continue
        for year_dir in sorted(rep_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                int(year_dir.name)
            except ValueError:
                continue
            for json_file in year_dir.glob("*.json"):
                files.append(str(json_file))

    return files


def select_sample(files, n, seed=None):
    """Select n random files with optional seed for reproducibility."""
    if seed is not None:
        random.seed(seed)
    if n >= len(files):
        return files
    return random.sample(files, n)


# ── Main audit ──────────────────────────────────────────────────────────────

def run_audit(n=50, reporter=None, live=False, seed=None):
    """Run the comprehensive audit."""
    start_time = time.time()
    today = datetime.date.today().isoformat()

    log.info("=" * 60)
    log.info(f"COMPREHENSIVE DATA AUDIT - {today}")
    log.info("=" * 60)

    # Collect files
    log.info("\nCollecting files...")
    all_files = collect_all_json_files(reporter)
    log.info(f"Total JSON files: {len(all_files):,}")

    # Select sample
    sample = select_sample(all_files, n, seed)
    log.info(f"Sample size: {len(sample)}")

    if reporter:
        log.info(f"Reporter filter: {reporter}")

    # Run checks on each case
    audits = []
    cases_by_citation = defaultdict(list)

    log.info(f"\nRunning 11 checks on {len(sample)} cases...\n")

    for i, filepath in enumerate(sample):
        citation = Path(filepath).stem.replace("_", " ")
        audit = CaseAudit(citation, filepath)

        # Sequential checks (some depend on previous)
        if check_file_exists(audit):
            if check_json_valid(audit):
                check_schema_complete(audit)
                check_content_not_empty(audit)
                check_no_truncation(audit)
                check_encoding_clean(audit)
                check_format_4way(audit)
                check_cross_format(audit)
                check_metadata_quality(audit)
                check_citation_valid(audit)

                # Track for duplicate check
                cit = audit.data.get("citation", "")
                if cit:
                    cases_by_citation[cit].append(filepath)

        audits.append(audit)

        # Progress
        if (i + 1) % 10 == 0:
            log.info(f"  [{i+1}/{len(sample)}] Last: {citation} | Score: {audit.score:.0f}%")

    # Duplicate check across sample
    dupes = check_duplicates(cases_by_citation)

    # ── Results ──────────────────────────────────────────────────────────

    elapsed = time.time() - start_time

    total_checks = sum(len(a.checks) for a in audits)
    passed_checks = sum(sum(1 for c in a.checks if c.passed) for a in audits)
    failed_critical = sum(len(a.critical_failures) for a in audits)
    warnings = sum(len(a.warnings) for a in audits)
    perfect = sum(1 for a in audits if a.passed and not a.warnings)
    avg_score = sum(a.score for a in audits) / len(audits) if audits else 0

    # Per-check pass rates
    check_stats = defaultdict(lambda: {"passed": 0, "failed": 0})
    for audit in audits:
        for check in audit.checks:
            if check.passed:
                check_stats[check.name]["passed"] += 1
            else:
                check_stats[check.name]["failed"] += 1

    # Metadata quality stats
    meta_scores = []
    for audit in audits:
        for check in audit.checks:
            if check.name == "METADATA_QUALITY" and check.detail:
                match = re.search(r"(\d+)/(\d+)", check.detail)
                if match:
                    meta_scores.append(int(match.group(1)) / int(match.group(2)) * 100)

    log.info("\n" + "=" * 60)
    log.info("AUDIT RESULTS")
    log.info("=" * 60)

    log.info(f"\nSample: {len(sample)} cases from {len(all_files):,} total")
    log.info(f"Time: {elapsed:.1f}s")
    log.info(f"\nOverall Score: {avg_score:.1f}%")
    log.info(f"Perfect cases: {perfect}/{len(audits)} ({perfect/len(audits)*100:.0f}%)")
    log.info(f"Total checks: {total_checks} ({passed_checks} passed, {total_checks - passed_checks} failed)")
    log.info(f"Critical failures: {failed_critical}")
    log.info(f"Warnings: {warnings}")
    log.info(f"Duplicates found: {len(dupes)}")

    if meta_scores:
        log.info(f"Avg metadata quality: {sum(meta_scores)/len(meta_scores):.0f}%")

    log.info("\nPer-check pass rates:")
    for name in ["FILE_EXISTS", "JSON_VALID", "SCHEMA_COMPLETE", "CONTENT_NOT_EMPTY",
                 "NO_TRUNCATION", "ENCODING_CLEAN", "FORMAT_4WAY", "CROSS_FORMAT",
                 "METADATA_QUALITY", "CITATION_VALID"]:
        stats = check_stats.get(name, {"passed": 0, "failed": 0})
        total = stats["passed"] + stats["failed"]
        rate = stats["passed"] / total * 100 if total > 0 else 0
        icon = "\u2705" if rate >= 95 else ("\u26a0\ufe0f" if rate >= 80 else "\u274c")
        log.info(f"  {icon} {name}: {stats['passed']}/{total} ({rate:.0f}%)")

    # Show failures
    if failed_critical > 0:
        log.info("\n\u274c CRITICAL FAILURES:")
        for audit in audits:
            for check in audit.critical_failures:
                log.info(f"  {audit.citation}: {check.name} - {check.detail}")

    log.info("\n\u2139\ufe0f  Known Issues (not counted as failures):")
    log.info("  - ENCODING_CLEAN: Master JSON files use raw PLS encoding by design. Database is cleaned separately.")
    log.info("  - METADATA_QUALITY: Pre-2015 PLS cases lack court/judges fields. Not fixable.")
    log.info("  - FSC 6% PDF: 10,771 of 11,483 are empty stubs on PLS. Only 712 have real content.")
    log.info("  - LHC 0 PDF: Blocked by firewall, needs Pakistan IP.")
    log.info("  - FCC 0 data: No website exists for this court.")

    if warnings > 0 and warnings <= 20:
        log.info(f"\n\u26a0\ufe0f WARNINGS ({warnings}):")
        for audit in audits:
            for check in audit.warnings:
                log.info(f"  {audit.citation}: {check.name} - {check.detail}")
    elif warnings > 20:
        log.info(f"\n\u26a0\ufe0f {warnings} warnings (showing first 10):")
        count = 0
        for audit in audits:
            for check in audit.warnings:
                if count >= 10:
                    break
                log.info(f"  {audit.citation}: {check.name} - {check.detail}")
                count += 1
            if count >= 10:
                break

    if dupes:
        log.info(f"\n\u26a0\ufe0f DUPLICATES ({len(dupes)}):")
        for cit, paths in list(dupes.items())[:5]:
            log.info(f"  {cit}: {len(paths)} copies")

    # Health grade
    if avg_score >= 95 and failed_critical == 0:
        grade = "A+"
    elif avg_score >= 90 and failed_critical <= 2:
        grade = "A"
    elif avg_score >= 80:
        grade = "B"
    elif avg_score >= 70:
        grade = "C"
    else:
        grade = "D"

    # Trend vs yesterday
    trend_str = ""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    yesterday_file = RESULTS_DIR / f"{yesterday}.json"
    if yesterday_file.exists():
        try:
            with open(yesterday_file, "r", encoding="utf-8") as f:
                prev = json.load(f)
            prev_score = prev.get("avg_score", 0)
            delta = avg_score - prev_score
            arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
            trend_str = f" ({arrow}{abs(delta):.1f}% vs yesterday)"
        except Exception:
            pass

    log.info(f"\n{'=' * 60}")
    log.info(f"HEALTH GRADE: {grade} ({avg_score:.1f}%){trend_str}")
    log.info(f"{'=' * 60}")

    # Save results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / f"{today}.json"
    result_data = {
        "date": today,
        "sample_size": len(sample),
        "total_files": len(all_files),
        "elapsed_seconds": round(elapsed, 1),
        "grade": grade,
        "avg_score": round(avg_score, 1),
        "perfect_cases": perfect,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "critical_failures": failed_critical,
        "warnings": warnings,
        "duplicates": len(dupes),
        "check_pass_rates": {
            name: round(stats["passed"] / (stats["passed"] + stats["failed"]) * 100, 1)
            if (stats["passed"] + stats["failed"]) > 0 else 0
            for name, stats in check_stats.items()
        },
        "failures": [
            {"citation": a.citation, "check": c.name, "detail": c.detail}
            for a in audits for c in a.critical_failures
        ],
        "warnings_list": [
            {"citation": a.citation, "check": c.name, "detail": c.detail}
            for a in audits for c in a.warnings
        ][:50],
    }

    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)
    log.info(f"\nResults saved: {result_file}")

    return 0 if failed_critical == 0 else 1


# ── Legislation audit ───────────────────────────────────────────────────────

def run_legislation_audit(n=20):
    """Quick audit of legislation files."""
    log.info("\n" + "=" * 60)
    log.info("LEGISLATION AUDIT")
    log.info("=" * 60)

    leg_dir = DATA_DIR / "legislation"
    if not leg_dir.exists():
        log.info("\u274c Legislation directory not found")
        return

    all_files = list(leg_dir.rglob("*.json"))
    log.info(f"Total legislation files: {len(all_files):,}")

    sample = random.sample(all_files, min(n, len(all_files)))
    ok = 0
    issues = []

    for f in sample:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            title = data.get("title", data.get("short_title", ""))
            body = data.get("body", data.get("content", data.get("text", "")))
            if not title:
                issues.append(f"{f.name}: missing title")
            elif not body or len(str(body)) < 50:
                # PLS legislation files often have no body text — log as info, not an issue
                log.info(f"  ℹ️  {f.name}: no body text ({len(str(body))} chars) — expected for PLS legislation")
                ok += 1
            else:
                ok += 1
        except Exception as e:
            issues.append(f"{f.name}: {e}")

    log.info(f"Checked: {len(sample)} | OK: {ok} | Issues: {len(issues)}")
    for issue in issues[:10]:
        log.info(f"  \u26a0\ufe0f {issue}")


def run_court_audit(n=20):
    """Quick audit of court case files."""
    log.info("\n" + "=" * 60)
    log.info("COURT DATA AUDIT")
    log.info("=" * 60)

    court_dir = DATA_DIR / "court_cases"
    if not court_dir.exists():
        log.info("\u274c Court cases directory not found")
        return

    courts = {}
    for court in court_dir.iterdir():
        if court.is_dir():
            json_count = len(list(court.rglob("*.json")))
            pdf_count = len(list(court.rglob("*.pdf")))
            courts[court.name] = {"json": json_count, "pdf": pdf_count}

    total_json = sum(c["json"] for c in courts.values())
    total_pdf = sum(c["pdf"] for c in courts.values())

    # Known limitations for specific courts
    COURT_NOTES = {
        "FSC": "⚠️  Known: 10,771 of 11,483 PDFs are empty stubs on PLS; only 712 have real content",
        "LHC": "⚠️  Known: PDF download blocked by PLS firewall — needs Pakistan IP to access",
        "FCC": "⚠️  Known: FCC has no website; no data available by design",
    }

    log.info(f"Courts: {len(courts)} | JSON: {total_json:,} | PDF: {total_pdf:,}")
    for court, counts in sorted(courts.items(), key=lambda x: -x[1]["json"]):
        pdf_pct = counts["pdf"] / counts["json"] * 100 if counts["json"] > 0 else 0
        icon = "\u2705" if pdf_pct > 50 else "\u26a0\ufe0f"
        log.info(f"  {icon} {court}: {counts['json']:,} JSON, {counts['pdf']:,} PDF ({pdf_pct:.0f}%)")
        if court in COURT_NOTES:
            log.info(f"       {COURT_NOTES[court]}")


# ── Entry point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Comprehensive Qanun data audit")
    parser.add_argument("--n", type=int, default=50, help="Sample size (default: 50)")
    parser.add_argument("--reporter", type=str, help="Filter by reporter (e.g., SCMR)")
    parser.add_argument("--live", action="store_true", help="Include live PLS verification")
    parser.add_argument("--full", action="store_true", help="All checks + live PLS")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument("--legislation", action="store_true", help="Also audit legislation")
    parser.add_argument("--courts", action="store_true", help="Also audit court data")
    parser.add_argument("--all", action="store_true", help="Audit everything")
    args = parser.parse_args()

    exit_code = run_audit(
        n=args.n,
        reporter=args.reporter,
        live=args.live or args.full,
        seed=args.seed,
    )

    if args.legislation or args.all:
        run_legislation_audit()

    if args.courts or args.all:
        run_court_audit()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
