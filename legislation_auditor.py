"""
Legislation Auditor — 10-Technique Quality Verification System
==============================================================
Comprehensive auditor for scraped legislation data. Uses 10 independent
audit techniques to verify completeness, accuracy, and data integrity.

Usage:
    python legislation_auditor.py                   # Full audit
    python legislation_auditor.py --letter A        # Single letter
    python legislation_auditor.py --verbose         # Detailed output
    python legislation_auditor.py --json-only       # JSON report only

Output:
    - Terminal: colored summary with grades per technique
    - File: data_v2/legislation/audit_report.json
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime
import difflib
import hashlib
import json
import math
import os
import pathlib
import re
import statistics
import sys
import time
from typing import Any

# ── ANSI Colors ───────────────────────────────────────────────────────────────

def _has_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

C_RED    = "\033[91m" if _has_color() else ""
C_GREEN  = "\033[92m" if _has_color() else ""
C_YELLOW = "\033[93m" if _has_color() else ""
C_BLUE   = "\033[94m" if _has_color() else ""
C_CYAN   = "\033[96m" if _has_color() else ""
C_BOLD   = "\033[1m"  if _has_color() else ""
C_RESET  = "\033[0m"  if _has_color() else ""

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = ["title", "full_text", "alphabet", "source_url"]
IMPORTANT_FIELDS = ["sections", "enactment_date", "jurisdiction", "status"]
KNOWN_JURISDICTIONS = {
    "Federal", "Punjab", "Sindh", "KPK", "Balochistan",
    "ICT", "AJK", "GB", "FATA", "Khyber Pakhtunkhwa",
    "North-West Frontier Province", "NWFP",
}
PLS_ERROR_PATTERNS = [
    "session expired", "please login", "please log in",
    "error occurred", "access denied", "page not found",
    "internal server error", "login required",
]
# PLS API sentinel values — section text = "-1" means "no text available"
PLS_SENTINEL_PATTERNS = ['"-1"', "'-1'", "\n-1\n", '"-1"\n']
HTML_ARTIFACT_PATTERNS = ["<html", "<div", "<span", "<p>", "<body", "\\u003c"]
DATE_MIN = 1836
DATE_MAX = 2026

# Regex for cross-reference scanning
ACT_REF_RE = re.compile(
    r"\b(?:the\s+)?([A-Z][A-Za-z\s,]+(?:Act|Ordinance|Rules?|Regulations?|Code|Order))[,\s]+(\d{4})\b"
)


# ── File Loader ───────────────────────────────────────────────────────────────

def load_json_file(path: str) -> tuple[str, dict[str, Any] | None, str | None]:
    """Load a JSON file. Returns (path, data, error_message)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return path, json.load(f), None
    except json.JSONDecodeError as e:
        return path, None, f"JSON decode error: {e}"
    except UnicodeDecodeError as e:
        return path, None, f"Unicode decode error: {e}"
    except OSError as e:
        return path, None, f"OS error: {e}"


def scan_letter(letter_path: str) -> list[tuple[str, dict | None, str | None]]:
    """Scan all JSON files in a letter directory."""
    results = []
    try:
        for fname in os.listdir(letter_path):
            if not fname.endswith(".json"):
                continue
            results.append(load_json_file(os.path.join(letter_path, fname)))
    except OSError:
        pass
    return results


# ── Data Loader ───────────────────────────────────────────────────────────────

class LegislationLoader:
    def __init__(self, base: pathlib.Path, letter_filter: str | None = None):
        self.base = base
        self.leg_dir = base / "legislation"
        self.html_dir = base / "html" / "legislation"
        self.federal_dir = base / "federal_laws"
        self.progress_path = self.leg_dir / "progress.json"
        self.letter_filter = letter_filter.upper() if letter_filter else None

        self.all_files: list[tuple[str, dict | None, str | None]] = []
        self.corrupt: list[tuple[str, str]] = []
        self.letter_files: dict[str, list[tuple[str, dict]]] = collections.defaultdict(list)
        self.completed_letters: list[str] = []
        self.progress: dict = {}

    def load(self) -> None:
        """Load all legislation files using ThreadPoolExecutor."""
        # Load progress
        if self.progress_path.exists():
            try:
                with open(self.progress_path, "r", encoding="utf-8") as f:
                    self.progress = json.load(f)
                self.completed_letters = self.progress.get("completed_alphabets", [])
            except (json.JSONDecodeError, OSError):
                pass

        # Find letter directories
        letter_dirs = []
        for d in sorted(os.listdir(self.leg_dir)):
            full = self.leg_dir / d
            if full.is_dir() and len(d) == 1 and d.isalpha():
                if self.letter_filter and d.upper() != self.letter_filter:
                    continue
                letter_dirs.append((d, str(full)))

        # Parallel load
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(scan_letter, path): letter for letter, path in letter_dirs}
            for future in concurrent.futures.as_completed(futures):
                letter = futures[future]
                try:
                    results = future.result()
                    for path, data, err in results:
                        self.all_files.append((path, data, err))
                        if err:
                            self.corrupt.append((path, err))
                        elif data is not None:
                            self.letter_files[letter].append((path, data))
                except Exception as e:
                    self.corrupt.append((str(letter), str(e)))


# ── Audit Techniques ──────────────────────────────────────────────────────────

def audit_schema(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 1: Schema validation — required and important fields."""
    issues: list[dict] = []
    missing_required: dict[str, int] = collections.defaultdict(int)
    empty_required: dict[str, int] = collections.defaultdict(int)
    missing_important: dict[str, int] = collections.defaultdict(int)

    for letter, files in loader.letter_files.items():
        for path, data in files:
            fname = os.path.basename(path)
            for field in REQUIRED_FIELDS:
                val = data.get(field)
                if val is None:
                    missing_required[field] += 1
                    issues.append({"severity": "critical", "file": fname, "letter": letter,
                                   "msg": f"Missing required field: {field}"})
                elif isinstance(val, str) and not val.strip():
                    empty_required[field] += 1
                    issues.append({"severity": "warning", "file": fname, "letter": letter,
                                   "msg": f"Empty required field: {field}"})
            for field in IMPORTANT_FIELDS:
                val = data.get(field)
                if val is None or (isinstance(val, str) and not val.strip()) or val == []:
                    missing_important[field] += 1

    critical = sum(1 for i in issues if i["severity"] == "critical")
    warnings = sum(1 for i in issues if i["severity"] == "warning")
    status = "FAIL" if critical > 0 else ("WARN" if warnings > 0 else "PASS")

    summary_parts = []
    for field, count in {**missing_required, **empty_required}.items():
        if count: summary_parts.append(f"{field} missing/empty: {count}")
    for field, count in missing_important.items():
        if count: summary_parts.append(f"{field} (important) absent: {count}")

    return {
        "status": status, "critical": critical, "warnings": warnings,
        "summary": "; ".join(summary_parts) if summary_parts else "All required fields present",
        "details": {"missing_required": dict(missing_required),
                    "empty_required": dict(empty_required),
                    "missing_important": dict(missing_important)},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_content(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 2: Content integrity — truncation, HTML artifacts, error pages."""
    issues: list[dict] = []
    truncated = 0
    html_artifacts = 0
    error_pages = 0
    title_only = 0

    no_text_at_all = 0
    pls_sentinel = 0
    for letter, files in loader.letter_files.items():
        for path, data in files:
            fname = os.path.basename(path)
            ft = data.get("full_text", "") or ""
            title = data.get("title", "") or ""

            # Completely empty (likely PLS metadata-only statute — no text digitized)
            if len(ft) == 0:
                no_text_at_all += 1
                issues.append({"severity": "warning", "file": fname, "letter": letter,
                               "msg": "Empty full_text (PLS metadata-only — text not digitized?)"})
                continue

            # PLS sentinel value ("-1") — section text was not available from API
            is_sentinel = any(pat in ft for pat in PLS_SENTINEL_PATTERNS)
            if is_sentinel and len(ft) < 50:
                pls_sentinel += 1
                issues.append({"severity": "warning", "file": fname, "letter": letter,
                               "msg": f"PLS sentinel value in full_text ({len(ft)} chars) — text not in API"})
                continue

            # Truncated (has some text but suspiciously short)
            if 0 < len(ft) < 100:
                truncated += 1
                issues.append({"severity": "critical", "file": fname, "letter": letter,
                               "msg": f"Truncated full_text ({len(ft)} chars)"})

            # HTML artifacts
            for pat in HTML_ARTIFACT_PATTERNS:
                if pat in ft[:500]:
                    html_artifacts += 1
                    issues.append({"severity": "warning", "file": fname, "letter": letter,
                                   "msg": f"HTML artifact in full_text: {pat!r}"})
                    break

            # PLS error pages
            ft_lower = ft.lower()[:500]
            for pat in PLS_ERROR_PATTERNS:
                if pat in ft_lower:
                    error_pages += 1
                    issues.append({"severity": "critical", "file": fname, "letter": letter,
                                   "msg": f"PLS error page content: {pat!r}"})
                    break

            # Title-only content
            if ft.strip() and ft.strip() == title.strip():
                title_only += 1
                issues.append({"severity": "warning", "file": fname, "letter": letter,
                               "msg": "full_text is identical to title (no content)"})

    critical = sum(1 for i in issues if i["severity"] == "critical")
    warnings = sum(1 for i in issues if i["severity"] == "warning")
    status = "FAIL" if critical > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": status, "critical": critical, "warnings": warnings,
        "summary": (f"Metadata-only (no text): {no_text_at_all}, PLS sentinel (-1): {pls_sentinel}, "
                    f"Truncated: {truncated}, HTML artifacts: {html_artifacts}, Error pages: {error_pages}"),
        "details": {"no_text_metadata_only": no_text_at_all, "pls_sentinel": pls_sentinel,
                    "truncated": truncated, "html_artifacts": html_artifacts,
                    "error_pages": error_pages, "title_only": title_only},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_anomaly(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 3: Statistical anomaly detection — file size and section count outliers."""
    issues: list[dict] = []
    letter_outliers: dict[str, list] = {}
    global_sizes: list[int] = []

    for letter, files in loader.letter_files.items():
        if not files:
            continue
        sizes = [os.path.getsize(path) for path, _ in files]
        global_sizes.extend(sizes)

        if len(sizes) < 3:
            continue
        mean = statistics.mean(sizes)
        stdev = statistics.stdev(sizes) if len(sizes) > 1 else 0
        if stdev == 0:
            continue

        outliers = []
        for path, data in files:
            sz = os.path.getsize(path)
            z = abs(sz - mean) / stdev
            if z > 3:
                outliers.append({"file": os.path.basename(path), "size": sz,
                                  "z_score": round(z, 2), "mean": round(mean)})
                issues.append({"severity": "warning", "file": os.path.basename(path),
                               "letter": letter, "msg": f"Size outlier: {sz} bytes (z={z:.1f})"})
        if outliers:
            letter_outliers[letter] = outliers

    # Section count distribution
    sec_counts = []
    no_sections = 0
    for _, files in loader.letter_files.items():
        for _, data in files:
            sc = len(data.get("sections", []) or [])
            sec_counts.append(sc)
            if sc == 0:
                no_sections += 1

    warnings = len(issues)
    status = "WARN" if warnings > 0 else "PASS"
    stats_summary = ""
    if global_sizes:
        stats_summary = (f"Size: min={min(global_sizes)}, median={statistics.median(global_sizes):.0f}, "
                         f"max={max(global_sizes)} bytes. "
                         f"Sections: avg={statistics.mean(sec_counts):.1f} per statute, "
                         f"{no_sections} with 0 sections.")

    return {
        "status": status, "critical": 0, "warnings": warnings,
        "summary": f"{warnings} outliers detected. {stats_summary}",
        "details": {"outliers_by_letter": letter_outliers,
                    "no_sections": no_sections,
                    "size_stats": {
                        "min": min(global_sizes) if global_sizes else 0,
                        "max": max(global_sizes) if global_sizes else 0,
                        "median": statistics.median(global_sizes) if global_sizes else 0,
                        "mean": round(statistics.mean(global_sizes)) if global_sizes else 0,
                    }},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_duplicates(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 4: Duplicate detection — exact titles, fuzzy titles, content hashes."""
    issues: list[dict] = []
    title_map: dict[str, list[str]] = collections.defaultdict(list)
    hash_map: dict[str, list[str]] = collections.defaultdict(list)

    all_items: list[tuple[str, str, str, str]] = []  # (path, letter, title, content_hash)

    for letter, files in loader.letter_files.items():
        for path, data in files:
            title = (data.get("title") or "").strip().lower()
            ft = (data.get("full_text") or "")[:500]
            content_hash = hashlib.md5(ft.encode("utf-8", errors="ignore")).hexdigest()
            fname = os.path.basename(path)
            title_map[title].append(f"{letter}/{fname}")
            hash_map[content_hash].append(f"{letter}/{fname}")
            all_items.append((path, letter, title, content_hash))

    exact_dupes = 0
    for title, paths in title_map.items():
        if len(paths) > 1:
            exact_dupes += 1
            issues.append({"severity": "critical", "file": paths[0], "letter": paths[0].split("/")[0],
                           "msg": f"Exact title duplicate ({len(paths)} copies): {title[:60]!r}"})

    content_dupes = 0
    for h, paths in hash_map.items():
        if len(paths) > 1:
            content_dupes += 1
            issues.append({"severity": "warning", "file": paths[0], "letter": paths[0].split("/")[0],
                           "msg": f"Content hash duplicate ({len(paths)} copies)"})

    # Fuzzy matching (sample — check within each letter to keep it fast)
    fuzzy_dupes = 0
    for letter, files in loader.letter_files.items():
        titles = [(os.path.basename(p), (d.get("title") or "").strip()) for p, d in files]
        for i in range(len(titles)):
            for j in range(i + 1, min(i + 30, len(titles))):  # window of 30
                ratio = difflib.SequenceMatcher(None, titles[i][1].lower(), titles[j][1].lower()).ratio()
                if ratio > 0.9 and titles[i][1] != titles[j][1]:
                    fuzzy_dupes += 1
                    issues.append({"severity": "warning", "file": titles[i][0], "letter": letter,
                                   "msg": f"Fuzzy title match ({ratio:.2f}): {titles[j][0]!r}"})

    critical = sum(1 for i in issues if i["severity"] == "critical")
    warnings = sum(1 for i in issues if i["severity"] == "warning")
    status = "FAIL" if critical > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": status, "critical": critical, "warnings": warnings,
        "summary": f"Exact title dupes: {exact_dupes}, Content dupes: {content_dupes}, Fuzzy dupes: {fuzzy_dupes}",
        "details": {"exact_title_duplicates": exact_dupes,
                    "content_hash_duplicates": content_dupes,
                    "fuzzy_title_duplicates": fuzzy_dupes},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_format(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 5: Format completeness — check readable HTML exists for each JSON."""
    issues: list[dict] = []
    missing_html = 0
    total_checked = 0

    for letter, files in loader.letter_files.items():
        html_letter_dir = loader.html_dir / letter
        for path, data in files:
            total_checked += 1
            fname = pathlib.Path(path).stem
            html_path = html_letter_dir / f"{fname}.html"
            if not html_path.exists():
                missing_html += 1
                issues.append({"severity": "warning", "file": os.path.basename(path),
                               "letter": letter, "msg": "No readable HTML counterpart"})

    coverage_pct = round((total_checked - missing_html) / total_checked * 100, 1) if total_checked else 0
    status = "FAIL" if missing_html > total_checked * 0.2 else ("WARN" if missing_html > 0 else "PASS")

    return {
        "status": status, "critical": 0, "warnings": min(missing_html, 9999),
        "summary": f"HTML coverage: {coverage_pct}% ({total_checked - missing_html}/{total_checked}). Missing: {missing_html}",
        "details": {"total": total_checked, "has_html": total_checked - missing_html,
                    "missing_html": missing_html, "coverage_pct": coverage_pct},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_session_death(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 6: Session death detection — time gaps, zero-file letters, tiny clusters."""
    issues: list[dict] = []
    zero_complete = []
    time_gaps: list[dict] = []
    tiny_clusters: dict[str, int] = {}

    # Only check letters that were actually loaded (respect letter_filter)
    letters_to_check = list(loader.letter_files.keys()) if loader.letter_filter else loader.completed_letters
    for letter in letters_to_check:
        if loader.letter_filter and letter != loader.letter_filter:
            continue
        files = loader.letter_files.get(letter, [])
        if letter in loader.completed_letters and len(files) == 0:
            zero_complete.append(letter)
            issues.append({"severity": "critical", "file": f"{letter}/", "letter": letter,
                           "msg": f"Completed letter {letter} has 0 files — session death false positive"})
            continue

        # Sort by creation time
        with_times = []
        for path, data in files:
            try:
                ct = os.path.getctime(path)
                sz = os.path.getsize(path)
                with_times.append((ct, path, sz))
            except OSError:
                pass
        with_times.sort()

        # Check for time gaps > 2 hours
        for i in range(1, len(with_times)):
            gap_hours = (with_times[i][0] - with_times[i - 1][0]) / 3600
            if gap_hours > 2:
                time_gaps.append({"letter": letter, "gap_hours": round(gap_hours, 1),
                                  "after_file": os.path.basename(with_times[i - 1][1]),
                                  "before_file": os.path.basename(with_times[i][1])})
                issues.append({"severity": "warning", "file": os.path.basename(with_times[i][1]),
                               "letter": letter,
                               "msg": f"Time gap of {gap_hours:.1f}h detected (possible session death/restart)"})

        # Check for clusters of tiny files
        tiny = [p for ct, p, sz in with_times if sz < 500]
        if tiny:
            tiny_clusters[letter] = len(tiny)
            issues.append({"severity": "warning", "file": f"{letter}/",
                           "letter": letter, "msg": f"{len(tiny)} tiny files (<500B) — possible error pages"})

    critical = sum(1 for i in issues if i["severity"] == "critical")
    warnings = sum(1 for i in issues if i["severity"] == "warning")
    status = "FAIL" if critical > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": status, "critical": critical, "warnings": warnings,
        "summary": (f"Zero-file completed letters: {zero_complete}. "
                    f"Time gaps: {len(time_gaps)}. Tiny file clusters: {len(tiny_clusters)}."),
        "details": {"zero_complete": zero_complete, "time_gaps": time_gaps,
                    "tiny_clusters": tiny_clusters},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_cross_source(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 7: Cross-source validation — compare against federal_laws."""
    issues: list[dict] = []
    federal_titles: list[str] = []

    # Load federal laws
    for subdir in ["acts", "ordinances"]:
        fed_dir = loader.federal_dir / subdir
        if not fed_dir.exists():
            continue
        for fname in os.listdir(fed_dir):
            if not fname.endswith(".json"):
                continue
            try:
                with open(fed_dir / fname, "r", encoding="utf-8") as f:
                    d = json.load(f)
                title = (d.get("title") or d.get("name") or "").strip()
                if title:
                    federal_titles.append(title)
            except (json.JSONDecodeError, OSError):
                pass

    if not federal_titles:
        return {"status": "SKIP", "critical": 0, "warnings": 0,
                "summary": "No federal laws data found for cross-validation",
                "details": {}, "issues": []}

    # Build legislation title set (lowercased, normalized)
    leg_titles_lower: set[str] = set()
    for _, files in loader.letter_files.items():
        for _, data in files:
            t = (data.get("title") or "").strip().lower()
            if t:
                leg_titles_lower.add(t)

    found = 0
    missing = []
    for fed_title in federal_titles:
        norm = fed_title.strip().lower()
        # Exact match
        if norm in leg_titles_lower:
            found += 1
            continue
        # Fuzzy match (check against top candidates using difflib)
        candidates = difflib.get_close_matches(norm, list(leg_titles_lower), n=1, cutoff=0.85)
        if candidates:
            found += 1
        else:
            missing.append(fed_title)
            issues.append({"severity": "warning", "file": "", "letter": "-",
                           "msg": f"Federal law NOT in PLS legislation: {fed_title[:80]!r}"})

    match_pct = round(found / len(federal_titles) * 100, 1) if federal_titles else 0
    status = "FAIL" if match_pct < 50 else ("WARN" if match_pct < 80 else "PASS")

    return {
        "status": status, "critical": 0, "warnings": len(missing),
        "summary": f"Federal laws matched: {found}/{len(federal_titles)} ({match_pct}%). Missing: {len(missing)}.",
        "details": {"total_federal": len(federal_titles), "matched": found,
                    "missing_count": len(missing), "match_pct": match_pct,
                    "missing_sample": missing[:20]},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_cross_reference(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 8: Internal cross-reference check — do referenced statutes exist?

    Uses year-indexed lookup for O(1) candidate filtering instead of full fuzzy scan.
    """
    issues: list[dict] = []

    # Build year → titles index for fast lookup
    year_index: dict[str, set[str]] = collections.defaultdict(set)
    YEAR_RE = re.compile(r"\b(\d{4})\b")
    for _, files in loader.letter_files.items():
        for _, data in files:
            t = (data.get("title") or "").strip().lower()
            if not t:
                continue
            for yr in YEAR_RE.findall(t):
                year_index[yr].add(t)

    broken_refs = 0
    total_refs = 0
    seen_refs: set[str] = set()  # deduplicate same reference across files

    for letter, files in loader.letter_files.items():
        for path, data in files:
            ft = data.get("full_text") or ""
            fname = os.path.basename(path)
            matches = ACT_REF_RE.findall(ft[:3000])  # scan first 3K chars only
            for ref_name, ref_year in matches:
                ref_key = f"{ref_name.strip().lower()}_{ref_year}"
                if ref_key in seen_refs:
                    continue
                seen_refs.add(ref_key)
                total_refs += 1

                # Fast path: check if year has any candidate titles
                candidates = year_index.get(ref_year, set())
                if not candidates:
                    broken_refs += 1
                    issues.append({"severity": "warning", "file": fname, "letter": letter,
                                   "msg": f"Cross-ref not found: {ref_name.strip()} {ref_year}"})
                    continue

                # Check if any candidate contains key words from ref_name
                ref_words = set(ref_name.strip().lower().split())
                ref_words -= {"the", "a", "an", "of", "and", "in", "to", "for"}
                found = any(ref_words.issubset(set(c.split())) for c in candidates)
                if not found:
                    # Quick fuzzy check against year-filtered candidates only
                    ref_title = f"{ref_name.strip()} {ref_year}".lower()
                    cands_list = list(candidates)[:200]  # cap at 200
                    match = difflib.get_close_matches(ref_title, cands_list, n=1, cutoff=0.7)
                    if not match:
                        broken_refs += 1
                        issues.append({"severity": "warning", "file": fname, "letter": letter,
                                       "msg": f"Cross-ref not found: {ref_name.strip()} {ref_year}"})

    status = "WARN" if broken_refs > 0 else "PASS"
    found_pct = round((total_refs - broken_refs) / total_refs * 100, 1) if total_refs else 100

    return {
        "status": status, "critical": 0, "warnings": min(broken_refs, 9999),
        "summary": (f"Unique cross-refs: {total_refs}. "
                    f"Resolvable: {total_refs - broken_refs} ({found_pct}%). "
                    f"Unresolvable: {broken_refs}."),
        "details": {"total_refs": total_refs, "broken_refs": broken_refs,
                    "found_pct": found_pct},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_alphabet(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 9: Alphabet placement — title starts with correct letter."""
    issues: list[dict] = []
    misplaced = 0
    THE_RE = re.compile(r"^the\s+", re.IGNORECASE)

    for letter, files in loader.letter_files.items():
        for path, data in files:
            title = (data.get("title") or "").strip()
            if not title:
                continue
            # Strip leading "The "
            clean = THE_RE.sub("", title).strip()
            if not clean:
                continue
            first_char = clean[0].upper()
            if first_char != letter.upper():
                misplaced += 1
                fname = os.path.basename(path)
                issues.append({"severity": "warning", "file": fname, "letter": letter,
                               "msg": f"Misplaced: title starts with {first_char!r}, dir is {letter!r} — {title[:60]!r}"})

    status = "WARN" if misplaced > 0 else "PASS"
    return {
        "status": status, "critical": 0, "warnings": misplaced,
        "summary": f"Misplaced statutes: {misplaced}",
        "details": {"misplaced": misplaced},
        "issues": issues[:50] if not verbose else issues,
    }


def audit_dates(loader: LegislationLoader, verbose: bool) -> dict[str, Any]:
    """Technique 10: Date and jurisdiction validation."""
    issues: list[dict] = []
    invalid_dates = 0
    unknown_jurisdictions: dict[str, int] = collections.defaultdict(int)
    date_distribution: dict[int, int] = collections.defaultdict(int)
    DATE_RE = re.compile(r"(\d{4})")

    for letter, files in loader.letter_files.items():
        for path, data in files:
            fname = os.path.basename(path)
            raw_date = data.get("enactment_date") or ""
            if raw_date:
                m = DATE_RE.search(str(raw_date))
                if m:
                    year = int(m.group(1))
                    if year < DATE_MIN or year > DATE_MAX:
                        invalid_dates += 1
                        issues.append({"severity": "warning", "file": fname, "letter": letter,
                                       "msg": f"Date out of range: {raw_date!r} (year={year})"})
                    else:
                        date_distribution[year] += 1

            jurisdiction = (data.get("jurisdiction") or "").strip()
            if jurisdiction and jurisdiction not in KNOWN_JURISDICTIONS:
                unknown_jurisdictions[jurisdiction] += 1
                issues.append({"severity": "warning", "file": fname, "letter": letter,
                               "msg": f"Unknown jurisdiction: {jurisdiction!r}"})

    warnings = len(issues)
    status = "WARN" if warnings > 0 else "PASS"

    # Top years
    top_years = sorted(date_distribution.items(), key=lambda x: -x[1])[:10]

    return {
        "status": status, "critical": 0, "warnings": warnings,
        "summary": (f"Invalid dates: {invalid_dates}. Unknown jurisdictions: {len(unknown_jurisdictions)}. "
                    f"Year range: {min(date_distribution) if date_distribution else 'N/A'}"
                    f"–{max(date_distribution) if date_distribution else 'N/A'}."),
        "details": {"invalid_dates": invalid_dates,
                    "unknown_jurisdictions": dict(unknown_jurisdictions),
                    "top_years": dict(top_years)},
        "issues": issues[:50] if not verbose else issues,
    }


# ── Grading ───────────────────────────────────────────────────────────────────

def compute_grade(total_critical: int, total_warnings: int) -> str:
    if total_critical == 0 and total_warnings < 10:
        return "A"
    if total_critical == 0 and total_warnings < 50:
        return "B"
    if 1 <= total_critical <= 5:
        return "C"
    if 6 <= total_critical <= 20:
        return "D"
    return "F"


GRADE_COLOR = {
    "A": C_GREEN, "B": C_GREEN, "C": C_YELLOW, "D": C_RED, "F": C_RED
}
STATUS_COLOR = {
    "PASS": C_GREEN, "WARN": C_YELLOW, "FAIL": C_RED, "SKIP": C_BLUE
}


# ── Main ──────────────────────────────────────────────────────────────────────

def run_audit(
    data_root: str,
    letter_filter: str | None = None,
    verbose: bool = False,
    json_only: bool = False,
) -> dict[str, Any]:
    start = time.time()
    base = pathlib.Path(data_root)

    if not json_only:
        print(f"\n{C_BOLD}{'='*60}{C_RESET}")
        print(f"{C_BOLD}  Legislation Auditor — 10-Technique Quality Verification{C_RESET}")
        print(f"{C_BOLD}{'='*60}{C_RESET}")
        if letter_filter:
            print(f"  Filter: Letter {letter_filter.upper()} only")
        print()

    # Load data
    if not json_only:
        print(f"{C_CYAN}Loading files...{C_RESET}", end=" ", flush=True)
    loader = LegislationLoader(base, letter_filter)
    loader.load()
    total_files = sum(len(v) for v in loader.letter_files.values())
    if not json_only:
        print(f"{C_GREEN}{total_files:,} files loaded, {len(loader.corrupt)} corrupt{C_RESET}")
        if loader.corrupt:
            print(f"{C_RED}  Corrupt: {[os.path.basename(p) for p, _ in loader.corrupt[:5]]}{C_RESET}")

    # Define techniques
    techniques = [
        ("schema",         "Schema Validation",         audit_schema),
        ("content",        "Content Integrity",         audit_content),
        ("anomaly",        "Statistical Anomaly",       audit_anomaly),
        ("duplicates",     "Duplicate Detection",       audit_duplicates),
        ("format",         "Format Completeness",       audit_format),
        ("session_death",  "Session Death Detection",   audit_session_death),
        ("cross_source",   "Cross-Source Validation",   audit_cross_source),
        ("cross_reference","Cross-Reference Check",     audit_cross_reference),
        ("alphabet",       "Alphabet Placement",        audit_alphabet),
        ("dates",          "Date & Jurisdiction",       audit_dates),
    ]

    results: dict[str, dict] = {}

    for key, name, fn in techniques:
        if not json_only:
            print(f"  {C_CYAN}[{key:15}]{C_RESET} Running...", end="\r", flush=True)
        t0 = time.time()
        try:
            result = fn(loader, verbose)
        except Exception as e:
            result = {"status": "ERROR", "critical": 0, "warnings": 0,
                      "summary": f"Technique crashed: {e}", "details": {}, "issues": []}
        result["elapsed_s"] = round(time.time() - t0, 2)
        results[key] = result

        if not json_only:
            sc = STATUS_COLOR.get(result["status"], C_RESET)
            crit_str = f"{C_RED}{result['critical']}C{C_RESET}" if result["critical"] > 0 else f"0C"
            warn_str = f"{C_YELLOW}{result['warnings']}W{C_RESET}" if result["warnings"] > 0 else f"0W"
            print(f"  {C_CYAN}[{key:15}]{C_RESET} {sc}{result['status']:4}{C_RESET} "
                  f"{crit_str} {warn_str}  {result['summary'][:80]}")

    # Totals
    total_critical = sum(r["critical"] for r in results.values())
    total_warnings = sum(r["warnings"] for r in results.values())
    grade = compute_grade(total_critical, total_warnings)
    elapsed = round(time.time() - start, 1)

    if not json_only:
        gc = GRADE_COLOR.get(grade, C_RESET)
        print(f"\n{C_BOLD}{'='*60}{C_RESET}")
        print(f"  {C_BOLD}Overall Grade: {gc}{grade}{C_RESET}  |  "
              f"{C_RED}{total_critical} critical{C_RESET}  "
              f"{C_YELLOW}{total_warnings} warnings{C_RESET}  |  "
              f"{total_files:,} files  |  {elapsed}s")
        print(f"{C_BOLD}{'='*60}{C_RESET}\n")

    # Build report
    report = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "data_root": str(data_root),
        "letter_filter": letter_filter,
        "total_files": total_files,
        "corrupt_files": len(loader.corrupt),
        "completed_letters": loader.completed_letters,
        "techniques": results,
        "overall_grade": grade,
        "total_critical": total_critical,
        "total_warnings": total_warnings,
        "elapsed_seconds": elapsed,
    }

    # Save report
    report_path = base / "legislation" / "audit_report.json"
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        if not json_only:
            print(f"  Report saved: {report_path}")
    except OSError as e:
        if not json_only:
            print(f"  {C_RED}Could not save report: {e}{C_RESET}")

    if json_only:
        print(json.dumps(report, indent=2, ensure_ascii=False))

    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Legislation Auditor — 10-Technique Quality Verification"
    )
    parser.add_argument("--data-root", default="data_v2",
                        help="Path to data root (default: data_v2)")
    parser.add_argument("--letter", metavar="X",
                        help="Audit a single letter only (e.g. --letter A)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show all issues (not just first 50)")
    parser.add_argument("--json-only", action="store_true",
                        help="Output JSON report only, no terminal formatting")
    args = parser.parse_args()

    run_audit(
        data_root=args.data_root,
        letter_filter=args.letter,
        verbose=args.verbose,
        json_only=args.json_only,
    )


if __name__ == "__main__":
    main()
