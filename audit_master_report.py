#!/usr/bin/env python3
"""
AUDIT SCRIPT 8: Master Report Generator
=========================================
Reads output from all other audit scripts and produces the definitive audit report.

Output: data_v2/audit/MASTER_AUDIT_2026-02-21.md
"""

import sys
import json
import time
from pathlib import Path
from collections import defaultdict
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
OUTPUT_FILE = AUDIT_DIR / f"MASTER_AUDIT_{datetime.now().strftime('%Y-%m-%d')}.md"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]


def load_json(filename):
    path = AUDIT_DIR / filename
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except:
            pass
    return None


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 8: Master Report Generator")
    print("=" * 60)

    # Load all audit outputs
    integrity = load_json("local_integrity.json")
    crossref = load_json("crossref_missing.json")
    orphans = load_json("orphan_citations.json")
    pls_counts = load_json("pls_vs_local_counts.json")
    missing_cits = load_json("missing_citations.json")
    content_verify = load_json("content_issues.json")
    pls_browse = load_json("pls_browse_missing.json")

    # Build report
    lines = []
    lines.append("# 🔍 MASTER AUDIT REPORT — Pakistan Law Site Dataset")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ============================================================
    # EXECUTIVE SUMMARY
    # ============================================================
    lines.append("## 📊 Executive Summary")
    lines.append("")

    # Count local files
    total_local = 0
    local_by_reporter = {}
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if reporter_dir.exists():
            count = sum(1 for d in reporter_dir.iterdir() 
                       if d.is_dir() and d.name.isdigit()
                       for f in d.glob("*.json"))
            local_by_reporter[reporter] = count
            total_local += count
        else:
            local_by_reporter[reporter] = 0

    lines.append(f"- **Total local JSON files:** {total_local:,}")

    if pls_counts:
        stats = pls_counts.get("stats", {})
        lines.append(f"- **Total PLS cases (API):** {stats.get('total_pls_cases', 'N/A'):,}")
        lines.append(f"- **Overall coverage:** {stats.get('overall_coverage_pct', 'N/A')}%")
        lines.append(f"- **Missing cases (PLS vs local):** {stats.get('total_missing', 'N/A'):,}")

    if integrity:
        i_stats = integrity.get("stats", {})
        lines.append(f"- **Files with integrity issues:** {i_stats.get('files_with_issues', 'N/A'):,}")

    if crossref:
        c_stats = crossref.get("stats", {})
        lines.append(f"- **Citations referenced but missing:** {c_stats.get('total_missing_citations', 'N/A'):,}")

    if orphans:
        o_stats = orphans.get("stats", {})
        lines.append(f"- **High-priority orphan citations (5+ refs):** {o_stats.get('high_priority_orphans_5plus_refs', 'N/A'):,}")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ============================================================
    # PER-REPORTER COVERAGE
    # ============================================================
    lines.append("## 📈 Per-Reporter Coverage")
    lines.append("")
    lines.append("| Reporter | Local Files | PLS Count | Coverage | Missing |")
    lines.append("|----------|------------|-----------|----------|---------|")

    if pls_counts:
        per_rep = pls_counts.get("per_reporter", {})
        for reporter in REPORTERS:
            r = per_rep.get(reporter, {})
            local = r.get("local_total", local_by_reporter.get(reporter, 0))
            pls = r.get("pls_total", "?")
            cov = r.get("coverage_pct", "?")
            missing = r.get("missing", "?")
            lines.append(f"| {reporter} | {local:,} | {pls:,} | {cov}% | {missing:,} |")
    else:
        for reporter in REPORTERS:
            lines.append(f"| {reporter} | {local_by_reporter.get(reporter, 0):,} | ? | ? | ? |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ============================================================
    # LOCAL INTEGRITY (Script 6)
    # ============================================================
    lines.append("## 🔧 Local Integrity Check (Script 6)")
    lines.append("")

    if integrity:
        i_stats = integrity.get("stats", {})
        lines.append(f"- Total files checked: {i_stats.get('total_files_checked', 0):,}")
        lines.append(f"- Files with issues: {i_stats.get('files_with_issues', 0):,}")
        lines.append(f"- Total issues: {i_stats.get('total_issues', 0):,}")
        lines.append("")
        lines.append("**Issue breakdown:**")
        lines.append("")
        breakdown = i_stats.get("issue_breakdown", {})
        for issue_type, count in sorted(breakdown.items(), key=lambda x: -x[1]):
            lines.append(f"- `{issue_type}`: {count:,}")
        lines.append("")

        # Per reporter
        lines.append("**Per reporter:**")
        lines.append("")
        per_rep = i_stats.get("per_reporter", {})
        for reporter in REPORTERS:
            r = per_rep.get(reporter, {})
            files = r.get("files", 0)
            issues = r.get("issues", 0)
            pct = (issues / files * 100) if files > 0 else 0
            lines.append(f"- {reporter}: {files:,} files, {issues:,} issues ({pct:.1f}%)")
    else:
        lines.append("*Script 6 not yet run.*")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ============================================================
    # CITATION CROSS-REFERENCE (Script 3)
    # ============================================================
    lines.append("## 🔗 Citation Cross-Reference (Script 3)")
    lines.append("")

    if crossref:
        c_stats = crossref.get("stats", {})
        lines.append(f"- Unique citations referenced in judgments: {c_stats.get('total_unique_citations_referenced', 0):,}")
        lines.append(f"- Missing (referenced but not in dataset): {c_stats.get('total_missing_citations', 0):,}")
        lines.append("")
        
        missing_by_rep = c_stats.get("missing_by_reporter", {})
        if missing_by_rep:
            lines.append("**Missing by reporter:**")
            lines.append("")
            for rep, count in sorted(missing_by_rep.items(), key=lambda x: -x[1]):
                lines.append(f"- {rep}: {count:,}")
            lines.append("")

        top_missing = c_stats.get("top_20_most_referenced_missing", [])
        if top_missing:
            lines.append("**Top 20 most-referenced missing citations:**")
            lines.append("")
            lines.append("| Citation | Referenced By |")
            lines.append("|----------|-------------|")
            for m in top_missing:
                lines.append(f"| {m['citation']} | {m['ref_count']} cases |")
            lines.append("")
    else:
        lines.append("*Script 3 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # ORPHAN CITATIONS (Script 7)
    # ============================================================
    lines.append("## 🏚️ High-Priority Orphan Citations (Script 7)")
    lines.append("")

    if orphans:
        o_stats = orphans.get("stats", {})
        lines.append(f"- High-priority orphans (5+ references): {o_stats.get('high_priority_orphans_5plus_refs', 0):,}")
        lines.append(f"- Medium-priority (3+ references): {o_stats.get('medium_priority_3plus_refs', 0):,}")
        lines.append("")

        orphan_by_rep = o_stats.get("orphan_by_reporter", {})
        if orphan_by_rep:
            lines.append("**Orphans by reporter:**")
            lines.append("")
            for rep, count in sorted(orphan_by_rep.items(), key=lambda x: -x[1]):
                lines.append(f"- {rep}: {count:,}")
            lines.append("")

        high_priority = orphans.get("high_priority_orphans", [])
        if high_priority:
            lines.append("**Top 30 highest-priority missing cases:**")
            lines.append("")
            lines.append("| Rank | Citation | References | Priority |")
            lines.append("|------|----------|-----------|----------|")
            for i, o in enumerate(high_priority[:30], 1):
                lines.append(f"| {i} | {o['citation']} | {o['reference_count']} | {o['priority']} |")
            lines.append("")
    else:
        lines.append("*Script 7 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # PLS COUNT COMPARISON (Script 1)
    # ============================================================
    lines.append("## 📊 PLS Count Comparison (Script 1)")
    lines.append("")

    if pls_counts:
        gaps = pls_counts.get("years_with_gaps", [])
        lines.append(f"- Year/reporter combinations with gaps: {len(gaps)}")
        lines.append("")

        if gaps:
            lines.append("**Top 30 biggest gaps:**")
            lines.append("")
            lines.append("| Year | Reporter | PLS | Local | Missing |")
            lines.append("|------|----------|-----|-------|---------|")
            for g in gaps[:30]:
                lines.append(f"| {g['year']} | {g['reporter']} | {g['pls']} | {g['local']} | {g['missing']} |")
            if len(gaps) > 30:
                lines.append(f"| ... | ... | ... | ... | {len(gaps) - 30} more |")
            lines.append("")
    else:
        lines.append("*Script 1 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # MISSING CITATIONS (Script 2)
    # ============================================================
    lines.append("## 📋 Exact Missing Citations (Script 2)")
    lines.append("")

    if missing_cits:
        m_stats = missing_cits.get("stats", {})
        lines.append(f"- Total missing citations identified: {m_stats.get('total_missing_citations', 0):,}")
        lines.append(f"- Extra local (not in PLS): {m_stats.get('total_extra_local', 0):,}")
        lines.append("")

        missing_by_rep = m_stats.get("missing_by_reporter", {})
        if missing_by_rep:
            lines.append("**Missing by reporter:**")
            lines.append("")
            for rep, count in sorted(missing_by_rep.items(), key=lambda x: -x[1]):
                lines.append(f"- {rep}: {count:,}")
            lines.append("")

        # Show first 50 missing
        flat = missing_cits.get("missing_citations", [])
        if flat:
            lines.append(f"**First 50 missing citations (of {len(flat):,} total):**")
            lines.append("")
            for m in flat[:50]:
                lines.append(f"- `{m['citation']}`")
            lines.append("")
    else:
        lines.append("*Script 2 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # CONTENT VERIFICATION (Script 5)
    # ============================================================
    lines.append("## 🔍 Content Verification (Script 5)")
    lines.append("")

    if content_verify:
        cv_stats = content_verify.get("stats", {})
        lines.append(f"- Sample size: {cv_stats.get('sample_size', 0)}")
        lines.append(f"- Verified: {cv_stats.get('total_verified', 0)}")
        lines.append(f"- Issues found: {cv_stats.get('total_issues', 0)}")
        lines.append("")

        breakdown = cv_stats.get("issue_breakdown", {})
        if breakdown:
            lines.append("**Issue breakdown:**")
            lines.append("")
            for it, count in sorted(breakdown.items(), key=lambda x: -x[1]):
                lines.append(f"- `{it}`: {count}")
            lines.append("")
    else:
        lines.append("*Script 5 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # PLS BROWSE (Script 4)
    # ============================================================
    lines.append("## 🌐 PLS Browse Results (Script 4)")
    lines.append("")

    if pls_browse:
        pb_stats = pls_browse.get("stats", {})
        lines.append(f"- Gap years checked: {pb_stats.get('gap_years_checked', 0)}")
        lines.append(f"- Missing cases found: {pb_stats.get('total_missing_found', 0)}")
        lines.append("")

        flat = pls_browse.get("all_missing_flat", [])
        if flat:
            lines.append(f"**Missing cases with PLS case IDs (for fetching) — first 50:**")
            lines.append("")
            lines.append("| Citation | Case ID | Reporter | Year |")
            lines.append("|----------|---------|----------|------|")
            for m in flat[:50]:
                lines.append(f"| {m['citation']} | `{m.get('case_id', '')}` | {m['reporter']} | {m['year']} |")
            if len(flat) > 50:
                lines.append(f"| ... | ... | ... | {len(flat) - 50} more |")
            lines.append("")
    else:
        lines.append("*Script 4 not yet run.*")

    lines.append("---")
    lines.append("")

    # ============================================================
    # ACTION PLAN
    # ============================================================
    lines.append("## 🎯 Action Plan — Priority Ranked")
    lines.append("")
    lines.append("### Priority 1: Fetch missing cases from PLS")
    lines.append("")
    lines.append("Use the historical_scraper.py with specific year/reporter combinations:")
    lines.append("")
    lines.append("```bash")

    if pls_counts:
        gaps = pls_counts.get("years_with_gaps", [])
        for g in gaps[:20]:
            lines.append(f"python historical_scraper.py --year {g['year']} --reporter {g['reporter']}")

    lines.append("```")
    lines.append("")
    lines.append("### Priority 2: Fix integrity issues")
    lines.append("")
    lines.append("29,240 files have missing year/reporter/page metadata. These need re-extraction from citation field.")
    lines.append("")
    lines.append("### Priority 3: Fetch orphan citations")
    lines.append("")
    lines.append("High-priority orphan citations (referenced 5+ times) should be fetched individually.")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("*This report was generated by the Bulletproof PLS Audit System.*")

    # Write report
    report_text = "\n".join(lines)
    OUTPUT_FILE.write_text(report_text, encoding='utf-8')

    print(f"\nMaster audit report written to: {OUTPUT_FILE}")
    print(f"Report length: {len(report_text):,} characters")


if __name__ == "__main__":
    main()
