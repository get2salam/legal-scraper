"""
PLS AUDIT ROUND 2 - 2026-02-22
Counts all local JSON files, compares vs PLS, generates full report.
"""

import os
import json
from pathlib import Path
from datetime import datetime

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper")
DATA_DIR = BASE_DIR / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

PLS_TOTALS = {  # from Round 1 audit
    "SCMR": 26272,
    "PLD": 23061,
    "PCrLJ": 26208,
    "MLD": 22272,
    "CLC": 20370,
    "YLR": 18612,
    "PTD": 15235,
    "PLC": 8865,
    "CLD": 5294,
    "GBLR": 339,
}
PLS_GRAND_TOTAL = sum(PLS_TOTALS.values())  # 166,528

# Round 1 local totals (for comparison)
ROUND1_LOCAL = {
    "SCMR": 25525, "PLD": 21666, "PCrLJ": 24182, "MLD": 20722,
    "CLC": 19464, "YLR": 17849, "PTD": 14708, "PLC": 7419,
    "CLD": 4556, "GBLR": 213,
}
ROUND1_GRAND_LOCAL = sum(ROUND1_LOCAL.values())

# ─── Step 1: Count local files ────────────────────────────────────────────────
print("=" * 60)
print("PLS AUDIT ROUND 2  —  2026-02-22")
print("=" * 60)
print(f"\nScanning {DATA_DIR} ...\n")

local_counts = {}          # {reporter: {year_str: count}}
reporter_totals = {}       # {reporter: total}

for reporter in REPORTERS:
    rep_dir = DATA_DIR / reporter
    year_counts = {}
    if rep_dir.exists():
        for year_dir in rep_dir.iterdir():
            if year_dir.is_dir():
                n = sum(1 for f in year_dir.iterdir() if f.suffix == ".json")
                if n > 0:
                    year_counts[year_dir.name] = n
    local_counts[reporter] = year_counts
    reporter_totals[reporter] = sum(year_counts.values())
    print(f"  {reporter:10s}: {reporter_totals[reporter]:,} files")

grand_local = sum(reporter_totals.values())
print(f"\n  {'TOTAL':10s}: {grand_local:,} files")
print(f"  {'PLS TARGET':10s}: {PLS_GRAND_TOTAL:,}")
print(f"  {'GAP':10s}: {PLS_GRAND_TOTAL - grand_local:,}")
print(f"  {'COVERAGE':10s}: {grand_local / PLS_GRAND_TOTAL * 100:.2f}%")

# ─── Save local counts ────────────────────────────────────────────────────────
local_counts_out = {
    "audit": "local_counts_round2",
    "timestamp": datetime.now().isoformat(timespec="seconds"),
    "grand_total": grand_local,
    "reporter_totals": reporter_totals,
    "per_year": local_counts,
}
out_path = AUDIT_DIR / "local_counts_round2.json"
with open(out_path, "w") as f:
    json.dump(local_counts_out, f, indent=2)
print(f"\n✓ Saved local counts → {out_path}")

# ─── Step 2: Load Round 1 PLS per-year counts ─────────────────────────────────
pls_json = AUDIT_DIR / "pls_vs_local_counts.json"
with open(pls_json) as f:
    round1 = json.load(f)

pls_year_counts = round1.get("detailed_counts", {})

# ─── Step 3: Compute gaps per year ────────────────────────────────────────────
year_gaps = []  # list of {reporter, year, pls, local, gap}

for reporter in REPORTERS:
    pls_years = pls_year_counts.get(reporter, {})
    local_years = local_counts.get(reporter, {})

    # Collect all years from PLS side
    for year_str, pls_data in pls_years.items():
        pls_n = pls_data.get("pls", 0) if isinstance(pls_data, dict) else 0
        if pls_n == 0:
            continue
        local_n = local_years.get(year_str, 0)
        gap = pls_n - local_n
        if gap > 0:
            year_gaps.append({
                "reporter": reporter,
                "year": int(year_str),
                "pls": pls_n,
                "local": local_n,
                "gap": gap,
            })

year_gaps.sort(key=lambda x: -x["gap"])

# ─── Step 4: Generate report ──────────────────────────────────────────────────
report_lines = []
add = report_lines.append

add("# PLS AUDIT — ROUND 2")
add(f"**Date:** 2026-02-22  |  **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
add("")
add("---")
add("")
add("## 📊 Overall Coverage")
add("")
add(f"| Metric | Value |")
add(f"|--------|-------|")
add(f"| Local files | **{grand_local:,}** |")
add(f"| PLS target | **{PLS_GRAND_TOTAL:,}** |")
add(f"| Gap | **{PLS_GRAND_TOTAL - grand_local:,}** |")
add(f"| Coverage | **{grand_local / PLS_GRAND_TOTAL * 100:.2f}%** |")
add("")
add("---")
add("")
add("## 📋 Per-Reporter Breakdown")
add("")
add("| Reporter | PLS | Local (R2) | Gap (R2) | Coverage | Local (R1) | Gap (R1) | Improvement |")
add("|----------|-----|------------|----------|----------|------------|----------|-------------|")

reporter_stats = {}
for reporter in REPORTERS:
    pls_n = PLS_TOTALS[reporter]
    local_n = reporter_totals[reporter]
    gap = pls_n - local_n
    cov = local_n / pls_n * 100

    r1_local = ROUND1_LOCAL[reporter]
    r1_gap = pls_n - r1_local
    improvement = local_n - r1_local

    reporter_stats[reporter] = {
        "pls": pls_n, "local": local_n, "gap": gap,
        "coverage_pct": round(cov, 1),
        "r1_local": r1_local, "r1_gap": r1_gap, "improvement": improvement,
    }

    add(f"| {reporter} | {pls_n:,} | {local_n:,} | {gap:,} | {cov:.1f}% | "
        f"{r1_local:,} | {r1_gap:,} | +{improvement:,} |")

# Totals row
r2_total_gap = PLS_GRAND_TOTAL - grand_local
r1_total_gap = PLS_GRAND_TOTAL - ROUND1_GRAND_LOCAL
total_improvement = grand_local - ROUND1_GRAND_LOCAL

add(f"| **TOTAL** | **{PLS_GRAND_TOTAL:,}** | **{grand_local:,}** | **{r2_total_gap:,}** | "
    f"**{grand_local / PLS_GRAND_TOTAL * 100:.1f}%** | **{ROUND1_GRAND_LOCAL:,}** | "
    f"**{r1_total_gap:,}** | **+{total_improvement:,}** |")

add("")
add("---")
add("")
add("## 🔍 Top 20 Biggest Year/Reporter Gaps Remaining")
add("")
add("| # | Reporter | Year | PLS | Local | Gap |")
add("|---|----------|------|-----|-------|-----|")

for i, g in enumerate(year_gaps[:20], 1):
    add(f"| {i} | {g['reporter']} | {g['year']} | {g['pls']:,} | {g['local']:,} | {g['gap']:,} |")

add("")
add("---")
add("")
add("## 📈 Comparison vs Round 1 (2026-02-21)")
add("")
add(f"- **Round 1 local:** {ROUND1_GRAND_LOCAL:,}  /  {PLS_GRAND_TOTAL:,}  ({ROUND1_GRAND_LOCAL/PLS_GRAND_TOTAL*100:.2f}%)")
add(f"- **Round 2 local:** {grand_local:,}  /  {PLS_GRAND_TOTAL:,}  ({grand_local/PLS_GRAND_TOTAL*100:.2f}%)")
add(f"- **Files added since Round 1:** +{total_improvement:,}")
add(f"- **Gap reduced from:** {r1_total_gap:,}  →  {r2_total_gap:,}  (filled {r1_total_gap - r2_total_gap:,})")
add("")
add("### Per-Reporter Progress")
add("")
add("| Reporter | R1 Gap | R2 Gap | Filled |")
add("|----------|--------|--------|--------|")
for reporter in REPORTERS:
    s = reporter_stats[reporter]
    filled = s["r1_gap"] - s["gap"]
    add(f"| {reporter} | {s['r1_gap']:,} | {s['gap']:,} | +{filled:,} |")

add("")
add("---")
add("")
add("## 📝 All Remaining Year Gaps")
add("")
add("| Reporter | Year | PLS | Local | Gap |")
add("|----------|------|-----|-------|-----|")
for g in year_gaps:
    add(f"| {g['reporter']} | {g['year']} | {g['pls']:,} | {g['local']:,} | {g['gap']:,} |")

add("")
add("---")
add("*Generated by audit_round2.py*")

report_text = "\n".join(report_lines)
report_path = AUDIT_DIR / "full_audit_2026-02-22.md"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(report_text)

print(f"✓ Saved report → {report_path}")

# ─── Step 5: Print summary ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("AUDIT ROUND 2 — SUMMARY")
print("=" * 60)
print(f"\n  Coverage: {grand_local:,} / {PLS_GRAND_TOTAL:,} ({grand_local/PLS_GRAND_TOTAL*100:.2f}%)")
print(f"  Remaining gap: {r2_total_gap:,}")
print(f"  Improvement vs Round 1: +{total_improvement:,} files filled")
print(f"  (Gap reduced: {r1_total_gap:,} → {r2_total_gap:,})\n")

print("  Reporter breakdown:")
for reporter in REPORTERS:
    s = reporter_stats[reporter]
    bar_filled = int(s["coverage_pct"] / 5)
    bar = "█" * bar_filled + "░" * (20 - bar_filled)
    print(f"  {reporter:8s} [{bar}] {s['coverage_pct']:5.1f}%  "
          f"({s['local']:,}/{s['pls']:,})  gap={s['gap']:,}  +{s['improvement']:,}")

print(f"\n  Top 5 remaining gaps:")
for g in year_gaps[:5]:
    print(f"    {g['reporter']} {g['year']}: {g['gap']:,} missing ({g['local']:,}/{g['pls']:,})")

print("\n" + "=" * 60)
print("AUDIT COMPLETE")
print("=" * 60)
