#!/usr/bin/env python3
"""
Case Statistics Analysis
========================
Generates comprehensive stats from all data_v2/REPORTER/YEAR/*.json files:
- Total cases per year, per reporter
- Average judgment length per year
- Most cited statutes across all cases
- Most cited case laws
- Court distribution (Supreme Court vs High Courts)
- Cases per judge (top 20 most prolific judges)

Saves to:
  - data_v2/audit/2026-02-13_case_statistics.json
  - data_v2/audit/2026-02-13_case_statistics.txt
"""

import os
import re
import json
import sys
import time
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"

REPORTERS = {'SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR'}

# Patterns for extracting citations from judgment text
STATUTE_PATTERNS = [
    re.compile(r'(?:Constitution of Pakistan|Constitution of Islamic Republic of Pakistan)(?:.*?Art(?:icle)?s?\.\s*(\d+))?', re.IGNORECASE),
    re.compile(r'((?:Pakistan )?Penal Code|P\.?P\.?C\.?)', re.IGNORECASE),
    re.compile(r'(Code of Criminal Procedure|Cr\.?P\.?C\.?)', re.IGNORECASE),
    re.compile(r'(Code of Civil Procedure|C\.?P\.?C\.?)', re.IGNORECASE),
    re.compile(r'(Qanun-e-Shahadat|Evidence Act)', re.IGNORECASE),
    re.compile(r'(Control of Narcotic Substances Act)', re.IGNORECASE),
    re.compile(r'(Anti-Terrorism Act)', re.IGNORECASE),
    re.compile(r'(National Accountability (?:Bureau )?Ordinance|NAB Ordinance)', re.IGNORECASE),
    re.compile(r'(West Pakistan Land Revenue Act)', re.IGNORECASE),
    re.compile(r'(Specific Relief Act)', re.IGNORECASE),
    re.compile(r'(Transfer of Property Act)', re.IGNORECASE),
    re.compile(r'(Companies (?:Act|Ordinance))', re.IGNORECASE),
    re.compile(r'(Income Tax Ordinance)', re.IGNORECASE),
    re.compile(r'(Customs Act)', re.IGNORECASE),
    re.compile(r'(Contract Act)', re.IGNORECASE),
    re.compile(r'(Family Courts Act)', re.IGNORECASE),
    re.compile(r'(Muslim Family Laws Ordinance)', re.IGNORECASE),
    re.compile(r'(Limitation Act)', re.IGNORECASE),
    re.compile(r'(Registration Act)', re.IGNORECASE),
    re.compile(r'(Stamp Act)', re.IGNORECASE),
    re.compile(r'(Prevention of Electronic Crimes Act|PECA)', re.IGNORECASE),
    re.compile(r'(West Pakistan (?:Urban|Land) (?:Rent|Revenue|Tenancy) (?:Restriction|Act))', re.IGNORECASE),
    re.compile(r'(Employees(?:\' | )Old-Age Benefits Act)', re.IGNORECASE),
    re.compile(r'(Industrial Relations (?:Act|Ordinance))', re.IGNORECASE),
    re.compile(r'(Motor Vehicles Ordinance)', re.IGNORECASE),
    re.compile(r'(Elections Act)', re.IGNORECASE),
]

# Pattern for case law citations in text
CASELAW_CITATION_PATTERN = re.compile(
    r'\b(\d{4})\s+(SCMR|PLD|MLD|CLC|PCrLJ|PTD|PLC|YLR|CLD|GBLR|PSC|NLR|PLJ|KLR|BLJ|SLR)\s+(\d+)\b'
)

# Court normalization
COURT_MAP = {
    'supreme court': 'Supreme Court of Pakistan',
    'lahore high court': 'Lahore High Court',
    'sindh high court': 'Sindh High Court', 
    'islamabad high court': 'Islamabad High Court',
    'peshawar high court': 'Peshawar High Court',
    'balochistan high court': 'Balochistan High Court',
    'federal shariat court': 'Federal Shariat Court',
}

def normalize_court(court_str):
    """Normalize court name."""
    if not court_str:
        return "Unknown"
    court_lower = court_str.lower().strip()
    for key, val in COURT_MAP.items():
        if key in court_lower:
            return val
    if 'high court' in court_lower:
        return court_str.strip()
    if 'supreme' in court_lower:
        return 'Supreme Court of Pakistan'
    if 'shariat' in court_lower or 'shariah' in court_lower:
        return 'Federal Shariat Court'
    if 'tribunal' in court_lower:
        return court_str.strip()
    return court_str.strip() if court_str.strip() else "Unknown"


def extract_statutes(text):
    """Extract mentioned statutes from judgment text."""
    if not text:
        return []
    found = []
    for pattern in STATUTE_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            # Use the pattern's first group or the full match
            for m in matches:
                if isinstance(m, tuple):
                    m = m[0] if m[0] else pattern.pattern.split('(')[1].split(')')[0][:50]
                if m:
                    found.append(m.strip())
    return found


def extract_cited_cases(text):
    """Extract case law citations from judgment text."""
    if not text:
        return []
    return [f"{m[0]} {m[1]} {m[2]}" for m in CASELAW_CITATION_PATTERN.findall(text)]


def run_stats():
    start = time.time()
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Data structures
    cases_per_year = defaultdict(int)
    cases_per_reporter = defaultdict(int)
    cases_per_reporter_year = defaultdict(lambda: defaultdict(int))
    
    judgment_lengths_per_year = defaultdict(list)
    
    statute_counter = Counter()
    caselaw_counter = Counter()
    court_counter = Counter()
    judge_counter = Counter()
    
    total_cases = 0
    total_with_judgment = 0
    
    print("Scanning case files...")
    
    for reporter_dir in sorted(DATA_DIR.iterdir()):
        if not reporter_dir.is_dir():
            continue
        reporter = reporter_dir.name
        if reporter not in REPORTERS:
            continue
        
        rep_count = 0
        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir() or not re.match(r'^\d{4}$', year_dir.name):
                continue
            
            year = int(year_dir.name)
            json_files = list(year_dir.glob("*.json"))
            
            for jf in json_files:
                try:
                    with open(jf, 'r', encoding='utf-8', errors='replace') as f:
                        data = json.load(f)
                except:
                    continue
                
                total_cases += 1
                rep_count += 1
                cases_per_year[year] += 1
                cases_per_reporter[reporter] += 1
                cases_per_reporter_year[reporter][year] += 1
                
                # Judgment length
                judgment = data.get("judgment_raw", "") or data.get("judgment_clean", "") or ""
                if judgment:
                    total_with_judgment += 1
                    judgment_lengths_per_year[year].append(len(judgment))
                
                # Headnotes + judgment for statute/case extraction
                full_text = (data.get("headnotes", "") or "") + " " + judgment
                
                # Extract statutes (from headnotes which are more structured)
                headnotes = data.get("headnotes", "") or ""
                statutes = extract_statutes(headnotes)
                for s in statutes:
                    # Normalize common abbreviations
                    s_norm = s.strip()
                    if s_norm:
                        statute_counter[s_norm] += 1
                
                # Extract cited case laws
                cited = extract_cited_cases(full_text)
                # Don't count self-citations
                self_citation = data.get("citation", "")
                for c in cited:
                    if c != self_citation:
                        caselaw_counter[c] += 1
                
                # Court
                court = normalize_court(data.get("court", ""))
                court_counter[court] += 1
                
                # Judges
                judges = data.get("judges", [])
                if isinstance(judges, list):
                    for j in judges:
                        if j and isinstance(j, str) and len(j) > 2:
                            judge_counter[j.strip()] += 1
                elif isinstance(judges, str) and judges:
                    # Sometimes comma-separated
                    for j in judges.split(','):
                        j = j.strip()
                        if j and len(j) > 2:
                            judge_counter[j] += 1
        
        print(f"  {reporter}: {rep_count} cases")
        sys.stdout.flush()
    
    # Compute averages
    avg_judgment_length_per_year = {}
    for year in sorted(judgment_lengths_per_year.keys()):
        lengths = judgment_lengths_per_year[year]
        avg_judgment_length_per_year[str(year)] = {
            "count": len(lengths),
            "avg_length": round(sum(lengths) / len(lengths)) if lengths else 0,
            "min_length": min(lengths) if lengths else 0,
            "max_length": max(lengths) if lengths else 0,
            "total_chars": sum(lengths),
        }
    
    elapsed = time.time() - start
    
    # Build report
    report = {
        "generated_at": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "total_cases": total_cases,
        "total_with_judgment": total_with_judgment,
        "cases_per_reporter": dict(cases_per_reporter),
        "cases_per_year": {str(k): v for k, v in sorted(cases_per_year.items())},
        "cases_per_reporter_year": {
            rep: {str(y): c for y, c in sorted(years.items())}
            for rep, years in sorted(cases_per_reporter_year.items())
        },
        "avg_judgment_length_per_year": avg_judgment_length_per_year,
        "top_statutes": dict(statute_counter.most_common(50)),
        "top_cited_cases": dict(caselaw_counter.most_common(50)),
        "court_distribution": dict(court_counter.most_common(30)),
        "top_judges": dict(judge_counter.most_common(30)),
    }
    
    # Save JSON
    json_path = AUDIT_DIR / "2026-02-13_case_statistics.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Generate human-readable summary
    txt_path = AUDIT_DIR / "2026-02-13_case_statistics.txt"
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("PAKISTAN CASE LAW DATABASE — STATISTICS REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")
        
        f.write(f"Total Cases: {total_cases:,}\n")
        f.write(f"Cases with Judgment Text: {total_with_judgment:,}\n\n")
        
        # Reporter breakdown
        f.write("-" * 50 + "\n")
        f.write("CASES PER REPORTER\n")
        f.write("-" * 50 + "\n")
        for rep, count in sorted(cases_per_reporter.items(), key=lambda x: -x[1]):
            f.write(f"  {rep:>8}: {count:>6,} cases\n")
        f.write("\n")
        
        # Year breakdown (summarized by decade)
        f.write("-" * 50 + "\n")
        f.write("CASES PER YEAR\n")
        f.write("-" * 50 + "\n")
        for year in sorted(cases_per_year.keys()):
            f.write(f"  {year}: {cases_per_year[year]:>5,} cases\n")
        f.write("\n")
        
        # Average judgment length trends
        f.write("-" * 50 + "\n")
        f.write("AVERAGE JUDGMENT LENGTH (chars) BY YEAR\n")
        f.write("-" * 50 + "\n")
        for year_str in sorted(avg_judgment_length_per_year.keys()):
            info = avg_judgment_length_per_year[year_str]
            f.write(f"  {year_str}: avg {info['avg_length']:>8,} chars  ({info['count']:>4} cases)\n")
        f.write("\n")
        
        # Court distribution
        f.write("-" * 50 + "\n")
        f.write("COURT DISTRIBUTION\n")
        f.write("-" * 50 + "\n")
        for court, count in court_counter.most_common(20):
            pct = (count / total_cases * 100) if total_cases else 0
            f.write(f"  {court:>45}: {count:>5,} ({pct:.1f}%)\n")
        f.write("\n")
        
        # Top judges
        f.write("-" * 50 + "\n")
        f.write("TOP 20 MOST PROLIFIC JUDGES\n")
        f.write("-" * 50 + "\n")
        for i, (judge, count) in enumerate(judge_counter.most_common(20), 1):
            f.write(f"  {i:>2}. {judge:>40}: {count:>5,} cases\n")
        f.write("\n")
        
        # Top statutes
        f.write("-" * 50 + "\n")
        f.write("TOP 30 MOST CITED STATUTES (from headnotes)\n")
        f.write("-" * 50 + "\n")
        for i, (statute, count) in enumerate(statute_counter.most_common(30), 1):
            f.write(f"  {i:>2}. {statute[:60]:>60}: {count:>5,}\n")
        f.write("\n")
        
        # Top cited cases
        f.write("-" * 50 + "\n")
        f.write("TOP 30 MOST CITED CASE LAWS\n")
        f.write("-" * 50 + "\n")
        for i, (case, count) in enumerate(caselaw_counter.most_common(30), 1):
            f.write(f"  {i:>2}. {case:>25}: cited {count:>4} times\n")
        f.write("\n")
        
        f.write("=" * 70 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 70 + "\n")
    
    # Print summary to stdout
    print(f"\n{'='*60}")
    print(f"CASE STATISTICS — {total_cases:,} total cases")
    print(f"{'='*60}")
    print(f"Cases with judgment: {total_with_judgment:,}")
    print(f"\nBy Reporter:")
    for rep, count in sorted(cases_per_reporter.items(), key=lambda x: -x[1]):
        print(f"  {rep:>8}: {count:>6,}")
    print(f"\nCourts:")
    for court, count in court_counter.most_common(8):
        print(f"  {court}: {count:,}")
    print(f"\nTop 5 Judges:")
    for judge, count in judge_counter.most_common(5):
        print(f"  {judge}: {count:,}")
    print(f"\nElapsed: {elapsed:.1f}s")
    print(f"Reports saved to:")
    print(f"  {json_path}")
    print(f"  {txt_path}")


if __name__ == "__main__":
    run_stats()
