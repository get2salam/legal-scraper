#!/usr/bin/env python3
"""
Case Law Analytics
==================
Analyze scraped Pakistani case law data and generate statistics.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"
CASES_DIR = DATA_DIR / "cases"


def load_all_cases():
    """Load all cases from JSONL files."""
    cases = []
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        cases.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return cases


def extract_acts_cited(text):
    """Extract Pakistani acts/laws cited in judgment text."""
    if not text:
        return []
    
    # Common patterns for Pakistani legislation
    patterns = [
        r"(Constitution of (?:the Islamic Republic of )?Pakistan,?\s*\d{4})",
        r"(Pakistan Penal Code,?\s*\d{4})",
        r"(Code of Criminal Procedure,?\s*\d{4})",
        r"(Code of Civil Procedure,?\s*\d{4})",
        r"(Evidence Act,?\s*\d{4})",
        r"(Contract Act,?\s*\d{4})",
        r"(Companies (?:Act|Ordinance),?\s*\d{4})",
        r"(Income Tax Ordinance,?\s*\d{4})",
        r"(Anti-Terrorism Act,?\s*\d{4})",
        r"(Control of Narcotic Substances Act,?\s*\d{4})",
        r"(National Accountability (?:Bureau )?Ordinance,?\s*\d{4})",
        r"(Prevention of Corruption Act,?\s*\d{4})",
        r"(Specific Relief Act,?\s*\d{4})",
        r"(Limitation Act,?\s*\d{4})",
        r"(Transfer of Property Act,?\s*\d{4})",
        r"(Registration Act,?\s*\d{4})",
        r"(Stamp Act,?\s*\d{4})",
        r"(Land Revenue Act,?\s*\d{4})",
        r"(West Pakistan [\w\s]+ Act,?\s*\d{4})",
        r"(Punjab [\w\s]+ Act,?\s*\d{4})",
        r"(Sindh [\w\s]+ Act,?\s*\d{4})",
        r"((?:Ordinance|Act) (?:No\.?\s*)?\w+ of \d{4})",
    ]
    
    acts = []
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        acts.extend(matches)
    
    return list(set(acts))


def extract_articles_cited(text):
    """Extract constitutional articles cited."""
    if not text:
        return []
    
    # Pattern for Article references
    pattern = r"Article\s+(\d+(?:\s*\(\d+\))?(?:\s*\([a-z]\))?)"
    matches = re.findall(pattern, text, re.IGNORECASE)
    return [f"Article {m}" for m in set(matches)]


def extract_sections_cited(text):
    """Extract section references from acts."""
    if not text:
        return []
    
    # Pattern for Section references
    pattern = r"[Ss]ection\s+(\d+(?:\s*\(\d+\))?(?:\s*\([a-z]\))?)"
    matches = re.findall(pattern, text)
    return [f"Section {m}" for m in matches[:20]]  # Limit to avoid noise


def analyze_cases(cases):
    """Generate comprehensive analytics."""
    stats = {
        "total_cases": len(cases),
        "by_book": Counter(),
        "by_year": Counter(),
        "by_court": Counter(),
        "by_book_year": defaultdict(Counter),
        "avg_judgment_length": 0,
        "acts_cited": Counter(),
        "articles_cited": Counter(),
        "top_judges": Counter(),
    }
    
    total_length = 0
    
    for case in cases:
        book = case.get("book", "Unknown")
        year = case.get("year", "Unknown")
        court = case.get("court", "Unknown")
        judgment = case.get("judgment", case.get("text", ""))
        judges = case.get("judges", "")
        
        stats["by_book"][book] += 1
        stats["by_year"][year] += 1
        stats["by_court"][court] += 1
        stats["by_book_year"][book][year] += 1
        
        if judgment:
            total_length += len(judgment)
            
            # Extract citations
            for act in extract_acts_cited(judgment):
                stats["acts_cited"][act] += 1
            for article in extract_articles_cited(judgment):
                stats["articles_cited"][article] += 1
        
        # Count judges
        if judges:
            if isinstance(judges, list):
                for j in judges:
                    stats["top_judges"][j] += 1
            else:
                for j in judges.split(","):
                    stats["top_judges"][j.strip()] += 1
    
    if cases:
        stats["avg_judgment_length"] = total_length // len(cases)
    
    return stats


def print_report(stats):
    """Print formatted analytics report."""
    print("=" * 70)
    print("  PAKISTAN CASE LAW ANALYTICS REPORT")
    print("  Generated:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)
    
    print(f"\n{'OVERVIEW':=^70}")
    print(f"  Total Cases:              {stats['total_cases']}")
    print(f"  Avg Judgment Length:      {stats['avg_judgment_length']:,} characters")
    
    print(f"\n{'CASES BY LAW REPORT':=^70}")
    for book, count in stats["by_book"].most_common():
        bar = "#" * (count // 2)
        print(f"  {book:12} {count:4}  {bar}")
    
    print(f"\n{'CASES BY YEAR':=^70}")
    for year, count in sorted(stats["by_year"].items(), reverse=True):
        bar = "#" * (count // 2)
        print(f"  {year}:  {count:4}  {bar}")
    
    print(f"\n{'TOP COURTS':=^70}")
    for court, count in stats["by_court"].most_common(10):
        if court and court != "Unknown":
            print(f"  {court[:40]:40} {count:4}")
    
    print(f"\n{'TOP LEGISLATION CITED':=^70}")
    for act, count in stats["acts_cited"].most_common(15):
        print(f"  {act[:50]:50} {count:4}")
    
    print(f"\n{'TOP CONSTITUTIONAL ARTICLES':=^70}")
    for article, count in stats["articles_cited"].most_common(15):
        print(f"  {article:30} {count:4}")
    
    print(f"\n{'TOP JUDGES':=^70}")
    for judge, count in stats["top_judges"].most_common(10):
        if judge and len(judge) > 3:
            print(f"  {judge[:40]:40} {count:4}")
    
    print("\n" + "=" * 70)


def save_report_json(stats, output_path="data/pakistanlawsite/analytics_report.json"):
    """Save analytics as JSON for further processing."""
    # Convert Counters to dicts for JSON serialization
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_cases": stats["total_cases"],
        "avg_judgment_length": stats["avg_judgment_length"],
        "by_book": dict(stats["by_book"]),
        "by_year": {str(k): v for k, v in stats["by_year"].items()},
        "by_court": dict(stats["by_court"].most_common(20)),
        "top_acts_cited": dict(stats["acts_cited"].most_common(30)),
        "top_articles_cited": dict(stats["articles_cited"].most_common(30)),
        "top_judges": dict(stats["top_judges"].most_common(20)),
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\nJSON report saved to: {output_path}")


def main():
    print("Loading cases...")
    cases = load_all_cases()
    
    if not cases:
        print("No cases found! Run the scraper first.")
        return
    
    print(f"Loaded {len(cases)} cases. Analyzing...")
    stats = analyze_cases(cases)
    
    print_report(stats)
    save_report_json(stats)


if __name__ == "__main__":
    main()
