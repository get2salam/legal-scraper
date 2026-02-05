#!/usr/bin/env python3
"""
Citation Extractor
==================
Extract case citations from judgment texts to build a citation network.
Identifies which cases cite other cases.
"""

import json
import re
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"


# Pakistani law report citation patterns
CITATION_PATTERNS = [
    # Standard format: YEAR REPORT PAGE (e.g., "2023 PLD 123", "2024 SCMR 456")
    r'(\d{4})\s+(PLD|SCMR|MLD|PCrLJ|CLC|YLR|PTD|CLD|GBLR|PLC|PLJ|NLR|PSC|PCr\.LJ)\s+(\d+)',
    
    # With court: "2023 PLD Lahore 123"
    r'(\d{4})\s+(PLD|SCMR|MLD)\s+(Supreme Court|Lahore|Karachi|Peshawar|Quetta|Islamabad|Federal Shariat Court)\s+(\d+)',
    
    # Abbreviated: "PLD 2023 SC 123"
    r'(PLD|SCMR|MLD|PCrLJ|CLC|YLR)\s+(\d{4})\s+(?:SC|Lah|Kar|Pesh|Quetta|Isl|FSC)?\s*(\d+)',
]


def extract_citations(text):
    """Extract all case citations from text."""
    if not text:
        return []
    
    citations = []
    
    for pattern in CITATION_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if len(match) >= 3:
                # Normalize citation format
                if match[0].isdigit():
                    # Format: YEAR REPORT PAGE
                    year, report, page = match[0], match[1].upper(), match[-1]
                else:
                    # Format: REPORT YEAR PAGE
                    report, year, page = match[0].upper(), match[1], match[-1]
                
                citation = f"{year} {report} {page}"
                if citation not in citations:
                    citations.append(citation)
    
    return citations


def build_citation_network(cases):
    """Build a network of which cases cite which other cases."""
    # Map our case IDs to their citations
    our_citations = {}
    for case in cases:
        case_id = case.get("id", case.get("case_id", ""))
        title = case.get("title", "")
        year = case.get("year", "")
        book = case.get("book", "")
        
        if title:
            # Extract citation from title (e.g., "2025 PLD 123")
            match = re.search(r'(\d{4})\s+(PLD|SCMR|MLD|PCrLJ|CLC|YLR|PTD|CLD)\s+(\d+)', title)
            if match:
                our_citations[f"{match.group(1)} {match.group(2)} {match.group(3)}"] = {
                    "id": case_id,
                    "title": title,
                    "year": year,
                    "book": book
                }
    
    # Build citation network
    network = defaultdict(list)
    citing_count = defaultdict(int)
    cited_count = defaultdict(int)
    
    for case in cases:
        case_id = case.get("id", "")
        case_title = case.get("title", "")
        judgment = case.get("judgment", case.get("text", ""))
        headnotes = case.get("headnotes", "")
        
        # Combine text
        full_text = f"{headnotes}\n{judgment}"
        
        # Extract citations
        citations = extract_citations(full_text)
        
        for cited in citations:
            # Check if this is one of our cases
            if cited in our_citations:
                cited_info = our_citations[cited]
                network[case_id].append({
                    "cited_case": cited,
                    "cited_id": cited_info["id"],
                })
                cited_count[cited] += 1
            else:
                # External citation (case we don't have)
                network[case_id].append({
                    "cited_case": cited,
                    "cited_id": None,
                })
            
            citing_count[case_id] += 1
    
    return network, citing_count, cited_count, our_citations


def analyze_network(network, citing_count, cited_count, our_citations):
    """Analyze the citation network."""
    print("=" * 70)
    print("  CITATION NETWORK ANALYSIS")
    print("=" * 70)
    
    total_citations = sum(len(cites) for cites in network.values())
    cases_with_citations = len([c for c in network.values() if c])
    
    print(f"\n{'OVERVIEW':=^70}")
    print(f"  Total citations found:      {total_citations}")
    print(f"  Cases with citations:       {cases_with_citations}")
    print(f"  Avg citations per case:     {total_citations / max(len(network), 1):.1f}")
    
    # Most citing cases (cases that cite the most other cases)
    print(f"\n{'TOP CITING CASES (cite the most)':=^70}")
    for case_id, count in sorted(citing_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {case_id:30} cites {count:3} cases")
    
    # Most cited cases (cases that are cited the most)
    print(f"\n{'MOST CITED CASES (cited by others)':=^70}")
    for citation, count in sorted(cited_count.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f"  {citation:30} cited {count:3} times")
    
    # Citation by year
    print(f"\n{'CITATIONS BY YEAR':=^70}")
    year_counts = defaultdict(int)
    for case_cites in network.values():
        for cite in case_cites:
            match = re.match(r'(\d{4})', cite.get("cited_case", ""))
            if match:
                year_counts[match.group(1)] += 1
    
    for year in sorted(year_counts.keys(), reverse=True)[:10]:
        bar = "#" * (year_counts[year] // 2)
        print(f"  {year}: {year_counts[year]:4}  {bar}")
    
    print("\n" + "=" * 70)
    
    return {
        "total_citations": total_citations,
        "cases_with_citations": cases_with_citations,
        "most_cited": dict(sorted(cited_count.items(), key=lambda x: x[1], reverse=True)[:20]),
        "citations_by_year": dict(year_counts),
    }


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


def main():
    print("Loading cases...")
    cases = load_all_cases()
    
    if not cases:
        print("No cases found!")
        return
    
    print(f"Loaded {len(cases)} cases. Extracting citations...")
    
    network, citing_count, cited_count, our_citations = build_citation_network(cases)
    stats = analyze_network(network, citing_count, cited_count, our_citations)
    
    # Save network data
    output = {
        "stats": stats,
        "our_cases": list(our_citations.keys()),
        "network": {k: v for k, v in network.items() if v},  # Only cases with citations
    }
    
    output_path = DATA_DIR / "citation_network.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nCitation network saved to: {output_path}")


if __name__ == "__main__":
    main()
