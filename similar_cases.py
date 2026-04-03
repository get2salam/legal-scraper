#!/usr/bin/env python3
"""
Similar Cases - Script 3/5
Computes similarity between cases using:
- Jaccard similarity on shared citations
- Same reporter + similar year = bonus
Saves top-10 similar cases for each case with citations.
Saves to data_v2/analytics/similar_cases.json
"""

import json
import time
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
ANALYTICS_DIR = BASE_DIR / "analytics"
CITATION_GRAPH_FILE = ANALYTICS_DIR / "citation_graph.json"
OUTPUT_FILE = ANALYTICS_DIR / "similar_cases.json"

def parse_citation_meta(citation):
    """Extract reporter and year from citation string."""
    parts = citation.split()
    if parts[0] == 'PLD' and len(parts) >= 4:
        try:
            return 'PLD', int(parts[1])
        except ValueError:
            return 'PLD', None
    elif len(parts) >= 3:
        try:
            return parts[1], int(parts[0])
        except ValueError:
            return None, None
    return None, None

def main():
    print("=" * 60)
    print("SIMILAR CASES - Computing Case Similarity")
    print("=" * 60)
    
    print(f"Loading citation graph...")
    with open(CITATION_GRAPH_FILE, 'r', encoding='utf-8') as f:
        citation_graph = json.load(f)
    print(f"  {len(citation_graph):,} cases with citations")
    
    # Build sets for each case
    case_sets = {k: set(v) for k, v in citation_graph.items() if v}
    case_list = list(case_sets.keys())
    print(f"  {len(case_list):,} cases with >0 citations")
    
    # Pre-compute metadata
    case_meta = {c: parse_citation_meta(c) for c in case_list}
    
    # Build inverted index: cited_case -> set of citing_cases
    print("Building inverted index...")
    cited_by = defaultdict(set)
    for citing_case, cited_cases in case_sets.items():
        for cited in cited_cases:
            cited_by[cited].add(citing_case)
    print(f"  {len(cited_by):,} unique cited cases in index")
    
    print("Computing similarities...")
    similar_cases = {}
    processed = 0
    start_time = time.time()
    
    for case_a in case_list:
        processed += 1
        
        if processed % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"  [{processed:>7,}/{len(case_list):,}] "
                  f"{len(similar_cases):,} w/similar | "
                  f"{processed/elapsed:.0f} cases/s | {elapsed:.0f}s")
        
        set_a = case_sets[case_a]
        meta_a = case_meta[case_a]
        
        # Find candidates: cases sharing at least one citation with case_a
        candidates = set()
        for cited in citation_graph[case_a]:
            candidates.update(cited_by.get(cited, set()))
        candidates.discard(case_a)
        
        if not candidates:
            continue
        
        scores = []
        for case_b in candidates:
            if case_b not in case_sets:
                continue
            
            set_b = case_sets[case_b]
            
            # Jaccard similarity
            intersection = len(set_a & set_b)
            union = len(set_a | set_b)
            if union == 0:
                continue
            jaccard = intersection / union
            
            if jaccard == 0:
                continue
            
            # Bonus for same reporter + similar year
            bonus = 0.0
            rep_a, yr_a = meta_a
            rep_b, yr_b = case_meta.get(case_b, (None, None))
            
            if rep_a and rep_b and rep_a == rep_b:
                bonus += 0.05
            if yr_a and yr_b:
                diff = abs(yr_a - yr_b)
                if diff <= 1:
                    bonus += 0.05
                elif diff <= 3:
                    bonus += 0.03
                elif diff <= 5:
                    bonus += 0.01
            
            score = min(jaccard + bonus, 1.0)
            scores.append((case_b, round(score, 4)))
        
        if scores:
            scores.sort(key=lambda x: x[1], reverse=True)
            similar_cases[case_a] = [{"citation": c, "score": s} for c, s in scores[:10]]
    
    elapsed = time.time() - start_time
    
    print(f"\nSaving similar cases ({len(similar_cases):,} entries)...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(similar_cases, f, ensure_ascii=False)
    
    # Stats
    if similar_cases:
        all_scores = [s['score'] for sims in similar_cases.values() for s in sims]
        avg_score = sum(all_scores) / len(all_scores)
        max_score = max(all_scores)
        avg_count = sum(len(v) for v in similar_cases.values()) / len(similar_cases)
    else:
        avg_score = max_score = avg_count = 0
    
    print("\n" + "=" * 60)
    print("SIMILAR CASES - SUMMARY")
    print("=" * 60)
    print(f"Cases analyzed:            {len(case_list):,}")
    print(f"Cases with similar found:  {len(similar_cases):,}")
    print(f"Avg similar per case:      {avg_count:.1f}")
    print(f"Avg similarity score:      {avg_score:.4f}")
    print(f"Max similarity score:      {max_score:.4f}")
    print(f"Time:                      {elapsed:.1f}s")
    print("=" * 60)

if __name__ == '__main__':
    main()
