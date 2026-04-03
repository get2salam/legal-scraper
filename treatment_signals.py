#!/usr/bin/env python3
"""
Treatment Signals - Script 2/5
For each citation in the graph, goes back to the citing case's judgment text,
finds the citation mention, extracts ±200 chars context, and classifies.
Saves to data_v2/analytics/treatment_signals.json
"""

import json
import re
import os
import time
import html
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
ANALYTICS_DIR = BASE_DIR / "analytics"
CITATION_GRAPH_FILE = ANALYTICS_DIR / "citation_graph.json"
CASE_INDEX_FILE = ANALYTICS_DIR / "case_index.json"
OUTPUT_FILE = ANALYTICS_DIR / "treatment_signals.json"

# Pre-compiled regexes for HTML stripping
_TAG_RE = re.compile(r'<[^>]+>')
_WS_RE = re.compile(r'\s+')
_UNICODE_RE = re.compile(r'\\u([0-9a-fA-F]{4})')

def _replace_unicode(m):
    return chr(int(m.group(1), 16))

def strip_html(text):
    """Fast HTML stripping."""
    if not text:
        return ""
    if isinstance(text, str) and text.startswith('"'):
        try:
            text = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            pass
    if '\\u' in text:
        text = _UNICODE_RE.sub(_replace_unicode, text)
    text = text.replace('\\r\\n', ' ').replace('\\n', ' ').replace('\\r', ' ')
    text = text.replace('\\t', ' ').replace("\\'", "'").replace('\\"', '"')
    text = _TAG_RE.sub(' ', text)
    text = html.unescape(text)
    text = _WS_RE.sub(' ', text)
    return text

# Signal classification keywords
RED_KEYWORDS = [
    'overruled', 'reversed', 'set aside', 'no longer good law',
    'disapproved', 'not followed', 'overrule', 'reverse',
    'setting aside', 'set-aside', 'over-ruled', 'over ruled',
    'was reversed', 'has been overruled', 'been reversed',
    'no longer applicable', 'bad law'
]

YELLOW_KEYWORDS = [
    'distinguished', 'questioned', 'doubted', 'criticized',
    'qualified', 'limited', 'distinguish', 'not directly applicable',
    'distinguishable', 'inapplicable', 'not relevant',
    'misplaced reliance', 'per incuriam', 'obiter'
]

GREEN_KEYWORDS = [
    'followed', 'affirmed', 'approved', 'applied', 'upheld',
    'relied upon', 'reaffirmed', 'relied on', 'relying upon',
    'relying on', 'following', 'affirming', 'applying',
    'upholding', 'maintained', 'confirmed', 'endorsed',
    'placed reliance', 'reliance was placed', 'lends support'
]

def classify_context(context_lower):
    """Classify the treatment signal from lowered context text."""
    for kw in RED_KEYWORDS:
        if kw in context_lower:
            return 'RED', kw
    for kw in YELLOW_KEYWORDS:
        if kw in context_lower:
            return 'YELLOW', kw
    for kw in GREEN_KEYWORDS:
        if kw in context_lower:
            return 'GREEN', kw
    return 'NEUTRAL', None

def find_citation_in_text(text, citation, context_chars=200):
    """Find citation in text, return list of context windows."""
    contexts = []
    # Direct search
    idx = 0
    cit_lower = citation.lower()
    text_lower = text.lower()
    while True:
        pos = text_lower.find(cit_lower, idx)
        if pos == -1:
            break
        start = max(0, pos - context_chars)
        end = min(len(text), pos + len(citation) + context_chars)
        contexts.append(text[start:end])
        idx = pos + len(citation)
    
    # For PLD, try spaced: "P L D"
    if not contexts and citation.startswith('PLD '):
        spaced_cit = citation.replace('PLD', 'P L D', 1).lower()
        idx = 0
        while True:
            pos = text_lower.find(spaced_cit, idx)
            if pos == -1:
                break
            start = max(0, pos - context_chars)
            end = min(len(text), pos + len(spaced_cit) + context_chars)
            contexts.append(text[start:end])
            idx = pos + len(spaced_cit)
    
    # For SCMR etc, try spaced: "S C M R"
    if not contexts:
        parts = citation.split()
        if len(parts) == 3:
            year, reporter, page = parts
            spaced_reporter = ' '.join(list(reporter))
            spaced_cit = f"{year} {spaced_reporter} {page}".lower()
            idx = 0
            while True:
                pos = text_lower.find(spaced_cit, idx)
                if pos == -1:
                    break
                start = max(0, pos - context_chars)
                end = min(len(text), pos + len(spaced_cit) + context_chars)
                contexts.append(text[start:end])
                idx = pos + len(spaced_cit)
    
    return contexts

def main():
    print("=" * 60)
    print("TREATMENT SIGNALS - Classifying Citation Treatments")
    print("=" * 60)
    
    print(f"Loading citation graph...")
    with open(CITATION_GRAPH_FILE, 'r', encoding='utf-8') as f:
        citation_graph = json.load(f)
    print(f"  {len(citation_graph):,} citing cases loaded")
    
    print(f"Loading case index...")
    with open(CASE_INDEX_FILE, 'r', encoding='utf-8') as f:
        case_index = json.load(f)
    print(f"  {len(case_index):,} case paths loaded")
    
    total_pairs = sum(len(v) for v in citation_graph.values())
    print(f"  {total_pairs:,} citation pairs to analyze")
    
    # Build treatments: cited_case -> list of {citing_case, signal, keyword, ...}
    treatments = defaultdict(list)
    signal_counts = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
    processed_cases = 0
    processed_pairs = 0
    errors = 0
    text_cache = {}  # Small LRU-like cache
    
    start_time = time.time()
    
    for citing_case, cited_list in citation_graph.items():
        processed_cases += 1
        
        if processed_cases % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"  [{processed_cases:>7,}/{len(citation_graph):,}] "
                  f"{processed_pairs:,} pairs | "
                  f"R:{signal_counts['RED']} Y:{signal_counts['YELLOW']} "
                  f"G:{signal_counts['GREEN']} N:{signal_counts['NEUTRAL']} | "
                  f"{elapsed:.0f}s")
        
        # Load citing case judgment
        filepath = case_index.get(citing_case)
        if not filepath or not os.path.exists(filepath):
            processed_pairs += len(cited_list)
            errors += 1
            continue
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            judgment = strip_html(data.get('judgment', ''))
            citing_court = data.get('court', '')
            citing_year = data.get('year', 0)
            
            if not judgment:
                processed_pairs += len(cited_list)
                continue
            
            for cited_case in cited_list:
                processed_pairs += 1
                
                contexts = find_citation_in_text(judgment, cited_case)
                
                best_signal = 'NEUTRAL'
                best_keyword = None
                best_context = ''
                
                if contexts:
                    priority = {'RED': 3, 'YELLOW': 2, 'GREEN': 1, 'NEUTRAL': 0}
                    for ctx in contexts:
                        signal, keyword = classify_context(ctx.lower())
                        if priority.get(signal, 0) > priority.get(best_signal, 0):
                            best_signal = signal
                            best_keyword = keyword
                            best_context = ctx[:150]
                
                signal_counts[best_signal] += 1
                treatments[cited_case].append({
                    'citing_case': citing_case,
                    'signal': best_signal,
                    'keyword': best_keyword,
                    'context': best_context,
                    'citing_court': citing_court,
                    'citing_year': citing_year
                })
                
        except Exception as e:
            errors += 1
            processed_pairs += len(cited_list)
            if errors <= 5:
                print(f"  WARN: {citing_case}: {e}")
    
    elapsed = time.time() - start_time
    
    print(f"\nSaving treatment signals ({len(treatments):,} cited cases)...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(dict(treatments), f, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("TREATMENT SIGNALS - SUMMARY")
    print("=" * 60)
    print(f"Citing cases analyzed:     {processed_cases:,}")
    print(f"Citation pairs processed:  {processed_pairs:,}")
    print(f"Unique cited cases:        {len(treatments):,}")
    print(f"Signal breakdown:")
    print(f"  RED (negative):          {signal_counts['RED']:,}")
    print(f"  YELLOW (cautionary):     {signal_counts['YELLOW']:,}")
    print(f"  GREEN (positive):        {signal_counts['GREEN']:,}")
    print(f"  NEUTRAL:                 {signal_counts['NEUTRAL']:,}")
    print(f"Errors:                    {errors:,}")
    print(f"Time:                      {elapsed:.1f}s")
    print("=" * 60)

if __name__ == '__main__':
    main()
