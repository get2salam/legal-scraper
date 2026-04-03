#!/usr/bin/env python3
"""
Citation Extractor - Script 1/5
Extracts legal citations from Pakistani case law judgment texts.
Builds a citation graph and saves to data_v2/analytics/citation_graph.json
"""

import json
import re
import os
import time
import html
from pathlib import Path

BASE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
ANALYTICS_DIR = BASE_DIR / "analytics"
OUTPUT_FILE = ANALYTICS_DIR / "citation_graph.json"

REPORTERS = ['SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR', 'PLCCS']
ALL_REPORTERS = ['SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR', 'NLR', 'PLJ']
PLD_COURTS = ['SC', 'Lahore', 'Karachi', 'Peshawar', 'Quetta', 'Islamabad', 'FSC', 'AJK', 'Sindh', 'Balochistan', 'KPK']

# Pre-compiled regexes
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

# Build all patterns once
NON_PLD = '|'.join(r for r in ALL_REPORTERS if r != 'PLD')
COURTS = '|'.join(re.escape(c) for c in PLD_COURTS)

# Combined pattern for speed - match all citation formats in one pass
# Pattern 1: PLD YYYY Court Page  (including P L D spaced)
_PLD_PAT = re.compile(
    r'P\s*L\s*D\s+(\d{4})\s+(' + COURTS + r')\s+(\d+)\b', re.IGNORECASE)

# Pattern 2: YYYY REPORTER Page
_YEAR_REPORTER_PAT = re.compile(
    r'\b(\d{4})\s+(' + NON_PLD + r')\s+(\d+)\b', re.IGNORECASE)

# Pattern 3: (YYYY) REPORTER Page  
_PAREN_PAT = re.compile(
    r'\((\d{4})\)\s+(' + NON_PLD + r')\s+(\d+)\b', re.IGNORECASE)

# Pattern 4: (YYYY) PLD Court Page
_PAREN_PLD_PAT = re.compile(
    r'\((\d{4})\)\s+P\s*L\s*D\s+(' + COURTS + r')\s+(\d+)\b', re.IGNORECASE)


def normalize_court(court):
    """Normalize PLD court name."""
    court = court.replace('Supreme Court', 'SC').replace('Federal Shariat Court', 'FSC')
    court = court.replace('AJ&K', 'AJK')
    return court

def extract_citations(text):
    """Extract all citations from text. Returns set of normalized citation strings."""
    citations = set()
    
    # PLD format
    for m in _PLD_PAT.finditer(text):
        y = int(m.group(1))
        if 1947 <= y <= 2026:
            court = normalize_court(m.group(2))
            citations.add(f"PLD {m.group(1)} {court} {m.group(3)}")
    
    # Year Reporter Page
    for m in _YEAR_REPORTER_PAT.finditer(text):
        y = int(m.group(1))
        if 1947 <= y <= 2026:
            citations.add(f"{m.group(1)} {m.group(2).upper()} {m.group(3)}")
    
    # (Year) Reporter Page
    for m in _PAREN_PAT.finditer(text):
        y = int(m.group(1))
        if 1947 <= y <= 2026:
            citations.add(f"{m.group(1)} {m.group(2).upper()} {m.group(3)}")
    
    # (Year) PLD Court Page
    for m in _PAREN_PLD_PAT.finditer(text):
        y = int(m.group(1))
        if 1947 <= y <= 2026:
            court = normalize_court(m.group(2))
            citations.add(f"PLD {m.group(1)} {court} {m.group(3)}")
    
    return citations


def main():
    print("=" * 60)
    print("CITATION EXTRACTOR - Building Citation Graph")
    print("=" * 60)
    
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    
    citation_graph = {}
    case_index = {}
    total_files = 0
    total_citations = 0
    cases_with_cit = 0
    errors = 0
    start_time = time.time()
    
    for reporter in REPORTERS:
        reporter_dir = BASE_DIR / reporter
        if not reporter_dir.exists():
            continue
        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            for json_file in sorted(year_dir.glob("*.json")):
                total_files += 1
                
                if total_files % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_files / elapsed if elapsed > 0 else 0
                    print(f"  [{total_files:>7,}] {cases_with_cit:,} w/citations | "
                          f"{total_citations:,} total | {rate:.0f} files/s | {elapsed:.0f}s")
                
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    case_citation = data.get('citation', '').strip()
                    if not case_citation:
                        continue
                    
                    case_index[case_citation] = str(json_file)
                    
                    judgment = data.get('judgment', '')
                    if not judgment or len(judgment) < 100:
                        continue
                    
                    text = strip_html(judgment)
                    if len(text) < 50:
                        continue
                    
                    found = extract_citations(text)
                    found.discard(case_citation)
                    
                    if found:
                        citation_graph[case_citation] = sorted(found)
                        total_citations += len(found)
                        cases_with_cit += 1
                        
                except json.JSONDecodeError:
                    errors += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  WARN: {json_file.name}: {e}")
    
    elapsed = time.time() - start_time
    
    print(f"\nSaving citation graph ({len(citation_graph):,} entries)...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(citation_graph, f, ensure_ascii=False)
    
    index_file = ANALYTICS_DIR / "case_index.json"
    print(f"Saving case index ({len(case_index):,} entries)...")
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(case_index, f, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("CITATION EXTRACTOR - SUMMARY")
    print("=" * 60)
    print(f"Files processed:         {total_files:,}")
    print(f"Cases with citations:    {cases_with_cit:,}")
    print(f"Total citations found:   {total_citations:,}")
    print(f"Avg per citing case:     {total_citations/max(cases_with_cit,1):.1f}")
    print(f"Errors:                  {errors:,}")
    print(f"Time:                    {elapsed:.1f}s ({total_files/elapsed:.0f} files/s)")
    print("=" * 60)

if __name__ == '__main__':
    main()
