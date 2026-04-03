#!/usr/bin/env python3
"""
Case Duration Extractor for Pakistani Legal Cases
Extracts filing dates, decision dates, and calculates case durations
"""

import json
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import statistics

@dataclass
class CaseTimeline:
    """Represents timeline data for a single case"""
    case_id: str
    citation: str
    court: str
    jurisdiction: str
    case_type: str
    filing_date: Optional[str]
    decision_date: Optional[str]
    duration_days: Optional[int]
    filing_source: str  # How filing date was extracted
    extraction_confidence: str  # high/medium/low

# Pakistani date formats
MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

# Court to jurisdiction mapping
COURT_JURISDICTION_MAP = {
    'lahore': 'Punjab',
    'karachi': 'Sindh',
    'peshawar': 'KPK',
    'quetta': 'Balochistan',
    'islamabad': 'Islamabad',
    'federal shariat court': 'Federal',
    'supreme court': 'Federal',
    'supreme court of pakistan': 'Federal',
    'sindh': 'Sindh',
    'punjab': 'Punjab',
    'balochistan': 'Balochistan',
    'kpk': 'KPK',
    'khyber pakhtunkhwa': 'KPK',
    'azad kashmir': 'AJK',
    'gilgit': 'Gilgit-Baltistan'
}

# Case type patterns
CASE_TYPE_PATTERNS = [
    (r'constitutional petition|writ petition|art(?:icle)?\.?\s*199', 'Constitutional'),
    (r'criminal appeal|cr\.?\s*a\.|murder|qatl|theft|robbery|dacoity|kidnapping', 'Criminal'),
    (r'civil appeal|c\.?\s*a\.|civil suit|civil revision', 'Civil'),
    (r'family|divorce|dissolution|maintenance|dower|haq mehr|custody|guardianship', 'Family'),
    (r'tax|income tax|sales tax|customs|excise|tariff', 'Tax'),
    (r'labour|labor|industrial|workmen|employment', 'Labour'),
    (r'service|government servant|civil servant', 'Service'),
    (r'land acquisition|compensation|acquisition', 'Land Acquisition'),
    (r'banking|financial|loan|mortgage|negotiable', 'Banking'),
    (r'company|corporate|winding up|insolvency', 'Corporate'),
    (r'election|electoral|disqualification|art(?:icle)?\.?\s*62|art(?:icle)?\.?\s*63', 'Election'),
    (r'anti.?terrorism|terrorism|ata|sect(?:arian)?', 'Anti-Terrorism'),
    (r'narcotics|drug|cnsa|opium|heroin', 'Narcotics'),
    (r'shariat|islamic|shariah', 'Shariat'),
    (r'appeal|revision|review', 'Appellate'),
]

def parse_ordinal(text: str) -> int:
    """Convert ordinal text to number (1st, 2nd, 3rd, etc.)"""
    text = text.lower().strip()
    # Remove ordinal suffixes
    text = re.sub(r'(st|nd|rd|th)$', '', text)
    try:
        return int(text)
    except:
        return 0

def parse_date_string(date_str: str) -> Optional[datetime]:
    """Parse various Pakistani date formats to datetime"""
    if not date_str:
        return None
    
    date_str = date_str.strip().replace('\r', ' ').replace('\n', ' ')
    date_str = re.sub(r'\s+', ' ', date_str)
    
    # Pattern 1: "14th June, 2023" or "1st January 2024"
    pattern1 = r'(\d{1,2})(?:st|nd|rd|th)?\s*(?:of\s+)?([a-zA-Z]+),?\s*(\d{4})'
    match = re.search(pattern1, date_str, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).lower()
        year = int(match.group(3))
        month = MONTH_MAP.get(month_str)
        if month and 1 <= day <= 31 and 1900 <= year <= 2030:
            try:
                return datetime(year, month, day)
            except:
                pass
    
    # Pattern 2: "18.03.2022" or "18/03/2022" or "18-03-2022"
    pattern2 = r'(\d{1,2})[./-](\d{1,2})[./-](\d{4})'
    match = re.search(pattern2, date_str)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2030:
            try:
                return datetime(year, month, day)
            except:
                pass
    
    # Pattern 3: "2022-03-18" (ISO format)
    pattern3 = r'(\d{4})[./-](\d{1,2})[./-](\d{1,2})'
    match = re.search(pattern3, date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2030:
            try:
                return datetime(year, month, day)
            except:
                pass
    
    return None

def extract_filing_date(text: str, decision_date: Optional[datetime]) -> Tuple[Optional[datetime], str, str]:
    """
    Extract filing date from judgment text.
    Returns: (date, source_description, confidence)
    """
    if not text:
        return None, "", "none"
    
    text_lower = text.lower()
    
    # Priority 1: FIR registration date (Criminal cases) - HIGH confidence
    fir_patterns = [
        r'f\.?i\.?r\.?\s*(?:no\.?)?\s*\d+[/-](\d{4})',
        r'case\s+f\.?i\.?r\.?\s*(?:no\.?)?\s*\d+[/-](\d{4})',
        r'crime\s+(?:no\.?)?\s*\d+[/-](\d{4})',
    ]
    for pattern in fir_patterns:
        match = re.search(pattern, text_lower)
        if match:
            year = int(match.group(1))
            if 1990 <= year <= 2025:
                # FIR year found, assume January 1 of that year as approximation
                return datetime(year, 1, 1), "FIR year", "medium"
    
    # Priority 2: Explicit filing date mentions - HIGH confidence
    filing_patterns = [
        (r'(?:suit|petition|appeal|application)\s+(?:was\s+)?filed\s+on\s+(\d{1,2}[./-]\d{1,2}[./-]\d{4})', 'explicit filing date'),
        (r'(?:suit|petition|appeal)\s+(?:was\s+)?instituted\s+on\s+(\d{1,2}[./-]\d{1,2}[./-]\d{4})', 'institution date'),
        (r'registered\s+(?:on|at)\s+.*?(\d{1,2}[./-]\d{1,2}[./-]\d{4})', 'registration date'),
        (r'filed\s+(?:this|the)\s+(?:suit|petition|appeal)\s+on\s+(\d{1,2}[./-]\d{1,2}[./-]\d{4})', 'filing date'),
    ]
    
    for pattern, source in filing_patterns:
        match = re.search(pattern, text_lower)
        if match:
            date = parse_date_string(match.group(1))
            if date and (not decision_date or date < decision_date):
                return date, source, "high"
    
    # Priority 3: Dated references with filing context - MEDIUM confidence
    dated_patterns = [
        (r'(?:suit|petition|appeal|application)\s+dated\s+(\d{1,2}[./-]\d{1,2}[./-]\d{4})', 'dated document'),
        (r'(?:suit|petition)\s+dated\s+(\d{1,2})(?:st|nd|rd|th)?\s+([a-zA-Z]+),?\s+(\d{4})', 'dated document'),
    ]
    
    for pattern, source in dated_patterns:
        match = re.search(pattern, text_lower)
        if match:
            if len(match.groups()) == 3:
                date_str = f"{match.group(1)} {match.group(2)} {match.group(3)}"
            else:
                date_str = match.group(1)
            date = parse_date_string(date_str)
            if date and (not decision_date or date < decision_date):
                return date, source, "medium"
    
    # Priority 4: First mentioned year in case context - LOW confidence
    year_match = re.search(r'(?:case|suit|petition|appeal)\s+(?:of|no\.?)\s+(\d{4})', text_lower)
    if year_match:
        year = int(year_match.group(1))
        if 1990 <= year <= 2025 and (not decision_date or year <= decision_date.year):
            return datetime(year, 6, 1), "case year reference", "low"
    
    # Priority 5: R.F.A No. XX of YEAR pattern
    rfa_match = re.search(r'r\.?f\.?a\.?\s*(?:no\.?)?\s*\d+\s*(?:of|/)\s*(\d{4})', text_lower)
    if rfa_match:
        year = int(rfa_match.group(1))
        if 1990 <= year <= 2025 and (not decision_date or year <= decision_date.year):
            return datetime(year, 6, 1), "appeal year reference", "low"
    
    return None, "", "none"

def extract_case_type(title: str, headnotes: str, judgment: str) -> str:
    """Determine case type from case content"""
    combined = f"{title} {headnotes} {judgment[:3000]}".lower()
    
    for pattern, case_type in CASE_TYPE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return case_type
    
    return "General"

def extract_court_level(court: str, citation: str) -> str:
    """Determine court level"""
    court_lower = court.lower() if court else ""
    citation_lower = citation.lower() if citation else ""
    
    if 'supreme' in court_lower or 'scmr' in citation_lower:
        return "Supreme Court"
    elif 'shariat' in court_lower:
        return "Federal Shariat Court"
    elif any(x in court_lower for x in ['lahore', 'sindh', 'karachi', 'peshawar', 'quetta', 'islamabad']):
        return "High Court"
    elif 'district' in court_lower or 'session' in court_lower:
        return "District Court"
    elif 'tribunal' in court_lower:
        return "Tribunal"
    
    # Infer from citation reporter
    if 'pld' in citation_lower:
        return "High Court"  # PLD reports major cases
    elif 'clc' in citation_lower:
        return "High Court"
    elif 'ylr' in citation_lower:
        return "High Court"
    elif 'mld' in citation_lower:
        return "High Court"
    elif 'plc' in citation_lower:
        return "High Court"
    elif 'pcrlj' in citation_lower:
        return "High Court"
    elif 'ptd' in citation_lower:
        return "Tribunal"
    
    return "High Court"  # Default

def extract_jurisdiction(court: str) -> str:
    """Determine jurisdiction from court name"""
    court_lower = court.lower() if court else ""
    
    for keyword, jurisdiction in COURT_JURISDICTION_MAP.items():
        if keyword in court_lower:
            return jurisdiction
    
    return "Federal"  # Default

def process_case(case_data: Dict) -> Optional[CaseTimeline]:
    """Process a single case and extract timeline data"""
    citation = case_data.get('citation', '')
    court = case_data.get('court', '')
    date_str = case_data.get('date', '')
    title = case_data.get('title', '')
    headnotes = case_data.get('headnotes', '')
    judgment = case_data.get('judgment_clean', '')
    
    # Parse decision date
    decision_date = parse_date_string(date_str)
    
    # Extract filing date
    full_text = f"{title}\n{headnotes}\n{judgment}"
    filing_date, filing_source, confidence = extract_filing_date(full_text, decision_date)
    
    # Calculate duration
    duration_days = None
    if filing_date and decision_date:
        delta = decision_date - filing_date
        if 0 <= delta.days <= 36500:  # Max 100 years
            duration_days = delta.days
    
    # Extract other metadata
    case_type = extract_case_type(title, headnotes, judgment)
    court_level = extract_court_level(court, citation)
    jurisdiction = extract_jurisdiction(court)
    
    return CaseTimeline(
        case_id=case_data.get('case_name', citation),
        citation=citation,
        court=court_level,
        jurisdiction=jurisdiction,
        case_type=case_type,
        filing_date=filing_date.strftime('%Y-%m-%d') if filing_date else None,
        decision_date=decision_date.strftime('%Y-%m-%d') if decision_date else None,
        duration_days=duration_days,
        filing_source=filing_source,
        extraction_confidence=confidence
    )

def load_all_cases(data_dir: str) -> List[Dict]:
    """Load all case JSON files from data directory"""
    cases = []
    data_path = Path(data_dir)
    
    for json_file in data_path.rglob('*.json'):
        # Skip non-case files
        if json_file.name in ['progress.json', 'manifest.json'] or 'verification' in json_file.name:
            continue
        # Skip backup and other directories
        if 'backup' in str(json_file) or 'original' in str(json_file):
            continue
            
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                case_data = json.load(f)
                cases.append(case_data)
        except Exception as e:
            print(f"Error loading {json_file}: {e}")
    
    return cases

def extract_all_durations(data_dir: str) -> Tuple[List[CaseTimeline], Dict]:
    """
    Extract duration data from all cases.
    Returns: (list of timelines, extraction statistics)
    """
    cases = load_all_cases(data_dir)
    timelines = []
    
    stats = {
        'total_cases': len(cases),
        'extracted_durations': 0,
        'high_confidence': 0,
        'medium_confidence': 0,
        'low_confidence': 0,
        'no_filing_date': 0,
        'by_case_type': defaultdict(int),
        'by_court': defaultdict(int),
        'by_jurisdiction': defaultdict(int),
        'by_filing_source': defaultdict(int),
    }
    
    for case in cases:
        timeline = process_case(case)
        if timeline:
            timelines.append(timeline)
            
            stats['by_case_type'][timeline.case_type] += 1
            stats['by_court'][timeline.court] += 1
            stats['by_jurisdiction'][timeline.jurisdiction] += 1
            
            if timeline.duration_days is not None:
                stats['extracted_durations'] += 1
                stats['by_filing_source'][timeline.filing_source] += 1
                
                if timeline.extraction_confidence == 'high':
                    stats['high_confidence'] += 1
                elif timeline.extraction_confidence == 'medium':
                    stats['medium_confidence'] += 1
                elif timeline.extraction_confidence == 'low':
                    stats['low_confidence'] += 1
            else:
                stats['no_filing_date'] += 1
    
    # Convert defaultdicts to regular dicts for JSON serialization
    stats['by_case_type'] = dict(stats['by_case_type'])
    stats['by_court'] = dict(stats['by_court'])
    stats['by_jurisdiction'] = dict(stats['by_jurisdiction'])
    stats['by_filing_source'] = dict(stats['by_filing_source'])
    
    return timelines, stats

def save_extraction_results(timelines: List[CaseTimeline], stats: Dict, output_dir: str):
    """Save extraction results to files"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save all timelines
    timelines_data = [asdict(t) for t in timelines]
    with open(output_path / 'case_durations.json', 'w', encoding='utf-8') as f:
        json.dump(timelines_data, f, indent=2, ensure_ascii=False)
    
    # Save only cases with extracted durations
    valid_durations = [t for t in timelines_data if t['duration_days'] is not None]
    with open(output_path / 'valid_durations.json', 'w', encoding='utf-8') as f:
        json.dump(valid_durations, f, indent=2, ensure_ascii=False)
    
    # Save extraction statistics
    with open(output_path / 'extraction_stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== Extraction Results ===")
    print(f"Total cases processed: {stats['total_cases']}")
    print(f"Durations extracted: {stats['extracted_durations']} ({100*stats['extracted_durations']/stats['total_cases']:.1f}%)")
    print(f"  - High confidence: {stats['high_confidence']}")
    print(f"  - Medium confidence: {stats['medium_confidence']}")
    print(f"  - Low confidence: {stats['low_confidence']}")
    print(f"No filing date found: {stats['no_filing_date']}")
    print(f"\nBy Case Type:")
    for ct, count in sorted(stats['by_case_type'].items(), key=lambda x: -x[1]):
        print(f"  {ct}: {count}")
    print(f"\nBy Court Level:")
    for court, count in sorted(stats['by_court'].items(), key=lambda x: -x[1]):
        print(f"  {court}: {count}")
    print(f"\nBy Jurisdiction:")
    for jur, count in sorted(stats['by_jurisdiction'].items(), key=lambda x: -x[1]):
        print(f"  {jur}: {count}")
    print(f"\nFiling Date Sources:")
    for src, count in sorted(stats['by_filing_source'].items(), key=lambda x: -x[1]):
        print(f"  {src}: {count}")

def main():
    """Main extraction workflow"""
    # Set paths
    script_dir = Path(__file__).parent
    data_dir = script_dir / 'data_v2'
    output_dir = script_dir / 'timeline_data'
    
    print("Starting case duration extraction...")
    print(f"Data directory: {data_dir}")
    
    # Extract durations
    timelines, stats = extract_all_durations(str(data_dir))
    
    # Save results
    save_extraction_results(timelines, stats, str(output_dir))
    
    print(f"\nResults saved to: {output_dir}")

if __name__ == '__main__':
    main()
