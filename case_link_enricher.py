#!/usr/bin/env python3
"""
Case Link Enricher
==================
Enriches case citations with full details from local case law database.
Creates bidirectional links between statutes and cases.

Features:
- Parses case citations to extract year, reporter, page
- Checks if case exists in local data_v2/{REPORTER}/{YEAR}/
- Extracts case metadata (title, court, date) if available
- Generates local file paths and URLs
- Tracks which cases need to be fetched

Usage:
    from case_link_enricher import enrich_case_links, enrich_statute_case_links
    
    enriched = enrich_case_links(["1983 PLD 176", "2007 SCMR 1872"])
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data directories
DATA_DIR = Path(__file__).parent / "data_v2"
BASE_URL = "https://www.pakistanlawsite.com"

# Valid reporters
REPORTERS = ["PLD", "SCMR", "CLC", "PCrLJ", "MLD", "YLR", "PTD", "PLC", "CLD", "GBLR"]


def parse_citation(citation: str) -> Optional[Dict]:
    """
    Parse a case citation into components.
    
    Args:
        citation: Citation string like "1983 PLD 176" or "2007 SCMR 1872"
        
    Returns:
        Dict with year, reporter, page or None if invalid
    """
    if not citation:
        return None
    
    # Normalize citation
    citation = citation.strip()
    citation = re.sub(r'\s+', ' ', citation)
    citation = citation.rstrip(',').rstrip('.')
    
    # Pattern: YEAR REPORTER PAGE
    pattern = r'^(\d{4})\s+(' + '|'.join(REPORTERS) + r')\s+(\d+)$'
    match = re.match(pattern, citation, re.IGNORECASE)
    
    if match:
        return {
            "year": match.group(1),
            "reporter": match.group(2).upper(),
            "page": match.group(3),
        }
    
    return None


def get_case_local_path(year: str, reporter: str, page: str) -> Path:
    """Generate the local file path for a case."""
    filename = f"{year}_{reporter}_{page}.json"
    return DATA_DIR / reporter / year / filename


def get_case_url(year: str, reporter: str, page: str) -> str:
    """Generate a placeholder URL for the case on PLS."""
    # Note: Actual PLS URLs use casetypeid, not citation
    # This is a placeholder that could be updated with actual URLs
    return f"{BASE_URL}/Login/ReadCaseLaw?citation={year}+{reporter}+{page}"


def load_case_metadata(case_path: Path) -> Optional[Dict]:
    """
    Load case metadata from local JSON file.
    
    Returns:
        Dict with case_title, court, date, judges or None if not found
    """
    if not case_path.exists():
        return None
    
    try:
        data = json.loads(case_path.read_text(encoding='utf-8'))
        return {
            "case_title": data.get("title", data.get("case_name", "")),
            "court": data.get("court", ""),
            "date": data.get("date", ""),
            "judges": data.get("judges", []),
        }
    except Exception as e:
        logger.warning(f"Failed to load case metadata from {case_path}: {e}")
        return None


def enrich_single_case_link(citation: str, section_ref: str = "") -> Dict:
    """
    Enrich a single case citation with full details.
    
    Args:
        citation: Case citation string
        section_ref: Which section references this case (e.g., "Section 2")
        
    Returns:
        Enriched case link dict
    """
    # Start with basic structure
    result = {
        "citation": citation,
        "year": "",
        "reporter": "",
        "page": "",
        "section_ref": section_ref,
        "url": "",
        "local_path": "",
        "exists_locally": False,
        "case_title": "",
        "court": "",
        "date": "",
        "judges": [],
    }
    
    # Parse citation
    parsed = parse_citation(citation)
    if not parsed:
        return result
    
    result["year"] = parsed["year"]
    result["reporter"] = parsed["reporter"]
    result["page"] = parsed["page"]
    
    # Generate paths
    local_path = get_case_local_path(parsed["year"], parsed["reporter"], parsed["page"])
    result["local_path"] = str(local_path.relative_to(DATA_DIR.parent)) if local_path.exists() else str(local_path.relative_to(DATA_DIR.parent))
    result["url"] = get_case_url(parsed["year"], parsed["reporter"], parsed["page"])
    
    # Check if exists locally and load metadata
    if local_path.exists():
        result["exists_locally"] = True
        metadata = load_case_metadata(local_path)
        if metadata:
            result["case_title"] = metadata.get("case_title", "")
            result["court"] = metadata.get("court", "")
            result["date"] = metadata.get("date", "")
            result["judges"] = metadata.get("judges", [])
    
    return result


def enrich_case_links(citations: List[str], section_ref: str = "") -> List[Dict]:
    """
    Enrich a list of case citations with full details.
    
    Args:
        citations: List of citation strings
        section_ref: Which section references these cases
        
    Returns:
        List of enriched case link dicts
    """
    enriched = []
    seen = set()
    
    for citation in citations:
        # Normalize and dedupe
        normalized = re.sub(r'\s+', ' ', citation.strip())
        if normalized in seen:
            continue
        seen.add(normalized)
        
        enriched.append(enrich_single_case_link(normalized, section_ref))
    
    return enriched


def enrich_statute_case_links(statute_data: Dict) -> Dict:
    """
    Enrich all case links in a statute with full details.
    Updates both section-level and statute-level cases_cited.
    
    Args:
        statute_data: Full statute dict
        
    Returns:
        Updated statute dict with enriched case links
    """
    all_enriched_cases = []
    seen_citations = set()
    
    # Process each section's case citations
    sections = statute_data.get("sections", [])
    for section in sections:
        section_num = section.get("number", "")
        section_ref = f"Section {section_num}" if section_num else ""
        
        # Get cases for this section
        section_cases = section.get("cases_cited", [])
        
        # Handle both old format (list of strings) and new format (list of dicts)
        if section_cases and isinstance(section_cases[0], str):
            enriched = enrich_case_links(section_cases, section_ref)
            section["cases_cited"] = enriched
        elif section_cases and isinstance(section_cases[0], dict):
            # Already enriched, but update section_ref if missing
            for case in section_cases:
                if not case.get("section_ref"):
                    case["section_ref"] = section_ref
                # Re-check if exists locally (might have been fetched since)
                if not case.get("exists_locally"):
                    parsed = parse_citation(case.get("citation", ""))
                    if parsed:
                        local_path = get_case_local_path(parsed["year"], parsed["reporter"], parsed["page"])
                        if local_path.exists():
                            case["exists_locally"] = True
                            metadata = load_case_metadata(local_path)
                            if metadata:
                                case.update(metadata)
            enriched = section_cases
        else:
            enriched = []
        
        # Collect for statute-level list
        for case in enriched:
            citation = case.get("citation", "")
            if citation and citation not in seen_citations:
                seen_citations.add(citation)
                all_enriched_cases.append(case.copy())
    
    # Update statute-level cases_cited with all unique cases
    statute_data["cases_cited"] = all_enriched_cases
    
    return statute_data


def get_missing_cases(statute_data: Dict) -> List[Dict]:
    """
    Get list of cases that are cited but don't exist locally.
    
    Args:
        statute_data: Statute dict with enriched cases_cited
        
    Returns:
        List of case dicts with exists_locally=False
    """
    missing = []
    cases = statute_data.get("cases_cited", [])
    
    for case in cases:
        if isinstance(case, dict) and not case.get("exists_locally", False):
            missing.append(case)
    
    return missing


def scan_all_missing_cases(legislation_dir: Path = None) -> List[Dict]:
    """
    Scan all statute JSONs and collect all missing cases.
    
    Returns:
        List of unique missing case dicts
    """
    if legislation_dir is None:
        legislation_dir = DATA_DIR / "legislation"
    
    missing = []
    seen = set()
    
    for letter_dir in sorted(legislation_dir.iterdir()):
        if not letter_dir.is_dir() or letter_dir.name == "original":
            continue
        
        for json_file in letter_dir.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding='utf-8'))
                cases = data.get("cases_cited", [])
                
                for case in cases:
                    if isinstance(case, dict) and not case.get("exists_locally", False):
                        citation = case.get("citation", "")
                        if citation and citation not in seen:
                            seen.add(citation)
                            case["source_statute"] = data.get("title", json_file.stem)
                            missing.append(case)
                            
            except Exception as e:
                logger.warning(f"Error processing {json_file}: {e}")
    
    return missing


def update_case_links_after_fetch(statute_path: Path) -> int:
    """
    Update a statute's case links after cases have been fetched.
    Re-checks exists_locally and loads metadata.
    
    Returns:
        Number of cases updated
    """
    try:
        data = json.loads(statute_path.read_text(encoding='utf-8'))
    except Exception as e:
        logger.error(f"Failed to load {statute_path}: {e}")
        return 0
    
    updated = 0
    
    # Update section-level cases
    for section in data.get("sections", []):
        cases = section.get("cases_cited", [])
        for case in cases:
            if isinstance(case, dict) and not case.get("exists_locally", False):
                parsed = parse_citation(case.get("citation", ""))
                if parsed:
                    local_path = get_case_local_path(parsed["year"], parsed["reporter"], parsed["page"])
                    if local_path.exists():
                        case["exists_locally"] = True
                        case["local_path"] = str(local_path.relative_to(DATA_DIR.parent))
                        metadata = load_case_metadata(local_path)
                        if metadata:
                            case.update(metadata)
                        updated += 1
    
    # Update statute-level cases
    for case in data.get("cases_cited", []):
        if isinstance(case, dict) and not case.get("exists_locally", False):
            parsed = parse_citation(case.get("citation", ""))
            if parsed:
                local_path = get_case_local_path(parsed["year"], parsed["reporter"], parsed["page"])
                if local_path.exists():
                    case["exists_locally"] = True
                    case["local_path"] = str(local_path.relative_to(DATA_DIR.parent))
                    metadata = load_case_metadata(local_path)
                    if metadata:
                        case.update(metadata)
                    updated += 1
    
    if updated > 0:
        statute_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    
    return updated


if __name__ == "__main__":
    # Test the enricher
    print("=== Testing Case Link Enricher ===\n")
    
    test_citations = [
        "1983 PLD 176",
        "2007 SCMR 1872",
        "1986 PLD 29",
        "2024 CLC 100",  # Likely exists
        "1999 XYZ 123",  # Invalid reporter
    ]
    
    for citation in test_citations:
        enriched = enrich_single_case_link(citation, "Section 2")
        print(f"Citation: {citation}")
        print(f"  Parsed: {enriched['year']} {enriched['reporter']} {enriched['page']}")
        print(f"  Exists locally: {enriched['exists_locally']}")
        if enriched['exists_locally']:
            print(f"  Title: {enriched['case_title'][:50]}..." if enriched['case_title'] else "  Title: (none)")
            print(f"  Court: {enriched['court']}")
        print()
    
    # Scan for missing cases
    print("=== Scanning for Missing Cases ===\n")
    missing = scan_all_missing_cases()
    print(f"Found {len(missing)} missing cases across all statutes")
    if missing:
        print("\nFirst 5 missing cases:")
        for case in missing[:5]:
            print(f"  {case['citation']} (cited in {case.get('source_statute', 'unknown')[:50]})")
