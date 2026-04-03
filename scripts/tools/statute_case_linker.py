#!/usr/bin/env python3
"""
Statute Case Linker
===================
Extracts and processes case links from scraped statute files.
Builds bidirectional mappings between statutes and cases.

Outputs:
- statute_case_links.jsonl: All statute -> case links
- case_statute_links.jsonl: Reverse mapping (case -> statutes)
- link_statistics.json: Summary statistics
"""

import os
import re
import json
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
OUTPUT_DIR = DATA_DIR
CASE_DATA_DIR = Path(__file__).parent / "data_v2"  # Case law data directory

STATUTE_CASE_FILE = OUTPUT_DIR / "statute_case_links.jsonl"
CASE_STATUTE_FILE = OUTPUT_DIR / "case_statute_links.jsonl"
STATS_FILE = OUTPUT_DIR / "link_statistics.json"

# Citation patterns
REPORTERS = ["PLD", "SCMR", "CLC", "PCrLJ", "MLD", "YLR", "PTD", "PLC", "CLD", "GBLR"]
CITATION_PATTERN = re.compile(
    r'(\d{4})\s+(' + '|'.join(REPORTERS) + r')\s+(\d+)',
    re.IGNORECASE
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Link Extraction
# ══════════════════════════════════════════════════════════════════════════════

class StatuteCaseLinker:
    """Extracts and processes statute-case links."""
    
    def __init__(self):
        self.statute_to_cases: Dict[str, Set[str]] = defaultdict(set)
        self.case_to_statutes: Dict[str, Set[str]] = defaultdict(set)
        self.statute_metadata: Dict[str, Dict] = {}
        self.statistics = {
            "total_statutes": 0,
            "statutes_with_cases": 0,
            "total_case_links": 0,
            "unique_cases": 0,
            "cases_by_reporter": defaultdict(int),
            "cases_by_year": defaultdict(int),
            "top_statutes_by_cases": [],
            "processed_at": None,
        }
    
    def extract_citations_from_text(self, text: str) -> List[Dict]:
        """Extract all case citations from text."""
        citations = []
        for match in CITATION_PATTERN.finditer(text):
            year, reporter, page = match.groups()
            citation = f"{year} {reporter.upper()} {page}"
            citations.append({
                "citation": citation,
                "year": year,
                "reporter": reporter.upper(),
                "page": page,
            })
        return citations
    
    def process_statute_file(self, json_path: Path) -> Dict:
        """Process a single statute JSON file."""
        try:
            data = json.loads(json_path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning(f"Failed to read {json_path}: {e}")
            return None
        
        statute_id = data.get("id", "")
        statute_title = data.get("title", "")
        
        if not statute_title:
            return None
        
        # Store metadata
        self.statute_metadata[statute_id] = {
            "title": statute_title,
            "alphabet": data.get("alphabet", ""),
            "jurisdiction": data.get("jurisdiction", ""),
            "enactment_date": data.get("enactment_date", ""),
        }
        
        all_citations = []
        
        # Extract from case_links field
        for link in data.get("case_links", []):
            if isinstance(link, dict):
                citation = link.get("citation", "")
                if citation:
                    all_citations.append({
                        **link,
                        "source": "case_links",
                    })
        
        # Extract from sections
        for section in data.get("sections", []):
            section_num = section.get("number", "") if isinstance(section, dict) else ""
            section_text = section.get("text", "") if isinstance(section, dict) else ""
            section_case_links = section.get("case_links", []) if isinstance(section, dict) else []
            
            # From section case_links
            for link in section_case_links:
                if isinstance(link, dict):
                    citation = link.get("citation", "")
                    if citation:
                        all_citations.append({
                            **link,
                            "section": section_num,
                            "source": "section_links",
                        })
            
            # From section text (additional extraction)
            if section_text:
                text_citations = self.extract_citations_from_text(section_text)
                for tc in text_citations:
                    all_citations.append({
                        **tc,
                        "section": section_num,
                        "source": "section_text",
                    })
        
        # From full text
        full_text = data.get("full_text", "")
        if full_text:
            text_citations = self.extract_citations_from_text(full_text)
            for tc in text_citations:
                tc["source"] = "full_text"
                all_citations.append(tc)
        
        # Deduplicate by citation
        seen = set()
        unique_citations = []
        for c in all_citations:
            key = c.get("citation", "")
            if key and key not in seen:
                seen.add(key)
                unique_citations.append(c)
        
        return {
            "statute_id": statute_id,
            "statute_title": statute_title,
            "citations": unique_citations,
        }
    
    def process_all_statutes(self):
        """Process all statute files in the data directory."""
        logger.info("Processing all statute files...")
        
        # Find all JSON files
        json_files = list(DATA_DIR.glob("*/*.json"))
        json_files = [f for f in json_files if f.name != "progress.json" 
                      and "index" not in f.name.lower()
                      and "statistics" not in f.name.lower()]
        
        logger.info(f"Found {len(json_files)} statute files")
        
        for i, json_path in enumerate(json_files):
            result = self.process_statute_file(json_path)
            
            if result and result["citations"]:
                statute_id = result["statute_id"]
                statute_title = result["statute_title"]
                
                for citation_info in result["citations"]:
                    citation = citation_info.get("citation", "")
                    if citation:
                        self.statute_to_cases[statute_title].add(citation)
                        self.case_to_statutes[citation].add(statute_title)
                        
                        # Update statistics
                        reporter = citation_info.get("reporter", "")
                        year = citation_info.get("year", "")
                        if reporter:
                            self.statistics["cases_by_reporter"][reporter] += 1
                        if year:
                            self.statistics["cases_by_year"][year] += 1
            
            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i + 1}/{len(json_files)} files")
        
        # Update statistics
        self.statistics["total_statutes"] = len(json_files)
        self.statistics["statutes_with_cases"] = len(self.statute_to_cases)
        self.statistics["total_case_links"] = sum(
            len(cases) for cases in self.statute_to_cases.values()
        )
        self.statistics["unique_cases"] = len(self.case_to_statutes)
        self.statistics["processed_at"] = datetime.now().isoformat()
        
        # Top statutes by case count
        top_statutes = sorted(
            self.statute_to_cases.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )[:20]
        self.statistics["top_statutes_by_cases"] = [
            {"statute": s, "case_count": len(c)} for s, c in top_statutes
        ]
        
        logger.info(f"Processed {len(json_files)} statutes")
        logger.info(f"Found {self.statistics['unique_cases']} unique cases")
    
    def save_outputs(self):
        """Save all output files."""
        logger.info("Saving output files...")
        
        # 1. Statute -> Cases JSONL
        with open(STATUTE_CASE_FILE, 'w', encoding='utf-8') as f:
            for statute, cases in sorted(self.statute_to_cases.items()):
                entry = {
                    "statute_title": statute,
                    "statute_id": self.statute_metadata.get(
                        next((k for k, v in self.statute_metadata.items() 
                              if v.get("title") == statute), ""),
                        {}
                    ).get("title", ""),
                    "case_count": len(cases),
                    "cases": sorted(list(cases)),
                }
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        logger.info(f"Saved statute->cases to {STATUTE_CASE_FILE}")
        
        # 2. Case -> Statutes JSONL
        with open(CASE_STATUTE_FILE, 'w', encoding='utf-8') as f:
            for case, statutes in sorted(self.case_to_statutes.items()):
                # Parse citation
                match = CITATION_PATTERN.match(case)
                entry = {
                    "citation": case,
                    "year": match.group(1) if match else "",
                    "reporter": match.group(2) if match else "",
                    "page": match.group(3) if match else "",
                    "statute_count": len(statutes),
                    "statutes": sorted(list(statutes)),
                }
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        logger.info(f"Saved case->statutes to {CASE_STATUTE_FILE}")
        
        # 3. Statistics JSON
        stats_output = {
            **self.statistics,
            "cases_by_reporter": dict(self.statistics["cases_by_reporter"]),
            "cases_by_year": dict(sorted(
                self.statistics["cases_by_year"].items(),
                key=lambda x: x[0],
                reverse=True
            )[:30]),  # Last 30 years
        }
        STATS_FILE.write_text(json.dumps(stats_output, indent=2, ensure_ascii=False))
        logger.info(f"Saved statistics to {STATS_FILE}")
    
    def match_with_case_database(self):
        """Match citations with our scraped case database."""
        logger.info("Matching with case database...")
        
        # Find all case files
        case_files = list(CASE_DATA_DIR.glob("*/*/*.json"))
        case_files = [f for f in case_files if "legislation" not in str(f)]
        
        existing_cases = set()
        for cf in case_files:
            try:
                data = json.loads(cf.read_text(encoding='utf-8'))
                citation = data.get("citation", "")
                if citation:
                    existing_cases.add(citation)
            except:
                pass
        
        logger.info(f"Found {len(existing_cases)} cases in database")
        
        # Check matches
        matched = 0
        unmatched = []
        for citation in self.case_to_statutes.keys():
            if citation in existing_cases:
                matched += 1
            else:
                unmatched.append(citation)
        
        logger.info(f"Matched: {matched}/{len(self.case_to_statutes)} cases")
        
        # Save unmatched for potential scraping
        if unmatched:
            unmatched_file = OUTPUT_DIR / "unmatched_cases.json"
            unmatched_file.write_text(json.dumps(unmatched[:1000], indent=2))
            logger.info(f"Saved {len(unmatched)} unmatched cases to {unmatched_file}")
        
        return matched, unmatched
    
    def run(self, match_cases: bool = True):
        """Run the full linking process."""
        self.process_all_statutes()
        self.save_outputs()
        
        if match_cases:
            self.match_with_case_database()
        
        # Print summary
        print("\n" + "=" * 60)
        print("Statute-Case Linking Summary")
        print("=" * 60)
        print(f"Total statutes processed: {self.statistics['total_statutes']}")
        print(f"Statutes with case links: {self.statistics['statutes_with_cases']}")
        print(f"Total case links: {self.statistics['total_case_links']}")
        print(f"Unique cases cited: {self.statistics['unique_cases']}")
        print(f"\nTop reporters:")
        for reporter, count in sorted(
            self.statistics['cases_by_reporter'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]:
            print(f"  {reporter}: {count} citations")
        print(f"\nTop statutes by case count:")
        for item in self.statistics['top_statutes_by_cases'][:5]:
            print(f"  {item['statute'][:50]}: {item['case_count']} cases")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Statute Case Linker")
    parser.add_argument("--no-match", action="store_true",
                        help="Skip matching with case database")
    
    args = parser.parse_args()
    
    linker = StatuteCaseLinker()
    linker.run(match_cases=not args.no_match)
