#!/usr/bin/env python3
"""
Section Parser for Pakistani Legislation
Breaks acts into individual sections for better search and retrieval
"""

import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class Section:
    """A section of an act"""
    act_title: str
    act_year: Optional[int]
    section_number: str
    section_title: str
    text: str
    subsections: List[Dict]
    
    def to_dict(self):
        return asdict(self)


@dataclass
class ParsedAct:
    """A fully parsed act"""
    title: str
    short_title: Optional[str]
    act_number: Optional[str]
    year: Optional[int]
    date: Optional[str]
    preamble: Optional[str]
    sections: List[Section]
    schedule: Optional[str]
    source_file: str
    
    def to_dict(self):
        return {
            **{k: v for k, v in asdict(self).items() if k != 'sections'},
            'sections': [s.to_dict() for s in self.sections],
            'section_count': len(self.sections)
        }


class ActParser:
    """Parse Pakistani legislation into structured sections"""
    
    # Common section patterns in Pakistani legislation
    SECTION_PATTERNS = [
        # Pattern: "1. Short title..." or "1.—Short title..."
        r'(?P<num>\d+[A-Z]?)[\.\—\-]\s*(?P<title>[A-Z][^\.]+?)[\.\—]\s*(?P<text>.*?)(?=\n\d+[A-Z]?[\.\—\-]|\nSCHEDULE|\nPART\s+[IVX]+|\Z)',
        
        # Pattern: "Section 1. Short title..."
        r'Section\s+(?P<num>\d+[A-Z]?)[\.\—\-]\s*(?P<title>[A-Z][^\.]+?)[\.\—]\s*(?P<text>.*?)(?=Section\s+\d+|\nSCHEDULE|\Z)',
    ]
    
    # Subsection patterns
    SUBSECTION_PATTERN = r'\((\d+)\)\s*([^(]+?)(?=\(\d+\)|\Z)'
    
    # Extract act metadata
    ACT_NUMBER_PATTERN = r'ACT\s+(?:NO\.?\s*)?([IVXLCDM]+|\d+)\s+OF\s+(\d{4})'
    DATE_PATTERN = r'\[(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})\]'
    
    def __init__(self):
        pass
    
    def extract_metadata(self, text: str) -> Dict:
        """Extract act metadata from text"""
        metadata = {
            'act_number': None,
            'year': None,
            'date': None,
            'short_title': None
        }
        
        # Extract act number
        act_match = re.search(self.ACT_NUMBER_PATTERN, text, re.I)
        if act_match:
            metadata['act_number'] = f"Act {act_match.group(1)} of {act_match.group(2)}"
            metadata['year'] = int(act_match.group(2))
        
        # Extract date
        date_match = re.search(self.DATE_PATTERN, text)
        if date_match:
            metadata['date'] = date_match.group(1)
        
        # Try to extract short title from first section
        short_title_match = re.search(
            r'(?:may be called|shall be called)\s+the\s+([^\.]+)',
            text, re.I
        )
        if short_title_match:
            metadata['short_title'] = short_title_match.group(1).strip()
        
        return metadata
    
    def extract_preamble(self, text: str) -> Optional[str]:
        """Extract the preamble (WHEREAS clauses)"""
        preamble_match = re.search(
            r'(WHEREAS.*?)(?=It is hereby enacted|Be it enacted)',
            text, re.S | re.I
        )
        if preamble_match:
            return preamble_match.group(1).strip()
        return None
    
    def extract_schedule(self, text: str) -> Optional[str]:
        """Extract schedule section"""
        schedule_match = re.search(
            r'SCHEDULE\s*(.*?)$',
            text, re.S | re.I
        )
        if schedule_match:
            return schedule_match.group(1).strip()
        return None
    
    def parse_subsections(self, text: str) -> List[Dict]:
        """Parse subsections from section text"""
        subsections = []
        matches = re.findall(self.SUBSECTION_PATTERN, text, re.S)
        for num, content in matches:
            subsections.append({
                'number': num,
                'text': content.strip()
            })
        return subsections
    
    def parse_sections(self, text: str, act_title: str, act_year: Optional[int]) -> List[Section]:
        """Parse individual sections from act text"""
        sections = []
        
        # Try each pattern
        for pattern in self.SECTION_PATTERNS:
            matches = list(re.finditer(pattern, text, re.S | re.I))
            if matches:
                for match in matches:
                    section_num = match.group('num')
                    section_title = match.group('title').strip()
                    section_text = match.group('text').strip()
                    
                    # Parse subsections
                    subsections = self.parse_subsections(section_text)
                    
                    section = Section(
                        act_title=act_title,
                        act_year=act_year,
                        section_number=section_num,
                        section_title=section_title,
                        text=section_text,
                        subsections=subsections
                    )
                    sections.append(section)
                
                break  # Use first pattern that matches
        
        return sections
    
    def parse(self, text: str, title: str, source_file: str = "") -> ParsedAct:
        """Parse a complete act"""
        
        # Extract metadata
        metadata = self.extract_metadata(text)
        
        # Extract preamble
        preamble = self.extract_preamble(text)
        
        # Extract schedule
        schedule = self.extract_schedule(text)
        
        # Parse sections
        sections = self.parse_sections(
            text, 
            title, 
            metadata.get('year')
        )
        
        return ParsedAct(
            title=title,
            short_title=metadata.get('short_title'),
            act_number=metadata.get('act_number'),
            year=metadata.get('year'),
            date=metadata.get('date'),
            preamble=preamble,
            sections=sections,
            schedule=schedule,
            source_file=source_file
        )


def parse_extracted_texts(input_file: str, output_file: str):
    """Parse all extracted texts and save structured data"""
    
    input_path = Path(input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    with open(input_path, encoding='utf-8') as f:
        extracted = json.load(f)
    
    parser = ActParser()
    parsed_acts = []
    total_sections = 0
    
    for item in extracted:
        if not item.get('text'):
            continue
        
        title = item.get('file', 'Unknown').replace('.pdf', '').replace('_', ' ')
        
        try:
            parsed = parser.parse(
                text=item['text'],
                title=title,
                source_file=item.get('file', '')
            )
            parsed_acts.append(parsed.to_dict())
            total_sections += len(parsed.sections)
            
            logger.info(f"Parsed {title}: {len(parsed.sections)} sections")
            
        except Exception as e:
            logger.warning(f"Error parsing {title}: {e}")
    
    # Save results
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(parsed_acts, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nParsed {len(parsed_acts)} acts with {total_sections} total sections")
    logger.info(f"Saved to {output_file}")
    
    # Print summary
    print("\n" + "="*50)
    print("PARSING SUMMARY")
    print("="*50)
    for act in parsed_acts:
        print(f"\n{act['title'][:50]}...")
        print(f"  Year: {act.get('year', 'Unknown')}")
        print(f"  Sections: {act['section_count']}")
        if act.get('short_title'):
            print(f"  Short title: {act['short_title']}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse Pakistani legislation into sections')
    parser.add_argument('--input', default='data/processed/extracted_texts.json',
                       help='Input file with extracted texts')
    parser.add_argument('--output', default='data/processed/parsed_acts.json',
                       help='Output file for parsed acts')
    
    args = parser.parse_args()
    parse_extracted_texts(args.input, args.output)


if __name__ == '__main__':
    main()
