"""
Judge Name Extractor for Pakistani Case Law
============================================
Extracts judge names from case files with role identification.
Handles both pre-extracted judges and text-based extraction.
"""

import re
import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum


class JudgeRole(str, Enum):
    """Role of a judge in a case."""
    AUTHOR = "author"           # Judge who wrote the judgment
    BENCH = "bench"             # Member of the bench
    CHIEF = "chief"             # Chief Justice
    UNKNOWN = "unknown"         # Role not determined


@dataclass
class JudgeInfo:
    """Information about a judge in a case."""
    name: str
    raw_name: str               # Original name as found
    role: JudgeRole = JudgeRole.UNKNOWN
    title: Optional[str] = None  # J., CJ, ACJ, etc.
    
    def to_dict(self):
        return {
            "name": self.name,
            "raw_name": self.raw_name,
            "role": self.role.value,
            "title": self.title
        }


@dataclass
class CaseJudges:
    """Extracted judge information for a case."""
    citation: str
    court: str
    date: str
    judges: list = field(default_factory=list)
    extraction_source: str = "metadata"  # metadata, text, or both
    
    def to_dict(self):
        return {
            "citation": self.citation,
            "court": self.court,
            "date": self.date,
            "judges": [j.to_dict() for j in self.judges],
            "extraction_source": self.extraction_source
        }


class JudgeExtractor:
    """
    Extracts and normalizes judge names from Pakistani case law.
    
    Patterns handled:
    - "Mr. Justice [Name]" / "Mrs. Justice [Name]"
    - "[Name], J." / "[Name], CJ" / "[Name], ACJ"
    - "Before: ..." / "Coram: ..." / "Present: ..."
    - "The Hon'ble Mr. Justice [Name]"
    - Multi-judge benches: "Mr. Justice X and Mr. Justice Y"
    """
    
    # Patterns for extracting judges from judgment text
    JUDGE_PATTERNS = [
        # Pattern: "JUDGE_NAME, J.---" at start of judgment (author)
        r'^([A-Z][A-Z\s\.\-]+),\s*J\.?\s*[-—]+',
        # Pattern: "Before: Mr. Justice Name" or "Before: Name, J."
        r'Before[:\s]+(?:Mr\.|Mrs\.|Ms\.)?(?:\s*The\s+Hon\'?ble)?(?:\s*Mr\.|Mrs\.|Ms\.)?\s*(?:Justice\s+)?([A-Za-z\s\.\-\']+?)(?:,\s*(?:C\.?J\.?|A\.?C\.?J\.?|J\.?))?(?:\s+and|\s*$)',
        # Pattern: "Coram: Name, J." or just "Coram: Name"
        r'Coram[:\s]+(?:Mr\.|Mrs\.|Ms\.)?(?:\s*The\s+Hon\'?ble)?(?:\s*Mr\.|Mrs\.|Ms\.)?\s*(?:Justice\s+)?([A-Za-z\s\.\-\']+?)(?:,\s*(?:C\.?J\.?|A\.?C\.?J\.?|J\.?))?(?:\s+and|\s*,|\s*$)',
        # Pattern: "Present: The Hon'ble Mr. Justice Name"
        r'Present[:\s]+(?:The\s+Hon\'?ble\s+)?(?:Mr\.|Mrs\.|Ms\.)?\s*(?:Justice\s+)?([A-Za-z\s\.\-\']+?)(?:,\s*(?:C\.?J\.?|A\.?C\.?J\.?|J\.?))?(?:\s+and|\s*$)',
        # Pattern: Justice Name (anywhere, less specific)
        r'(?:Mr\.|Mrs\.|Ms\.)?\s*Justice\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
    ]
    
    # Pattern for judgment author (usually at start with "NAME, J.---")
    AUTHOR_PATTERN = re.compile(
        r'^(?:JUDGMENT\s*\n)?([A-Z][A-Z\s\.\-\']+?),?\s*(C\.?J\.?|A\.?C\.?J\.?|J\.?)\s*[-—]+',
        re.MULTILINE | re.IGNORECASE
    )
    
    # Pattern for multi-judge extraction
    MULTI_JUDGE_PATTERN = re.compile(
        r'(?:Mr\.|Mrs\.|Ms\.)?\s*(?:Justice\s+)?([A-Za-z]+(?:\s+[A-Za-z]+)+)\s+and\s+(?:Mr\.|Mrs\.|Ms\.)?\s*(?:Justice\s+)?([A-Za-z]+(?:\s+[A-Za-z]+)+)',
        re.IGNORECASE
    )
    
    # Common non-judge words to filter out
    NON_JUDGE_WORDS = {
        'the', 'and', 'or', 'of', 'for', 'in', 'at', 'to', 'by', 'with',
        'court', 'appeal', 'petition', 'judgment', 'order', 'case',
        'pakistan', 'punjab', 'sindh', 'balochistan', 'kpk', 'islamabad',
        'supreme', 'high', 'federal', 'shariat', 'tribunal',
        'before', 'coram', 'present', 'hon', 'honble', "hon'ble",
        'mr', 'mrs', 'ms', 'justice', 'chief', 'acting',
        'petitioner', 'respondent', 'appellant', 'applicant',
    }
    
    # Title mappings
    TITLE_MAP = {
        'j': 'J.',
        'j.': 'J.',
        'cj': 'CJ',
        'cj.': 'CJ',
        'c.j.': 'CJ',
        'acj': 'ACJ',
        'acj.': 'ACJ',
        'a.c.j.': 'ACJ',
    }
    
    def __init__(self):
        self.compiled_patterns = [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in self.JUDGE_PATTERNS]
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize a judge name for consistent storage.
        - Remove titles (Mr., Justice, etc.)
        - Normalize spacing
        - Title case
        """
        if not name:
            return ""
        
        # Remove common prefixes/titles
        name = re.sub(r'^(?:The\s+)?(?:Hon\'?ble\s+)?', '', name, flags=re.IGNORECASE)
        name = re.sub(r'^(?:Mr\.|Mrs\.|Ms\.)\s*', '', name, flags=re.IGNORECASE)
        name = re.sub(r'^Justice\s+', '', name, flags=re.IGNORECASE)
        
        # Remove trailing titles
        name = re.sub(r',?\s*(?:C\.?J\.?|A\.?C\.?J\.?|J\.?)$', '', name, flags=re.IGNORECASE)
        
        # Clean up whitespace and special chars
        name = re.sub(r'\s+', ' ', name).strip()
        name = re.sub(r'[-—]+$', '', name).strip()
        
        # Title case each word
        parts = name.split()
        normalized_parts = []
        for part in parts:
            if part.lower() not in self.NON_JUDGE_WORDS:
                # Handle names like "Ul", "Ud", etc.
                if part.lower() in ['ul', 'ud', 'ur', 'al', 'bin', 'ibn']:
                    normalized_parts.append(part.lower())
                else:
                    normalized_parts.append(part.title())
        
        return ' '.join(normalized_parts)
    
    def is_valid_judge_name(self, name: str) -> bool:
        """Check if extracted name is likely a valid judge name."""
        if not name or len(name) < 3:
            return False
        
        # Must have at least 2 words (first and last name)
        parts = name.split()
        if len(parts) < 2:
            return False
        
        # Filter out common non-names
        lower_name = name.lower()
        if any(word in lower_name for word in ['court', 'pakistan', 'judgment', 'order', 'case']):
            return False
        
        # Should contain mostly letters
        alpha_chars = sum(1 for c in name if c.isalpha() or c.isspace())
        if alpha_chars / len(name) < 0.8:
            return False
        
        return True
    
    def extract_title(self, text: str, name: str) -> Optional[str]:
        """Extract the title (J., CJ, ACJ) associated with a judge name."""
        # Look for title near the name
        pattern = rf'{re.escape(name)},?\s*(C\.?J\.?|A\.?C\.?J\.?|J\.?)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            title = match.group(1).lower().replace('.', '')
            return self.TITLE_MAP.get(title, 'J.')
        return None
    
    def extract_author_from_text(self, judgment_text: str) -> Optional[JudgeInfo]:
        """
        Extract the author judge from judgment text.
        The author is typically at the start: "NAME, J.---" or after "JUDGMENT"
        """
        if not judgment_text:
            return None
        
        # Look for pattern at start of judgment
        match = self.AUTHOR_PATTERN.search(judgment_text[:2000])
        if match:
            raw_name = match.group(1).strip()
            title_code = match.group(2).lower().replace('.', '') if match.group(2) else 'j'
            title = self.TITLE_MAP.get(title_code, 'J.')
            
            normalized = self.normalize_name(raw_name)
            if self.is_valid_judge_name(normalized):
                role = JudgeRole.CHIEF if 'cj' in title_code else JudgeRole.AUTHOR
                return JudgeInfo(
                    name=normalized,
                    raw_name=raw_name,
                    role=role,
                    title=title
                )
        
        return None
    
    def extract_from_text(self, text: str) -> list[JudgeInfo]:
        """Extract judge names from judgment text."""
        if not text:
            return []
        
        judges = []
        seen_names = set()
        
        # First, try to get the author
        author = self.extract_author_from_text(text)
        if author:
            judges.append(author)
            seen_names.add(author.name.lower())
        
        # Extract from various patterns
        for pattern in self.compiled_patterns:
            for match in pattern.finditer(text[:5000]):  # Check first 5000 chars
                raw_name = match.group(1).strip()
                normalized = self.normalize_name(raw_name)
                
                if normalized.lower() not in seen_names and self.is_valid_judge_name(normalized):
                    title = self.extract_title(text, raw_name)
                    role = JudgeRole.CHIEF if title == 'CJ' else JudgeRole.BENCH
                    
                    judges.append(JudgeInfo(
                        name=normalized,
                        raw_name=raw_name,
                        role=role,
                        title=title or 'J.'
                    ))
                    seen_names.add(normalized.lower())
        
        # Check for multi-judge patterns
        for match in self.MULTI_JUDGE_PATTERN.finditer(text[:5000]):
            for group in [match.group(1), match.group(2)]:
                normalized = self.normalize_name(group)
                if normalized.lower() not in seen_names and self.is_valid_judge_name(normalized):
                    judges.append(JudgeInfo(
                        name=normalized,
                        raw_name=group,
                        role=JudgeRole.BENCH,
                        title='J.'
                    ))
                    seen_names.add(normalized.lower())
        
        return judges
    
    def extract_from_metadata(self, judges_list: list) -> list[JudgeInfo]:
        """Process pre-extracted judges from metadata."""
        if not judges_list:
            return []
        
        judges = []
        for i, raw_name in enumerate(judges_list):
            normalized = self.normalize_name(raw_name)
            if self.is_valid_judge_name(normalized):
                # First judge in list is often the author
                role = JudgeRole.AUTHOR if i == 0 else JudgeRole.BENCH
                judges.append(JudgeInfo(
                    name=normalized,
                    raw_name=raw_name,
                    role=role,
                    title='J.'
                ))
        
        return judges
    
    def extract_from_case(self, case_data: dict) -> CaseJudges:
        """
        Extract all judge information from a case.
        Uses metadata first, falls back to text extraction.
        """
        citation = case_data.get('citation', '')
        court = case_data.get('court', '')
        date = case_data.get('date', '')
        
        result = CaseJudges(
            citation=citation,
            court=court,
            date=date
        )
        
        # Try metadata first
        metadata_judges = case_data.get('judges', [])
        if metadata_judges:
            result.judges = self.extract_from_metadata(metadata_judges)
            result.extraction_source = "metadata"
        
        # If no metadata judges, try text extraction
        if not result.judges:
            judgment_text = case_data.get('judgment_clean', '')
            result.judges = self.extract_from_text(judgment_text)
            result.extraction_source = "text"
        
        # If we have both sources, enrich with text extraction
        elif case_data.get('judgment_clean'):
            text_judges = self.extract_from_text(case_data['judgment_clean'])
            
            # Try to identify author from text
            for tj in text_judges:
                if tj.role == JudgeRole.AUTHOR:
                    # Update matching judge in metadata
                    for mj in result.judges:
                        if mj.name.lower() == tj.name.lower():
                            mj.role = JudgeRole.AUTHOR
                            mj.title = tj.title
                            break
                    break
            
            result.extraction_source = "both"
        
        return result


def process_case_file(file_path: Path) -> Optional[CaseJudges]:
    """Process a single case file and extract judges."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            case_data = json.load(f)
        
        extractor = JudgeExtractor()
        return extractor.extract_from_case(case_data)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def main():
    """Test the extractor on sample cases."""
    import sys
    
    data_dir = Path(__file__).parent / 'data_v2'
    
    # Find all case files
    case_files = list(data_dir.glob('**/2024/*.json'))
    print(f"Found {len(case_files)} case files")
    
    # Process first 20 for testing
    extractor = JudgeExtractor()
    
    results = []
    for i, file_path in enumerate(case_files[:20]):
        result = process_case_file(file_path)
        if result:
            results.append(result)
            print(f"\n{result.citation} ({result.court})")
            print(f"  Source: {result.extraction_source}")
            for j in result.judges:
                print(f"  - {j.name} [{j.role.value}] {j.title or ''}")
    
    # Save sample output
    output = [r.to_dict() for r in results]
    output_path = Path(__file__).parent / 'judge_extraction_sample.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n\nSaved {len(results)} results to {output_path}")


if __name__ == '__main__':
    main()
