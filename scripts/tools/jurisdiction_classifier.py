"""
Jurisdiction Classifier for Pakistani Legal Cases

Classifies cases by jurisdiction based on:
1. Citation format (SCMR = Supreme Court, etc.)
2. Court field in case JSON
3. Citation suffix patterns (PLD Lah = Lahore High Court)
"""

import re
import json
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum


class Jurisdiction(Enum):
    """Pakistan's legal jurisdictions"""
    FEDERAL = "Federal"              # Supreme Court of Pakistan
    SINDH = "Sindh"                  # Sindh High Court (Karachi)
    PUNJAB = "Punjab"                # Lahore High Court
    KPK = "KPK"                      # Peshawar High Court
    BALOCHISTAN = "Balochistan"      # Balochistan High Court (Quetta)
    ISLAMABAD = "Islamabad"          # Islamabad High Court
    AJK = "AJK"                      # Azad Jammu & Kashmir High Court
    FSC = "FSC"                      # Federal Shariat Court
    UNKNOWN = "Unknown"


@dataclass
class ClassificationResult:
    """Result of jurisdiction classification"""
    jurisdiction: Jurisdiction
    confidence: float
    source: str  # 'citation', 'court_field', 'citation_suffix', 'combined'
    details: str


class JurisdictionClassifier:
    """
    Classify Pakistani legal cases by jurisdiction.
    
    Supports multiple classification strategies:
    1. Reporter-based (SCMR is always Supreme Court)
    2. Court field parsing (direct match)
    3. Citation suffix parsing (PLD Lah = Lahore)
    """
    
    # Reporters that are always from a specific jurisdiction
    REPORTER_JURISDICTIONS = {
        'SCMR': Jurisdiction.FEDERAL,  # Supreme Court Monthly Review
    }
    
    # Court field mappings (normalized lowercase)
    COURT_MAPPINGS = {
        # Federal/Supreme Court
        'supreme court': Jurisdiction.FEDERAL,
        'supreme court of pakistan': Jurisdiction.FEDERAL,
        'sc': Jurisdiction.FEDERAL,
        'apex court': Jurisdiction.FEDERAL,
        
        # Federal Shariat Court
        'federal shariat court': Jurisdiction.FSC,
        'fsc': Jurisdiction.FSC,
        'shariat court': Jurisdiction.FSC,
        
        # Sindh High Court
        'sindh': Jurisdiction.SINDH,
        'sindh high court': Jurisdiction.SINDH,
        'shc': Jurisdiction.SINDH,
        'karachi': Jurisdiction.SINDH,
        'high court of sindh': Jurisdiction.SINDH,
        
        # Lahore High Court (Punjab)
        'lahore': Jurisdiction.PUNJAB,
        'lahore high court': Jurisdiction.PUNJAB,
        'lhc': Jurisdiction.PUNJAB,
        'punjab': Jurisdiction.PUNJAB,
        'high court of punjab': Jurisdiction.PUNJAB,
        
        # Peshawar High Court (KPK)
        'peshawar': Jurisdiction.KPK,
        'peshawar high court': Jurisdiction.KPK,
        'phc': Jurisdiction.KPK,
        'kpk': Jurisdiction.KPK,
        'khyber pakhtunkhwa': Jurisdiction.KPK,
        'nwfp': Jurisdiction.KPK,  # Old name
        'high court of peshawar': Jurisdiction.KPK,
        
        # Balochistan High Court
        'balochistan': Jurisdiction.BALOCHISTAN,
        'balochistan high court': Jurisdiction.BALOCHISTAN,
        'bhc': Jurisdiction.BALOCHISTAN,
        'quetta': Jurisdiction.BALOCHISTAN,
        'high court of balochistan': Jurisdiction.BALOCHISTAN,
        
        # Islamabad High Court
        'islamabad': Jurisdiction.ISLAMABAD,
        'islamabad high court': Jurisdiction.ISLAMABAD,
        'ihc': Jurisdiction.ISLAMABAD,
        'high court of islamabad': Jurisdiction.ISLAMABAD,
        
        # AJK High Court
        'ajk': Jurisdiction.AJK,
        'azad kashmir': Jurisdiction.AJK,
        'azad jammu': Jurisdiction.AJK,
        'azad jammu and kashmir': Jurisdiction.AJK,
        'azad jammu & kashmir': Jurisdiction.AJK,
        'mirpur': Jurisdiction.AJK,
        'muzaffarabad': Jurisdiction.AJK,
        'high court of azad kashmir': Jurisdiction.AJK,
    }
    
    # Citation suffix patterns (e.g., "PLD Lah" = Lahore)
    CITATION_SUFFIX_MAPPINGS = {
        # Supreme Court
        'sc': Jurisdiction.FEDERAL,
        's.c.': Jurisdiction.FEDERAL,
        'supreme court': Jurisdiction.FEDERAL,
        
        # Federal Shariat Court
        'fsc': Jurisdiction.FSC,
        'f.s.c.': Jurisdiction.FSC,
        'fed. shariat court': Jurisdiction.FSC,
        
        # Sindh
        'kar': Jurisdiction.SINDH,
        'karachi': Jurisdiction.SINDH,
        'sindh': Jurisdiction.SINDH,
        'sin': Jurisdiction.SINDH,
        
        # Punjab/Lahore
        'lah': Jurisdiction.PUNJAB,
        'lahore': Jurisdiction.PUNJAB,
        'punjab': Jurisdiction.PUNJAB,
        
        # KPK/Peshawar
        'pesh': Jurisdiction.KPK,
        'peshawar': Jurisdiction.KPK,
        'kpk': Jurisdiction.KPK,
        
        # Balochistan
        'bal': Jurisdiction.BALOCHISTAN,
        'balochistan': Jurisdiction.BALOCHISTAN,
        'quetta': Jurisdiction.BALOCHISTAN,
        'quet': Jurisdiction.BALOCHISTAN,
        
        # Islamabad
        'isl': Jurisdiction.ISLAMABAD,
        'islamabad': Jurisdiction.ISLAMABAD,
        
        # AJK
        'ajk': Jurisdiction.AJK,
        'azad kashmir': Jurisdiction.AJK,
    }
    
    # Regex pattern for citation parsing
    # Matches: "2024 PLD Lah 123" or "PLD 2024 Lahore 456"
    CITATION_PATTERN = re.compile(
        r'(\d{4})\s+([A-Z]+)\s+([A-Za-z.\s]+)?\s*(\d+)',
        re.IGNORECASE
    )
    
    # Pattern for extracting court suffix from citation
    COURT_SUFFIX_PATTERN = re.compile(
        r'\d{4}\s+[A-Z]+\s+([A-Za-z.\s]+)\s+\d+',
        re.IGNORECASE
    )
    
    def __init__(self):
        self.stats = {
            'total': 0,
            'by_jurisdiction': {},
            'by_source': {},
            'unknown': 0
        }
    
    def classify_from_reporter(self, citation: str) -> Optional[ClassificationResult]:
        """
        Classify based on reporter code (e.g., SCMR is always Supreme Court)
        """
        for reporter, jurisdiction in self.REPORTER_JURISDICTIONS.items():
            if reporter.upper() in citation.upper():
                return ClassificationResult(
                    jurisdiction=jurisdiction,
                    confidence=1.0,
                    source='reporter',
                    details=f"Reporter {reporter} is always {jurisdiction.value}"
                )
        return None
    
    def classify_from_court_field(self, court: str) -> Optional[ClassificationResult]:
        """
        Classify based on the court field in case JSON
        """
        if not court:
            return None
        
        court_lower = court.lower().strip()
        
        # Direct match
        if court_lower in self.COURT_MAPPINGS:
            return ClassificationResult(
                jurisdiction=self.COURT_MAPPINGS[court_lower],
                confidence=0.95,
                source='court_field',
                details=f"Direct match on court field: '{court}'"
            )
        
        # Partial match
        for pattern, jurisdiction in self.COURT_MAPPINGS.items():
            if pattern in court_lower or court_lower in pattern:
                return ClassificationResult(
                    jurisdiction=jurisdiction,
                    confidence=0.85,
                    source='court_field',
                    details=f"Partial match '{pattern}' in court field: '{court}'"
                )
        
        return None
    
    def classify_from_citation_suffix(self, citation: str) -> Optional[ClassificationResult]:
        """
        Classify based on citation suffix (e.g., "PLD Lah" = Lahore)
        """
        match = self.COURT_SUFFIX_PATTERN.search(citation)
        if not match:
            return None
        
        suffix = match.group(1).lower().strip()
        
        # Direct match
        if suffix in self.CITATION_SUFFIX_MAPPINGS:
            return ClassificationResult(
                jurisdiction=self.CITATION_SUFFIX_MAPPINGS[suffix],
                confidence=0.9,
                source='citation_suffix',
                details=f"Citation suffix '{suffix}' maps to jurisdiction"
            )
        
        # Partial match
        for pattern, jurisdiction in self.CITATION_SUFFIX_MAPPINGS.items():
            if pattern in suffix or suffix.startswith(pattern[:3]):
                return ClassificationResult(
                    jurisdiction=jurisdiction,
                    confidence=0.75,
                    source='citation_suffix',
                    details=f"Partial match '{pattern}' in citation suffix '{suffix}'"
                )
        
        return None
    
    def classify(self, case_data: Dict) -> ClassificationResult:
        """
        Classify a case by jurisdiction using multiple strategies.
        
        Priority:
        1. Reporter-based (highest confidence for SCMR etc.)
        2. Court field (direct from data)
        3. Citation suffix (fallback)
        
        Args:
            case_data: Dictionary with 'citation' and 'court' fields
            
        Returns:
            ClassificationResult with jurisdiction and confidence
        """
        citation = case_data.get('citation', '')
        court = case_data.get('court', '')
        
        results = []
        
        # Try reporter-based classification first
        result = self.classify_from_reporter(citation)
        if result:
            results.append(result)
        
        # Try court field
        result = self.classify_from_court_field(court)
        if result:
            results.append(result)
        
        # Try citation suffix
        result = self.classify_from_citation_suffix(citation)
        if result:
            results.append(result)
        
        # Select best result
        if results:
            # Sort by confidence and return highest
            results.sort(key=lambda r: r.confidence, reverse=True)
            best = results[0]
            
            # If multiple sources agree, boost confidence
            if len(results) > 1:
                jurisdictions = [r.jurisdiction for r in results]
                if jurisdictions[0] == jurisdictions[1]:
                    best = ClassificationResult(
                        jurisdiction=best.jurisdiction,
                        confidence=min(1.0, best.confidence + 0.05),
                        source='combined',
                        details=f"Multiple sources agree: {best.details}"
                    )
            
            self._update_stats(best)
            return best
        
        # Unknown jurisdiction
        unknown = ClassificationResult(
            jurisdiction=Jurisdiction.UNKNOWN,
            confidence=0.0,
            source='none',
            details=f"Could not classify: citation='{citation}', court='{court}'"
        )
        self._update_stats(unknown)
        return unknown
    
    def _update_stats(self, result: ClassificationResult):
        """Update classification statistics"""
        self.stats['total'] += 1
        
        j = result.jurisdiction.value
        self.stats['by_jurisdiction'][j] = self.stats['by_jurisdiction'].get(j, 0) + 1
        
        s = result.source
        self.stats['by_source'][s] = self.stats['by_source'].get(s, 0) + 1
        
        if result.jurisdiction == Jurisdiction.UNKNOWN:
            self.stats['unknown'] += 1
    
    def get_stats(self) -> Dict:
        """Get classification statistics"""
        return self.stats
    
    def classify_batch(self, cases: List[Dict]) -> List[Tuple[Dict, ClassificationResult]]:
        """
        Classify a batch of cases.
        
        Args:
            cases: List of case dictionaries
            
        Returns:
            List of (case, result) tuples
        """
        return [(case, self.classify(case)) for case in cases]


def load_cases_from_directory(data_dir: Path) -> List[Dict]:
    """
    Load all cases from the data directory structure.
    
    Expected structure: data_v2/REPORTER/YEAR/*.json
    """
    cases = []
    
    for reporter_dir in data_dir.iterdir():
        if not reporter_dir.is_dir():
            continue
        if reporter_dir.name in ['audit', 'backup', 'html', 'logs']:
            continue
            
        for year_dir in reporter_dir.iterdir():
            if not year_dir.is_dir():
                continue
            if year_dir.name == 'original':
                continue
                
            for json_file in year_dir.glob('*.json'):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        case = json.load(f)
                        case['_file'] = str(json_file)
                        case['_reporter'] = reporter_dir.name
                        cases.append(case)
                except Exception as e:
                    print(f"Error loading {json_file}: {e}")
    
    return cases


def main():
    """
    Main function to demonstrate jurisdiction classification.
    """
    import sys
    
    # Initialize classifier
    classifier = JurisdictionClassifier()
    
    # Default data directory
    data_dir = Path(__file__).parent / 'data_v2'
    
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)
    
    print(f"Loading cases from {data_dir}...")
    cases = load_cases_from_directory(data_dir)
    print(f"Loaded {len(cases)} cases")
    
    # Classify all cases
    print("\nClassifying cases...")
    results = classifier.classify_batch(cases)
    
    # Print statistics
    stats = classifier.get_stats()
    print("\n" + "="*60)
    print("JURISDICTION CLASSIFICATION STATISTICS")
    print("="*60)
    
    print(f"\nTotal cases classified: {stats['total']}")
    print(f"Unknown jurisdiction: {stats['unknown']} ({100*stats['unknown']/max(1,stats['total']):.1f}%)")
    
    print("\nBy Jurisdiction:")
    for j, count in sorted(stats['by_jurisdiction'].items(), key=lambda x: -x[1]):
        pct = 100 * count / stats['total']
        print(f"  {j:15} {count:6} ({pct:5.1f}%)")
    
    print("\nBy Source:")
    for s, count in sorted(stats['by_source'].items(), key=lambda x: -x[1]):
        pct = 100 * count / stats['total']
        print(f"  {s:15} {count:6} ({pct:5.1f}%)")
    
    # Show sample classifications by jurisdiction
    print("\n" + "="*60)
    print("SAMPLE CLASSIFICATIONS")
    print("="*60)
    
    by_jurisdiction = {}
    for case, result in results:
        j = result.jurisdiction.value
        if j not in by_jurisdiction:
            by_jurisdiction[j] = []
        if len(by_jurisdiction[j]) < 2:
            by_jurisdiction[j].append((case, result))
    
    for j, samples in sorted(by_jurisdiction.items()):
        print(f"\n{j}:")
        for case, result in samples:
            print(f"  Citation: {case.get('citation', 'N/A')}")
            print(f"  Court: {case.get('court', 'N/A')}")
            print(f"  Confidence: {result.confidence:.2f} (source: {result.source})")
            print()
    
    # Save classification results
    output_file = data_dir / 'jurisdiction_classification.json'
    output_data = {
        'stats': stats,
        'classifications': [
            {
                'citation': case.get('citation'),
                'court': case.get('court'),
                'file': case.get('_file'),
                'jurisdiction': result.jurisdiction.value,
                'confidence': result.confidence,
                'source': result.source
            }
            for case, result in results
        ]
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nClassification results saved to: {output_file}")
    
    return results


if __name__ == '__main__':
    main()
