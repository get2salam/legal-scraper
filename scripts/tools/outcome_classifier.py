#!/usr/bin/env python3
"""
Case Outcome Classifier for Pakistani Legal Research Platform
Classifies case outcomes from judgments using pattern matching and ML.

Outcomes:
- ALLOWED: Petition/appeal successful
- DISMISSED: Petition/appeal failed  
- PARTIALLY_ALLOWED: Mixed outcome
- REMANDED: Sent back to lower court
- WITHDRAWN: Case withdrawn
- DISPOSED: Disposed of (ambiguous outcome)
- UNKNOWN: Cannot determine
"""

import re
import json
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from collections import Counter


class Outcome(Enum):
    ALLOWED = "allowed"
    DISMISSED = "dismissed"
    PARTIALLY_ALLOWED = "partially_allowed"
    REMANDED = "remanded"
    WITHDRAWN = "withdrawn"
    DISPOSED = "disposed"
    UNKNOWN = "unknown"


@dataclass
class ClassificationResult:
    outcome: Outcome
    confidence: float
    matched_patterns: List[str]
    case_type: Optional[str] = None  # petition, appeal, revision, etc.
    relief_granted: Optional[str] = None


# Pattern definitions for outcome classification
# Priority order matters - more specific patterns first

OUTCOME_PATTERNS = {
    # ALLOWED patterns (petition/appeal successful)
    Outcome.ALLOWED: [
        # Strong positive indicators
        (r'(?:petition|appeal|writ|suit|application)\s+(?:is|are|was|were)\s+(?:hereby\s+)?allowed', 0.95),
        (r'(?:petition|appeal|writ)\s+(?:is|are)\s+(?:hereby\s+)?granted', 0.95),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?accepted', 0.95),
        (r'writ\s+of\s+(?:mandamus|certiorari|prohibition|habeas corpus)\s+(?:is|are)\s+(?:hereby\s+)?issued', 0.90),
        (r'impugned\s+(?:order|judgment|decree)\s+(?:is|are)\s+(?:hereby\s+)?(?:set\s+aside|quashed|reversed)', 0.90),
        (r'judgment\s+(?:and\s+decree\s+)?(?:is|are)\s+(?:hereby\s+)?reversed', 0.90),
        (r'appeal\s+(?:is|are)\s+(?:hereby\s+)?(?:allowed|successful|succeeds)', 0.95),
        (r'conviction\s+(?:is|are)\s+(?:hereby\s+)?(?:set\s+aside|quashed|overturned)', 0.90),
        (r'sentence\s+(?:is|are)\s+(?:hereby\s+)?(?:set\s+aside|reduced|modified)', 0.85),
        (r'bail\s+(?:is|are)\s+(?:hereby\s+)?(?:granted|allowed|confirmed)', 0.90),
        (r'(?:ad-interim|interim)\s+relief\s+(?:is|are)\s+(?:hereby\s+)?(?:granted|confirmed)', 0.85),
        (r'stay\s+(?:is|are)\s+(?:hereby\s+)?(?:granted|continued)', 0.80),
        (r'direction\s+(?:is|are)\s+(?:hereby\s+)?issued', 0.75),
        (r'decree\s+(?:is|are)\s+(?:hereby\s+)?passed\s+in\s+(?:favour|favor)\s+of', 0.90),
        (r'prayer\s+clause[s]?\s+(?:is|are)\s+(?:hereby\s+)?(?:granted|allowed)', 0.90),
        (r'(?:resultantly|consequently|accordingly)[,]?\s+(?:the\s+)?(?:petition|appeal)\s+(?:is|are)\s+allowed', 0.95),
    ],
    
    # DISMISSED patterns (petition/appeal failed)
    Outcome.DISMISSED: [
        # Strong negative indicators
        (r'(?:petition|appeal|writ|suit|application)\s+(?:is|are|was|were)\s+(?:hereby\s+)?dismissed', 0.95),
        (r'(?:petition|appeal|writ)\s+(?:is|are)\s+(?:hereby\s+)?(?:rejected|refused|declined)', 0.95),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?(?:not\s+)?maintainable', 0.85),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:without\s+)?merit(?:less)?', 0.85),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?(?:devoid|bereft)\s+of\s+merit', 0.90),
        (r'(?:petition|appeal)\s+(?:has|have)\s+no\s+(?:force|merit|substance)', 0.85),
        (r'conviction\s+(?:is|are)\s+(?:hereby\s+)?(?:maintained|upheld|confirmed)', 0.85),
        (r'sentence\s+(?:is|are)\s+(?:hereby\s+)?(?:maintained|upheld|confirmed)', 0.85),
        (r'judgment\s+(?:and\s+decree\s+)?(?:is|are)\s+(?:hereby\s+)?(?:maintained|upheld|confirmed|affirmed)', 0.90),
        (r'impugned\s+(?:order|judgment|decree)\s+(?:is|are)\s+(?:hereby\s+)?(?:maintained|upheld|confirmed)', 0.90),
        (r'bail\s+(?:is|are)\s+(?:hereby\s+)?(?:refused|rejected|declined|cancelled)', 0.90),
        (r'no\s+(?:case|ground)\s+(?:is|has been)\s+made\s+out', 0.80),
        (r'prayer\s+(?:is|are)\s+(?:hereby\s+)?(?:refused|rejected|declined)', 0.90),
        (r'(?:petition|appeal)\s+(?:stands|is)\s+dismissed', 0.95),
        (r'(?:resultantly|consequently|accordingly)[,]?\s+(?:the\s+)?(?:petition|appeal)\s+(?:is|are)\s+dismissed', 0.95),
        (r'leave\s+to\s+appeal\s+(?:is|are)\s+(?:hereby\s+)?(?:refused|rejected|declined)', 0.90),
    ],
    
    # PARTIALLY ALLOWED patterns
    Outcome.PARTIALLY_ALLOWED: [
        (r'(?:petition|appeal|writ)\s+(?:is|are)\s+(?:hereby\s+)?(?:partially|partly)\s+(?:allowed|granted|accepted)', 0.95),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?allowed\s+(?:in\s+part|to\s+(?:the|some)\s+extent)', 0.95),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?(?:allowed|dismissed)\s+(?:in\s+part|partially)', 0.90),
        (r'(?:partially|partly)\s+(?:allowed|accepted)', 0.85),
        (r'allowed\s+to\s+(?:the|some)\s+extent', 0.85),
        (r'(?:appeal|petition)\s+(?:partly|partially)\s+(?:succeeds|allowed|accepted)', 0.90),
        (r'sentence\s+(?:is|are)\s+(?:hereby\s+)?(?:modified|reduced)', 0.75),
        (r'(?:modified|reduced)\s+(?:the\s+)?sentence', 0.75),
    ],
    
    # REMANDED patterns
    Outcome.REMANDED: [
        (r'(?:case|matter|petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?remanded', 0.95),
        (r'(?:case|matter)\s+(?:is|are)\s+(?:hereby\s+)?(?:sent|remitted)\s+back', 0.95),
        (r'remand(?:ed|ing)\s+(?:the\s+)?(?:case|matter)\s+(?:to|back\s+to)', 0.90),
        (r'(?:case|matter)\s+(?:is|are)\s+(?:hereby\s+)?(?:returned|restored)\s+to', 0.85),
        (r'(?:trial|lower)\s+court\s+(?:is|are)\s+(?:hereby\s+)?directed\s+to\s+(?:decide|rehear|retry)', 0.85),
        (r'(?:fresh|de\s+novo|denovo)\s+(?:trial|hearing|decision)\s+(?:is|are)\s+(?:hereby\s+)?(?:ordered|directed)', 0.85),
        (r'(?:impugned|appealed)\s+(?:order|judgment|decree)\s+(?:is|are)\s+(?:hereby\s+)?(?:set\s+aside|quashed)\s+(?:and|with)\s+(?:case|matter)\s+(?:is|are)\s+(?:hereby\s+)?remanded', 0.95),
    ],
    
    # WITHDRAWN patterns
    Outcome.WITHDRAWN: [
        (r'(?:petition|appeal|writ|suit|application)\s+(?:is|are)\s+(?:hereby\s+)?withdrawn', 0.95),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?(?:dismissed\s+as\s+)?withdrawn', 0.95),
        (r'leave\s+to\s+withdraw\s+(?:is|are)\s+(?:hereby\s+)?(?:granted|allowed)', 0.90),
        (r'(?:petition|appeal)\s+(?:is|are)\s+(?:hereby\s+)?(?:not\s+pressed|abandoned)', 0.85),
        (r'counsel\s+(?:does\s+not|declined\s+to)\s+press', 0.80),
    ],
    
    # DISPOSED patterns (ambiguous)
    Outcome.DISPOSED: [
        (r'(?:petition|appeal|writ|suit|application)\s+(?:is|are)\s+(?:hereby\s+)?disposed\s+of', 0.80),
        (r'disposed\s+of\s+(?:in\s+(?:the\s+)?(?:above|aforesaid)\s+terms|accordingly)', 0.85),
        (r'(?:petition|appeal)\s+(?:stands|is)\s+disposed\s+of', 0.85),
        (r'(?:case|matter)\s+(?:is|are)\s+(?:hereby\s+)?(?:closed|concluded)', 0.75),
    ],
}

# Case type detection patterns
CASE_TYPE_PATTERNS = {
    'constitutional_petition': [
        r'constitutional\s+petition',
        r'writ\s+petition',
        r'article\s+(?:199|184)',
        r'fundamental\s+right',
    ],
    'civil_appeal': [
        r'civil\s+appeal',
        r'first\s+appeal',
        r'second\s+appeal',
        r'r\.?\s*f\.?\s*a',
    ],
    'criminal_appeal': [
        r'criminal\s+appeal',
        r'jail\s+criminal\s+appeal',
        r'murder\s+reference',
    ],
    'civil_revision': [
        r'civil\s+revision',
        r'c\.?\s*r\.?\s*p',
    ],
    'criminal_revision': [
        r'criminal\s+revision',
        r'cr\.?\s*r\.?\s*p',
    ],
    'review_petition': [
        r'review\s+petition',
        r'c\.?\s*m\.?\s*a',
    ],
    'intra_court_appeal': [
        r'intra[\s-]?court\s+appeal',
        r'i\.?\s*c\.?\s*a',
    ],
    'civil_suit': [
        r'civil\s+suit',
        r'suit\s+for\s+(?:declaration|specific\s+performance|injunction)',
    ],
    'bail_petition': [
        r'bail\s+(?:petition|application)',
        r'pre[\s-]?arrest\s+bail',
        r'post[\s-]?arrest\s+bail',
        r'confirmatory\s+bail',
    ],
    'execution_petition': [
        r'execution\s+(?:petition|application|proceedings)',
    ],
    'family_suit': [
        r'family\s+(?:suit|case|matter)',
        r'dissolution\s+of\s+marriage',
        r'khula',
        r'maintenance',
        r'custody',
        r'guardianship',
    ],
    'rent_petition': [
        r'rent\s+(?:petition|case|matter)',
        r'ejectment\s+(?:petition|case)',
    ],
    'tax_reference': [
        r'tax\s+(?:reference|appeal)',
        r'income\s+tax\s+(?:reference|appeal)',
        r'sales\s+tax\s+(?:reference|appeal)',
        r'customs\s+(?:reference|appeal)',
    ],
    'service_matter': [
        r'service\s+(?:appeal|tribunal|matter)',
        r'termination\s+(?:from\s+service)?',
    ],
    'land_acquisition': [
        r'land\s+acquisition',
        r'compensation\s+(?:case|matter)',
        r'reference\s+(?:under|u/s)\s+(?:section\s+)?18',
    ],
}


class OutcomeClassifier:
    """
    Classifies case outcomes from judgment text using pattern matching.
    """
    
    def __init__(self, use_ml: bool = False):
        """
        Initialize classifier.
        
        Args:
            use_ml: Whether to use ML model (future feature)
        """
        self.use_ml = use_ml
        self.patterns = OUTCOME_PATTERNS
        self.case_type_patterns = CASE_TYPE_PATTERNS
        
    def _extract_final_paragraph(self, text: str, chars: int = 5000) -> str:
        """Extract the final portion of judgment where outcome is usually stated."""
        if not text:
            return ""
        
        # Clean text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Get last portion
        if len(text) > chars:
            return text[-chars:]
        return text
    
    def _extract_order_section(self, text: str) -> Optional[str]:
        """Extract ORDER/RESULT section if present."""
        # Common order section markers
        order_markers = [
            r'(?:^|\n)\s*ORDER\s*(?:\n|$)',
            r'(?:^|\n)\s*RESULT\s*(?:\n|$)',
            r'(?:^|\n)\s*JUDGMENT\s*(?:\n|$)',
            r'(?:^|\n)\s*SHORT\s+ORDER\s*(?:\n|$)',
            r'(?:^|\n)\s*(?:For\s+the\s+)?(?:foregoing|above)\s+reasons',
            r'(?:^|\n)\s*In\s+(?:the\s+)?(?:view|light)\s+of\s+(?:the\s+)?(?:above|foregoing)',
        ]
        
        for marker in order_markers:
            match = re.search(marker, text, re.IGNORECASE | re.MULTILINE)
            if match:
                return text[match.start():]
        
        return None
    
    def _detect_case_type(self, text: str) -> Optional[str]:
        """Detect the type of case from text."""
        text_lower = text.lower()
        
        for case_type, patterns in self.case_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    return case_type
        
        return None
    
    def _match_patterns(self, text: str) -> List[Tuple[Outcome, float, str]]:
        """
        Match patterns against text.
        
        Returns list of (outcome, confidence, matched_pattern) tuples.
        """
        matches = []
        text_lower = text.lower()
        
        for outcome, patterns in self.patterns.items():
            for pattern, base_confidence in patterns:
                match = re.search(pattern, text_lower, re.IGNORECASE)
                if match:
                    matches.append((outcome, base_confidence, match.group(0)))
        
        return matches
    
    def _resolve_conflicts(self, matches: List[Tuple[Outcome, float, str]]) -> ClassificationResult:
        """
        Resolve conflicting pattern matches.
        Priority: PARTIALLY_ALLOWED > REMANDED > WITHDRAWN > ALLOWED/DISMISSED > DISPOSED
        """
        if not matches:
            return ClassificationResult(
                outcome=Outcome.UNKNOWN,
                confidence=0.0,
                matched_patterns=[]
            )
        
        # Count matches by outcome
        outcome_counts = Counter(m[0] for m in matches)
        outcome_confidences = {}
        outcome_patterns = {}
        
        for outcome, confidence, pattern in matches:
            if outcome not in outcome_confidences:
                outcome_confidences[outcome] = []
                outcome_patterns[outcome] = []
            outcome_confidences[outcome].append(confidence)
            outcome_patterns[outcome].append(pattern)
        
        # Check for PARTIALLY_ALLOWED first (takes priority)
        if Outcome.PARTIALLY_ALLOWED in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.PARTIALLY_ALLOWED,
                confidence=max(outcome_confidences[Outcome.PARTIALLY_ALLOWED]),
                matched_patterns=outcome_patterns[Outcome.PARTIALLY_ALLOWED]
            )
        
        # Check for REMANDED
        if Outcome.REMANDED in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.REMANDED,
                confidence=max(outcome_confidences[Outcome.REMANDED]),
                matched_patterns=outcome_patterns[Outcome.REMANDED]
            )
        
        # Check for WITHDRAWN
        if Outcome.WITHDRAWN in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.WITHDRAWN,
                confidence=max(outcome_confidences[Outcome.WITHDRAWN]),
                matched_patterns=outcome_patterns[Outcome.WITHDRAWN]
            )
        
        # Compare ALLOWED vs DISMISSED
        allowed_count = outcome_counts.get(Outcome.ALLOWED, 0)
        dismissed_count = outcome_counts.get(Outcome.DISMISSED, 0)
        
        if allowed_count > 0 and dismissed_count > 0:
            # Both present - use confidence to decide
            allowed_conf = max(outcome_confidences.get(Outcome.ALLOWED, [0]))
            dismissed_conf = max(outcome_confidences.get(Outcome.DISMISSED, [0]))
            
            if allowed_conf >= dismissed_conf:
                return ClassificationResult(
                    outcome=Outcome.ALLOWED,
                    confidence=allowed_conf * 0.8,  # Reduce confidence due to conflict
                    matched_patterns=outcome_patterns[Outcome.ALLOWED]
                )
            else:
                return ClassificationResult(
                    outcome=Outcome.DISMISSED,
                    confidence=dismissed_conf * 0.8,
                    matched_patterns=outcome_patterns[Outcome.DISMISSED]
                )
        
        # Single clear outcome
        if Outcome.ALLOWED in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.ALLOWED,
                confidence=max(outcome_confidences[Outcome.ALLOWED]),
                matched_patterns=outcome_patterns[Outcome.ALLOWED]
            )
        
        if Outcome.DISMISSED in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.DISMISSED,
                confidence=max(outcome_confidences[Outcome.DISMISSED]),
                matched_patterns=outcome_patterns[Outcome.DISMISSED]
            )
        
        # Check DISPOSED
        if Outcome.DISPOSED in outcome_counts:
            return ClassificationResult(
                outcome=Outcome.DISPOSED,
                confidence=max(outcome_confidences[Outcome.DISPOSED]),
                matched_patterns=outcome_patterns[Outcome.DISPOSED]
            )
        
        # Fallback
        return ClassificationResult(
            outcome=Outcome.UNKNOWN,
            confidence=0.0,
            matched_patterns=[]
        )
    
    def _clean_html(self, text: str) -> str:
        """Strip HTML tags from text."""
        if not text:
            return ""
        import re
        from html import unescape
        
        # Unescape HTML entities
        text = unescape(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Remove escaped unicode
        text = text.encode().decode('unicode_escape', errors='ignore')
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def classify(self, judgment_text: str, title: str = "", headnotes: str = "", judgment_html: str = "") -> ClassificationResult:
        """
        Classify the outcome of a case.
        
        Args:
            judgment_text: Full judgment text (clean)
            title: Case title
            headnotes: Case headnotes
            judgment_html: Raw HTML judgment (fallback if judgment_text empty)
            
        Returns:
            ClassificationResult with outcome, confidence, and matched patterns
        """
        # If judgment_text is empty, try to extract from HTML
        if not judgment_text and judgment_html:
            judgment_text = self._clean_html(judgment_html)
        
        if not judgment_text:
            return ClassificationResult(
                outcome=Outcome.UNKNOWN,
                confidence=0.0,
                matched_patterns=[]
            )
        
        # Extract sections to analyze
        final_section = self._extract_final_paragraph(judgment_text)
        order_section = self._extract_order_section(judgment_text)
        
        # Priority: ORDER section > Final paragraph > Headnotes
        sections_to_check = []
        
        if order_section:
            sections_to_check.append(('order', order_section))
        
        sections_to_check.append(('final', final_section))
        
        if headnotes:
            sections_to_check.append(('headnotes', headnotes))
        
        # Try each section
        best_result = None
        
        for section_name, section_text in sections_to_check:
            matches = self._match_patterns(section_text)
            
            if matches:
                result = self._resolve_conflicts(matches)
                
                if best_result is None or result.confidence > best_result.confidence:
                    best_result = result
        
        if best_result is None:
            best_result = ClassificationResult(
                outcome=Outcome.UNKNOWN,
                confidence=0.0,
                matched_patterns=[]
            )
        
        # Add case type
        full_text = f"{title} {headnotes} {judgment_text}"
        best_result.case_type = self._detect_case_type(full_text)
        
        return best_result
    
    def classify_batch(self, cases: List[Dict]) -> List[Dict]:
        """
        Classify outcomes for a batch of cases.
        
        Args:
            cases: List of case dictionaries with 'judgment_clean', 'title', 'headnotes'
            
        Returns:
            List of cases with added 'outcome_classification' field
        """
        results = []
        
        for case in cases:
            result = self.classify(
                judgment_text=case.get('judgment_clean', ''),
                title=case.get('title', ''),
                headnotes=case.get('headnotes', '')
            )
            
            case_with_outcome = case.copy()
            case_with_outcome['outcome_classification'] = asdict(result)
            case_with_outcome['outcome_classification']['outcome'] = result.outcome.value
            results.append(case_with_outcome)
        
        return results


def classify_all_cases(jsonl_path: Path, output_path: Path) -> Dict[str, int]:
    """
    Classify all cases in a JSONL file.
    
    Args:
        jsonl_path: Path to input JSONL
        output_path: Path to output JSONL with classifications
        
    Returns:
        Dictionary with outcome counts
    """
    classifier = OutcomeClassifier()
    outcome_counts = Counter()
    
    with open(jsonl_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            if not line.strip():
                continue
            
            case = json.loads(line)
            result = classifier.classify(
                judgment_text=case.get('judgment_clean', ''),
                title=case.get('title', ''),
                headnotes=case.get('headnotes', ''),
                judgment_html=case.get('judgment', '')  # Fallback to HTML
            )
            
            # Add classification to case
            case['outcome'] = result.outcome.value
            case['outcome_confidence'] = result.confidence
            case['outcome_patterns'] = result.matched_patterns
            case['case_type'] = result.case_type
            
            outcome_counts[result.outcome.value] += 1
            
            outfile.write(json.dumps(case, ensure_ascii=False) + '\n')
    
    return dict(outcome_counts)


def main():
    """CLI interface for outcome classification."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Classify case outcomes')
    parser.add_argument('--input', '-i', type=Path, default=Path('data_v2/all_cases.jsonl'),
                       help='Input JSONL file')
    parser.add_argument('--output', '-o', type=Path, default=Path('data_v2/cases_classified.jsonl'),
                       help='Output JSONL file')
    parser.add_argument('--test', '-t', action='store_true',
                       help='Run on first 100 cases only')
    parser.add_argument('--stats', '-s', action='store_true',
                       help='Show classification statistics')
    
    args = parser.parse_args()
    
    if args.test:
        # Test mode - classify first 100 cases
        classifier = OutcomeClassifier()
        outcome_counts = Counter()
        
        print("Testing on first 100 cases...\n")
        
        with open(args.input, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 100:
                    break
                
                if not line.strip():
                    continue
                
                case = json.loads(line)
                result = classifier.classify(
                    judgment_text=case.get('judgment_clean', ''),
                    title=case.get('title', ''),
                    headnotes=case.get('headnotes', ''),
                    judgment_html=case.get('judgment', '')  # Fallback to HTML
                )
                
                outcome_counts[result.outcome.value] += 1
                
                if i < 10:  # Show first 10 results
                    print(f"Case: {case.get('citation', 'Unknown')}")
                    print(f"  Outcome: {result.outcome.value}")
                    print(f"  Confidence: {result.confidence:.2f}")
                    print(f"  Case Type: {result.case_type or 'Unknown'}")
                    print(f"  Patterns: {result.matched_patterns[:2] if result.matched_patterns else 'None'}")
                    print()
        
        print("\n--- Statistics (100 cases) ---")
        total = sum(outcome_counts.values())
        for outcome, count in sorted(outcome_counts.items(), key=lambda x: -x[1]):
            print(f"  {outcome}: {count} ({count/total*100:.1f}%)")
    
    else:
        # Full classification
        print(f"Classifying cases from {args.input}...")
        
        counts = classify_all_cases(args.input, args.output)
        
        print(f"\nClassification complete. Output: {args.output}")
        print("\n--- Statistics ---")
        total = sum(counts.values())
        for outcome, count in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {outcome}: {count} ({count/total*100:.1f}%)")


if __name__ == '__main__':
    main()
