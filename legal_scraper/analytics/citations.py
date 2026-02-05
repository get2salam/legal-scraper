"""
Citation extraction from legal texts.

Extracts references to statutes, case law, and constitutional provisions.
"""

import re
from collections import Counter
from typing import Optional


class CitationExtractor:
    """
    Extract legal citations from text.
    
    Supports:
    - Case citations (e.g., "2024 SC 445")
    - Statute references (e.g., "Section 302 PPC")
    - Constitutional articles (e.g., "Article 199")
    """
    
    # Common patterns (customize for your jurisdiction)
    PATTERNS = {
        "case_citation": r'\d{4}\s+[A-Z]{2,6}\s+\d+',
        "statute_section": r'[Ss]ection\s+\d+[A-Za-z]?(?:\s*\([a-z0-9]+\))?',
        "article": r'[Aa]rticle\s+\d+[A-Za-z]?(?:\s*\([a-z0-9]+\))?',
        "order_rule": r'[Oo]rder\s+[IVXLCDM]+\s+[Rr]ule\s+\d+',
    }
    
    def __init__(self, custom_patterns: dict = None):
        """
        Initialize extractor.
        
        Args:
            custom_patterns: Additional regex patterns to use
        """
        self.patterns = {**self.PATTERNS}
        if custom_patterns:
            self.patterns.update(custom_patterns)
        
        # Compile patterns
        self._compiled = {
            name: re.compile(pattern)
            for name, pattern in self.patterns.items()
        }
    
    def extract(self, text: str) -> dict[str, list[str]]:
        """
        Extract all citations from text.
        
        Args:
            text: Legal document text
        
        Returns:
            Dict mapping citation type to list of citations found
        """
        results = {}
        for name, pattern in self._compiled.items():
            matches = pattern.findall(text)
            if matches:
                results[name] = list(set(matches))  # Deduplicate
        return results
    
    def extract_all(self, text: str) -> list[str]:
        """Extract all citations as flat list."""
        all_citations = []
        for citations in self.extract(text).values():
            all_citations.extend(citations)
        return all_citations
    
    def count(self, text: str) -> Counter:
        """Count occurrences of each citation."""
        all_citations = self.extract_all(text)
        return Counter(all_citations)


def extract_citations(text: str, patterns: dict = None) -> list[str]:
    """
    Convenience function to extract citations.
    
    Args:
        text: Legal document text
        patterns: Optional custom patterns
    
    Returns:
        List of all citations found
    """
    extractor = CitationExtractor(patterns)
    return extractor.extract_all(text)


def analyze_citations(cases: list[dict], text_field: str = "text") -> dict:
    """
    Analyze citations across multiple cases.
    
    Args:
        cases: List of case dicts
        text_field: Key containing the case text
    
    Returns:
        Analysis results including most cited, totals, etc.
    """
    extractor = CitationExtractor()
    all_citations = Counter()
    cases_with_citations = 0
    
    for case in cases:
        text = case.get(text_field, "")
        if text:
            citations = extractor.count(text)
            if citations:
                cases_with_citations += 1
                all_citations.update(citations)
    
    return {
        "total_citations": sum(all_citations.values()),
        "unique_citations": len(all_citations),
        "cases_analyzed": len(cases),
        "cases_with_citations": cases_with_citations,
        "most_cited": all_citations.most_common(20),
        "avg_per_case": sum(all_citations.values()) / max(len(cases), 1),
    }
