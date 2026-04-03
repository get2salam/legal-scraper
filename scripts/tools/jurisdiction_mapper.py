"""
Jurisdiction Mapper for Pakistani Legal Cases

Maps the same legal issues across different jurisdictions to:
1. Identify consensus (same ruling across jurisdictions)
2. Identify conflicts (different rulings)
3. Identify gaps (no ruling in some jurisdictions)
"""

import json
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from enum import Enum
import hashlib

try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SEMANTIC_AVAILABLE = True
except ImportError:
    SEMANTIC_AVAILABLE = False
    print("Note: sentence-transformers not installed. Using keyword-based matching only.")

from jurisdiction_classifier import Jurisdiction, JurisdictionClassifier, ClassificationResult
from issue_extractor import LegalIssue, ExtractionResult, LegalIssueExtractor


class MatchType(Enum):
    """Type of match between jurisdictions"""
    CONSENSUS = "consensus"      # Same ruling
    CONFLICT = "conflict"        # Different rulings
    PARTIAL = "partial"          # Some agreement, some difference
    SINGLE = "single"            # Only one jurisdiction has ruled


@dataclass
class JurisdictionPosition:
    """A jurisdiction's position on a legal issue"""
    jurisdiction: str
    citation: str
    holding: str
    confidence: float
    date: str = ''
    statutes: List[str] = field(default_factory=list)


@dataclass
class IssueMapping:
    """Mapping of a legal issue across jurisdictions"""
    issue_id: str
    normalized_issue: str
    category: str
    positions: Dict[str, JurisdictionPosition]  # jurisdiction -> position
    match_type: MatchType
    consensus_points: List[str] = field(default_factory=list)
    conflict_points: List[str] = field(default_factory=list)
    gap_jurisdictions: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)


@dataclass
class CaseWithClassification:
    """Case data with its jurisdiction classification"""
    case_data: Dict
    classification: ClassificationResult
    extraction: Optional[ExtractionResult] = None


class JurisdictionMapper:
    """
    Map legal issues across Pakistan's jurisdictions.
    
    Features:
    - Semantic similarity matching for issues
    - Keyword-based fallback matching
    - Conflict and consensus detection
    - Gap identification
    """
    
    # All jurisdictions to track
    ALL_JURISDICTIONS = [
        Jurisdiction.FEDERAL.value,
        Jurisdiction.SINDH.value,
        Jurisdiction.PUNJAB.value,
        Jurisdiction.KPK.value,
        Jurisdiction.BALOCHISTAN.value,
        Jurisdiction.ISLAMABAD.value,
        Jurisdiction.AJK.value,
        Jurisdiction.FSC.value,
    ]
    
    def __init__(self, use_semantic: bool = True):
        """
        Initialize the mapper.
        
        Args:
            use_semantic: Use semantic similarity (requires sentence-transformers)
        """
        self.classifier = JurisdictionClassifier()
        self.extractor = LegalIssueExtractor()
        
        self.use_semantic = use_semantic and SEMANTIC_AVAILABLE
        self.model = None
        
        if self.use_semantic:
            print("Loading semantic model...")
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
            print("Model loaded.")
        
        # Storage for processed data
        self.cases_by_jurisdiction: Dict[str, List[CaseWithClassification]] = defaultdict(list)
        self.issues_by_category: Dict[str, List[Tuple[str, LegalIssue, str]]] = defaultdict(list)  # (citation, issue, jurisdiction)
        self.issue_embeddings: Dict[str, np.ndarray] = {} if SEMANTIC_AVAILABLE else {}
        
        self.stats = {
            'cases_processed': 0,
            'issues_mapped': 0,
            'consensus_found': 0,
            'conflicts_found': 0,
            'gaps_identified': 0
        }
    
    def process_case(self, case_data: Dict) -> CaseWithClassification:
        """
        Process a single case: classify and extract issues.
        """
        # Classify jurisdiction
        classification = self.classifier.classify(case_data)
        
        # Extract issues
        extraction = self.extractor.extract(case_data, classification.jurisdiction.value)
        
        result = CaseWithClassification(
            case_data=case_data,
            classification=classification,
            extraction=extraction
        )
        
        # Store by jurisdiction
        self.cases_by_jurisdiction[classification.jurisdiction.value].append(result)
        
        # Store issues by category
        for issue in extraction.issues:
            self.issues_by_category[issue.category].append(
                (extraction.citation, issue, classification.jurisdiction.value)
            )
            
            # Generate embedding if using semantic matching
            if self.use_semantic and self.model:
                self.issue_embeddings[issue.id] = self.model.encode(issue.normalized_text)
        
        self.stats['cases_processed'] += 1
        
        return result
    
    def process_cases(self, cases: List[Dict]) -> List[CaseWithClassification]:
        """Process a batch of cases"""
        results = []
        total = len(cases)
        
        for i, case in enumerate(cases):
            if i % 100 == 0:
                print(f"Processing case {i+1}/{total}...")
            
            result = self.process_case(case)
            results.append(result)
        
        return results
    
    def find_similar_issues(
        self, 
        issue: LegalIssue, 
        category: str = None,
        threshold: float = 0.7
    ) -> List[Tuple[str, LegalIssue, str, float]]:
        """
        Find issues similar to the given issue.
        
        Args:
            issue: The issue to match
            category: Optional category to filter
            threshold: Similarity threshold (0-1)
            
        Returns:
            List of (citation, similar_issue, jurisdiction, similarity) tuples
        """
        similar = []
        
        # Get candidate issues
        if category:
            candidates = self.issues_by_category.get(category, [])
        else:
            candidates = []
            for cat_issues in self.issues_by_category.values():
                candidates.extend(cat_issues)
        
        if self.use_semantic and self.model and issue.id in self.issue_embeddings:
            # Semantic similarity
            query_embedding = self.issue_embeddings[issue.id]
            
            for citation, candidate_issue, jurisdiction in candidates:
                if candidate_issue.id == issue.id:
                    continue
                
                if candidate_issue.id in self.issue_embeddings:
                    candidate_embedding = self.issue_embeddings[candidate_issue.id]
                    similarity = cosine_similarity(
                        query_embedding.reshape(1, -1),
                        candidate_embedding.reshape(1, -1)
                    )[0][0]
                    
                    if similarity >= threshold:
                        similar.append((citation, candidate_issue, jurisdiction, similarity))
        else:
            # Keyword-based matching
            query_keywords = set(issue.keywords)
            query_statutes = set(issue.statutes)
            
            for citation, candidate_issue, jurisdiction in candidates:
                if candidate_issue.id == issue.id:
                    continue
                
                # Calculate overlap
                keyword_overlap = len(query_keywords & set(candidate_issue.keywords))
                statute_overlap = len(query_statutes & set(candidate_issue.statutes))
                
                # Simple similarity score
                total = len(query_keywords) + len(query_statutes)
                if total > 0:
                    similarity = (keyword_overlap + statute_overlap * 2) / (total + 2)
                    
                    if similarity >= threshold * 0.5:  # Lower threshold for keyword matching
                        similar.append((citation, candidate_issue, jurisdiction, similarity))
        
        # Sort by similarity
        similar.sort(key=lambda x: -x[3])
        
        return similar
    
    def map_issue_across_jurisdictions(
        self,
        issue: LegalIssue,
        source_citation: str,
        source_jurisdiction: str
    ) -> IssueMapping:
        """
        Map an issue across all jurisdictions.
        
        Args:
            issue: The legal issue to map
            source_citation: Citation of the source case
            source_jurisdiction: Jurisdiction of the source case
            
        Returns:
            IssueMapping with positions from all relevant jurisdictions
        """
        positions = {}
        
        # Add source position
        source_case = None
        for case in self.cases_by_jurisdiction[source_jurisdiction]:
            if case.extraction and case.extraction.citation == source_citation:
                source_case = case
                break
        
        if source_case:
            positions[source_jurisdiction] = JurisdictionPosition(
                jurisdiction=source_jurisdiction,
                citation=source_citation,
                holding=source_case.extraction.holding if source_case.extraction else '',
                confidence=1.0,
                date=source_case.case_data.get('date', ''),
                statutes=issue.statutes
            )
        
        # Find similar issues in other jurisdictions
        similar_issues = self.find_similar_issues(issue, issue.category)
        
        for citation, similar_issue, jurisdiction, similarity in similar_issues:
            if jurisdiction == source_jurisdiction:
                continue
            
            if jurisdiction not in positions:
                # Find the case to get holding
                case_holding = ''
                case_date = ''
                for case in self.cases_by_jurisdiction[jurisdiction]:
                    if case.extraction and case.extraction.citation == citation:
                        case_holding = case.extraction.holding
                        case_date = case.case_data.get('date', '')
                        break
                
                positions[jurisdiction] = JurisdictionPosition(
                    jurisdiction=jurisdiction,
                    citation=citation,
                    holding=case_holding,
                    confidence=similarity,
                    date=case_date,
                    statutes=similar_issue.statutes
                )
        
        # Determine match type and gaps
        covered_jurisdictions = set(positions.keys())
        gap_jurisdictions = [j for j in self.ALL_JURISDICTIONS if j not in covered_jurisdictions]
        
        if len(positions) <= 1:
            match_type = MatchType.SINGLE
        else:
            # Analyze holdings for consensus/conflict
            match_type, consensus_points, conflict_points = self._analyze_positions(positions)
        
        if len(positions) <= 1:
            consensus_points = []
            conflict_points = []
        
        self.stats['issues_mapped'] += 1
        if match_type == MatchType.CONSENSUS:
            self.stats['consensus_found'] += 1
        elif match_type == MatchType.CONFLICT:
            self.stats['conflicts_found'] += 1
        self.stats['gaps_identified'] += len(gap_jurisdictions)
        
        return IssueMapping(
            issue_id=issue.id,
            normalized_issue=issue.normalized_text,
            category=issue.category,
            positions=positions,
            match_type=match_type,
            consensus_points=consensus_points if len(positions) > 1 else [],
            conflict_points=conflict_points if len(positions) > 1 else [],
            gap_jurisdictions=gap_jurisdictions,
            keywords=issue.keywords
        )
    
    def _analyze_positions(
        self, 
        positions: Dict[str, JurisdictionPosition]
    ) -> Tuple[MatchType, List[str], List[str]]:
        """
        Analyze positions to determine consensus/conflict.
        
        This is a simplified analysis. In production, you'd use
        more sophisticated NLP to compare holdings.
        """
        holdings = [p.holding.lower() for p in positions.values() if p.holding]
        
        if len(holdings) < 2:
            return MatchType.SINGLE, [], []
        
        consensus_points = []
        conflict_points = []
        
        # Simple keyword-based analysis
        # Check for common positive/negative indicators
        allowed_keywords = ['allowed', 'granted', 'accepted', 'upheld', 'affirmed']
        dismissed_keywords = ['dismissed', 'rejected', 'denied', 'refused', 'overruled']
        
        allowed_count = sum(1 for h in holdings if any(kw in h for kw in allowed_keywords))
        dismissed_count = sum(1 for h in holdings if any(kw in h for kw in dismissed_keywords))
        
        total = len(holdings)
        
        if allowed_count == total or dismissed_count == total:
            match_type = MatchType.CONSENSUS
            if allowed_count == total:
                consensus_points.append("All jurisdictions allowed/upheld the claim")
            else:
                consensus_points.append("All jurisdictions dismissed/rejected the claim")
        elif (allowed_count > 0 and dismissed_count > 0):
            match_type = MatchType.CONFLICT
            if allowed_count > 0:
                conflict_points.append(f"{allowed_count} jurisdiction(s) allowed the claim")
            if dismissed_count > 0:
                conflict_points.append(f"{dismissed_count} jurisdiction(s) dismissed the claim")
        else:
            match_type = MatchType.PARTIAL
        
        return match_type, consensus_points, conflict_points
    
    def get_issue_mappings(self, category: str = None, limit: int = 100) -> List[IssueMapping]:
        """
        Get issue mappings for a category or all categories.
        
        Args:
            category: Optional category filter
            limit: Maximum number of mappings to return
            
        Returns:
            List of IssueMappings
        """
        mappings = []
        seen_issues = set()
        
        if category:
            categories = [category]
        else:
            categories = list(self.issues_by_category.keys())
        
        for cat in categories:
            for citation, issue, jurisdiction in self.issues_by_category.get(cat, []):
                if issue.id in seen_issues:
                    continue
                
                seen_issues.add(issue.id)
                
                mapping = self.map_issue_across_jurisdictions(issue, citation, jurisdiction)
                mappings.append(mapping)
                
                if len(mappings) >= limit:
                    break
            
            if len(mappings) >= limit:
                break
        
        return mappings
    
    def get_conflicts(self, limit: int = 50) -> List[IssueMapping]:
        """Get all identified conflicts"""
        mappings = self.get_issue_mappings(limit=1000)
        conflicts = [m for m in mappings if m.match_type == MatchType.CONFLICT]
        return conflicts[:limit]
    
    def get_consensus(self, limit: int = 50) -> List[IssueMapping]:
        """Get all identified consensus positions"""
        mappings = self.get_issue_mappings(limit=1000)
        consensus = [m for m in mappings if m.match_type == MatchType.CONSENSUS]
        return consensus[:limit]
    
    def get_jurisdiction_coverage(self) -> Dict[str, Dict]:
        """Get coverage statistics per jurisdiction"""
        coverage = {}
        
        for jurisdiction in self.ALL_JURISDICTIONS:
            cases = self.cases_by_jurisdiction.get(jurisdiction, [])
            issues = sum(
                len(c.extraction.issues) if c.extraction else 0 
                for c in cases
            )
            
            categories = defaultdict(int)
            for case in cases:
                if case.extraction:
                    for issue in case.extraction.issues:
                        categories[issue.category] += 1
            
            coverage[jurisdiction] = {
                'cases': len(cases),
                'issues': issues,
                'categories': dict(categories)
            }
        
        return coverage
    
    def get_stats(self) -> Dict:
        """Get mapping statistics"""
        return {
            **self.stats,
            'classifier_stats': self.classifier.get_stats(),
            'extractor_stats': self.extractor.get_stats(),
            'jurisdiction_coverage': self.get_jurisdiction_coverage()
        }


def main():
    """Demonstrate jurisdiction mapping"""
    from jurisdiction_classifier import load_cases_from_directory
    
    data_dir = Path(__file__).parent / 'data_v2'
    
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        return
    
    print(f"Loading cases from {data_dir}...")
    cases = load_cases_from_directory(data_dir)
    print(f"Loaded {len(cases)} cases")
    
    # Initialize mapper
    mapper = JurisdictionMapper(use_semantic=SEMANTIC_AVAILABLE)
    
    # Process cases
    print("\nProcessing cases...")
    mapper.process_cases(cases)
    
    # Get statistics
    stats = mapper.get_stats()
    print("\n" + "="*60)
    print("JURISDICTION MAPPING STATISTICS")
    print("="*60)
    
    print(f"\nCases processed: {stats['cases_processed']}")
    print(f"Issues mapped: {stats['issues_mapped']}")
    print(f"Consensus found: {stats['consensus_found']}")
    print(f"Conflicts found: {stats['conflicts_found']}")
    
    print("\nJurisdiction Coverage:")
    for jurisdiction, info in stats['jurisdiction_coverage'].items():
        print(f"  {jurisdiction:15} {info['cases']:5} cases, {info['issues']:5} issues")
    
    # Get sample mappings
    print("\n" + "="*60)
    print("SAMPLE ISSUE MAPPINGS")
    print("="*60)
    
    mappings = mapper.get_issue_mappings(limit=5)
    
    for mapping in mappings:
        print(f"\nIssue: {mapping.normalized_issue[:80]}...")
        print(f"Category: {mapping.category}")
        print(f"Match Type: {mapping.match_type.value}")
        print(f"Jurisdictions: {len(mapping.positions)}")
        
        for jurisdiction, position in mapping.positions.items():
            print(f"  {jurisdiction}:")
            print(f"    Citation: {position.citation}")
            print(f"    Confidence: {position.confidence:.2f}")
        
        if mapping.gap_jurisdictions:
            print(f"Gaps: {', '.join(mapping.gap_jurisdictions)}")
        
        if mapping.conflict_points:
            print(f"Conflicts: {'; '.join(mapping.conflict_points)}")
    
    # Show conflicts
    print("\n" + "="*60)
    print("IDENTIFIED CONFLICTS")
    print("="*60)
    
    conflicts = mapper.get_conflicts(limit=3)
    for conflict in conflicts:
        print(f"\nIssue: {conflict.normalized_issue[:80]}...")
        print(f"Conflict points: {'; '.join(conflict.conflict_points)}")
        for j, p in conflict.positions.items():
            print(f"  {j}: {p.holding[:100]}..." if p.holding else f"  {j}: No holding extracted")
    
    # Save results
    output_file = data_dir / 'jurisdiction_mappings.json'
    
    def serialize_mapping(m: IssueMapping) -> Dict:
        return {
            'issue_id': m.issue_id,
            'normalized_issue': m.normalized_issue,
            'category': m.category,
            'positions': {
                k: asdict(v) for k, v in m.positions.items()
            },
            'match_type': m.match_type.value,
            'consensus_points': m.consensus_points,
            'conflict_points': m.conflict_points,
            'gap_jurisdictions': m.gap_jurisdictions,
            'keywords': m.keywords
        }
    
    output_data = {
        'stats': {
            'cases_processed': stats['cases_processed'],
            'issues_mapped': stats['issues_mapped'],
            'consensus_found': stats['consensus_found'],
            'conflicts_found': stats['conflicts_found'],
            'jurisdiction_coverage': stats['jurisdiction_coverage']
        },
        'mappings': [serialize_mapping(m) for m in mappings],
        'conflicts': [serialize_mapping(c) for c in conflicts]
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nMapping results saved to: {output_file}")


if __name__ == '__main__':
    main()
