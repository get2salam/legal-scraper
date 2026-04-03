"""
Comparison Engine for Pakistani Legal Jurisdictions

Generates side-by-side comparisons of legal rulings across jurisdictions.
Supports natural language queries and structured output.
"""

import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from collections import defaultdict
import re

try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SEMANTIC_AVAILABLE = True
except ImportError:
    SEMANTIC_AVAILABLE = False

try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    print("Note: ChromaDB not installed. Using in-memory search only.")

from jurisdiction_classifier import Jurisdiction, JurisdictionClassifier, load_cases_from_directory
from issue_extractor import LegalIssueExtractor, LegalIssue
from jurisdiction_mapper import JurisdictionMapper, IssueMapping, JurisdictionPosition, MatchType


@dataclass
class JurisdictionHolding:
    """A jurisdiction's holding on a specific question"""
    jurisdiction: str
    holding: str
    citation: str
    date: str
    confidence: float
    statutes: List[str] = field(default_factory=list)
    judges: List[str] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Result of a jurisdiction comparison query"""
    query: str
    timestamp: str
    jurisdictions: Dict[str, JurisdictionHolding]
    consensus: List[str]
    conflicts: List[str]
    gaps: List[str]
    related_issues: List[str] = field(default_factory=list)
    total_cases_found: int = 0


class ComparisonEngine:
    """
    Engine for comparing legal rulings across Pakistani jurisdictions.
    
    Features:
    - Natural language query processing
    - Vector search for relevant cases (with ChromaDB)
    - Multi-jurisdiction comparison
    - Conflict and consensus detection
    """
    
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
    
    def __init__(
        self,
        data_dir: Optional[Path] = None,
        use_chromadb: bool = True,
        use_semantic: bool = True
    ):
        """
        Initialize the comparison engine.
        
        Args:
            data_dir: Path to data directory
            use_chromadb: Use ChromaDB for vector search
            use_semantic: Use semantic similarity matching
        """
        self.data_dir = data_dir or Path(__file__).parent / 'data_v2'
        
        self.classifier = JurisdictionClassifier()
        self.extractor = LegalIssueExtractor()
        
        self.use_semantic = use_semantic and SEMANTIC_AVAILABLE
        self.use_chromadb = use_chromadb and CHROMADB_AVAILABLE
        
        self.model = None
        self.chroma_client = None
        self.collection = None
        
        if self.use_semantic:
            print("Loading embedding model...")
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        if self.use_chromadb:
            print("Initializing ChromaDB...")
            self.chroma_client = chromadb.Client(Settings(
                anonymized_telemetry=False
            ))
            self.collection = self.chroma_client.get_or_create_collection(
                name="legal_issues",
                metadata={"hnsw:space": "cosine"}
            )
        
        # In-memory storage for cases
        self.cases: Dict[str, Dict] = {}  # citation -> case data
        self.case_jurisdictions: Dict[str, str] = {}  # citation -> jurisdiction
        self.issue_index: Dict[str, List[str]] = defaultdict(list)  # keyword -> citations
        self.loaded = False
    
    def load_data(self):
        """Load and index all case data"""
        if self.loaded:
            return
        
        print(f"Loading cases from {self.data_dir}...")
        cases = load_cases_from_directory(self.data_dir)
        print(f"Loaded {len(cases)} cases")
        
        print("Processing and indexing cases...")
        for i, case in enumerate(cases):
            if i % 100 == 0:
                print(f"  Processing {i+1}/{len(cases)}...")
            
            citation = case.get('citation', '')
            if not citation:
                continue
            
            # Store case
            self.cases[citation] = case
            
            # Classify jurisdiction
            classification = self.classifier.classify(case)
            self.case_jurisdictions[citation] = classification.jurisdiction.value
            
            # Extract issues and index
            extraction = self.extractor.extract(case, classification.jurisdiction.value)
            
            # Index by keywords
            for issue in extraction.issues:
                for keyword in issue.keywords:
                    self.issue_index[keyword.lower()].append(citation)
            
            # Index by statutes
            for statute in extraction.statutes_cited:
                self.issue_index[statute.lower()].append(citation)
            
            # Add to ChromaDB if available
            if self.use_chromadb and extraction.issues:
                combined_text = ' '.join([
                    issue.normalized_text for issue in extraction.issues
                ])
                
                self.collection.add(
                    documents=[combined_text],
                    metadatas=[{
                        'citation': citation,
                        'jurisdiction': classification.jurisdiction.value,
                        'category': extraction.issues[0].category if extraction.issues else 'general',
                        'holding': extraction.holding[:500] if extraction.holding else ''
                    }],
                    ids=[citation]
                )
        
        self.loaded = True
        print(f"Indexed {len(self.cases)} cases across {len(self.issue_index)} keywords")
    
    def search_by_query(
        self,
        query: str,
        jurisdiction: Optional[str] = None,
        limit: int = 50
    ) -> List[Tuple[str, float]]:
        """
        Search for cases relevant to a query.
        
        Args:
            query: Natural language query
            jurisdiction: Optional jurisdiction filter
            limit: Maximum results
            
        Returns:
            List of (citation, relevance_score) tuples
        """
        results = []
        
        if self.use_chromadb and self.collection:
            # Vector search
            where_filter = None
            if jurisdiction:
                where_filter = {"jurisdiction": jurisdiction}
            
            search_results = self.collection.query(
                query_texts=[query],
                n_results=limit,
                where=where_filter
            )
            
            if search_results and search_results['ids']:
                for i, citation in enumerate(search_results['ids'][0]):
                    # ChromaDB returns distances, convert to similarity
                    distance = search_results['distances'][0][i] if search_results.get('distances') else 0
                    similarity = 1 - distance
                    results.append((citation, similarity))
        
        else:
            # Keyword-based search
            query_lower = query.lower()
            keywords = re.findall(r'\b\w+\b', query_lower)
            
            citation_scores = defaultdict(float)
            
            for keyword in keywords:
                for indexed_keyword, citations in self.issue_index.items():
                    if keyword in indexed_keyword or indexed_keyword in keyword:
                        for citation in citations:
                            if jurisdiction and self.case_jurisdictions.get(citation) != jurisdiction:
                                continue
                            citation_scores[citation] += 1
            
            # Normalize scores
            max_score = max(citation_scores.values()) if citation_scores else 1
            results = [
                (citation, score / max_score)
                for citation, score in citation_scores.items()
            ]
        
        # Sort by score
        results.sort(key=lambda x: -x[1])
        return results[:limit]
    
    def get_jurisdiction_holding(
        self,
        query: str,
        jurisdiction: str,
        limit: int = 5
    ) -> Optional[JurisdictionHolding]:
        """
        Get the best holding for a jurisdiction on a query.
        
        Args:
            query: The legal question
            jurisdiction: Target jurisdiction
            limit: Number of cases to consider
            
        Returns:
            JurisdictionHolding or None
        """
        results = self.search_by_query(query, jurisdiction=jurisdiction, limit=limit)
        
        if not results:
            return None
        
        best_citation, best_score = results[0]
        case = self.cases.get(best_citation, {})
        
        # Extract holding
        extraction = self.extractor.extract(case, jurisdiction)
        
        return JurisdictionHolding(
            jurisdiction=jurisdiction,
            holding=extraction.holding or "Holding not extracted",
            citation=best_citation,
            date=case.get('date', ''),
            confidence=best_score,
            statutes=extraction.statutes_cited[:5],
            judges=case.get('judges', [])
        )
    
    def compare(self, query: str) -> ComparisonResult:
        """
        Compare rulings across all jurisdictions for a query.
        
        Args:
            query: Natural language legal question
            
        Returns:
            ComparisonResult with multi-jurisdiction comparison
        """
        if not self.loaded:
            self.load_data()
        
        print(f"Searching for: {query}")
        
        # Get holdings from each jurisdiction
        jurisdictions = {}
        total_cases = 0
        
        for jurisdiction in self.ALL_JURISDICTIONS:
            holding = self.get_jurisdiction_holding(query, jurisdiction)
            if holding:
                jurisdictions[jurisdiction] = holding
                total_cases += 1
        
        # Analyze consensus and conflicts
        consensus, conflicts = self._analyze_holdings(jurisdictions)
        
        # Identify gaps
        gaps = [j for j in self.ALL_JURISDICTIONS if j not in jurisdictions]
        
        # Find related issues
        all_results = self.search_by_query(query, limit=10)
        related_issues = []
        
        for citation, score in all_results[:5]:
            case = self.cases.get(citation, {})
            headnotes = case.get('headnotes', '')
            if headnotes:
                # Extract first substantive point
                first_point = headnotes.split('---')[0] if '---' in headnotes else headnotes[:200]
                if first_point and first_point not in related_issues:
                    related_issues.append(first_point.strip()[:100])
        
        return ComparisonResult(
            query=query,
            timestamp=datetime.now().isoformat(),
            jurisdictions=jurisdictions,
            consensus=consensus,
            conflicts=conflicts,
            gaps=[f"No ruling found from {j}" for j in gaps],
            related_issues=related_issues,
            total_cases_found=total_cases
        )
    
    def _analyze_holdings(
        self,
        jurisdictions: Dict[str, JurisdictionHolding]
    ) -> Tuple[List[str], List[str]]:
        """
        Analyze holdings across jurisdictions for consensus/conflicts.
        """
        if len(jurisdictions) < 2:
            return [], []
        
        consensus = []
        conflicts = []
        
        holdings = {j: h.holding.lower() for j, h in jurisdictions.items()}
        
        # Check for common outcomes
        allowed_jurisdictions = []
        dismissed_jurisdictions = []
        
        allowed_keywords = ['allowed', 'granted', 'accepted', 'upheld', 'affirmed', 'successful']
        dismissed_keywords = ['dismissed', 'rejected', 'denied', 'refused', 'unsuccessful', 'overruled']
        
        for j, holding in holdings.items():
            if any(kw in holding for kw in allowed_keywords):
                allowed_jurisdictions.append(j)
            elif any(kw in holding for kw in dismissed_keywords):
                dismissed_jurisdictions.append(j)
        
        # Determine consensus/conflict
        if len(allowed_jurisdictions) >= len(holdings) - 1 and len(allowed_jurisdictions) > 1:
            consensus.append(f"Most jurisdictions ({', '.join(allowed_jurisdictions)}) favor the petitioner/claimant")
        elif len(dismissed_jurisdictions) >= len(holdings) - 1 and len(dismissed_jurisdictions) > 1:
            consensus.append(f"Most jurisdictions ({', '.join(dismissed_jurisdictions)}) reject the claim")
        
        if allowed_jurisdictions and dismissed_jurisdictions:
            conflicts.append(
                f"Conflict: {', '.join(allowed_jurisdictions)} allow while "
                f"{', '.join(dismissed_jurisdictions)} dismiss"
            )
        
        # Check for common statutes
        statutes_by_jurisdiction = {
            j: set(h.statutes) for j, h in jurisdictions.items()
        }
        
        if statutes_by_jurisdiction:
            common_statutes = set.intersection(*statutes_by_jurisdiction.values()) if len(statutes_by_jurisdiction) > 1 else set()
            if common_statutes:
                consensus.append(f"All jurisdictions cite: {', '.join(list(common_statutes)[:3])}")
        
        return consensus, conflicts
    
    def get_jurisdiction_stats(self, jurisdiction: str) -> Dict:
        """
        Get statistics for a specific jurisdiction.
        """
        if not self.loaded:
            self.load_data()
        
        cases = [
            c for c, j in self.case_jurisdictions.items()
            if j == jurisdiction
        ]
        
        categories = defaultdict(int)
        statutes = defaultdict(int)
        
        for citation in cases:
            case = self.cases.get(citation, {})
            extraction = self.extractor.extract(case, jurisdiction)
            
            for issue in extraction.issues:
                categories[issue.category] += 1
            
            for statute in extraction.statutes_cited:
                statutes[statute] += 1
        
        return {
            'jurisdiction': jurisdiction,
            'total_cases': len(cases),
            'categories': dict(sorted(categories.items(), key=lambda x: -x[1])[:10]),
            'top_statutes': dict(sorted(statutes.items(), key=lambda x: -x[1])[:10])
        }
    
    def format_comparison_as_json(self, result: ComparisonResult) -> Dict:
        """Format comparison result as JSON-serializable dict"""
        return {
            'question': result.query,
            'timestamp': result.timestamp,
            'jurisdictions': {
                j: {
                    'holding': h.holding,
                    'citation': h.citation,
                    'date': h.date,
                    'confidence': h.confidence,
                    'statutes': h.statutes,
                    'judges': h.judges
                }
                for j, h in result.jurisdictions.items()
            },
            'consensus': result.consensus,
            'conflicts': result.conflicts,
            'gaps': result.gaps,
            'related_issues': result.related_issues,
            'total_cases_found': result.total_cases_found
        }
    
    def format_comparison_as_text(self, result: ComparisonResult) -> str:
        """Format comparison result as readable text"""
        lines = []
        lines.append("=" * 70)
        lines.append(f"QUERY: {result.query}")
        lines.append("=" * 70)
        lines.append(f"Timestamp: {result.timestamp}")
        lines.append(f"Cases found: {result.total_cases_found}")
        lines.append("")
        
        lines.append("JURISDICTION HOLDINGS:")
        lines.append("-" * 70)
        
        for j, h in result.jurisdictions.items():
            lines.append(f"\n{j}:")
            lines.append(f"  Citation: {h.citation}")
            lines.append(f"  Date: {h.date}")
            lines.append(f"  Confidence: {h.confidence:.2f}")
            lines.append(f"  Holding: {h.holding[:300]}...")
            if h.statutes:
                lines.append(f"  Statutes: {', '.join(h.statutes[:3])}")
        
        if result.consensus:
            lines.append("\nCONSENSUS:")
            for c in result.consensus:
                lines.append(f"  • {c}")
        
        if result.conflicts:
            lines.append("\nCONFLICTS:")
            for c in result.conflicts:
                lines.append(f"  ⚠ {c}")
        
        if result.gaps:
            lines.append("\nGAPS:")
            for g in result.gaps:
                lines.append(f"  ○ {g}")
        
        return "\n".join(lines)


def main():
    """Demonstrate the comparison engine"""
    
    engine = ComparisonEngine()
    engine.load_data()
    
    # Sample queries
    queries = [
        "What is the limitation period for breach of contract?",
        "Is bail allowed in murder cases?",
        "What constitutes specific performance of contract?",
    ]
    
    for query in queries:
        print("\n" + "=" * 70)
        result = engine.compare(query)
        print(engine.format_comparison_as_text(result))
    
    # Save results
    output_file = engine.data_dir / 'comparison_results.json'
    
    results = [engine.format_comparison_as_json(engine.compare(q)) for q in queries]
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'queries': queries,
            'results': results
        }, f, indent=2)
    
    print(f"\n\nResults saved to: {output_file}")
    
    # Show jurisdiction stats
    print("\n" + "=" * 70)
    print("JURISDICTION STATISTICS")
    print("=" * 70)
    
    for j in engine.ALL_JURISDICTIONS[:3]:  # Just show first 3
        stats = engine.get_jurisdiction_stats(j)
        print(f"\n{j}: {stats['total_cases']} cases")
        print(f"  Top categories: {list(stats['categories'].keys())[:3]}")


if __name__ == '__main__':
    main()
