#!/usr/bin/env python3
"""
Precedent Matcher for Pakistani Legal Research Platform
Matches draft petitions to relevant case precedents using semantic search.

Features:
- Semantic similarity using sentence-transformers
- ChromaDB vector search
- Filter by court level, outcome, date, practice area
- Rank by relevance and recency
- Return supporting and contrary precedents
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from collections import defaultdict
import hashlib

try:
    import chromadb
    from chromadb.config import Settings
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False


# Court hierarchy for ranking
COURT_HIERARCHY = {
    'supreme court': 5,
    'supreme': 5,
    'sc': 5,
    'federal shariat court': 4,
    'shariat court': 4,
    'fsc': 4,
    'high court': 3,
    'lahore high court': 3,
    'sindh high court': 3,
    'peshawar high court': 3,
    'balochistan high court': 3,
    'islamabad high court': 3,
    'lahore': 3,
    'sindh': 3,
    'peshawar': 3,
    'balochistan': 3,
    'islamabad': 3,
    'service tribunal': 2,
    'tribunal': 2,
    'district court': 1,
    'civil court': 1,
    'session court': 1,
    'sessions': 1,
}

# Reporter abbreviations to full names
REPORTERS = {
    'SCMR': 'Supreme Court Monthly Review',
    'PLD': 'Pakistan Legal Decisions',
    'CLC': 'Civil Law Cases',
    'PCrLJ': 'Pakistan Criminal Law Journal',
    'PTD': 'Pakistan Tax Decisions',
    'PLC': 'Pakistan Labour Cases',
    'MLD': 'Monthly Law Digest',
    'YLR': 'Yearly Law Reports',
    'NLR': 'National Law Reports',
}

# Configuration
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
CHROMADB_PATH = Path("data_v2/chromadb")
COLLECTION_NAME = "pakistan_cases"


@dataclass
class Precedent:
    """A matched precedent case."""
    citation: str
    title: str
    court: str
    date: str
    outcome: str
    relevance_score: float
    summary: str
    headnotes: str = ""
    matched_provisions: List[str] = None
    court_level: int = 0
    
    def __post_init__(self):
        if self.matched_provisions is None:
            self.matched_provisions = []


@dataclass
class MatchResult:
    """Result of precedent matching."""
    supporting_precedents: List[Precedent]
    contrary_precedents: List[Precedent]
    neutral_precedents: List[Precedent]
    query_summary: str
    total_matches: int
    filters_applied: Dict


class PrecedentMatcher:
    """
    Matches draft petitions to relevant precedents.
    """
    
    def __init__(self, 
                 chromadb_path: Path = CHROMADB_PATH,
                 collection_name: str = COLLECTION_NAME,
                 embedding_model: str = EMBEDDING_MODEL,
                 use_gpu: bool = False):
        """
        Initialize the matcher.
        
        Args:
            chromadb_path: Path to ChromaDB directory
            collection_name: Name of the collection
            embedding_model: Sentence transformer model name
            use_gpu: Whether to use GPU for embeddings
        """
        self.chromadb_path = chromadb_path
        self.collection_name = collection_name
        self.embedding_model_name = embedding_model
        
        self._embedder = None
        self._client = None
        self._collection = None
        
        # Load cases index for fallback
        self.cases_index: Dict[str, Dict] = {}
        
    @property
    def embedder(self):
        """Lazy load sentence transformer model."""
        if self._embedder is None:
            if not TRANSFORMERS_AVAILABLE:
                raise ImportError("sentence-transformers required. Install with: pip install sentence-transformers")
            self._embedder = SentenceTransformer(self.embedding_model_name)
        return self._embedder
    
    @property
    def client(self):
        """Lazy load ChromaDB client."""
        if self._client is None:
            if not CHROMA_AVAILABLE:
                raise ImportError("chromadb required. Install with: pip install chromadb")
            
            self._client = chromadb.PersistentClient(
                path=str(self.chromadb_path),
                settings=Settings(anonymized_telemetry=False)
            )
        return self._client
    
    @property
    def collection(self):
        """Get or create the ChromaDB collection."""
        if self._collection is None:
            try:
                self._collection = self.client.get_collection(name=self.collection_name)
            except Exception:
                # Collection doesn't exist - need to create and populate
                self._collection = self.client.create_collection(
                    name=self.collection_name,
                    metadata={"hnsw:space": "cosine"}
                )
        return self._collection
    
    def load_cases_index(self, jsonl_path: Path) -> int:
        """
        Load cases into memory index for fast lookup.
        
        Args:
            jsonl_path: Path to JSONL file with cases
            
        Returns:
            Number of cases loaded
        """
        count = 0
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                case = json.loads(line)
                citation = case.get('citation', '')
                if citation:
                    self.cases_index[citation] = case
                    count += 1
        return count
    
    def _get_court_level(self, court: str) -> int:
        """Get court hierarchy level."""
        if not court:
            return 0
        court_lower = court.lower()
        for key, level in COURT_HIERARCHY.items():
            if key in court_lower:
                return level
        return 0
    
    def _extract_year(self, citation: str) -> Optional[int]:
        """Extract year from citation."""
        match = re.search(r'(\d{4})', citation)
        if match:
            return int(match.group(1))
        return None
    
    def _create_summary(self, case: Dict, max_length: int = 300) -> str:
        """Create a summary from case headnotes."""
        headnotes = case.get('headnotes', '')
        if not headnotes:
            headnotes = case.get('judgment_clean', '')[:500]
        
        # Clean and truncate
        summary = re.sub(r'\s+', ' ', headnotes).strip()
        if len(summary) > max_length:
            summary = summary[:max_length].rsplit(' ', 1)[0] + '...'
        
        return summary
    
    def _embed_text(self, text: str) -> List[float]:
        """Generate embedding for text."""
        return self.embedder.encode(text, normalize_embeddings=True).tolist()
    
    def _semantic_search(self, 
                         query: str, 
                         n_results: int = 50,
                         where: Optional[Dict] = None) -> List[Dict]:
        """
        Perform semantic search in ChromaDB.
        
        Args:
            query: Query text
            n_results: Number of results to return
            where: Filter conditions
            
        Returns:
            List of matching cases with scores
        """
        # Generate query embedding
        query_embedding = self._embed_text(query)
        
        # Build query params
        query_params = {
            "query_embeddings": [query_embedding],
            "n_results": n_results,
        }
        
        if where:
            query_params["where"] = where
        
        # Execute search
        results = self.collection.query(**query_params)
        
        # Process results
        matches = []
        if results and results['ids'] and results['ids'][0]:
            for i, doc_id in enumerate(results['ids'][0]):
                distance = results['distances'][0][i] if results['distances'] else 0
                metadata = results['metadatas'][0][i] if results['metadatas'] else {}
                document = results['documents'][0][i] if results['documents'] else ""
                
                # Convert distance to similarity score (cosine)
                similarity = 1 - distance
                
                matches.append({
                    'id': doc_id,
                    'score': similarity,
                    'metadata': metadata,
                    'document': document,
                })
        
        return matches
    
    def _keyword_search(self, 
                        query: str, 
                        cases: List[Dict],
                        n_results: int = 50) -> List[Dict]:
        """
        Fallback keyword search when ChromaDB is not available.
        
        Args:
            query: Query text
            cases: List of cases to search
            n_results: Number of results
            
        Returns:
            Matched cases with relevance scores
        """
        query_terms = set(query.lower().split())
        
        scores = []
        for case in cases:
            # Combine searchable text
            text = f"{case.get('title', '')} {case.get('headnotes', '')} {case.get('judgment_clean', '')[:2000]}"
            text_lower = text.lower()
            
            # Simple term frequency scoring
            score = sum(1 for term in query_terms if term in text_lower)
            score = score / len(query_terms) if query_terms else 0
            
            if score > 0:
                scores.append((case, score))
        
        # Sort by score
        scores.sort(key=lambda x: -x[1])
        
        return [{'case': c, 'score': s} for c, s in scores[:n_results]]
    
    def match(self,
              draft_text: str,
              provisions: Optional[List[str]] = None,
              court_filter: Optional[str] = None,
              date_from: Optional[str] = None,
              date_to: Optional[str] = None,
              outcome_filter: Optional[str] = None,
              n_results: int = 20,
              use_semantic: bool = True) -> MatchResult:
        """
        Match draft petition to relevant precedents.
        
        Args:
            draft_text: Draft petition text
            provisions: List of statutory provisions to filter by
            court_filter: Filter by court (e.g., "supreme", "high court")
            date_from: Filter cases from this date
            date_to: Filter cases until this date
            outcome_filter: Filter by outcome
            n_results: Number of results per category
            use_semantic: Whether to use semantic search
            
        Returns:
            MatchResult with supporting and contrary precedents
        """
        # Build filter conditions
        where_conditions = {}
        
        if court_filter:
            where_conditions["court"] = {"$contains": court_filter}
        
        if outcome_filter:
            where_conditions["outcome"] = outcome_filter
        
        filters_applied = {
            'court': court_filter,
            'date_from': date_from,
            'date_to': date_to,
            'outcome': outcome_filter,
            'provisions': provisions,
        }
        
        # Perform search
        all_matches = []
        
        if use_semantic and CHROMA_AVAILABLE and TRANSFORMERS_AVAILABLE:
            try:
                # Create query combining draft and provisions
                query = draft_text
                if provisions:
                    query = f"{' '.join(provisions)} {draft_text[:1000]}"
                
                matches = self._semantic_search(
                    query=query[:2000],  # Limit query length
                    n_results=n_results * 3,  # Get more for filtering
                    where=where_conditions if where_conditions else None
                )
                
                all_matches = matches
                
            except Exception as e:
                print(f"Semantic search failed: {e}, falling back to keyword search")
                use_semantic = False
        
        if not use_semantic or not all_matches:
            # Fallback to keyword search using cases index
            if self.cases_index:
                keyword_results = self._keyword_search(
                    query=draft_text[:1000],
                    cases=list(self.cases_index.values()),
                    n_results=n_results * 3
                )
                
                for result in keyword_results:
                    case = result['case']
                    all_matches.append({
                        'id': case.get('citation', ''),
                        'score': result['score'],
                        'metadata': case,
                        'document': case.get('headnotes', ''),
                    })
        
        # Categorize by outcome
        supporting = []
        contrary = []
        neutral = []
        
        for match in all_matches:
            metadata = match.get('metadata', {})
            citation = metadata.get('citation') or match.get('id', '')
            
            # Try to get full case data
            case_data = self.cases_index.get(citation, metadata)
            
            outcome = case_data.get('outcome', 'unknown').lower()
            court = case_data.get('court', '')
            
            precedent = Precedent(
                citation=citation,
                title=case_data.get('title', ''),
                court=court,
                date=case_data.get('date', ''),
                outcome=outcome,
                relevance_score=match.get('score', 0),
                summary=self._create_summary(case_data),
                headnotes=case_data.get('headnotes', '')[:500],
                court_level=self._get_court_level(court),
            )
            
            # Categorize
            if outcome in ['allowed', 'granted', 'accepted']:
                supporting.append(precedent)
            elif outcome in ['dismissed', 'rejected', 'declined']:
                contrary.append(precedent)
            else:
                neutral.append(precedent)
        
        # Sort each category by relevance and court level
        def sort_key(p: Precedent):
            return (-p.relevance_score, -p.court_level)
        
        supporting.sort(key=sort_key)
        contrary.sort(key=sort_key)
        neutral.sort(key=sort_key)
        
        return MatchResult(
            supporting_precedents=supporting[:n_results],
            contrary_precedents=contrary[:n_results],
            neutral_precedents=neutral[:n_results // 2],
            query_summary=draft_text[:200] + '...' if len(draft_text) > 200 else draft_text,
            total_matches=len(all_matches),
            filters_applied=filters_applied,
        )
    
    def find_similar_cases(self, 
                           citation: str, 
                           n_results: int = 10) -> List[Precedent]:
        """
        Find cases similar to a given case.
        
        Args:
            citation: Citation of the reference case
            n_results: Number of similar cases to return
            
        Returns:
            List of similar precedents
        """
        # Get the reference case
        ref_case = self.cases_index.get(citation)
        if not ref_case:
            return []
        
        # Create query from case
        query = f"{ref_case.get('title', '')} {ref_case.get('headnotes', '')[:1000]}"
        
        # Search
        result = self.match(
            draft_text=query,
            n_results=n_results + 1  # +1 to exclude self
        )
        
        # Combine all results, excluding the reference case
        all_precedents = (
            result.supporting_precedents + 
            result.contrary_precedents + 
            result.neutral_precedents
        )
        
        return [p for p in all_precedents if p.citation != citation][:n_results]


def initialize_chromadb(jsonl_path: Path, 
                        chromadb_path: Path = CHROMADB_PATH,
                        collection_name: str = COLLECTION_NAME,
                        batch_size: int = 100) -> int:
    """
    Initialize ChromaDB with case embeddings.
    
    Args:
        jsonl_path: Path to JSONL file with classified cases
        chromadb_path: Path to ChromaDB directory
        collection_name: Name of collection
        batch_size: Batch size for embedding
        
    Returns:
        Number of cases indexed
    """
    if not CHROMA_AVAILABLE or not TRANSFORMERS_AVAILABLE:
        raise ImportError("chromadb and sentence-transformers required")
    
    # Initialize
    embedder = SentenceTransformer(EMBEDDING_MODEL)
    client = chromadb.PersistentClient(
        path=str(chromadb_path),
        settings=Settings(anonymized_telemetry=False)
    )
    
    # Delete existing collection if exists
    try:
        client.delete_collection(collection_name)
    except Exception:
        pass
    
    collection = client.create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine"}
    )
    
    # Process cases in batches
    batch_ids = []
    batch_documents = []
    batch_metadatas = []
    batch_embeddings = []
    count = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            
            case = json.loads(line)
            citation = case.get('citation', '')
            
            if not citation:
                continue
            
            # Create document text for embedding
            doc_text = f"{case.get('title', '')} {case.get('headnotes', '')[:1500]}"
            
            # Generate embedding
            embedding = embedder.encode(doc_text, normalize_embeddings=True).tolist()
            
            # Prepare metadata
            metadata = {
                'citation': citation,
                'title': case.get('title', '')[:500],
                'court': case.get('court', ''),
                'date': case.get('date', ''),
                'outcome': case.get('outcome', 'unknown'),
            }
            
            # Generate unique ID
            doc_id = hashlib.md5(citation.encode()).hexdigest()
            
            batch_ids.append(doc_id)
            batch_documents.append(doc_text[:5000])
            batch_metadatas.append(metadata)
            batch_embeddings.append(embedding)
            count += 1
            
            # Insert batch
            if len(batch_ids) >= batch_size:
                collection.add(
                    ids=batch_ids,
                    documents=batch_documents,
                    metadatas=batch_metadatas,
                    embeddings=batch_embeddings,
                )
                batch_ids = []
                batch_documents = []
                batch_metadatas = []
                batch_embeddings = []
                print(f"Indexed {count} cases...")
    
    # Insert remaining
    if batch_ids:
        collection.add(
            ids=batch_ids,
            documents=batch_documents,
            metadatas=batch_metadatas,
            embeddings=batch_embeddings,
        )
    
    print(f"Total: {count} cases indexed")
    return count


def main():
    """CLI interface for precedent matching."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Match precedents to draft petitions')
    parser.add_argument('--init', '-i', action='store_true',
                       help='Initialize ChromaDB with case embeddings')
    parser.add_argument('--input', type=Path, default=Path('data_v2/cases_classified.jsonl'),
                       help='Input JSONL file for initialization')
    parser.add_argument('--query', '-q', type=str,
                       help='Query text to match')
    parser.add_argument('--file', '-f', type=Path,
                       help='Query file to match')
    parser.add_argument('--court', '-c', type=str,
                       help='Filter by court')
    parser.add_argument('--outcome', '-o', type=str,
                       help='Filter by outcome')
    parser.add_argument('--num', '-n', type=int, default=10,
                       help='Number of results')
    
    args = parser.parse_args()
    
    if args.init:
        print("Initializing ChromaDB...")
        count = initialize_chromadb(args.input)
        print(f"Done! Indexed {count} cases.")
        return
    
    # Load matcher
    matcher = PrecedentMatcher()
    
    # Load cases index
    if args.input.exists():
        print(f"Loading cases from {args.input}...")
        count = matcher.load_cases_index(args.input)
        print(f"Loaded {count} cases")
    
    # Get query text
    query_text = args.query
    if args.file and args.file.exists():
        query_text = args.file.read_text(encoding='utf-8')
    
    if not query_text:
        print("Please provide query text with --query or --file")
        return
    
    # Perform matching
    print(f"\nMatching precedents...")
    result = matcher.match(
        draft_text=query_text,
        court_filter=args.court,
        outcome_filter=args.outcome,
        n_results=args.num,
    )
    
    print(f"\n{'='*60}")
    print(f"Total matches: {result.total_matches}")
    print(f"Query: {result.query_summary}")
    
    print(f"\n--- Supporting Precedents ({len(result.supporting_precedents)}) ---")
    for p in result.supporting_precedents[:5]:
        print(f"\n  {p.citation} (Score: {p.relevance_score:.2f})")
        print(f"    Court: {p.court} | Outcome: {p.outcome}")
        print(f"    Summary: {p.summary[:150]}...")
    
    print(f"\n--- Contrary Precedents ({len(result.contrary_precedents)}) ---")
    for p in result.contrary_precedents[:5]:
        print(f"\n  {p.citation} (Score: {p.relevance_score:.2f})")
        print(f"    Court: {p.court} | Outcome: {p.outcome}")
        print(f"    Summary: {p.summary[:150]}...")


if __name__ == '__main__':
    main()
