#!/usr/bin/env python3
"""
Enhanced Vector Store for Qanoon AI Research Copilot
Implements intelligent chunking, contextual retrieval, and hybrid search.
"""

import json
import re
import hashlib
from pathlib import Path
from typing import Generator, Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, field
from html import unescape
from datetime import datetime
import pickle

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import numpy as np

# Optional: BM25 for hybrid search
try:
    from rank_bm25 import BM25Okapi
    HAS_BM25 = True
except ImportError:
    HAS_BM25 = False
    print("Warning: rank_bm25 not installed. Hybrid search disabled. Install with: pip install rank-bm25")


# Configuration
JSONL_PATH = Path("data_v2/all_cases.jsonl")
CHROMADB_PATH = Path("data_v2/chromadb_enhanced")
BM25_INDEX_PATH = Path("data_v2/bm25_index.pkl")
COLLECTION_NAME = "pakistan_cases_enhanced"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # 384 dims, good quality
BATCH_SIZE = 50  # Documents per batch for embedding


@dataclass
class Chunk:
    """Represents a chunk of legal text with metadata."""
    text: str
    citation: str
    case_name: str
    court: str
    date: str
    judges: List[str]
    chunk_type: str  # 'headnotes', 'intro', 'facts', 'arguments', 'holdings', 'conclusion'
    chunk_idx: int
    total_chunks: int
    char_start: int
    char_end: int
    paragraph_nums: List[int]
    statutes_cited: List[str] = field(default_factory=list)
    cases_cited: List[str] = field(default_factory=list)
    practice_areas: List[str] = field(default_factory=list)
    
    def to_metadata(self) -> Dict[str, Any]:
        """Convert to ChromaDB-compatible metadata."""
        return {
            'citation': self.citation[:200],
            'case_name': self.case_name[:200],
            'court': self.court[:200],
            'date': self.date[:50],
            'judges': '; '.join(self.judges)[:500],
            'chunk_type': self.chunk_type,
            'chunk_idx': self.chunk_idx,
            'total_chunks': self.total_chunks,
            'char_start': self.char_start,
            'char_end': self.char_end,
            'paragraph_nums': ','.join(map(str, self.paragraph_nums))[:100],
            'statutes_cited': '; '.join(self.statutes_cited[:10])[:500],
            'cases_cited': '; '.join(self.cases_cited[:10])[:500],
            'practice_areas': '; '.join(self.practice_areas[:5])[:200],
        }


class LegalTextCleaner:
    """Clean and normalize legal text."""
    
    @staticmethod
    def clean_html(text: str) -> str:
        """Remove HTML tags and clean up text."""
        if not text:
            return ""
        
        # Unescape HTML entities
        text = unescape(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Remove CSS/style content
        text = re.sub(r'\{[^}]+\}', ' ', text)
        
        # Clean up whitespace but preserve paragraph breaks
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\t+', ' ', text)
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'&[a-zA-Z]+;', ' ', text)
        
        return text.strip()
    
    @staticmethod
    def normalize_citations(text: str) -> str:
        """Normalize citation formats for consistency."""
        # Standard Pakistani citation formats
        patterns = [
            (r'(\d{4})\s+SCMR\s+(\d+)', r'\1 SCMR \2'),
            (r'(\d{4})\s+PLD\s+(\w+)\s+(\d+)', r'\1 PLD \2 \3'),
            (r'(\d{4})\s+CLC\s+(\d+)', r'\1 CLC \2'),
            (r'(\d{4})\s+MLD\s+(\d+)', r'\1 MLD \2'),
            (r'(\d{4})\s+YLR\s+(\d+)', r'\1 YLR \2'),
            (r'(\d{4})\s+PCr\.?LJ\s+(\d+)', r'\1 PCrLJ \2'),
            (r'(\d{4})\s+PTD\s+(\d+)', r'\1 PTD \2'),
            (r'(\d{4})\s+PLC\s+(\d+)', r'\1 PLC \2'),
        ]
        for pattern, replacement in patterns:
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        return text


class PracticeAreaClassifier:
    """Classify legal texts into practice areas."""
    
    PRACTICE_AREAS = {
        'constitutional': [
            'constitution', 'fundamental right', 'article 199', 'writ petition',
            'constitutional petition', 'article 184', 'article 185', 'vires'
        ],
        'criminal': [
            'murder', 'section 302', 'section 307', 'theft', 'robbery', 'dacoity',
            'fir', 'bail', 'acquittal', 'conviction', 'sentence', 'penal code',
            'criminal procedure', 'qatl', 'qisas', 'diyat', 'tazir'
        ],
        'civil': [
            'contract', 'specific performance', 'damages', 'tort', 'negligence',
            'civil procedure', 'suit', 'decree', 'injunction', 'declaration'
        ],
        'property': [
            'land', 'property', 'tenant', 'landlord', 'rent', 'eviction',
            'transfer of property', 'sale deed', 'mutation', 'partition',
            'easement', 'pre-emption'
        ],
        'family': [
            'divorce', 'khula', 'talaq', 'maintenance', 'custody', 'dower',
            'mehr', 'nikah', 'marriage', 'guardian', 'family court', 'iddat'
        ],
        'corporate': [
            'company', 'shareholder', 'director', 'winding up', 'partnership',
            'secp', 'securities', 'stock exchange', 'corporate'
        ],
        'tax': [
            'income tax', 'sales tax', 'customs', 'duty', 'fbr', 'taxation',
            'ptd', 'tax tribunal', 'assessment', 'tax recovery'
        ],
        'labor': [
            'employment', 'termination', 'wages', 'industrial dispute', 'nirc',
            'labor court', 'workman', 'employer', 'gratuity', 'pension'
        ],
        'banking': [
            'bank', 'loan', 'mortgage', 'banking court', 'recovery', 'cheque',
            'negotiable instrument', 'financial institution'
        ],
        'intellectual_property': [
            'trademark', 'copyright', 'patent', 'intellectual property', 'infringement'
        ],
        'administrative': [
            'service tribunal', 'civil servant', 'government service', 'pension',
            'seniority', 'promotion', 'disciplinary', 'service rules'
        ],
        'shariat': [
            'islamic', 'shariat', 'hudood', 'zina', 'qazf', 'federal shariat court',
            'injunctions of islam', 'quran', 'sunnah', 'shariah'
        ]
    }
    
    @classmethod
    def classify(cls, text: str) -> List[str]:
        """Classify text into practice areas based on keyword matching."""
        text_lower = text.lower()
        detected = []
        
        for area, keywords in cls.PRACTICE_AREAS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score >= 2:  # At least 2 keywords matched
                detected.append((area, score))
        
        # Return top 3 by score
        detected.sort(key=lambda x: x[1], reverse=True)
        return [area for area, _ in detected[:3]]


class IntelligentChunker:
    """
    Intelligent chunking for legal documents.
    Chunks by paragraph structure while respecting semantic boundaries.
    """
    
    # Section markers in legal judgments
    SECTION_PATTERNS = [
        (r'^JUDGMENT\b', 'intro'),
        (r'^FACTS[:\s]', 'facts'),
        (r'^BACKGROUND[:\s]', 'facts'),
        (r'^ARGUMENTS?\s*(OF|BY)', 'arguments'),
        (r'^CONTENTIONS?[:\s]', 'arguments'),
        (r'^SUBMISSIONS?[:\s]', 'arguments'),
        (r'^HELD[:\s]', 'holdings'),
        (r'^HOLDING[:\s]', 'holdings'),
        (r'^FINDINGS?[:\s]', 'holdings'),
        (r'^ORDER[:\s]', 'conclusion'),
        (r'^DISPOSITION[:\s]', 'conclusion'),
        (r'^CONCLUSION[:\s]', 'conclusion'),
        (r'^HEADNOTES?[:\s]', 'headnotes'),
    ]
    
    def __init__(self, min_chunk_size: int = 500, max_chunk_size: int = 2000, 
                 overlap_sentences: int = 2):
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.overlap_sentences = overlap_sentences
    
    def detect_section_type(self, text: str) -> str:
        """Detect the type of legal section from text."""
        text_start = text[:100].upper()
        for pattern, section_type in self.SECTION_PATTERNS:
            if re.search(pattern, text_start, re.IGNORECASE):
                return section_type
        return 'body'
    
    def split_into_paragraphs(self, text: str) -> List[Tuple[str, int, int]]:
        """
        Split text into paragraphs with their character positions.
        Returns: List of (paragraph_text, start_pos, end_pos)
        """
        paragraphs = []
        
        # Split by double newlines or numbered paragraphs
        parts = re.split(r'\n\s*\n|\n(?=\d+\.\s)', text)
        
        current_pos = 0
        for part in parts:
            part = part.strip()
            if part:
                # Find actual position in original text
                start = text.find(part, current_pos)
                if start == -1:
                    start = current_pos
                end = start + len(part)
                paragraphs.append((part, start, end))
                current_pos = end
        
        return paragraphs
    
    def chunk_document(self, headnotes: str, judgment: str, 
                       case_metadata: Dict) -> List[Chunk]:
        """
        Chunk a legal document into semantic chunks.
        """
        chunks = []
        chunk_idx = 0
        
        cleaner = LegalTextCleaner()
        classifier = PracticeAreaClassifier()
        
        # Extract common metadata
        citation = case_metadata.get('citation', '')
        case_name = case_metadata.get('case_name', '')
        court = cleaner.clean_html(case_metadata.get('court', ''))
        date = case_metadata.get('date', '')
        judges = case_metadata.get('judges', [])
        if isinstance(judges, str):
            judges = [judges]
        statutes = case_metadata.get('statutes_cited', [])
        cases_cited = case_metadata.get('cases_cited', [])
        
        # Clean texts
        headnotes_clean = cleaner.clean_html(headnotes)
        judgment_clean = cleaner.clean_html(judgment)
        
        full_text = f"{headnotes_clean}\n\n{judgment_clean}"
        practice_areas = classifier.classify(full_text)
        
        # Process headnotes as a single chunk (usually summary)
        if headnotes_clean and len(headnotes_clean) > 100:
            chunks.append(Chunk(
                text=headnotes_clean[:self.max_chunk_size],
                citation=citation,
                case_name=case_name,
                court=court,
                date=date,
                judges=judges,
                chunk_type='headnotes',
                chunk_idx=chunk_idx,
                total_chunks=0,  # Will be updated later
                char_start=0,
                char_end=len(headnotes_clean),
                paragraph_nums=[0],
                statutes_cited=statutes if isinstance(statutes, list) else [],
                cases_cited=cases_cited if isinstance(cases_cited, list) else [],
                practice_areas=practice_areas
            ))
            chunk_idx += 1
        
        # Split judgment into paragraphs
        paragraphs = self.split_into_paragraphs(judgment_clean)
        
        current_chunk_text = ""
        current_chunk_start = 0
        current_paragraphs = []
        paragraph_num = 1
        
        for para_text, para_start, para_end in paragraphs:
            # Detect section type
            section_type = self.detect_section_type(para_text)
            
            # Check if adding this paragraph would exceed max size
            if len(current_chunk_text) + len(para_text) > self.max_chunk_size:
                # Save current chunk if it has content
                if len(current_chunk_text) >= self.min_chunk_size:
                    chunks.append(Chunk(
                        text=current_chunk_text.strip(),
                        citation=citation,
                        case_name=case_name,
                        court=court,
                        date=date,
                        judges=judges,
                        chunk_type=section_type,
                        chunk_idx=chunk_idx,
                        total_chunks=0,
                        char_start=current_chunk_start,
                        char_end=para_start,
                        paragraph_nums=current_paragraphs.copy(),
                        statutes_cited=statutes if isinstance(statutes, list) else [],
                        cases_cited=cases_cited if isinstance(cases_cited, list) else [],
                        practice_areas=practice_areas
                    ))
                    chunk_idx += 1
                
                # Start new chunk (with overlap from last sentences)
                if self.overlap_sentences > 0 and current_chunk_text:
                    sentences = re.split(r'(?<=[.!?])\s+', current_chunk_text)
                    overlap = ' '.join(sentences[-self.overlap_sentences:])
                    current_chunk_text = overlap + '\n\n' + para_text
                else:
                    current_chunk_text = para_text
                
                current_chunk_start = para_start
                current_paragraphs = [paragraph_num]
            else:
                current_chunk_text += '\n\n' + para_text if current_chunk_text else para_text
                current_paragraphs.append(paragraph_num)
            
            paragraph_num += 1
        
        # Don't forget the last chunk
        if current_chunk_text and len(current_chunk_text) >= self.min_chunk_size:
            section_type = self.detect_section_type(current_chunk_text)
            chunks.append(Chunk(
                text=current_chunk_text.strip(),
                citation=citation,
                case_name=case_name,
                court=court,
                date=date,
                judges=judges,
                chunk_type=section_type,
                chunk_idx=chunk_idx,
                total_chunks=0,
                char_start=current_chunk_start,
                char_end=len(judgment_clean),
                paragraph_nums=current_paragraphs,
                statutes_cited=statutes if isinstance(statutes, list) else [],
                cases_cited=cases_cited if isinstance(cases_cited, list) else [],
                practice_areas=practice_areas
            ))
        
        # Update total_chunks for all
        for chunk in chunks:
            chunk.total_chunks = len(chunks)
        
        return chunks


class ContextualRetrieval:
    """
    Implements contextual retrieval by prepending context to chunks.
    This helps embeddings capture document-level context.
    """
    
    CONTEXT_TEMPLATE = """Case: {citation}
Court: {court}
Date: {date}
Topic: {practice_areas}
Section: {chunk_type}

{text}"""
    
    @classmethod
    def create_contextual_text(cls, chunk: Chunk) -> str:
        """Create text with prepended context for embedding."""
        return cls.CONTEXT_TEMPLATE.format(
            citation=chunk.citation,
            court=chunk.court,
            date=chunk.date,
            practice_areas=', '.join(chunk.practice_areas) if chunk.practice_areas else 'General',
            chunk_type=chunk.chunk_type.title(),
            text=chunk.text
        )


class HybridSearchEngine:
    """
    Hybrid search combining dense (semantic) and sparse (BM25) retrieval.
    """
    
    def __init__(self, chromadb_path: Path, bm25_path: Path, 
                 collection_name: str, embedding_model: str):
        self.chromadb_path = chromadb_path
        self.bm25_path = bm25_path
        self.collection_name = collection_name
        
        # Load embedding model
        self.embed_model = SentenceTransformer(embedding_model)
        
        # Connect to ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(chromadb_path),
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Pakistan case law with contextual embeddings"}
        )
        
        # Load BM25 index if exists
        self.bm25 = None
        self.bm25_corpus = None
        self.bm25_ids = None
        if HAS_BM25 and bm25_path.exists():
            self._load_bm25_index()
    
    def _load_bm25_index(self):
        """Load BM25 index from disk."""
        try:
            with open(self.bm25_path, 'rb') as f:
                data = pickle.load(f)
                self.bm25 = data['bm25']
                self.bm25_corpus = data['corpus']
                self.bm25_ids = data['ids']
            print(f"✅ Loaded BM25 index with {len(self.bm25_ids)} documents")
        except Exception as e:
            print(f"⚠️ Could not load BM25 index: {e}")
    
    def _save_bm25_index(self, corpus: List[List[str]], ids: List[str]):
        """Save BM25 index to disk."""
        if not HAS_BM25:
            return
        
        self.bm25 = BM25Okapi(corpus)
        self.bm25_corpus = corpus
        self.bm25_ids = ids
        
        with open(self.bm25_path, 'wb') as f:
            pickle.dump({
                'bm25': self.bm25,
                'corpus': corpus,
                'ids': ids
            }, f)
        print(f"✅ Saved BM25 index with {len(ids)} documents")
    
    def search(self, query: str, n_results: int = 10, 
               filters: Dict = None, 
               dense_weight: float = 0.7,
               sparse_weight: float = 0.3) -> List[Dict]:
        """
        Hybrid search combining dense and sparse retrieval.
        
        Args:
            query: Search query
            n_results: Number of results to return
            filters: ChromaDB where filters
            dense_weight: Weight for semantic search (0-1)
            sparse_weight: Weight for BM25 (0-1)
        
        Returns:
            List of results with scores
        """
        results = {}
        
        # Dense search (semantic)
        query_embedding = self.embed_model.encode(query).tolist()
        dense_results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results * 2,  # Get more for fusion
            where=filters,
            include=["documents", "metadatas", "distances"]
        )
        
        if dense_results['ids'][0]:
            max_dist = max(dense_results['distances'][0]) if dense_results['distances'][0] else 1
            for i, doc_id in enumerate(dense_results['ids'][0]):
                # Convert distance to similarity (0-1)
                similarity = 1 - (dense_results['distances'][0][i] / max_dist) if max_dist > 0 else 0
                results[doc_id] = {
                    'id': doc_id,
                    'text': dense_results['documents'][0][i],
                    'metadata': dense_results['metadatas'][0][i],
                    'dense_score': similarity * dense_weight,
                    'sparse_score': 0,
                    'final_score': similarity * dense_weight
                }
        
        # Sparse search (BM25) if available
        if HAS_BM25 and self.bm25 is not None:
            tokenized_query = query.lower().split()
            bm25_scores = self.bm25.get_scores(tokenized_query)
            
            # Normalize BM25 scores
            max_score = max(bm25_scores) if max(bm25_scores) > 0 else 1
            
            # Get top N from BM25
            top_indices = np.argsort(bm25_scores)[-n_results * 2:][::-1]
            
            for idx in top_indices:
                doc_id = self.bm25_ids[idx]
                norm_score = bm25_scores[idx] / max_score
                
                if doc_id in results:
                    results[doc_id]['sparse_score'] = norm_score * sparse_weight
                    results[doc_id]['final_score'] += norm_score * sparse_weight
                else:
                    # Need to fetch from ChromaDB
                    try:
                        doc_data = self.collection.get(ids=[doc_id], include=["documents", "metadatas"])
                        if doc_data['ids']:
                            results[doc_id] = {
                                'id': doc_id,
                                'text': doc_data['documents'][0] if doc_data['documents'] else '',
                                'metadata': doc_data['metadatas'][0] if doc_data['metadatas'] else {},
                                'dense_score': 0,
                                'sparse_score': norm_score * sparse_weight,
                                'final_score': norm_score * sparse_weight
                            }
                    except:
                        pass
        
        # Sort by final score and return top N
        sorted_results = sorted(results.values(), key=lambda x: x['final_score'], reverse=True)
        return sorted_results[:n_results]


class EnhancedVectorStore:
    """Main class for the enhanced vector store."""
    
    def __init__(self, jsonl_path: Path = JSONL_PATH, 
                 chromadb_path: Path = CHROMADB_PATH,
                 bm25_path: Path = BM25_INDEX_PATH,
                 collection_name: str = COLLECTION_NAME,
                 embedding_model: str = EMBEDDING_MODEL):
        self.jsonl_path = jsonl_path
        self.chromadb_path = chromadb_path
        self.bm25_path = bm25_path
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        
        self.chunker = IntelligentChunker()
        self.embed_model = None
        self.client = None
        self.collection = None
    
    def load_cases(self) -> Generator[dict, None, None]:
        """Load cases from JSONL file."""
        with open(self.jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    
    def build_index(self, force_rebuild: bool = False):
        """Build the enhanced vector index."""
        print("=" * 60)
        print("Enhanced Vector Store Builder")
        print("Qanoon AI Research Copilot")
        print("=" * 60)
        
        # Initialize embedding model
        print(f"\n📦 Loading embedding model: {self.embedding_model}")
        self.embed_model = SentenceTransformer(self.embedding_model)
        
        # Initialize ChromaDB
        print(f"🗄️  Initializing ChromaDB at: {self.chromadb_path}")
        self.chromadb_path.mkdir(parents=True, exist_ok=True)
        
        self.client = chromadb.PersistentClient(
            path=str(self.chromadb_path),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Delete existing collection if force rebuild
        if force_rebuild:
            try:
                self.client.delete_collection(self.collection_name)
                print(f"   Deleted existing collection: {self.collection_name}")
            except:
                pass
        
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"description": "Pakistan case law with contextual embeddings"}
        )
        
        # Check if already has data
        existing_count = self.collection.count()
        if existing_count > 0 and not force_rebuild:
            print(f"   Collection already has {existing_count} documents. Use force_rebuild=True to rebuild.")
            return
        
        # Count total cases
        print(f"\n📄 Loading cases from: {self.jsonl_path}")
        total_cases = sum(1 for _ in self.load_cases())
        print(f"   Found {total_cases} cases to process")
        
        # Process cases
        batch_ids = []
        batch_texts = []
        batch_metadatas = []
        batch_embeddings = []
        
        bm25_corpus = []
        bm25_ids = []
        
        total_chunks = 0
        cases_processed = 0
        
        print(f"\n🔄 Processing and embedding documents with contextual retrieval...")
        
        for case in tqdm(self.load_cases(), total=total_cases, desc="Cases"):
            # Get content
            headnotes = case.get('headnotes', '')
            judgment = case.get('judgment_clean', '') or case.get('judgment', '')
            
            if not judgment and not headnotes:
                continue
            
            # Chunk the document
            chunks = self.chunker.chunk_document(headnotes, judgment, case)
            
            for chunk in chunks:
                # Create contextual text for embedding
                contextual_text = ContextualRetrieval.create_contextual_text(chunk)
                
                # Generate unique ID
                doc_id = f"{chunk.citation}__chunk_{chunk.chunk_idx}".replace(' ', '_')
                doc_id = re.sub(r'[^\w\-_.]', '_', doc_id)
                
                # Generate embedding
                embedding = self.embed_model.encode(contextual_text, show_progress_bar=False).tolist()
                
                batch_ids.append(doc_id)
                batch_texts.append(chunk.text)  # Store original text, not contextual
                batch_metadatas.append(chunk.to_metadata())
                batch_embeddings.append(embedding)
                
                # For BM25
                if HAS_BM25:
                    bm25_corpus.append(chunk.text.lower().split())
                    bm25_ids.append(doc_id)
                
                total_chunks += 1
                
                # Insert batch when full
                if len(batch_ids) >= BATCH_SIZE:
                    self.collection.add(
                        ids=batch_ids,
                        documents=batch_texts,
                        metadatas=batch_metadatas,
                        embeddings=batch_embeddings
                    )
                    batch_ids = []
                    batch_texts = []
                    batch_metadatas = []
                    batch_embeddings = []
            
            cases_processed += 1
        
        # Insert remaining batch
        if batch_ids:
            self.collection.add(
                ids=batch_ids,
                documents=batch_texts,
                metadatas=batch_metadatas,
                embeddings=batch_embeddings
            )
        
        # Build BM25 index
        if HAS_BM25 and bm25_corpus:
            print(f"\n📚 Building BM25 index...")
            bm25 = BM25Okapi(bm25_corpus)
            with open(self.bm25_path, 'wb') as f:
                pickle.dump({
                    'bm25': bm25,
                    'corpus': bm25_corpus,
                    'ids': bm25_ids
                }, f)
            print(f"   Saved BM25 index to: {self.bm25_path}")
        
        print(f"\n✅ Enhanced index build complete!")
        print(f"   Cases processed: {cases_processed}")
        print(f"   Total chunks: {total_chunks}")
        print(f"   Collection count: {self.collection.count()}")
        print(f"   ChromaDB location: {self.chromadb_path.absolute()}")
        if HAS_BM25:
            print(f"   BM25 index location: {self.bm25_path.absolute()}")
    
    def get_search_engine(self) -> HybridSearchEngine:
        """Get the hybrid search engine."""
        return HybridSearchEngine(
            chromadb_path=self.chromadb_path,
            bm25_path=self.bm25_path,
            collection_name=self.collection_name,
            embedding_model=self.embedding_model
        )


def main():
    """Main entry point for building the enhanced index."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build enhanced vector store for legal search")
    parser.add_argument('--force', action='store_true', help='Force rebuild of index')
    parser.add_argument('--test', action='store_true', help='Test search after building')
    args = parser.parse_args()
    
    store = EnhancedVectorStore()
    store.build_index(force_rebuild=args.force)
    
    if args.test:
        print("\n" + "=" * 60)
        print("Testing Hybrid Search")
        print("=" * 60)
        
        engine = store.get_search_engine()
        
        test_queries = [
            "landlord eviction notice tenant",
            "fundamental rights constitutional petition",
            "Section 302 murder conviction appeal"
        ]
        
        for query in test_queries:
            print(f"\n🔍 Query: {query}")
            print("-" * 40)
            
            results = engine.search(query, n_results=3)
            
            for i, result in enumerate(results):
                print(f"\n[{i+1}] {result['metadata'].get('citation', 'N/A')}")
                print(f"    Score: {result['final_score']:.3f} (dense: {result['dense_score']:.3f}, sparse: {result['sparse_score']:.3f})")
                print(f"    Court: {result['metadata'].get('court', 'N/A')[:60]}")
                print(f"    Type: {result['metadata'].get('chunk_type', 'N/A')}")
                print(f"    Text: {result['text'][:150]}...")


if __name__ == "__main__":
    main()
