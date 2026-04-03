"""
ChromaDB Importer
=================

Imports cases to ChromaDB for semantic search with embeddings.

Features:
- Text chunking with overlap
- Sentence-transformer embeddings
- Deduplication by citation
- Batch processing

Example:
    from qanoon.db import ChromaDBImporter
    
    importer = ChromaDBImporter()
    importer.import_all()
"""

import re
import json
import logging
from pathlib import Path
from html import unescape
from typing import Generator, Dict, List

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent.parent.parent / "data_v2"
JSONL_PATH = DATA_DIR / "all_cases.jsonl"
CHROMADB_PATH = DATA_DIR / "chromadb"
COLLECTION_NAME = "pakistan_cases"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200
BATCH_SIZE = 100


def clean_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not text:
        return ""
    
    text = unescape(text)
    
    try:
        text = text.encode().decode('unicode_escape', errors='ignore')
    except:
        pass
    
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\{[^}]+\}', ' ', text)
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'&[a-zA-Z]+;', ' ', text)
    
    return text.strip()


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks at sentence boundaries."""
    if not text or len(text) <= chunk_size:
        return [text] if text else []
    
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        if len(sentence) > chunk_size:
            if current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = ""
            
            for i in range(0, len(sentence), chunk_size - overlap):
                chunks.append(sentence[i:i + chunk_size].strip())
            continue
        
        if len(current_chunk) + len(sentence) + 1 > chunk_size:
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            if chunks and overlap > 0:
                last_chunk = chunks[-1]
                overlap_text = last_chunk[-overlap:] if len(last_chunk) > overlap else last_chunk
                overlap_start = overlap_text.rfind('. ')
                if overlap_start > 0:
                    overlap_text = overlap_text[overlap_start + 2:]
                current_chunk = overlap_text + " " + sentence
            else:
                current_chunk = sentence
        else:
            current_chunk = current_chunk + " " + sentence if current_chunk else sentence
    
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks


def normalize_citation(citation: str) -> str:
    """Normalize citation for deduplication."""
    if not citation:
        return ""
    normalized = re.sub(r'\s+', ' ', citation.strip().upper())
    return normalized


def load_cases(jsonl_path: Path, dedupe: bool = True) -> Generator[Dict, None, None]:
    """Load cases from JSONL file."""
    seen_citations = set()
    duplicates = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                case = json.loads(line)
                
                if dedupe:
                    citation = case.get('citation', '')
                    normalized = normalize_citation(citation)
                    if normalized in seen_citations:
                        duplicates += 1
                        continue
                    seen_citations.add(normalized)
                
                yield case
    
    if duplicates > 0:
        logger.info(f"Skipped {duplicates} duplicate cases")


class ChromaDBImporter:
    """
    Imports cases to ChromaDB for semantic search.
    
    Args:
        db_path: Path to ChromaDB storage
        collection_name: Name of the collection
        embedding_model: Sentence-transformer model name
    """
    
    def __init__(
        self,
        db_path: Path = None,
        collection_name: str = COLLECTION_NAME,
        embedding_model: str = EMBEDDING_MODEL
    ):
        self.db_path = db_path or CHROMADB_PATH
        self.collection_name = collection_name
        self.embedding_model_name = embedding_model
        self.model = None
        self.client = None
        self.collection = None
    
    def setup(self, reset: bool = False):
        """Initialize ChromaDB and embedding model."""
        logger.info(f"Loading embedding model: {self.embedding_model_name}")
        self.model = SentenceTransformer(self.embedding_model_name)
        
        logger.info(f"Initializing ChromaDB at: {self.db_path}")
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        self.client = chromadb.PersistentClient(
            path=str(self.db_path),
            settings=Settings(anonymized_telemetry=False)
        )
        
        if reset:
            try:
                self.client.delete_collection(self.collection_name)
                logger.info(f"Deleted existing collection: {self.collection_name}")
            except:
                pass
        
        self.collection = self.client.create_collection(
            name=self.collection_name,
            metadata={"description": "Pakistan case law for Qanoon legal research"}
        )
        logger.info(f"Created collection: {self.collection_name}")
    
    def prepare_document(
        self, 
        case: Dict, 
        chunk_idx: int, 
        chunk_text: str, 
        total_chunks: int
    ) -> tuple:
        """Prepare a document for ChromaDB insertion."""
        citation = case.get('citation', '')
        
        doc_id = f"{citation}__chunk_{chunk_idx}" if total_chunks > 1 else citation
        doc_id = re.sub(r'[^\w\-_.]', '_', doc_id)
        
        judges = case.get('judges', [])
        if isinstance(judges, list):
            judges_str = "; ".join(judges) if judges else ""
        else:
            judges_str = str(judges) if judges else ""
        
        statutes = case.get('statutes_cited', [])
        if isinstance(statutes, list):
            statutes_str = "; ".join(statutes[:20])
        else:
            statutes_str = str(statutes) if statutes else ""
        
        cases_cited = case.get('cases_cited', [])
        if isinstance(cases_cited, list):
            cases_cited_str = "; ".join(cases_cited[:20])
        else:
            cases_cited_str = str(cases_cited) if cases_cited else ""
        
        court = clean_html(case.get('court', ''))[:200]
        
        metadata = {
            'citation': citation,
            'case_name': case.get('case_name', '')[:200],
            'title': clean_html(case.get('title', ''))[:500],
            'court': court,
            'date': case.get('date', '')[:50],
            'judges': judges_str[:500],
            'statutes_cited': statutes_str[:1000],
            'cases_cited': cases_cited_str[:1000],
            'chunk_idx': chunk_idx,
            'total_chunks': total_chunks,
            'content_type': 'judgment'
        }
        
        return doc_id, chunk_text, metadata
    
    def import_all(
        self, 
        jsonl_path: Path = None, 
        reset: bool = True
    ) -> Dict[str, int]:
        """Import all cases from JSONL to ChromaDB."""
        jsonl_path = jsonl_path or JSONL_PATH
        
        logger.info("=" * 60)
        logger.info("ChromaDB Import for Qanoon Legal Research Platform")
        logger.info("=" * 60)
        
        self.setup(reset=reset)
        
        logger.info(f"Loading cases from: {jsonl_path}")
        
        cases_list = list(load_cases(jsonl_path, dedupe=True))
        total_cases = len(cases_list)
        logger.info(f"Found {total_cases} unique cases to import")
        
        batch_ids = []
        batch_texts = []
        batch_metadatas = []
        batch_embeddings = []
        
        total_chunks = 0
        cases_processed = 0
        seen_chunk_ids = set()
        
        logger.info("Processing and embedding documents...")
        
        for case in tqdm(cases_list, total=total_cases, desc="Cases"):
            headnotes = clean_html(case.get('headnotes', ''))
            judgment = clean_html(case.get('judgment', ''))
            
            if headnotes and judgment:
                full_text = f"HEADNOTES:\n{headnotes}\n\nJUDGMENT:\n{judgment}"
            elif judgment:
                full_text = judgment
            elif headnotes:
                full_text = headnotes
            else:
                continue
            
            chunks = chunk_text(full_text)
            
            if not chunks:
                continue
            
            for idx, chunk in enumerate(chunks):
                doc_id, text, metadata = self.prepare_document(case, idx, chunk, len(chunks))
                
                if doc_id in seen_chunk_ids:
                    continue
                seen_chunk_ids.add(doc_id)
                
                embedding = self.model.encode(text, show_progress_bar=False).tolist()
                
                batch_ids.append(doc_id)
                batch_texts.append(text)
                batch_metadatas.append(metadata)
                batch_embeddings.append(embedding)
                total_chunks += 1
                
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
        
        if batch_ids:
            self.collection.add(
                ids=batch_ids,
                documents=batch_texts,
                metadatas=batch_metadatas,
                embeddings=batch_embeddings
            )
        
        logger.info("Import complete!")
        logger.info(f"  Cases processed: {cases_processed}")
        logger.info(f"  Total chunks: {total_chunks}")
        logger.info(f"  Collection count: {self.collection.count()}")
        
        return {
            'cases_processed': cases_processed,
            'total_chunks': total_chunks,
            'collection_count': self.collection.count()
        }
