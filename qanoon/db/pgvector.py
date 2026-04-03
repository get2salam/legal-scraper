"""
pgvector Migrator
=================

Migrates data to PostgreSQL with pgvector for hybrid search (FTS + vector).

Features:
- pgvector extension setup
- Nomic embeddings (768 dims)
- Full-text search columns
- IVFFlat indexes

Example:
    from qanoon.db import PgVectorMigrator
    
    migrator = PgVectorMigrator()
    migrator.setup()
    migrator.migrate()
"""

import os
import re
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from html import unescape

import psycopg
from psycopg.rows import dict_row
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent.parent.parent / "data_v2"
EMBEDDING_MODEL = "nomic-ai/nomic-embed-text-v1.5"
EMBEDDING_DIM = 768
CHUNK_SIZE = 2000
CHUNK_OVERLAP = 200
BATCH_SIZE = 50

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/qanoon")


def clean_html(text: str) -> str:
    """Remove HTML tags and clean text."""
    if not text:
        return ""
    
    text = unescape(text)
    
    try:
        text = text.encode().decode('unicode_escape', errors='ignore')
    except:
        pass
    
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\{[^}]+\}', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks."""
    if not text or len(text) <= chunk_size:
        return [text] if text else []
    
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current = ""
    
    for sentence in sentences:
        if len(current) + len(sentence) > chunk_size:
            if current:
                chunks.append(current.strip())
            current = sentence
        else:
            current = current + " " + sentence if current else sentence
    
    if current.strip():
        chunks.append(current.strip())
    
    return chunks


class PgVectorMigrator:
    """
    Migrates data to PostgreSQL with pgvector for hybrid search.
    
    Args:
        db_url: PostgreSQL connection URL
        embedding_model: Sentence-transformer model name
    """
    
    def __init__(
        self,
        db_url: str = None,
        embedding_model: str = EMBEDDING_MODEL,
        data_dir: Path = None
    ):
        self.db_url = db_url or DB_URL
        self.embedding_model_name = embedding_model
        self.data_dir = data_dir or DATA_DIR
        self.model = None
        self.conn = None
    
    def connect(self):
        """Get database connection."""
        self.conn = psycopg.connect(self.db_url, row_factory=dict_row)
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def setup(self):
        """Create tables and indexes for hybrid search."""
        logger.info("Setting up pgvector database...")
        
        self.connect()
        
        with self.conn.cursor() as cur:
            # Enable pgvector
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # Cases table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS cases (
                    id SERIAL PRIMARY KEY,
                    citation TEXT UNIQUE NOT NULL,
                    reporter TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    title TEXT,
                    court TEXT,
                    bench TEXT,
                    decided_on DATE,
                    judgment_text TEXT,
                    judgment_raw TEXT,
                    statutes_cited TEXT[],
                    cases_cited TEXT[],
                    headnotes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    fts tsvector GENERATED ALWAYS AS (
                        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
                        setweight(to_tsvector('english', coalesce(headnotes, '')), 'B') ||
                        setweight(to_tsvector('english', coalesce(judgment_text, '')), 'C')
                    ) STORED
                );
                
                CREATE INDEX IF NOT EXISTS idx_cases_fts ON cases USING GIN(fts);
                CREATE INDEX IF NOT EXISTS idx_cases_reporter ON cases(reporter);
                CREATE INDEX IF NOT EXISTS idx_cases_year ON cases(year);
                CREATE INDEX IF NOT EXISTS idx_cases_court ON cases(court);
            """)
            
            # Embeddings table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS case_embeddings (
                    id SERIAL PRIMARY KEY,
                    case_id INTEGER REFERENCES cases(id) ON DELETE CASCADE,
                    citation TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    embedding vector({EMBEDDING_DIM}),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(citation, chunk_index)
                );
                
                CREATE INDEX IF NOT EXISTS idx_embeddings_vector 
                ON case_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 1000);
                
                CREATE INDEX IF NOT EXISTS idx_embeddings_citation ON case_embeddings(citation);
            """)
            
            # Statutes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS statutes (
                    id SERIAL PRIMARY KEY,
                    title TEXT UNIQUE NOT NULL,
                    short_title TEXT,
                    year INTEGER,
                    act_number TEXT,
                    content_html TEXT,
                    content_text TEXT,
                    sections JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    fts tsvector GENERATED ALWAYS AS (
                        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
                        setweight(to_tsvector('english', coalesce(content_text, '')), 'B')
                    ) STORED
                );
                
                CREATE INDEX IF NOT EXISTS idx_statutes_fts ON statutes USING GIN(fts);
            """)
        
        self.conn.commit()
        logger.info("Schema setup complete")
    
    def load_embedding_model(self):
        """Load the embedding model."""
        if self.model is None:
            logger.info(f"Loading embedding model: {self.embedding_model_name}")
            self.model = SentenceTransformer(self.embedding_model_name)
        return self.model
    
    def migrate_cases(self, jsonl_path: Path = None) -> Dict[str, int]:
        """Migrate cases from JSONL to pgvector."""
        jsonl_path = jsonl_path or self.data_dir / "all_cases.jsonl"
        
        logger.info(f"Migrating cases from: {jsonl_path}")
        
        self.load_embedding_model()
        self.connect()
        
        stats = {'cases': 0, 'chunks': 0, 'errors': 0}
        
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            cases = [json.loads(line) for line in f if line.strip()]
        
        logger.info(f"Found {len(cases)} cases to migrate")
        
        for case in tqdm(cases, desc="Migrating"):
            try:
                citation = case.get('citation', '')
                if not citation:
                    continue
                
                parts = citation.split()
                year = int(parts[0]) if parts and parts[0].isdigit() else 0
                reporter = parts[1] if len(parts) > 1 else ''
                
                judgment_text = clean_html(case.get('judgment', ''))
                headnotes = clean_html(case.get('headnotes', ''))
                
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO cases (
                            citation, reporter, year, title, court,
                            judgment_text, judgment_raw, statutes_cited,
                            cases_cited, headnotes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (citation) DO UPDATE SET
                            judgment_text = EXCLUDED.judgment_text,
                            headnotes = EXCLUDED.headnotes
                        RETURNING id
                    """, (
                        citation,
                        reporter,
                        year,
                        case.get('title', ''),
                        case.get('court', ''),
                        judgment_text,
                        case.get('judgment', ''),
                        case.get('statutes_cited', []),
                        case.get('cases_cited', []),
                        headnotes
                    ))
                    case_id = cur.fetchone()['id']
                
                # Generate embeddings for chunks
                full_text = f"{headnotes}\n\n{judgment_text}" if headnotes else judgment_text
                chunks = chunk_text(full_text)
                
                for idx, chunk in enumerate(chunks):
                    embedding = self.model.encode(chunk).tolist()
                    
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO case_embeddings (case_id, citation, chunk_index, chunk_text, embedding)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (citation, chunk_index) DO UPDATE SET
                                chunk_text = EXCLUDED.chunk_text,
                                embedding = EXCLUDED.embedding
                        """, (case_id, citation, idx, chunk, embedding))
                    
                    stats['chunks'] += 1
                
                stats['cases'] += 1
                
                if stats['cases'] % 100 == 0:
                    self.conn.commit()
                    logger.info(f"Migrated {stats['cases']} cases, {stats['chunks']} chunks")
                
            except Exception as e:
                logger.error(f"Error migrating {case.get('citation', 'unknown')}: {e}")
                stats['errors'] += 1
        
        self.conn.commit()
        self.close()
        
        logger.info("Migration complete!")
        logger.info(f"  Cases: {stats['cases']}")
        logger.info(f"  Chunks: {stats['chunks']}")
        logger.info(f"  Errors: {stats['errors']}")
        
        return stats
    
    def hybrid_search(
        self, 
        query: str, 
        limit: int = 10,
        fts_weight: float = 0.4,
        vector_weight: float = 0.6
    ) -> List[Dict]:
        """
        Perform hybrid search combining FTS and vector similarity.
        
        Args:
            query: Search query
            limit: Max results
            fts_weight: Weight for full-text search score
            vector_weight: Weight for vector similarity score
        """
        self.load_embedding_model()
        self.connect()
        
        query_embedding = self.model.encode(query).tolist()
        
        with self.conn.cursor() as cur:
            cur.execute("""
                WITH fts_results AS (
                    SELECT id, citation, title, ts_rank(fts, plainto_tsquery('english', %s)) as fts_score
                    FROM cases
                    WHERE fts @@ plainto_tsquery('english', %s)
                ),
                vector_results AS (
                    SELECT DISTINCT ON (ce.citation)
                        c.id, c.citation, c.title,
                        1 - (ce.embedding <=> %s::vector) as vector_score
                    FROM case_embeddings ce
                    JOIN cases c ON c.id = ce.case_id
                    ORDER BY ce.citation, (ce.embedding <=> %s::vector)
                    LIMIT 100
                )
                SELECT 
                    COALESCE(f.citation, v.citation) as citation,
                    COALESCE(f.title, v.title) as title,
                    COALESCE(f.fts_score, 0) as fts_score,
                    COALESCE(v.vector_score, 0) as vector_score,
                    (COALESCE(f.fts_score, 0) * %s + COALESCE(v.vector_score, 0) * %s) as hybrid_score
                FROM fts_results f
                FULL OUTER JOIN vector_results v ON f.citation = v.citation
                ORDER BY hybrid_score DESC
                LIMIT %s
            """, (query, query, query_embedding, query_embedding, fts_weight, vector_weight, limit))
            
            results = cur.fetchall()
        
        self.close()
        return results
