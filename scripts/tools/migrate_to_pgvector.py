#!/usr/bin/env python3
"""
Migrate from ChromaDB to pgvector with hybrid search.

This script:
1. Creates pgvector schema with hybrid search (FTS + vector)
2. Migrates existing case data from JSON files
3. Generates embeddings with nomic-embed-text-v1.5 (768 dims)
4. Sets up indexes for fast retrieval

Usage:
    python migrate_to_pgvector.py --setup       # Create tables and indexes
    python migrate_to_pgvector.py --migrate     # Migrate all cases
    python migrate_to_pgvector.py --test        # Test hybrid search
    
Requires: PostgreSQL 14+ with pgvector extension
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

# Third party
import psycopg
from psycopg.rows import dict_row
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
console = Console()

# Configuration
DATA_DIR = Path(__file__).parent / "data_v2"
EMBEDDING_MODEL = "nomic-ai/nomic-embed-text-v1.5"  # 768 dims, 8K context
EMBEDDING_DIM = 768
CHUNK_SIZE = 2000  # Characters per chunk
CHUNK_OVERLAP = 200
BATCH_SIZE = 50  # Cases per batch for embedding

# Database connection
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/qanoon")


def get_db_connection():
    """Get database connection."""
    return psycopg.connect(DB_URL, row_factory=dict_row)


def setup_database():
    """Create tables and indexes for hybrid search."""
    console.print("[bold blue]Setting up pgvector database...[/bold blue]")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Enable pgvector extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # Cases table (main metadata)
            cur.execute("""
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
                    
                    -- Full-text search column
                    fts tsvector GENERATED ALWAYS AS (
                        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
                        setweight(to_tsvector('english', coalesce(headnotes, '')), 'B') ||
                        setweight(to_tsvector('english', coalesce(judgment_text, '')), 'C')
                    ) STORED
                );
                
                -- FTS index
                CREATE INDEX IF NOT EXISTS idx_cases_fts ON cases USING GIN(fts);
                
                -- Common query indexes
                CREATE INDEX IF NOT EXISTS idx_cases_reporter ON cases(reporter);
                CREATE INDEX IF NOT EXISTS idx_cases_year ON cases(year);
                CREATE INDEX IF NOT EXISTS idx_cases_court ON cases(court);
                CREATE INDEX IF NOT EXISTS idx_cases_decided ON cases(decided_on);
            """)
            
            # Case embeddings (chunked for better retrieval)
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
                
                -- Vector similarity index (IVFFlat for ~2M vectors)
                CREATE INDEX IF NOT EXISTS idx_embeddings_vector 
                ON case_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 1000);
                
                -- Case lookup index
                CREATE INDEX IF NOT EXISTS idx_embeddings_citation ON case_embeddings(citation);
                CREATE INDEX IF NOT EXISTS idx_embeddings_case_id ON case_embeddings(case_id);
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
            
            # Statute-case links
            cur.execute("""
                CREATE TABLE IF NOT EXISTS statute_case_links (
                    id SERIAL PRIMARY KEY,
                    statute_id INTEGER REFERENCES statutes(id),
                    case_id INTEGER REFERENCES cases(id),
                    statute_title TEXT NOT NULL,
                    case_citation TEXT NOT NULL,
                    section_cited TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(statute_title, case_citation, section_cited)
                );
                
                CREATE INDEX IF NOT EXISTS idx_links_statute ON statute_case_links(statute_id);
                CREATE INDEX IF NOT EXISTS idx_links_case ON statute_case_links(case_id);
            """)
            
            # Judges table (for Judge Intelligence feature)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS judges (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    normalized_name TEXT,
                    courts TEXT[],
                    case_count INTEGER DEFAULT 0,
                    first_case_date DATE,
                    last_case_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_judges_name ON judges(normalized_name);
            """)
            
            # Judge-case assignments
            cur.execute("""
                CREATE TABLE IF NOT EXISTS judge_cases (
                    id SERIAL PRIMARY KEY,
                    judge_id INTEGER REFERENCES judges(id),
                    case_id INTEGER REFERENCES cases(id),
                    role TEXT,  -- 'author', 'bench', 'dissenting'
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(judge_id, case_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_judge_cases_judge ON judge_cases(judge_id);
                CREATE INDEX IF NOT EXISTS idx_judge_cases_case ON judge_cases(case_id);
            """)
            
            conn.commit()
    
    console.print("[bold green]✓ Database setup complete![/bold green]")


def load_embedding_model():
    """Load nomic-embed-text-v1.5 model."""
    console.print(f"[yellow]Loading embedding model: {EMBEDDING_MODEL}[/yellow]")
    model = SentenceTransformer(EMBEDDING_MODEL, trust_remote_code=True)
    model.max_seq_length = 8192  # Use full 8K context
    console.print("[green]✓ Model loaded[/green]")
    return model


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks."""
    if not text or len(text) <= chunk_size:
        return [text] if text else []
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to break at sentence boundary
        if end < len(text):
            last_period = chunk.rfind('. ')
            if last_period > chunk_size // 2:
                chunk = chunk[:last_period + 1]
                end = start + last_period + 1
        
        chunks.append(chunk.strip())
        start = end - overlap
    
    return chunks


def load_cases_from_json(reporter: str = None, year: int = None) -> List[Dict]:
    """Load case data from JSON files."""
    cases = []
    
    # Build path pattern
    if reporter and year:
        pattern = DATA_DIR / reporter / str(year) / "*.json"
    elif reporter:
        pattern = DATA_DIR / reporter / "*" / "*.json"
    else:
        pattern = DATA_DIR / "*" / "*" / "*.json"
    
    for json_path in Path(DATA_DIR).glob("**/[0-9]*.json"):
        # Skip non-case files
        if json_path.name.startswith('_') or 'backup' in str(json_path):
            continue
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                case = json.load(f)
                if 'citation' in case:
                    cases.append(case)
        except Exception as e:
            logger.warning(f"Failed to load {json_path}: {e}")
    
    return cases


def migrate_cases(model: SentenceTransformer, limit: int = None):
    """Migrate cases from JSON to pgvector with embeddings."""
    console.print("[bold blue]Migrating cases to pgvector...[/bold blue]")
    
    cases = load_cases_from_json()
    if limit:
        cases = cases[:limit]
    
    console.print(f"Found {len(cases)} cases to migrate")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for case in tqdm(cases, desc="Migrating"):
                try:
                    # Insert case metadata
                    citation = case.get('citation', '')
                    
                    # Parse year and reporter from citation
                    parts = citation.split()
                    year = int(parts[0]) if parts and parts[0].isdigit() else 2024
                    reporter = parts[1] if len(parts) > 1 else 'UNKNOWN'
                    
                    # Parse decided date
                    decided_str = case.get('decided_on', '')
                    decided_date = None
                    if decided_str:
                        try:
                            decided_date = datetime.strptime(decided_str, '%d %B, %Y').date()
                        except:
                            pass
                    
                    # Insert case
                    cur.execute("""
                        INSERT INTO cases (
                            citation, reporter, year, title, court, bench,
                            decided_on, judgment_text, judgment_raw,
                            statutes_cited, cases_cited, headnotes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (citation) DO UPDATE SET
                            title = EXCLUDED.title,
                            judgment_text = EXCLUDED.judgment_text
                        RETURNING id
                    """, (
                        citation,
                        reporter,
                        year,
                        case.get('case_title', case.get('title', '')),
                        case.get('court', ''),
                        case.get('bench', ''),
                        decided_date,
                        case.get('judgment', case.get('judgment_text', '')),
                        case.get('judgment_raw', ''),
                        case.get('statutes_cited', []),
                        case.get('cases_cited', []),
                        case.get('headnotes', '')
                    ))
                    
                    case_id = cur.fetchone()['id']
                    
                    # Chunk and embed judgment
                    judgment = case.get('judgment', case.get('judgment_text', ''))
                    if judgment:
                        chunks = chunk_text(judgment)
                        
                        for i, chunk in enumerate(chunks):
                            # Generate embedding with search_document prefix
                            embedding = model.encode(
                                f"search_document: {chunk}",
                                normalize_embeddings=True
                            ).tolist()
                            
                            cur.execute("""
                                INSERT INTO case_embeddings (
                                    case_id, citation, chunk_index, chunk_text, embedding
                                ) VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (citation, chunk_index) DO UPDATE SET
                                    chunk_text = EXCLUDED.chunk_text,
                                    embedding = EXCLUDED.embedding
                            """, (case_id, citation, i, chunk, embedding))
                    
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"Failed to migrate {case.get('citation', 'unknown')}: {e}")
                    conn.rollback()
    
    console.print("[bold green]✓ Migration complete![/bold green]")


def hybrid_search(query: str, model: SentenceTransformer, limit: int = 10) -> List[Dict]:
    """Perform hybrid search combining FTS and vector similarity."""
    
    # Generate query embedding
    query_embedding = model.encode(
        f"search_query: {query}",
        normalize_embeddings=True
    ).tolist()
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Hybrid search: combine FTS rank and vector similarity
            cur.execute("""
                WITH fts_results AS (
                    SELECT 
                        id, citation, title, court, year,
                        ts_rank(fts, websearch_to_tsquery('english', %s)) as fts_score
                    FROM cases
                    WHERE fts @@ websearch_to_tsquery('english', %s)
                    ORDER BY fts_score DESC
                    LIMIT 50
                ),
                vector_results AS (
                    SELECT DISTINCT ON (citation)
                        e.citation,
                        1 - (e.embedding <=> %s::vector) as vector_score,
                        e.chunk_text
                    FROM case_embeddings e
                    ORDER BY citation, e.embedding <=> %s::vector
                    LIMIT 50
                )
                SELECT 
                    c.id, c.citation, c.title, c.court, c.year, c.headnotes,
                    COALESCE(f.fts_score, 0) as fts_score,
                    COALESCE(v.vector_score, 0) as vector_score,
                    (COALESCE(f.fts_score, 0) * 0.3 + COALESCE(v.vector_score, 0) * 0.7) as combined_score,
                    v.chunk_text as matched_chunk
                FROM cases c
                LEFT JOIN fts_results f ON c.id = f.id
                LEFT JOIN vector_results v ON c.citation = v.citation
                WHERE f.id IS NOT NULL OR v.citation IS NOT NULL
                ORDER BY combined_score DESC
                LIMIT %s
            """, (query, query, query_embedding, query_embedding, limit))
            
            results = cur.fetchall()
    
    return results


def test_search(model: SentenceTransformer):
    """Test hybrid search with sample queries."""
    console.print("\n[bold blue]Testing hybrid search...[/bold blue]\n")
    
    test_queries = [
        "landlord tenant eviction notice",
        "constitutional rights Article 199",
        "property dispute limitation period",
        "criminal bail conditions",
    ]
    
    for query in test_queries:
        console.print(f"\n[yellow]Query: {query}[/yellow]")
        
        results = hybrid_search(query, model, limit=5)
        
        table = Table(title=f"Results for: {query}")
        table.add_column("Citation", style="cyan")
        table.add_column("FTS", justify="right")
        table.add_column("Vector", justify="right")
        table.add_column("Combined", justify="right")
        
        for r in results:
            table.add_row(
                r['citation'],
                f"{r['fts_score']:.3f}",
                f"{r['vector_score']:.3f}",
                f"{r['combined_score']:.3f}"
            )
        
        console.print(table)


def get_stats():
    """Get database statistics."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM cases")
            cases = cur.fetchone()['count']
            
            cur.execute("SELECT COUNT(*) as count FROM case_embeddings")
            embeddings = cur.fetchone()['count']
            
            cur.execute("SELECT COUNT(*) as count FROM statutes")
            statutes = cur.fetchone()['count']
            
            cur.execute("""
                SELECT reporter, year, COUNT(*) as count 
                FROM cases 
                GROUP BY reporter, year 
                ORDER BY year DESC, reporter
            """)
            breakdown = cur.fetchall()
    
    console.print("\n[bold]Database Statistics[/bold]")
    console.print(f"  Cases: {cases:,}")
    console.print(f"  Embeddings: {embeddings:,}")
    console.print(f"  Statutes: {statutes:,}")
    
    if breakdown:
        console.print("\n[bold]Cases by Reporter/Year:[/bold]")
        for row in breakdown[:10]:
            console.print(f"  {row['year']} {row['reporter']}: {row['count']}")


def main():
    parser = argparse.ArgumentParser(description="Migrate to pgvector with hybrid search")
    parser.add_argument('--setup', action='store_true', help='Create tables and indexes')
    parser.add_argument('--migrate', action='store_true', help='Migrate cases with embeddings')
    parser.add_argument('--test', action='store_true', help='Test hybrid search')
    parser.add_argument('--stats', action='store_true', help='Show database stats')
    parser.add_argument('--limit', type=int, help='Limit number of cases to migrate')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_database()
    
    if args.migrate or args.test:
        model = load_embedding_model()
        
        if args.migrate:
            migrate_cases(model, limit=args.limit)
        
        if args.test:
            test_search(model)
    
    if args.stats:
        get_stats()
    
    if not any([args.setup, args.migrate, args.test, args.stats]):
        parser.print_help()


if __name__ == "__main__":
    main()
