"""
Qanoon Database Module
======================

Database import and migration utilities for PostgreSQL, ChromaDB, and pgvector.

Classes:
    PostgresImporter: Import cases to PostgreSQL with full-text search
    ChromaDBImporter: Import cases to ChromaDB for semantic search
    PgVectorMigrator: Migrate to pgvector for hybrid search

Example:
    from qanoon.db import PostgresImporter
    
    importer = PostgresImporter(db_url="postgresql://localhost/qanoon")
    importer.import_jsonl("data_v2/all_cases.jsonl")

Note: These require optional dependencies (psycopg2, chromadb, sentence-transformers).
Install with: pip install qanoon[db,ml]
"""

__all__ = [
    "PostgresImporter",
    "ChromaDBImporter", 
    "PgVectorMigrator",
]

def __getattr__(name):
    """Lazy import to handle missing optional dependencies."""
    if name == "PostgresImporter":
        from .postgres import PostgresImporter
        return PostgresImporter
    elif name == "ChromaDBImporter":
        from .chromadb import ChromaDBImporter
        return ChromaDBImporter
    elif name == "PgVectorMigrator":
        from .pgvector import PgVectorMigrator
        return PgVectorMigrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
