#!/usr/bin/env python3
"""
ChromaDB Import Script for Qanoon Legal Research Platform
Imports Pakistan case law from JSONL into a vector database for semantic search.
"""

import json
import re
import hashlib
from pathlib import Path
from typing import Generator, Optional
from html import unescape

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# Configuration
JSONL_PATH = Path("data_v2/all_cases.jsonl")
CHROMADB_PATH = Path("data_v2/chromadb")
COLLECTION_NAME = "pakistan_cases"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # Fast, good quality, 384 dims
CHUNK_SIZE = 1500  # Characters per chunk (roughly 300-400 tokens)
CHUNK_OVERLAP = 200  # Overlap between chunks for context continuity
BATCH_SIZE = 100  # Documents per batch for embedding


def clean_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not text:
        return ""
    
    # Unescape HTML entities
    text = unescape(text)
    
    # Remove escaped unicode
    text = text.encode().decode('unicode_escape', errors='ignore')
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Remove CSS/style content
    text = re.sub(r'\{[^}]+\}', ' ', text)
    
    # Clean up whitespace
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'&[a-zA-Z]+;', ' ', text)  # Remaining HTML entities
    
    return text.strip()


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Split text into overlapping chunks at sentence boundaries.
    Returns list of chunks.
    """
    if not text or len(text) <= chunk_size:
        return [text] if text else []
    
    # Split into sentences (rough approximation)
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        # If single sentence exceeds chunk size, split it
        if len(sentence) > chunk_size:
            if current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = ""
            
            # Hard split long sentences
            for i in range(0, len(sentence), chunk_size - overlap):
                chunks.append(sentence[i:i + chunk_size].strip())
            continue
        
        # Check if adding sentence exceeds chunk size
        if len(current_chunk) + len(sentence) + 1 > chunk_size:
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # Start new chunk with overlap from previous
            if chunks and overlap > 0:
                # Take last portion of previous chunk for context
                last_chunk = chunks[-1]
                overlap_text = last_chunk[-overlap:] if len(last_chunk) > overlap else last_chunk
                # Find sentence boundary in overlap
                overlap_start = overlap_text.rfind('. ')
                if overlap_start > 0:
                    overlap_text = overlap_text[overlap_start + 2:]
                current_chunk = overlap_text + " " + sentence
            else:
                current_chunk = sentence
        else:
            current_chunk = current_chunk + " " + sentence if current_chunk else sentence
    
    # Don't forget the last chunk
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks


def normalize_citation(citation: str) -> str:
    """Normalize citation for deduplication (handles whitespace/formatting variations)."""
    if not citation:
        return ""
    # Collapse multiple spaces, strip, uppercase for comparison
    normalized = re.sub(r'\s+', ' ', citation.strip().upper())
    return normalized


def load_cases(jsonl_path: Path, dedupe: bool = True) -> Generator[dict, None, None]:
    """Load cases from JSONL file, optionally deduplicating by citation."""
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
                        continue  # Skip duplicate
                    seen_citations.add(normalized)
                
                yield case
    
    if duplicates > 0:
        print(f"    [!] Skipped {duplicates} duplicate cases")


def prepare_document(case: dict, chunk_idx: int, chunk_text: str, total_chunks: int) -> tuple[str, str, dict]:
    """
    Prepare a document for ChromaDB insertion.
    Returns: (doc_id, text, metadata)
    """
    citation = case.get('citation', '')
    
    # Create unique ID: citation + chunk index
    doc_id = f"{citation}__chunk_{chunk_idx}" if total_chunks > 1 else citation
    # Sanitize ID (ChromaDB requires certain format)
    doc_id = re.sub(r'[^\w\-_.]', '_', doc_id)
    
    # Prepare metadata
    judges = case.get('judges', [])
    if isinstance(judges, list):
        judges_str = "; ".join(judges) if judges else ""
    else:
        judges_str = str(judges) if judges else ""
    
    statutes = case.get('statutes_cited', [])
    if isinstance(statutes, list):
        statutes_str = "; ".join(statutes[:20])  # Limit to avoid huge metadata
    else:
        statutes_str = str(statutes) if statutes else ""
    
    cases_cited = case.get('cases_cited', [])
    if isinstance(cases_cited, list):
        cases_cited_str = "; ".join(cases_cited[:20])
    else:
        cases_cited_str = str(cases_cited) if cases_cited else ""
    
    # Clean court field (sometimes has HTML garbage)
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
        'content_type': 'judgment'  # Could also be 'headnotes'
    }
    
    return doc_id, chunk_text, metadata


def main():
    """Main import process."""
    print("=" * 60)
    print("ChromaDB Import for Qanoon Legal Research Platform")
    print("=" * 60)
    
    # Initialize embedding model
    print(f"\n[*] Loading embedding model: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Initialize ChromaDB
    print(f"[*] Initializing ChromaDB at: {CHROMADB_PATH}")
    CHROMADB_PATH.mkdir(parents=True, exist_ok=True)
    
    client = chromadb.PersistentClient(
        path=str(CHROMADB_PATH),
        settings=Settings(anonymized_telemetry=False)
    )
    
    # Delete existing collection if exists (fresh import)
    try:
        client.delete_collection(COLLECTION_NAME)
        print(f"    Deleted existing collection: {COLLECTION_NAME}")
    except:
        pass
    
    # Create collection with custom embedding function
    collection = client.create_collection(
        name=COLLECTION_NAME,
        metadata={"description": "Pakistan case law for Qanoon legal research"}
    )
    print(f"    Created collection: {COLLECTION_NAME}")
    
    # Process cases
    print(f"\n[*] Loading cases from: {JSONL_PATH}")
    
    # Count total unique cases first (with deduplication)
    cases_list = list(load_cases(JSONL_PATH, dedupe=True))
    total_cases = len(cases_list)
    print(f"    Found {total_cases} unique cases to import")
    
    # Batch storage
    batch_ids = []
    batch_texts = []
    batch_metadatas = []
    batch_embeddings = []
    
    total_chunks = 0
    cases_processed = 0
    skipped_duplicates = 0
    seen_chunk_ids = set()  # Track ALL chunk IDs to prevent any duplicates
    
    print(f"\n[*] Processing and embedding documents...")
    
    for case in tqdm(cases_list, total=total_cases, desc="Cases"):
        # Clean and combine headnotes + judgment
        headnotes = clean_html(case.get('headnotes', ''))
        judgment = clean_html(case.get('judgment', ''))
        
        # Combine with clear separation
        if headnotes and judgment:
            full_text = f"HEADNOTES:\n{headnotes}\n\nJUDGMENT:\n{judgment}"
        elif judgment:
            full_text = judgment
        elif headnotes:
            full_text = headnotes
        else:
            continue  # Skip cases with no content
        
        # Chunk the text
        chunks = chunk_text(full_text)
        
        if not chunks:
            continue
        
        # Process each chunk
        for idx, chunk in enumerate(chunks):
            doc_id, text, metadata = prepare_document(case, idx, chunk, len(chunks))
            
            # Skip if we've already seen this chunk ID (prevents duplicates)
            if doc_id in seen_chunk_ids:
                skipped_duplicates += 1
                continue
            seen_chunk_ids.add(doc_id)
            
            # Generate embedding
            embedding = model.encode(text, show_progress_bar=False).tolist()
            
            batch_ids.append(doc_id)
            batch_texts.append(text)
            batch_metadatas.append(metadata)
            batch_embeddings.append(embedding)
            total_chunks += 1
            
            # Insert batch when full
            if len(batch_ids) >= BATCH_SIZE:
                collection.add(
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
        collection.add(
            ids=batch_ids,
            documents=batch_texts,
            metadatas=batch_metadatas,
            embeddings=batch_embeddings
        )
    
    print(f"\n[OK] Import complete!")
    print(f"     Cases processed: {cases_processed}")
    print(f"     Total chunks: {total_chunks}")
    if skipped_duplicates > 0:
        print(f"     Duplicate chunks skipped: {skipped_duplicates}")
    print(f"     Collection count: {collection.count()}")
    print(f"     ChromaDB location: {CHROMADB_PATH.absolute()}")


if __name__ == "__main__":
    main()
