#!/usr/bin/env python3
"""
ChromaDB Search Script for Qanoon Legal Research Platform
Simple test script to query the Pakistan case law vector database.
"""

import argparse
from pathlib import Path

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

# Configuration
CHROMADB_PATH = Path("data_v2/chromadb")
COLLECTION_NAME = "pakistan_cases"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


def search(query: str, n_results: int = 5, court_filter: str = None, 
           year_filter: str = None, show_full: bool = False):
    """
    Search the vector database for relevant cases.
    
    Args:
        query: Search query text
        n_results: Number of results to return
        court_filter: Filter by court name (partial match)
        year_filter: Filter by year in date
        show_full: Show full text of results
    """
    print(f"\n🔍 Searching for: '{query}'")
    print("-" * 60)
    
    # Load embedding model
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Connect to ChromaDB
    client = chromadb.PersistentClient(
        path=str(CHROMADB_PATH),
        settings=Settings(anonymized_telemetry=False)
    )
    
    collection = client.get_collection(COLLECTION_NAME)
    print(f"📚 Collection has {collection.count()} documents")
    
    # Generate query embedding
    query_embedding = model.encode(query).tolist()
    
    # Build where filter
    where_filter = None
    if court_filter or year_filter:
        conditions = []
        if court_filter:
            conditions.append({"court": {"$contains": court_filter}})
        if year_filter:
            conditions.append({"date": {"$contains": year_filter}})
        
        if len(conditions) == 1:
            where_filter = conditions[0]
        else:
            where_filter = {"$and": conditions}
    
    # Search
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        where=where_filter,
        include=["documents", "metadatas", "distances"]
    )
    
    if not results['ids'][0]:
        print("❌ No results found.")
        return
    
    print(f"\n📋 Found {len(results['ids'][0])} results:\n")
    
    for i, (doc_id, doc, metadata, distance) in enumerate(zip(
        results['ids'][0],
        results['documents'][0],
        results['metadatas'][0],
        results['distances'][0]
    )):
        similarity = 1 - distance  # Convert distance to similarity
        
        print(f"{'='*60}")
        print(f"Result {i+1} | Similarity: {similarity:.3f}")
        print(f"{'='*60}")
        print(f"📌 Citation: {metadata.get('citation', 'N/A')}")
        print(f"🏛️  Court: {metadata.get('court', 'N/A')}")
        print(f"📅 Date: {metadata.get('date', 'N/A')}")
        print(f"👨‍⚖️ Judges: {metadata.get('judges', 'N/A')[:100]}...")
        
        if metadata.get('total_chunks', 1) > 1:
            print(f"📄 Chunk: {metadata.get('chunk_idx', 0) + 1}/{metadata.get('total_chunks', 1)}")
        
        if metadata.get('statutes_cited'):
            print(f"📜 Statutes: {metadata.get('statutes_cited', '')[:150]}...")
        
        print(f"\n📝 Text snippet:")
        if show_full:
            print(doc)
        else:
            # Show first 500 chars
            snippet = doc[:500] + "..." if len(doc) > 500 else doc
            print(snippet)
        
        print()
    
    return results


def list_stats():
    """Show collection statistics."""
    client = chromadb.PersistentClient(
        path=str(CHROMADB_PATH),
        settings=Settings(anonymized_telemetry=False)
    )
    
    collection = client.get_collection(COLLECTION_NAME)
    
    print("\n📊 Collection Statistics")
    print("-" * 40)
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Total documents: {collection.count()}")
    
    # Get sample to analyze
    sample = collection.get(limit=100, include=["metadatas"])
    
    courts = set()
    citations = set()
    for meta in sample['metadatas']:
        if meta.get('court'):
            courts.add(meta['court'][:50])
        if meta.get('citation'):
            citations.add(meta['citation'].split()[0] if meta['citation'] else '')
    
    print(f"\nSample courts found:")
    for court in list(courts)[:10]:
        print(f"  - {court}")
    
    print(f"\nCitation types found:")
    for cit in list(citations)[:10]:
        print(f"  - {cit}")


def interactive_mode():
    """Interactive search mode."""
    print("\n🔎 Interactive Search Mode")
    print("Type 'quit' or 'exit' to stop")
    print("Type 'stats' to show statistics")
    print("-" * 40)
    
    # Load model once
    model = SentenceTransformer(EMBEDDING_MODEL)
    client = chromadb.PersistentClient(
        path=str(CHROMADB_PATH),
        settings=Settings(anonymized_telemetry=False)
    )
    collection = client.get_collection(COLLECTION_NAME)
    
    while True:
        try:
            query = input("\n🔍 Enter search query: ").strip()
            
            if query.lower() in ('quit', 'exit', 'q'):
                print("👋 Goodbye!")
                break
            
            if query.lower() == 'stats':
                list_stats()
                continue
            
            if not query:
                continue
            
            # Search
            query_embedding = model.encode(query).tolist()
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=5,
                include=["documents", "metadatas", "distances"]
            )
            
            if not results['ids'][0]:
                print("❌ No results found.")
                continue
            
            print(f"\n📋 Top {len(results['ids'][0])} results:\n")
            
            for i, (doc_id, doc, metadata, distance) in enumerate(zip(
                results['ids'][0],
                results['documents'][0],
                results['metadatas'][0],
                results['distances'][0]
            )):
                similarity = 1 - distance
                print(f"\n[{i+1}] {metadata.get('citation', 'N/A')} (sim: {similarity:.3f})")
                print(f"    Court: {metadata.get('court', 'N/A')[:60]}")
                print(f"    Date: {metadata.get('date', 'N/A')}")
                print(f"    Snippet: {doc[:200]}...")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Search Pakistan case law in ChromaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python search_chromadb.py "contract breach damages"
  python search_chromadb.py "murder Section 302" -n 10
  python search_chromadb.py "property transfer" --court "Supreme"
  python search_chromadb.py --interactive
  python search_chromadb.py --stats
        """
    )
    
    parser.add_argument('query', nargs='?', help='Search query')
    parser.add_argument('-n', '--results', type=int, default=5, help='Number of results (default: 5)')
    parser.add_argument('--court', help='Filter by court name')
    parser.add_argument('--year', help='Filter by year in date')
    parser.add_argument('--full', action='store_true', help='Show full document text')
    parser.add_argument('--stats', action='store_true', help='Show collection statistics')
    parser.add_argument('-i', '--interactive', action='store_true', help='Interactive mode')
    
    args = parser.parse_args()
    
    if args.stats:
        list_stats()
    elif args.interactive:
        interactive_mode()
    elif args.query:
        search(args.query, args.results, args.court, args.year, args.full)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
