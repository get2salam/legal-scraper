#!/usr/bin/env python3
"""
Simple Semantic Search for Pakistani Legal Data
A lightweight search tool for testing embeddings
"""

import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleSearch:
    """Simple semantic search using cosine similarity"""
    
    def __init__(self, embeddings_file: str = "data/embeddings/legal_embeddings.json"):
        self.embeddings_file = Path(embeddings_file)
        self.documents = []
        self.vectors = None
        self.embedder = None
        
        self._load_embeddings()
    
    def _load_embeddings(self):
        """Load pre-computed embeddings"""
        if not self.embeddings_file.exists():
            logger.warning(f"Embeddings file not found: {self.embeddings_file}")
            logger.info("Run: python embedding_generator.py --input data/processed/parsed_acts.json")
            return
        
        logger.info(f"Loading embeddings from {self.embeddings_file}")
        
        with open(self.embeddings_file, encoding='utf-8') as f:
            data = json.load(f)
        
        self.documents = data.get('documents', [])
        self.vectors = np.array([doc['embedding'] for doc in self.documents])
        
        logger.info(f"Loaded {len(self.documents)} documents")
        logger.info(f"Vector shape: {self.vectors.shape}")
    
    def _get_embedder(self):
        """Lazy load the embedding model"""
        if self.embedder is None:
            try:
                from sentence_transformers import SentenceTransformer
                self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            except ImportError:
                raise ImportError("Install sentence-transformers: pip install sentence-transformers")
        return self.embedder
    
    def _cosine_similarity(self, query_vec: np.ndarray, doc_vecs: np.ndarray) -> np.ndarray:
        """Compute cosine similarity between query and documents"""
        query_norm = query_vec / np.linalg.norm(query_vec)
        doc_norms = doc_vecs / np.linalg.norm(doc_vecs, axis=1, keepdims=True)
        return np.dot(doc_norms, query_norm)
    
    def search(
        self,
        query: str,
        top_k: int = 5,
        min_score: float = 0.0,
        filter_type: Optional[str] = None
    ) -> List[Dict]:
        """
        Search for relevant documents
        
        Args:
            query: Search query
            top_k: Number of results to return
            min_score: Minimum similarity score (0-1)
            filter_type: Filter by document type ('section' or 'preamble')
        
        Returns:
            List of matching documents with scores
        """
        if self.vectors is None or len(self.documents) == 0:
            logger.error("No embeddings loaded")
            return []
        
        # Generate query embedding
        embedder = self._get_embedder()
        query_vec = embedder.encode(query)
        
        # Compute similarities
        similarities = self._cosine_similarity(query_vec, self.vectors)
        
        # Get top results
        results = []
        for idx in np.argsort(similarities)[::-1]:
            score = float(similarities[idx])
            
            if score < min_score:
                break
            
            doc = self.documents[idx]
            
            # Apply type filter if specified
            if filter_type and doc.get('metadata', {}).get('type') != filter_type:
                continue
            
            results.append({
                'score': score,
                'text': doc['text'],
                'metadata': doc.get('metadata', {}),
                'id': doc['id']
            })
            
            if len(results) >= top_k:
                break
        
        return results
    
    def search_by_act(self, query: str, act_title: str, top_k: int = 5) -> List[Dict]:
        """Search within a specific act"""
        results = []
        
        embedder = self._get_embedder()
        query_vec = embedder.encode(query)
        
        # Filter documents by act
        filtered_docs = []
        filtered_vecs = []
        
        for i, doc in enumerate(self.documents):
            if act_title.lower() in doc.get('metadata', {}).get('act_title', '').lower():
                filtered_docs.append(doc)
                filtered_vecs.append(self.vectors[i])
        
        if not filtered_docs:
            logger.warning(f"No documents found for act: {act_title}")
            return []
        
        filtered_vecs = np.array(filtered_vecs)
        similarities = self._cosine_similarity(query_vec, filtered_vecs)
        
        for idx in np.argsort(similarities)[::-1][:top_k]:
            results.append({
                'score': float(similarities[idx]),
                'text': filtered_docs[idx]['text'],
                'metadata': filtered_docs[idx].get('metadata', {}),
                'id': filtered_docs[idx]['id']
            })
        
        return results
    
    def list_acts(self) -> List[str]:
        """List all unique acts in the database"""
        acts = set()
        for doc in self.documents:
            act_title = doc.get('metadata', {}).get('act_title')
            if act_title:
                acts.add(act_title)
        return sorted(list(acts))


def interactive_search():
    """Interactive search CLI"""
    print("\n" + "="*60)
    print("PAKISTANI LEGAL SEARCH (Local Embeddings)")
    print("="*60)
    
    search = SimpleSearch()
    
    if not search.documents:
        print("\nNo documents loaded. Generate embeddings first:")
        print("  python embedding_generator.py --input data/processed/parsed_acts.json")
        return
    
    print(f"\nLoaded {len(search.documents)} document chunks")
    print(f"Acts: {len(search.list_acts())}")
    print("\nCommands:")
    print("  <query>     - Search all documents")
    print("  acts        - List all acts")
    print("  quit        - Exit")
    print("-"*60)
    
    while True:
        try:
            query = input("\nSearch: ").strip()
            
            if not query:
                continue
            
            if query.lower() == 'quit':
                break
            
            if query.lower() == 'acts':
                print("\nAvailable Acts:")
                for act in search.list_acts():
                    print(f"  • {act[:60]}...")
                continue
            
            # Perform search
            results = search.search(query, top_k=5)
            
            if not results:
                print("No results found.")
                continue
            
            print(f"\n--- Top {len(results)} Results ---")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. [Score: {result['score']:.3f}]")
                print(f"   Act: {result['metadata'].get('act_title', 'Unknown')[:50]}")
                if result['metadata'].get('section_number'):
                    print(f"   Section: {result['metadata'].get('section_number')} - {result['metadata'].get('section_title', '')[:40]}")
                print(f"   Text: {result['text'][:200]}...")
        
        except KeyboardInterrupt:
            break
    
    print("\nGoodbye!")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Search Pakistani legal documents')
    parser.add_argument('--query', '-q', help='Search query (interactive mode if not provided)')
    parser.add_argument('--top-k', type=int, default=5, help='Number of results')
    parser.add_argument('--embeddings', default='data/embeddings/legal_embeddings.json',
                       help='Path to embeddings file')
    
    args = parser.parse_args()
    
    if args.query:
        search = SimpleSearch(args.embeddings)
        results = search.search(args.query, top_k=args.top_k)
        
        for i, result in enumerate(results, 1):
            print(f"\n{i}. [Score: {result['score']:.3f}]")
            print(f"   {result['metadata'].get('act_title', 'Unknown')}")
            print(f"   {result['text'][:300]}...")
    else:
        interactive_search()


if __name__ == '__main__':
    main()
