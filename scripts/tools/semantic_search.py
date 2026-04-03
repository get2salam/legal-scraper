#!/usr/bin/env python3
"""
Semantic Search (Simple)
========================
Vector-based semantic search using sentence-transformers.
No ChromaDB required - uses NumPy for similarity.
"""

import json
import pickle
from pathlib import Path
from typing import List, Tuple
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("Install: pip install sentence-transformers")
    exit(1)

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"
EMBEDDINGS_DIR = DATA_DIR / "embeddings"

# Default model (small and fast)
DEFAULT_MODEL = "all-MiniLM-L6-v2"


def load_all_cases():
    """Load all cases from JSONL files."""
    cases = []
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        cases.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return cases


def get_case_text(case: dict) -> str:
    """Extract searchable text from case."""
    parts = [
        case.get("title", ""),
        case.get("headnotes", ""),
        # Use first 2000 chars of judgment for embedding
        (case.get("judgment", case.get("text", "")) or "")[:2000],
    ]
    return " ".join(p for p in parts if p)


class SemanticSearcher:
    """Simple semantic search using sentence-transformers."""
    
    def __init__(self, model_name: str = DEFAULT_MODEL):
        print(f"Loading model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.cases = []
        self.embeddings = None
        self.index_path = EMBEDDINGS_DIR / f"index_{model_name.replace('/', '_')}.pkl"
    
    def build_index(self, cases: List[dict] = None):
        """Build embeddings index from cases."""
        if cases is None:
            print("Loading cases...")
            cases = load_all_cases()
        
        self.cases = cases
        texts = [get_case_text(c) for c in cases]
        
        print(f"Generating embeddings for {len(texts)} cases...")
        self.embeddings = self.model.encode(texts, show_progress_bar=True)
        
        # Save index
        EMBEDDINGS_DIR.mkdir(parents=True, exist_ok=True)
        with open(self.index_path, "wb") as f:
            pickle.dump({"cases": self.cases, "embeddings": self.embeddings}, f)
        
        print(f"Index saved to {self.index_path}")
    
    def load_index(self) -> bool:
        """Load existing index if available."""
        if self.index_path.exists():
            print(f"Loading index from {self.index_path}")
            with open(self.index_path, "rb") as f:
                data = pickle.load(f)
                self.cases = data["cases"]
                self.embeddings = data["embeddings"]
            print(f"Loaded {len(self.cases)} cases")
            return True
        return False
    
    def search(self, query: str, top_k: int = 5) -> List[Tuple[dict, float]]:
        """Search for similar cases."""
        if self.embeddings is None:
            raise ValueError("Index not built. Run build_index() first.")
        
        # Encode query
        query_embedding = self.model.encode([query])[0]
        
        # Compute cosine similarity
        similarities = np.dot(self.embeddings, query_embedding) / (
            np.linalg.norm(self.embeddings, axis=1) * np.linalg.norm(query_embedding)
        )
        
        # Get top-k indices
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        results = []
        for idx in top_indices:
            results.append((self.cases[idx], float(similarities[idx])))
        
        return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Semantic search for case law")
    parser.add_argument("query", nargs="?", help="Search query")
    parser.add_argument("--build", "-b", action="store_true", help="Build/rebuild index")
    parser.add_argument("--top", "-k", type=int, default=5, help="Number of results")
    parser.add_argument("--model", "-m", default=DEFAULT_MODEL, help="Model name")
    
    args = parser.parse_args()
    
    searcher = SemanticSearcher(args.model)
    
    if args.build or not searcher.load_index():
        searcher.build_index()
    
    if args.query:
        print(f"\nSearching for: '{args.query}'")
        print("=" * 60)
        
        results = searcher.search(args.query, top_k=args.top)
        
        for i, (case, score) in enumerate(results, 1):
            print(f"\n[{i}] Score: {score:.4f}")
            print(f"    {case.get('title', 'Unknown')}")
            print(f"    {case.get('book', '')} {case.get('year', '')}")
            headnotes = case.get("headnotes", "")
            if headnotes:
                print(f"    {headnotes[:200]}...")
    else:
        # Interactive mode
        print("\nEnter queries (Ctrl+C to exit):")
        while True:
            try:
                query = input("\n> ").strip()
                if not query:
                    continue
                
                results = searcher.search(query, top_k=args.top)
                
                for i, (case, score) in enumerate(results, 1):
                    print(f"\n  [{i}] {score:.3f} | {case.get('title', 'Unknown')}")
                    print(f"      {case.get('book', '')} {case.get('year', '')}")
                    
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break


if __name__ == "__main__":
    main()
