#!/usr/bin/env python3
"""
Embedding Generator for Pakistani Legal Data
Generates vector embeddings for semantic search using various providers

Supports:
- OpenAI text-embedding-3-small
- Google Gemini embeddings
- Local embeddings (sentence-transformers)
"""

import json
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Document:
    """A document chunk for embedding"""
    id: str
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'text': self.text,
            'metadata': self.metadata,
            'embedding': self.embedding
        }


class EmbeddingProvider(ABC):
    """Base class for embedding providers"""
    
    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts"""
        pass
    
    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the embedding dimension"""
        pass


class OpenAIEmbeddings(EmbeddingProvider):
    """OpenAI embeddings provider"""
    
    def __init__(self, model: str = "text-embedding-3-small", api_key: Optional[str] = None):
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self._dimension = 1536 if "small" in model else 3072
        
        if not self.api_key:
            raise ValueError("OpenAI API key required")
        
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key)
        except ImportError:
            raise ImportError("Install openai: pip install openai")
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using OpenAI"""
        response = self.client.embeddings.create(
            model=self.model,
            input=texts
        )
        return [item.embedding for item in response.data]
    
    @property
    def dimension(self) -> int:
        return self._dimension


class GoogleEmbeddings(EmbeddingProvider):
    """Google Gemini embeddings provider"""
    
    def __init__(self, model: str = "models/text-embedding-004", api_key: Optional[str] = None):
        self.model = model
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        
        if not self.api_key:
            raise ValueError("Google API key required")
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self.genai = genai
        except ImportError:
            raise ImportError("Install google-generativeai: pip install google-generativeai")
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Google"""
        embeddings = []
        for text in texts:
            result = self.genai.embed_content(
                model=self.model,
                content=text,
                task_type="retrieval_document"
            )
            embeddings.append(result['embedding'])
            time.sleep(0.1)  # Rate limiting
        return embeddings
    
    @property
    def dimension(self) -> int:
        return 768


class LocalEmbeddings(EmbeddingProvider):
    """Local embeddings using sentence-transformers (free, runs offline)"""
    
    def __init__(self, model: str = "all-MiniLM-L6-v2"):
        self.model_name = model
        
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading local model: {model}")
            self.model = SentenceTransformer(model)
            self._dimension = self.model.get_sentence_embedding_dimension()
        except ImportError:
            raise ImportError("Install sentence-transformers: pip install sentence-transformers")
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings locally"""
        embeddings = self.model.encode(texts, show_progress_bar=True)
        return [emb.tolist() for emb in embeddings]
    
    @property
    def dimension(self) -> int:
        return self._dimension


class EmbeddingGenerator:
    """Generate and manage embeddings for legal documents"""
    
    def __init__(
        self,
        provider: str = "local",
        output_dir: str = "data/embeddings",
        chunk_size: int = 500,
        chunk_overlap: int = 50
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
        # Initialize provider
        self.provider = self._get_provider(provider)
        logger.info(f"Using {provider} embeddings (dimension: {self.provider.dimension})")
    
    def _get_provider(self, name: str) -> EmbeddingProvider:
        """Get embedding provider by name"""
        providers = {
            "openai": lambda: OpenAIEmbeddings(),
            "google": lambda: GoogleEmbeddings(),
            "local": lambda: LocalEmbeddings(),
        }
        
        if name not in providers:
            raise ValueError(f"Unknown provider: {name}. Choose from: {list(providers.keys())}")
        
        return providers[name]()
    
    def _chunk_text(self, text: str, metadata: dict) -> List[Document]:
        """Split text into overlapping chunks"""
        if len(text) <= self.chunk_size:
            return [Document(
                id=self._generate_id(text, metadata),
                text=text,
                metadata=metadata
            )]
        
        chunks = []
        start = 0
        chunk_num = 0
        
        while start < len(text):
            end = start + self.chunk_size
            chunk_text = text[start:end]
            
            # Try to break at sentence boundary
            if end < len(text):
                last_period = chunk_text.rfind('.')
                if last_period > self.chunk_size // 2:
                    chunk_text = chunk_text[:last_period + 1]
                    end = start + last_period + 1
            
            chunk_metadata = {
                **metadata,
                'chunk_num': chunk_num,
                'char_start': start,
                'char_end': end
            }
            
            chunks.append(Document(
                id=self._generate_id(chunk_text, chunk_metadata),
                text=chunk_text.strip(),
                metadata=chunk_metadata
            ))
            
            start = end - self.chunk_overlap
            chunk_num += 1
        
        return chunks
    
    def _generate_id(self, text: str, metadata: dict) -> str:
        """Generate unique ID for a chunk"""
        content = f"{text}:{json.dumps(metadata, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def process_parsed_acts(self, input_file: str, batch_size: int = 32) -> str:
        """Process parsed acts and generate embeddings"""
        
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_path, encoding='utf-8') as f:
            acts = json.load(f)
        
        logger.info(f"Processing {len(acts)} acts...")
        
        # Create documents from acts and sections
        all_documents = []
        
        for act in acts:
            act_title = act.get('title', 'Unknown')
            act_year = act.get('year')
            
            # Create document for act preamble/overview
            if act.get('preamble'):
                chunks = self._chunk_text(
                    act['preamble'],
                    {
                        'type': 'preamble',
                        'act_title': act_title,
                        'year': act_year,
                        'source_file': act.get('source_file', '')
                    }
                )
                all_documents.extend(chunks)
            
            # Create documents for each section
            for section in act.get('sections', []):
                section_text = f"{section.get('section_title', '')}\n\n{section.get('text', '')}"
                
                chunks = self._chunk_text(
                    section_text,
                    {
                        'type': 'section',
                        'act_title': act_title,
                        'year': act_year,
                        'section_number': section.get('section_number'),
                        'section_title': section.get('section_title'),
                        'source_file': act.get('source_file', '')
                    }
                )
                all_documents.extend(chunks)
        
        logger.info(f"Created {len(all_documents)} document chunks")
        
        # Generate embeddings in batches
        total_batches = (len(all_documents) + batch_size - 1) // batch_size
        
        for i in range(0, len(all_documents), batch_size):
            batch = all_documents[i:i + batch_size]
            texts = [doc.text for doc in batch]
            
            batch_num = i // batch_size + 1
            logger.info(f"Generating embeddings batch {batch_num}/{total_batches}")
            
            try:
                embeddings = self.provider.embed(texts)
                for doc, emb in zip(batch, embeddings):
                    doc.embedding = emb
            except Exception as e:
                logger.error(f"Error generating embeddings: {e}")
                continue
        
        # Save embeddings
        output_file = self.output_dir / "legal_embeddings.json"
        
        result = {
            'metadata': {
                'total_documents': len(all_documents),
                'embedding_dimension': self.provider.dimension,
                'chunk_size': self.chunk_size,
                'source_file': str(input_file)
            },
            'documents': [doc.to_dict() for doc in all_documents if doc.embedding]
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
        
        logger.info(f"Saved {len(result['documents'])} embeddings to {output_file}")
        
        # Also save in a format suitable for vector DBs (just embeddings + ids)
        vectors_file = self.output_dir / "vectors.npy"
        ids_file = self.output_dir / "ids.json"
        
        vectors = np.array([doc.embedding for doc in all_documents if doc.embedding])
        ids = [doc.id for doc in all_documents if doc.embedding]
        
        np.save(vectors_file, vectors)
        with open(ids_file, 'w') as f:
            json.dump(ids, f)
        
        logger.info(f"Saved vectors to {vectors_file} (shape: {vectors.shape})")
        
        return str(output_file)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate embeddings for legal documents')
    parser.add_argument('--input', default='data/processed/parsed_acts.json',
                       help='Input file with parsed acts')
    parser.add_argument('--output-dir', default='data/embeddings',
                       help='Output directory for embeddings')
    parser.add_argument('--provider', choices=['local', 'openai', 'google'],
                       default='local', help='Embedding provider')
    parser.add_argument('--chunk-size', type=int, default=500,
                       help='Maximum chunk size')
    parser.add_argument('--batch-size', type=int, default=32,
                       help='Batch size for embedding generation')
    
    args = parser.parse_args()
    
    generator = EmbeddingGenerator(
        provider=args.provider,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size
    )
    
    output = generator.process_parsed_acts(
        input_file=args.input,
        batch_size=args.batch_size
    )
    
    print(f"\nEmbeddings saved to: {output}")


if __name__ == '__main__':
    main()
