#!/usr/bin/env python3
"""
RAG Pipeline for Qanoon AI Research Copilot
Implements retrieval-augmented generation with citation grounding.
"""

import os
import re
import json
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime

# Local imports
from query_processor import QueryProcessor, ProcessedQuery, QueryIntent
from enhanced_vectorstore import HybridSearchEngine, CHROMADB_PATH, BM25_INDEX_PATH, COLLECTION_NAME, EMBEDDING_MODEL

# Optional: Anthropic for generation
try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


@dataclass
class RetrievedPassage:
    """A retrieved passage with metadata."""
    text: str
    citation: str
    court: str
    date: str
    chunk_type: str
    relevance_score: float
    rerank_score: Optional[float] = None
    passage_id: str = ""
    

@dataclass
class CitedClaim:
    """A claim with its supporting citation."""
    claim: str
    citation: str
    quote: str
    relevance: float
    verified: bool = True


@dataclass
class CopilotResponse:
    """Response from the AI Research Copilot."""
    question: str
    answer: str
    citations: List[Dict]
    confidence: float
    related_questions: List[str]
    intent: str
    practice_areas: List[str]
    retrieved_passages: List[Dict]
    processing_time_ms: int = 0
    model_used: str = "mock"
    
    def to_dict(self) -> Dict:
        return {
            'question': self.question,
            'answer': self.answer,
            'citations': self.citations,
            'confidence': self.confidence,
            'related_questions': self.related_questions,
            'intent': self.intent,
            'practice_areas': self.practice_areas,
            'retrieved_passages': self.retrieved_passages,
            'processing_time_ms': self.processing_time_ms,
            'model_used': self.model_used
        }


class CrossEncoderReranker:
    """
    Rerank results using a cross-encoder model.
    Falls back to simple keyword matching if model not available.
    """
    
    def __init__(self):
        self.model = None
        try:
            from sentence_transformers import CrossEncoder
            self.model = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
        except Exception:
            pass
    
    def rerank(self, query: str, passages: List[RetrievedPassage], 
               top_k: int = 5) -> List[RetrievedPassage]:
        """Rerank passages using cross-encoder or fallback."""
        if not passages:
            return []
        
        if self.model is not None:
            # Use cross-encoder
            pairs = [(query, p.text) for p in passages]
            scores = self.model.predict(pairs)
            
            for i, passage in enumerate(passages):
                passage.rerank_score = float(scores[i])
            
            passages.sort(key=lambda x: x.rerank_score or 0, reverse=True)
        else:
            # Fallback: boost based on keyword overlap
            query_words = set(query.lower().split())
            
            for passage in passages:
                passage_words = set(passage.text.lower().split())
                overlap = len(query_words & passage_words)
                # Combine with original score
                passage.rerank_score = passage.relevance_score + (overlap * 0.01)
            
            passages.sort(key=lambda x: x.rerank_score or 0, reverse=True)
        
        return passages[:top_k]


class AnswerGenerator:
    """Generate answers using LLM or mock."""
    
    SYSTEM_PROMPT = """You are an expert Pakistani legal research assistant. Your role is to answer legal questions based ONLY on the provided case law passages.

RULES:
1. Base your answer ONLY on the provided passages - never make up information
2. Cite cases using their citations (e.g., "2024 PLD 123") inline in your answer
3. Use quotes from passages to support key points
4. If the passages don't contain enough information, say "Based on the available cases, I cannot fully answer this question"
5. Structure your answer clearly with the legal principle first, then supporting cases
6. Be precise about courts (Supreme Court, High Court, etc.) and dates
7. Use professional legal language but remain accessible

FORMAT:
- Start with a direct answer to the question
- Support with specific case citations and quotes
- Note any limitations or caveats
- Keep answers focused and concise (200-400 words typically)"""

    ANSWER_PROMPT = """Based on the following Pakistani case law passages, answer this question:

QUESTION: {question}

RELEVANT PASSAGES:
{passages}

Provide a well-cited answer based ONLY on these passages. Include inline citations in format (Citation, Court, Year)."""

    def __init__(self, api_key: Optional[str] = None):
        self.client = None
        if HAS_ANTHROPIC and api_key:
            self.client = anthropic.Anthropic(api_key=api_key)
        elif HAS_ANTHROPIC:
            # Try environment variable
            env_key = os.environ.get('ANTHROPIC_API_KEY')
            if env_key:
                self.client = anthropic.Anthropic(api_key=env_key)
    
    def _format_passages(self, passages: List[RetrievedPassage]) -> str:
        """Format passages for the prompt."""
        formatted = []
        for i, p in enumerate(passages, 1):
            formatted.append(f"""[Passage {i}]
Citation: {p.citation}
Court: {p.court}
Date: {p.date}
Type: {p.chunk_type}
Relevance: {p.relevance_score:.2f}

{p.text[:1500]}...

---""")
        return "\n".join(formatted)
    
    def generate(self, question: str, passages: List[RetrievedPassage],
                 processed_query: ProcessedQuery) -> Tuple[str, List[CitedClaim], float]:
        """
        Generate an answer with citations.
        Returns: (answer_text, cited_claims, confidence)
        """
        if not passages:
            return (
                "I couldn't find any relevant case law to answer your question. Please try rephrasing or ask about a different topic.",
                [],
                0.0
            )
        
        formatted_passages = self._format_passages(passages)
        
        if self.client:
            # Use Claude
            try:
                response = self.client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1500,
                    system=self.SYSTEM_PROMPT,
                    messages=[{
                        "role": "user",
                        "content": self.ANSWER_PROMPT.format(
                            question=question,
                            passages=formatted_passages
                        )
                    }]
                )
                answer = response.content[0].text
                
                # Extract citations mentioned in answer
                cited_claims = self._extract_citations_from_answer(answer, passages)
                
                # Calculate confidence based on rerank scores
                avg_score = sum(p.rerank_score or p.relevance_score for p in passages[:3]) / min(3, len(passages))
                confidence = min(0.95, avg_score)
                
                return answer, cited_claims, confidence
                
            except Exception as e:
                print(f"API error: {e}")
                return self._generate_mock(question, passages, processed_query)
        else:
            return self._generate_mock(question, passages, processed_query)
    
    def _generate_mock(self, question: str, passages: List[RetrievedPassage],
                       processed_query: ProcessedQuery) -> Tuple[str, List[CitedClaim], float]:
        """Generate a mock answer when API is not available."""
        # Build a template-based answer
        answer_parts = []
        
        # Opening based on intent
        if processed_query.intent == QueryIntent.QUESTION:
            answer_parts.append(f"Based on Pakistani case law, here is the answer to your question:\n\n")
        elif processed_query.intent == QueryIntent.RESEARCH:
            answer_parts.append(f"Here are the relevant legal findings from Pakistani case law:\n\n")
        elif processed_query.intent == QueryIntent.PRECEDENT:
            answer_parts.append(f"The following precedents are relevant to your query:\n\n")
        else:
            answer_parts.append(f"Based on the available case law:\n\n")
        
        # Add key findings from top passages
        cited_claims = []
        for i, passage in enumerate(passages[:3], 1):
            # Extract a key quote
            sentences = re.split(r'(?<=[.!?])\s+', passage.text)
            key_quote = sentences[0] if sentences else passage.text[:200]
            
            if len(key_quote) > 300:
                key_quote = key_quote[:300] + "..."
            
            answer_parts.append(f"**{i}. {passage.citation}** ({passage.court})\n")
            answer_parts.append(f'   > "{key_quote}"\n\n')
            
            cited_claims.append(CitedClaim(
                claim=f"Finding from {passage.citation}",
                citation=passage.citation,
                quote=key_quote,
                relevance=passage.relevance_score,
                verified=True
            ))
        
        # Add caveat
        answer_parts.append("\n*Note: This is a summary based on the retrieved passages. For comprehensive legal advice, please consult a qualified lawyer.*")
        
        answer = "".join(answer_parts)
        
        # Calculate confidence
        avg_score = sum(p.relevance_score for p in passages[:3]) / min(3, len(passages))
        confidence = min(0.85, avg_score * 0.9)  # Mock answers get lower confidence
        
        return answer, cited_claims, confidence
    
    def _extract_citations_from_answer(self, answer: str, 
                                        passages: List[RetrievedPassage]) -> List[CitedClaim]:
        """Extract and verify citations mentioned in the answer."""
        cited_claims = []
        
        # Find all citation patterns in answer
        citation_pattern = r'\b(\d{4}\s+(?:SCMR|PLD|CLC|MLD|YLR|PCrLJ|PTD|PLC)\s+(?:\w+\s+)?\d+)\b'
        found_citations = re.findall(citation_pattern, answer, re.IGNORECASE)
        
        # Map to passages
        for citation in found_citations:
            citation_norm = re.sub(r'\s+', ' ', citation.upper())
            
            for passage in passages:
                passage_citation_norm = re.sub(r'\s+', ' ', passage.citation.upper())
                if citation_norm in passage_citation_norm or passage_citation_norm in citation_norm:
                    # Find relevant quote
                    sentences = re.split(r'(?<=[.!?])\s+', passage.text)
                    quote = sentences[0] if sentences else passage.text[:200]
                    
                    cited_claims.append(CitedClaim(
                        claim=f"Referenced {passage.citation}",
                        citation=passage.citation,
                        quote=quote[:300],
                        relevance=passage.relevance_score,
                        verified=True
                    ))
                    break
        
        return cited_claims


class RelatedQuestionGenerator:
    """Generate related questions for follow-up."""
    
    QUESTION_TEMPLATES = {
        'criminal': [
            "What is the punishment for {concept} in Pakistan?",
            "Can bail be granted in {concept} cases?",
            "What are the defenses available in {concept} cases?",
        ],
        'property': [
            "What is the procedure for {concept} in Pakistan?",
            "What are the rights of {concept}?",
            "How is {concept} resolved in Pakistani courts?",
        ],
        'family': [
            "What are the grounds for {concept} in Pakistan?",
            "What is the procedure for {concept}?",
            "How does Islamic law apply to {concept}?",
        ],
        'constitutional': [
            "Can a {concept} be challenged under Article 199?",
            "What fundamental rights apply to {concept}?",
            "How has the Supreme Court interpreted {concept}?",
        ],
        'general': [
            "What are the leading cases on {concept}?",
            "How have Pakistani courts addressed {concept}?",
            "What is the legal position on {concept} in Pakistan?",
        ]
    }
    
    def generate(self, processed_query: ProcessedQuery, 
                 passages: List[RetrievedPassage]) -> List[str]:
        """Generate related questions based on query and passages."""
        related = []
        
        # Get templates based on practice area
        practice_area = processed_query.practice_areas[0] if processed_query.practice_areas else 'general'
        templates = self.QUESTION_TEMPLATES.get(practice_area, self.QUESTION_TEMPLATES['general'])
        
        # Get concepts from query
        concepts = [e.normalized for e in processed_query.entities if e.entity_type == 'concept']
        if not concepts:
            concepts = processed_query.keywords[:2]
        
        # Generate questions
        for template in templates[:2]:
            for concept in concepts[:2]:
                question = template.format(concept=concept)
                if question not in related and question != processed_query.original_query:
                    related.append(question)
        
        # Add questions based on cited cases in passages
        for passage in passages[:2]:
            if passage.citation:
                related.append(f"What was the holding in {passage.citation}?")
        
        return related[:5]


class RAGPipeline:
    """Main RAG pipeline orchestrating all components."""
    
    def __init__(self, 
                 chromadb_path: Path = CHROMADB_PATH,
                 bm25_path: Path = BM25_INDEX_PATH,
                 collection_name: str = COLLECTION_NAME,
                 embedding_model: str = EMBEDDING_MODEL,
                 anthropic_api_key: Optional[str] = None):
        
        self.query_processor = QueryProcessor()
        self.search_engine = HybridSearchEngine(
            chromadb_path=chromadb_path,
            bm25_path=bm25_path,
            collection_name=collection_name,
            embedding_model=embedding_model
        )
        self.reranker = CrossEncoderReranker()
        self.generator = AnswerGenerator(api_key=anthropic_api_key)
        self.related_generator = RelatedQuestionGenerator()
    
    def retrieve(self, query: str, n_results: int = 10,
                 filters: Dict = None) -> List[RetrievedPassage]:
        """Retrieve relevant passages for a query."""
        results = self.search_engine.search(query, n_results=n_results, filters=filters)
        
        passages = []
        for r in results:
            passages.append(RetrievedPassage(
                text=r['text'],
                citation=r['metadata'].get('citation', ''),
                court=r['metadata'].get('court', ''),
                date=r['metadata'].get('date', ''),
                chunk_type=r['metadata'].get('chunk_type', 'body'),
                relevance_score=r['final_score'],
                passage_id=r['id']
            ))
        
        return passages
    
    def ask(self, question: str, n_retrieve: int = 15, 
            n_rerank: int = 5) -> CopilotResponse:
        """
        Answer a legal question with citations.
        
        Args:
            question: Natural language legal question
            n_retrieve: Number of passages to retrieve initially
            n_rerank: Number of passages to keep after reranking
        
        Returns:
            CopilotResponse with answer, citations, and metadata
        """
        import time
        start_time = time.time()
        
        # Process query
        processed = self.query_processor.process(question)
        
        # Get search queries
        search_queries = self.query_processor.get_search_queries(processed)
        
        # Retrieve for all queries and deduplicate
        all_passages = []
        seen_ids = set()
        
        for sq in search_queries:
            passages = self.retrieve(sq, n_results=n_retrieve // len(search_queries) + 2)
            for p in passages:
                if p.passage_id not in seen_ids:
                    seen_ids.add(p.passage_id)
                    all_passages.append(p)
        
        # Rerank
        reranked = self.reranker.rerank(question, all_passages, top_k=n_rerank)
        
        # Generate answer
        answer, cited_claims, confidence = self.generator.generate(
            question, reranked, processed
        )
        
        # Generate related questions
        related_questions = self.related_generator.generate(processed, reranked)
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)
        
        # Build response
        citations = [
            {
                'citation': c.citation,
                'quote': c.quote,
                'relevance': round(c.relevance, 2),
                'verified': c.verified
            }
            for c in cited_claims
        ]
        
        retrieved_passages = [
            {
                'citation': p.citation,
                'court': p.court,
                'date': p.date,
                'chunk_type': p.chunk_type,
                'relevance': round(p.relevance_score, 3),
                'rerank_score': round(p.rerank_score, 3) if p.rerank_score else None,
                'text_preview': p.text[:300] + '...' if len(p.text) > 300 else p.text
            }
            for p in reranked
        ]
        
        return CopilotResponse(
            question=question,
            answer=answer,
            citations=citations,
            confidence=round(confidence, 2),
            related_questions=related_questions,
            intent=processed.intent.value,
            practice_areas=processed.practice_areas,
            retrieved_passages=retrieved_passages,
            processing_time_ms=processing_time,
            model_used="claude-sonnet-4-20250514" if self.generator.client else "mock"
        )
    
    def research(self, topic: str, depth: int = 3) -> Dict:
        """
        Deep research mode - comprehensive analysis of a topic.
        
        Args:
            topic: Research topic
            depth: Number of query iterations (1-5)
        
        Returns:
            Comprehensive research results
        """
        depth = max(1, min(5, depth))
        
        all_passages = []
        all_citations = set()
        sub_questions = [topic]
        
        # Iterative retrieval
        for i in range(depth):
            for q in sub_questions[:3]:  # Limit questions per iteration
                passages = self.retrieve(q, n_results=10)
                for p in passages:
                    if p.citation not in all_citations:
                        all_citations.add(p.citation)
                        all_passages.append(p)
            
            # Generate new sub-questions for next iteration
            if i < depth - 1:
                processed = self.query_processor.process(topic)
                sub_questions = self.related_generator.generate(processed, all_passages[:5])
        
        # Rerank all passages
        reranked = self.reranker.rerank(topic, all_passages, top_k=15)
        
        # Generate comprehensive answer
        answer, cited_claims, confidence = self.generator.generate(
            f"Provide a comprehensive analysis of: {topic}",
            reranked,
            self.query_processor.process(topic)
        )
        
        return {
            'topic': topic,
            'summary': answer,
            'total_cases_found': len(all_citations),
            'key_citations': [
                {
                    'citation': p.citation,
                    'court': p.court,
                    'relevance': round(p.relevance_score, 2)
                }
                for p in reranked[:10]
            ],
            'confidence': round(confidence, 2),
            'depth': depth
        }


def main():
    """Test the RAG pipeline."""
    print("=" * 70)
    print("RAG Pipeline Test - Qanoon AI Research Copilot")
    print("=" * 70)
    
    # Check if vector store exists
    if not CHROMADB_PATH.exists():
        print(f"\n❌ Vector store not found at {CHROMADB_PATH}")
        print("Please run: python enhanced_vectorstore.py --force")
        return
    
    pipeline = RAGPipeline()
    
    test_questions = [
        "Can a landlord evict a tenant without notice in Pakistan?",
        "What is the punishment for murder under Section 302 PPC?",
        "What are the grounds for divorce in Pakistani family law?",
    ]
    
    for question in test_questions:
        print(f"\n{'='*70}")
        print(f"Question: {question}")
        print("-" * 70)
        
        response = pipeline.ask(question)
        
        print(f"\nIntent: {response.intent}")
        print(f"Practice Areas: {', '.join(response.practice_areas)}")
        print(f"Confidence: {response.confidence}")
        print(f"Processing Time: {response.processing_time_ms}ms")
        print(f"Model: {response.model_used}")
        
        print(f"\n📝 Answer:\n{response.answer}")
        
        if response.citations:
            print(f"\n📚 Citations:")
            for c in response.citations:
                print(f"  - {c['citation']} (relevance: {c['relevance']})")
        
        if response.related_questions:
            print(f"\n❓ Related Questions:")
            for q in response.related_questions:
                print(f"  - {q}")
        
        print(f"\n📄 Retrieved Passages: {len(response.retrieved_passages)}")


if __name__ == "__main__":
    main()
