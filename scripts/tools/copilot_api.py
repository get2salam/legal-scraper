#!/usr/bin/env python3
"""
Copilot API for Qanoon AI Research Copilot
FastAPI endpoints for the legal research assistant.
"""

import os
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
from collections import defaultdict
import json

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Local imports
from rag_pipeline import RAGPipeline, CopilotResponse
from query_processor import QueryProcessor, QueryIntent
from citation_verifier import CitationVerifier, AnswerSanitizer
from enhanced_vectorstore import CHROMADB_PATH, BM25_INDEX_PATH, COLLECTION_NAME, EMBEDDING_MODEL


# ============================================================================
# Pydantic Models
# ============================================================================

class AskRequest(BaseModel):
    """Request model for /ask endpoint."""
    question: str = Field(..., min_length=5, max_length=1000, description="Legal question to answer")
    n_results: int = Field(default=5, ge=1, le=20, description="Number of citations to return")
    verify_citations: bool = Field(default=True, description="Verify citations exist in database")
    session_id: Optional[str] = Field(default=None, description="Session ID for conversation history")
    

class ResearchRequest(BaseModel):
    """Request model for /research endpoint."""
    topic: str = Field(..., min_length=5, max_length=500, description="Research topic")
    depth: int = Field(default=3, ge=1, le=5, description="Research depth (1-5)")
    include_analysis: bool = Field(default=True, description="Include analytical summary")


class FollowupRequest(BaseModel):
    """Request model for /followup endpoint."""
    session_id: str = Field(..., description="Session ID from previous interaction")
    question: str = Field(..., min_length=5, max_length=1000, description="Follow-up question")


class SuggestRequest(BaseModel):
    """Request model for /suggest endpoint."""
    topic: Optional[str] = Field(default=None, description="Topic to get suggestions for")
    practice_area: Optional[str] = Field(default=None, description="Practice area filter")


class Citation(BaseModel):
    """Citation model for responses."""
    citation: str
    quote: str
    relevance: float
    verified: bool = True


class AskResponse(BaseModel):
    """Response model for /ask endpoint."""
    question: str
    answer: str
    citations: List[Citation]
    confidence: float
    confidence_level: str
    related_questions: List[str]
    intent: str
    practice_areas: List[str]
    processing_time_ms: int
    model_used: str
    session_id: str
    verification: Optional[Dict] = None


class ResearchResponse(BaseModel):
    """Response model for /research endpoint."""
    topic: str
    summary: str
    total_cases_found: int
    key_citations: List[Dict]
    confidence: float
    depth: int
    processing_time_ms: int


class SuggestResponse(BaseModel):
    """Response model for /suggest endpoint."""
    suggestions: List[str]
    practice_areas: List[str]


class HistoryItem(BaseModel):
    """Single item in conversation history."""
    id: str
    question: str
    answer: str
    timestamp: str
    citations_count: int


class HistoryResponse(BaseModel):
    """Response model for /history endpoint."""
    session_id: str
    items: List[HistoryItem]
    total_questions: int


class HealthResponse(BaseModel):
    """Response model for /health endpoint."""
    status: str
    database_connected: bool
    total_documents: int
    model: str
    version: str


# ============================================================================
# Session Storage (In-Memory for Demo)
# ============================================================================

class SessionStore:
    """Simple in-memory session storage."""
    
    def __init__(self, max_sessions: int = 1000, max_history_per_session: int = 50):
        self.sessions: Dict[str, List[Dict]] = defaultdict(list)
        self.max_sessions = max_sessions
        self.max_history = max_history_per_session
    
    def create_session(self) -> str:
        """Create a new session and return its ID."""
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = []
        
        # Cleanup old sessions if needed
        if len(self.sessions) > self.max_sessions:
            oldest = list(self.sessions.keys())[:len(self.sessions) - self.max_sessions]
            for old_id in oldest:
                del self.sessions[old_id]
        
        return session_id
    
    def add_interaction(self, session_id: str, question: str, response: CopilotResponse):
        """Add an interaction to session history."""
        if session_id not in self.sessions:
            self.sessions[session_id] = []
        
        interaction = {
            'id': str(uuid.uuid4()),
            'question': question,
            'answer': response.answer,
            'citations': response.citations,
            'timestamp': datetime.utcnow().isoformat(),
            'practice_areas': response.practice_areas
        }
        
        self.sessions[session_id].append(interaction)
        
        # Limit history size
        if len(self.sessions[session_id]) > self.max_history:
            self.sessions[session_id] = self.sessions[session_id][-self.max_history:]
    
    def get_history(self, session_id: str) -> List[Dict]:
        """Get history for a session."""
        return self.sessions.get(session_id, [])
    
    def get_context(self, session_id: str, last_n: int = 3) -> str:
        """Get conversation context for follow-up questions."""
        history = self.get_history(session_id)
        if not history:
            return ""
        
        context_parts = []
        for item in history[-last_n:]:
            context_parts.append(f"Q: {item['question']}\nA: {item['answer'][:500]}...")
        
        return "\n\n".join(context_parts)


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Qanoon AI Research Copilot",
    description="AI-powered legal research assistant for Pakistani case law",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
pipeline: Optional[RAGPipeline] = None
verifier: Optional[CitationVerifier] = None
session_store = SessionStore()


def get_pipeline() -> RAGPipeline:
    """Get or create the RAG pipeline."""
    global pipeline
    if pipeline is None:
        # Check if database exists
        if not CHROMADB_PATH.exists():
            raise HTTPException(
                status_code=503,
                detail=f"Vector database not found at {CHROMADB_PATH}. Please run 'python enhanced_vectorstore.py --force' first."
            )
        
        # Get API key from environment
        api_key = os.environ.get('ANTHROPIC_API_KEY')
        
        pipeline = RAGPipeline(
            chromadb_path=CHROMADB_PATH,
            bm25_path=BM25_INDEX_PATH,
            collection_name=COLLECTION_NAME,
            embedding_model=EMBEDDING_MODEL,
            anthropic_api_key=api_key
        )
    
    return pipeline


def get_verifier() -> CitationVerifier:
    """Get or create the citation verifier."""
    global verifier
    if verifier is None:
        verifier = CitationVerifier(
            chromadb_path=CHROMADB_PATH,
            collection_name=COLLECTION_NAME
        )
    return verifier


def get_confidence_level(confidence: float) -> str:
    """Convert confidence score to human-readable level."""
    if confidence >= 0.85:
        return "HIGH"
    elif confidence >= 0.7:
        return "MEDIUM"
    elif confidence >= 0.5:
        return "LOW"
    else:
        return "VERY LOW"


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", tags=["General"])
async def root():
    """API root endpoint."""
    return {
        "name": "Qanoon AI Research Copilot",
        "version": "1.0.0",
        "description": "AI-powered legal research assistant for Pakistani case law",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["General"])
async def health_check():
    """Check API health and database status."""
    try:
        rag = get_pipeline()
        doc_count = rag.search_engine.collection.count()
        
        return HealthResponse(
            status="healthy",
            database_connected=True,
            total_documents=doc_count,
            model=EMBEDDING_MODEL,
            version="1.0.0"
        )
    except Exception as e:
        return HealthResponse(
            status="degraded",
            database_connected=False,
            total_documents=0,
            model=EMBEDDING_MODEL,
            version="1.0.0"
        )


@app.post("/ask", response_model=AskResponse, tags=["Core"])
async def ask_question(request: AskRequest):
    """
    Answer a legal question with cited precedents.
    
    This is the main endpoint for legal research. It:
    1. Processes your natural language question
    2. Searches 1,729+ Pakistani case laws
    3. Returns an answer grounded in actual cases
    4. Provides verified citations you can trust
    
    **Example questions:**
    - "Can a landlord evict a tenant without notice?"
    - "What is the punishment for murder under Section 302 PPC?"
    - "What are the grounds for divorce in Pakistan?"
    """
    import time
    start = time.time()
    
    try:
        rag = get_pipeline()
        
        # Get or create session
        session_id = request.session_id
        if not session_id:
            session_id = session_store.create_session()
        
        # Add context from previous interactions
        context = session_store.get_context(session_id)
        question = request.question
        if context:
            question = f"Previous context:\n{context}\n\nNew question: {request.question}"
        
        # Get answer
        response = rag.ask(request.question, n_rerank=request.n_results)
        
        # Verify citations if requested
        verification = None
        if request.verify_citations:
            ver = get_verifier()
            passages = [p['text_preview'] for p in response.retrieved_passages]
            ver_result = ver.verify_answer(response.answer, passages)
            verification = ver_result.to_dict()
        
        # Store in session
        session_store.add_interaction(session_id, request.question, response)
        
        processing_time = int((time.time() - start) * 1000)
        
        return AskResponse(
            question=request.question,
            answer=response.answer,
            citations=[
                Citation(
                    citation=c['citation'],
                    quote=c['quote'],
                    relevance=c['relevance'],
                    verified=c.get('verified', True)
                )
                for c in response.citations
            ],
            confidence=response.confidence,
            confidence_level=get_confidence_level(response.confidence),
            related_questions=response.related_questions,
            intent=response.intent,
            practice_areas=response.practice_areas,
            processing_time_ms=processing_time,
            model_used=response.model_used,
            session_id=session_id,
            verification=verification
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/research", response_model=ResearchResponse, tags=["Core"])
async def deep_research(request: ResearchRequest):
    """
    Perform deep research on a legal topic.
    
    Unlike /ask which answers a specific question, /research performs
    iterative retrieval to gather comprehensive information on a topic.
    
    **Use for:**
    - Researching a new area of law
    - Finding all relevant precedents on a topic
    - Preparing for litigation
    
    **Depth levels:**
    - 1: Quick overview (1 iteration)
    - 3: Standard research (default)
    - 5: Comprehensive research (may take longer)
    """
    import time
    start = time.time()
    
    try:
        rag = get_pipeline()
        
        result = rag.research(request.topic, depth=request.depth)
        
        processing_time = int((time.time() - start) * 1000)
        
        return ResearchResponse(
            topic=result['topic'],
            summary=result['summary'],
            total_cases_found=result['total_cases_found'],
            key_citations=result['key_citations'],
            confidence=result['confidence'],
            depth=result['depth'],
            processing_time_ms=processing_time
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/followup", response_model=AskResponse, tags=["Core"])
async def followup_question(request: FollowupRequest):
    """
    Ask a follow-up question in an existing conversation.
    
    Uses the conversation history to understand context.
    
    **Example:**
    1. First ask: "What are the grounds for divorce in Pakistan?"
    2. Follow-up: "What about for non-Muslims?"
    """
    # Check session exists
    history = session_store.get_history(request.session_id)
    if not history:
        raise HTTPException(
            status_code=404,
            detail=f"Session {request.session_id} not found. Start a new conversation with /ask."
        )
    
    # Use /ask with session context
    ask_request = AskRequest(
        question=request.question,
        session_id=request.session_id
    )
    
    return await ask_question(ask_request)


@app.get("/suggest", response_model=SuggestResponse, tags=["Discovery"])
async def get_suggestions(
    topic: Optional[str] = Query(default=None, description="Topic to get suggestions for"),
    practice_area: Optional[str] = Query(default=None, description="Practice area filter")
):
    """
    Get suggested questions and topics.
    
    Useful for:
    - Discovering what to ask
    - Exploring a practice area
    - Getting started with research
    """
    # Default suggestions by practice area
    suggestions_db = {
        'criminal': [
            "What is the punishment for murder under Section 302 PPC?",
            "What are the grounds for bail in NAB cases?",
            "How is robbery different from dacoity under Pakistani law?",
            "What are the requirements for a valid FIR?",
            "Can bail be granted in terrorism cases?"
        ],
        'property': [
            "Can a landlord evict a tenant without notice?",
            "What is the procedure for property mutation?",
            "What are the rights of a tenant in Punjab?",
            "How is pre-emption right established?",
            "What is the limitation period for partition suits?"
        ],
        'family': [
            "What are the grounds for divorce in Pakistan?",
            "How is child custody determined?",
            "What is the procedure for khula?",
            "How is maintenance amount calculated?",
            "What is the wife's right to dower (mehr)?"
        ],
        'constitutional': [
            "What is a constitutional petition under Article 199?",
            "When can Supreme Court take suo motu notice?",
            "What are fundamental rights under the Constitution?",
            "Can fundamental rights be suspended during emergency?",
            "What is the scope of judicial review?"
        ],
        'tax': [
            "What is the penalty for tax evasion?",
            "How to appeal a tax assessment?",
            "What is the procedure for tax refund?",
            "What are the powers of FBR officials?",
            "How is capital gains tax calculated?"
        ],
        'labor': [
            "What is wrongful termination under labor law?",
            "How is gratuity calculated?",
            "What are the powers of NIRC?",
            "Can an employer terminate without notice?",
            "What is the procedure for industrial disputes?"
        ]
    }
    
    practice_areas = list(suggestions_db.keys())
    
    if practice_area and practice_area.lower() in suggestions_db:
        return SuggestResponse(
            suggestions=suggestions_db[practice_area.lower()],
            practice_areas=practice_areas
        )
    
    # Mix from all areas
    all_suggestions = []
    for area_suggestions in suggestions_db.values():
        all_suggestions.extend(area_suggestions[:2])
    
    return SuggestResponse(
        suggestions=all_suggestions[:10],
        practice_areas=practice_areas
    )


@app.get("/history", response_model=HistoryResponse, tags=["Session"])
async def get_history(
    session_id: str = Query(..., description="Session ID to get history for")
):
    """
    Get conversation history for a session.
    
    Use this to:
    - Review past questions and answers
    - Resume a previous research session
    """
    history = session_store.get_history(session_id)
    
    if not history:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} not found or has no history."
        )
    
    items = [
        HistoryItem(
            id=item['id'],
            question=item['question'],
            answer=item['answer'][:500] + '...' if len(item['answer']) > 500 else item['answer'],
            timestamp=item['timestamp'],
            citations_count=len(item.get('citations', []))
        )
        for item in history
    ]
    
    return HistoryResponse(
        session_id=session_id,
        items=items,
        total_questions=len(items)
    )


@app.delete("/history/{session_id}", tags=["Session"])
async def clear_history(session_id: str):
    """
    Clear conversation history for a session.
    """
    if session_id in session_store.sessions:
        del session_store.sessions[session_id]
        return {"status": "success", "message": f"Session {session_id} cleared."}
    
    raise HTTPException(
        status_code=404,
        detail=f"Session {session_id} not found."
    )


@app.get("/stats", tags=["Admin"])
async def get_stats():
    """Get API usage statistics."""
    try:
        rag = get_pipeline()
        doc_count = rag.search_engine.collection.count()
        
        return {
            "total_documents": doc_count,
            "active_sessions": len(session_store.sessions),
            "embedding_model": EMBEDDING_MODEL,
            "database_path": str(CHROMADB_PATH),
            "has_bm25": BM25_INDEX_PATH.exists()
        }
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Run the API server."""
    print("=" * 60)
    print("Qanoon AI Research Copilot API")
    print("=" * 60)
    
    # Check database
    if not CHROMADB_PATH.exists():
        print(f"\n❌ Vector database not found at {CHROMADB_PATH}")
        print("Please run: python enhanced_vectorstore.py --force")
        print("Then restart this server.")
        return
    
    print(f"\n✅ Database found at {CHROMADB_PATH}")
    print(f"📚 Starting server...")
    print(f"\n🌐 API Documentation: http://localhost:8000/docs")
    print(f"📖 Alternative Docs: http://localhost:8000/redoc")
    print(f"\n💡 Try: POST http://localhost:8000/ask")
    print('   Body: {"question": "What are the grounds for divorce in Pakistan?"}')
    
    uvicorn.run(
        "copilot_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    main()
