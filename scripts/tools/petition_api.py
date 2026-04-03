#!/usr/bin/env python3
"""
Petition Draft Analyzer API for Pakistani Legal Research Platform
FastAPI endpoints for analyzing draft petitions and finding precedents.

Endpoints:
- POST /analyze - Analyze draft petition text
- POST /analyze/file - Analyze uploaded PDF/DOCX
- GET /provisions/{section} - Get success rate for provision
- GET /precedents/search - Search precedents by query
- GET /stats - Get system statistics
"""

import json
import io
import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import asdict

from fastapi import FastAPI, HTTPException, UploadFile, File, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Import our modules
from claim_extractor import ClaimExtractor, ExtractedClaims
from outcome_classifier import OutcomeClassifier, Outcome
from precedent_matcher import PrecedentMatcher, MatchResult
from success_calculator import SuccessCalculator, SuccessAnalysis

# Optional dependencies for file parsing
try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    from docx import Document
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False


# Configuration
DATA_PATH = Path("data_v2")
CLASSIFIED_CASES = DATA_PATH / "cases_classified.jsonl"
PROVISION_STATS = DATA_PATH / "provision_stats.json"


# Pydantic models for API
class AnalyzeRequest(BaseModel):
    """Request model for petition analysis."""
    text: str = Field(..., description="Draft petition text to analyze")
    include_precedents: bool = Field(True, description="Include matching precedents")
    include_success_rates: bool = Field(True, description="Include success rate analysis")
    max_precedents: int = Field(10, description="Maximum precedents per category")
    court_filter: Optional[str] = Field(None, description="Filter by court level")
    

class PrecedentResponse(BaseModel):
    """Response model for a single precedent."""
    citation: str
    title: str
    court: str
    date: str
    outcome: str
    relevance_score: float
    summary: str


class ProvisionSuccessResponse(BaseModel):
    """Response model for provision success rate."""
    provision: str
    success_rate: float
    total_cases: int
    allowed: int
    dismissed: int
    sample_citations: List[str]


class AnalyzeResponse(BaseModel):
    """Response model for petition analysis."""
    claims_extracted: List[str]
    statutory_references: List[Dict]
    constitutional_articles: List[Dict]
    legal_principles: List[Dict]
    reliefs_sought: List[str]
    supporting_precedents: List[PrecedentResponse]
    contrary_precedents: List[PrecedentResponse]
    success_analysis: Dict[str, Dict]
    recommendation: str
    processing_time_ms: float


class SearchRequest(BaseModel):
    """Request model for precedent search."""
    query: str = Field(..., description="Search query")
    court: Optional[str] = Field(None, description="Filter by court")
    outcome: Optional[str] = Field(None, description="Filter by outcome")
    limit: int = Field(20, description="Maximum results")


class StatsResponse(BaseModel):
    """Response model for system statistics."""
    total_cases: int
    provisions_tracked: int
    last_updated: Optional[str]
    reporters_covered: List[str]


# Initialize FastAPI app
app = FastAPI(
    title="Pakistan Petition Draft Analyzer",
    description="Analyze draft petitions and find supporting/contrary precedents from Pakistani case law",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global service instances (lazy loaded)
_claim_extractor: Optional[ClaimExtractor] = None
_outcome_classifier: Optional[OutcomeClassifier] = None
_precedent_matcher: Optional[PrecedentMatcher] = None
_success_calculator: Optional[SuccessCalculator] = None
_initialized: bool = False


def get_claim_extractor() -> ClaimExtractor:
    global _claim_extractor
    if _claim_extractor is None:
        _claim_extractor = ClaimExtractor()
    return _claim_extractor


def get_outcome_classifier() -> OutcomeClassifier:
    global _outcome_classifier
    if _outcome_classifier is None:
        _outcome_classifier = OutcomeClassifier()
    return _outcome_classifier


def get_precedent_matcher() -> PrecedentMatcher:
    global _precedent_matcher
    if _precedent_matcher is None:
        _precedent_matcher = PrecedentMatcher()
        # Load cases index
        if CLASSIFIED_CASES.exists():
            _precedent_matcher.load_cases_index(CLASSIFIED_CASES)
    return _precedent_matcher


def get_success_calculator() -> SuccessCalculator:
    global _success_calculator
    if _success_calculator is None:
        _success_calculator = SuccessCalculator()
        # Load from pre-computed stats or build from cases
        if CLASSIFIED_CASES.exists():
            _success_calculator.load_cases(CLASSIFIED_CASES)
    return _success_calculator


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global _initialized
    print("Initializing petition analyzer services...")
    
    # Lazy load - actual loading happens on first request
    _initialized = True
    print("Services ready (lazy loading enabled)")


def extract_text_from_pdf(file_content: bytes) -> str:
    """Extract text from PDF file."""
    if not PDF_AVAILABLE:
        raise HTTPException(
            status_code=501, 
            detail="PDF parsing not available. Install pdfplumber: pip install pdfplumber"
        )
    
    text_parts = []
    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)
    
    return "\n\n".join(text_parts)


def extract_text_from_docx(file_content: bytes) -> str:
    """Extract text from DOCX file."""
    if not DOCX_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="DOCX parsing not available. Install python-docx: pip install python-docx"
        )
    
    doc = Document(io.BytesIO(file_content))
    text_parts = []
    
    for para in doc.paragraphs:
        if para.text.strip():
            text_parts.append(para.text)
    
    return "\n\n".join(text_parts)


def generate_recommendation(
    claims: ExtractedClaims,
    success_rates: Dict[str, Dict],
    supporting_count: int,
    contrary_count: int
) -> str:
    """Generate a recommendation based on analysis."""
    recommendations = []
    
    # Analyze success rates
    high_success = []
    low_success = []
    
    for provision, data in success_rates.items():
        rate = data.get('success_rate', 0)
        sample = data.get('sample_size', 0)
        
        if sample >= 5:
            if rate >= 0.7:
                high_success.append((provision, rate))
            elif rate <= 0.3:
                low_success.append((provision, rate))
    
    # Build recommendation
    if high_success:
        best = max(high_success, key=lambda x: x[1])
        recommendations.append(
            f"Strong argument based on {best[0]} (success rate: {best[1]*100:.0f}%)"
        )
    
    if low_success:
        worst = min(low_success, key=lambda x: x[1])
        recommendations.append(
            f"Caution: Arguments under {worst[0]} have low success rate ({worst[1]*100:.0f}%)"
        )
    
    if supporting_count > contrary_count * 2:
        recommendations.append(
            f"Favorable precedent landscape: {supporting_count} supporting vs {contrary_count} contrary cases"
        )
    elif contrary_count > supporting_count * 2:
        recommendations.append(
            f"Challenging precedent landscape: {contrary_count} contrary vs {supporting_count} supporting cases"
        )
    
    if not recommendations:
        if supporting_count > 0:
            recommendations.append(
                f"Found {supporting_count} potentially relevant supporting precedents to consider"
            )
        else:
            recommendations.append(
                "Limited precedent data available. Consider broader research."
            )
    
    return " | ".join(recommendations)


@app.post("/analyze", response_model=AnalyzeResponse, tags=["Analysis"])
async def analyze_petition(request: AnalyzeRequest):
    """
    Analyze draft petition text.
    
    Extracts legal claims, finds relevant precedents, and calculates success rates.
    """
    start_time = time.time()
    
    if not request.text or len(request.text.strip()) < 50:
        raise HTTPException(
            status_code=400,
            detail="Petition text too short. Minimum 50 characters required."
        )
    
    # Extract claims
    extractor = get_claim_extractor()
    claims = extractor.extract(request.text)
    
    # Get provision keys for success rate lookup
    provision_keys = claims.get_provision_keys()
    
    # Find precedents
    supporting_precedents = []
    contrary_precedents = []
    
    if request.include_precedents:
        matcher = get_precedent_matcher()
        match_result = matcher.match(
            draft_text=request.text,
            provisions=provision_keys,
            court_filter=request.court_filter,
            n_results=request.max_precedents,
        )
        
        # Convert to response format
        for p in match_result.supporting_precedents:
            supporting_precedents.append(PrecedentResponse(
                citation=p.citation,
                title=p.title,
                court=p.court,
                date=p.date,
                outcome=p.outcome,
                relevance_score=round(p.relevance_score, 3),
                summary=p.summary,
            ))
        
        for p in match_result.contrary_precedents:
            contrary_precedents.append(PrecedentResponse(
                citation=p.citation,
                title=p.title,
                court=p.court,
                date=p.date,
                outcome=p.outcome,
                relevance_score=round(p.relevance_score, 3),
                summary=p.summary,
            ))
    
    # Calculate success rates
    success_analysis = {}
    
    if request.include_success_rates and provision_keys:
        calculator = get_success_calculator()
        
        for key in provision_keys[:10]:  # Limit to 10 provisions
            stats = calculator.get_success_rate(key)
            if stats and stats.total_cases >= 3:
                success_analysis[key] = {
                    "success_rate": round(stats.success_rate, 3),
                    "sample_size": stats.total_cases,
                    "allowed": stats.allowed,
                    "dismissed": stats.dismissed,
                }
    
    # Generate recommendation
    recommendation = generate_recommendation(
        claims=claims,
        success_rates=success_analysis,
        supporting_count=len(supporting_precedents),
        contrary_count=len(contrary_precedents),
    )
    
    # Calculate processing time
    processing_time = (time.time() - start_time) * 1000
    
    # Format claims for response
    claims_extracted = []
    for ref in claims.statutory_references[:5]:
        claims_extracted.append(f"Section {ref.section} - {ref.act}")
    for art in claims.constitutional_articles[:5]:
        claims_extracted.append(f"Article {art['article']} - {art.get('description', 'Constitution')}")
    for prin in claims.legal_principles[:3]:
        claims_extracted.append(prin.principle)
    
    return AnalyzeResponse(
        claims_extracted=claims_extracted,
        statutory_references=[asdict(r) for r in claims.statutory_references[:10]],
        constitutional_articles=claims.constitutional_articles[:10],
        legal_principles=[asdict(p) for p in claims.legal_principles[:10]],
        reliefs_sought=[r.relief_type for r in claims.reliefs_sought[:10]],
        supporting_precedents=supporting_precedents,
        contrary_precedents=contrary_precedents,
        success_analysis=success_analysis,
        recommendation=recommendation,
        processing_time_ms=round(processing_time, 2),
    )


@app.post("/analyze/file", response_model=AnalyzeResponse, tags=["Analysis"])
async def analyze_file(
    file: UploadFile = File(...),
    include_precedents: bool = Query(True),
    include_success_rates: bool = Query(True),
    max_precedents: int = Query(10),
):
    """
    Analyze uploaded PDF or DOCX petition file.
    """
    # Validate file type
    filename = file.filename.lower()
    if not (filename.endswith('.pdf') or filename.endswith('.docx')):
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Only PDF and DOCX files are accepted."
        )
    
    # Read file content
    content = await file.read()
    
    # Extract text based on file type
    if filename.endswith('.pdf'):
        text = extract_text_from_pdf(content)
    else:
        text = extract_text_from_docx(content)
    
    if not text or len(text.strip()) < 50:
        raise HTTPException(
            status_code=400,
            detail="Could not extract sufficient text from file."
        )
    
    # Analyze extracted text
    request = AnalyzeRequest(
        text=text,
        include_precedents=include_precedents,
        include_success_rates=include_success_rates,
        max_precedents=max_precedents,
    )
    
    return await analyze_petition(request)


@app.get("/provisions/{provision}", response_model=ProvisionSuccessResponse, tags=["Provisions"])
async def get_provision_success_rate(
    provision: str,
):
    """
    Get success rate for a specific legal provision.
    
    Examples:
    - /provisions/Section%2012%20CPC
    - /provisions/Article%20199
    """
    calculator = get_success_calculator()
    stats = calculator.get_success_rate(provision)
    
    if not stats:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for provision: {provision}"
        )
    
    return ProvisionSuccessResponse(
        provision=stats.provision,
        success_rate=round(stats.success_rate, 3),
        total_cases=stats.total_cases,
        allowed=stats.allowed,
        dismissed=stats.dismissed,
        sample_citations=stats.citations[:10],
    )


@app.get("/provisions", tags=["Provisions"])
async def list_provisions(
    min_cases: int = Query(5, description="Minimum cases for inclusion"),
    limit: int = Query(50, description="Maximum provisions to return"),
    sort_by: str = Query("success_rate", description="Sort by: success_rate or total_cases"),
):
    """
    List tracked provisions with their success rates.
    """
    calculator = get_success_calculator()
    
    top_provisions = calculator.get_top_provisions(
        n=limit,
        min_cases=min_cases,
        sort_by=sort_by,
    )
    
    return [
        {
            "provision": s.provision,
            "success_rate": round(s.success_rate, 3),
            "total_cases": s.total_cases,
            "allowed": s.allowed,
            "dismissed": s.dismissed,
        }
        for s in top_provisions
    ]


@app.post("/precedents/search", tags=["Precedents"])
async def search_precedents(request: SearchRequest):
    """
    Search for precedents by query text.
    """
    if not request.query or len(request.query) < 10:
        raise HTTPException(
            status_code=400,
            detail="Query too short. Minimum 10 characters required."
        )
    
    matcher = get_precedent_matcher()
    result = matcher.match(
        draft_text=request.query,
        court_filter=request.court,
        outcome_filter=request.outcome,
        n_results=request.limit,
    )
    
    all_precedents = (
        result.supporting_precedents + 
        result.contrary_precedents + 
        result.neutral_precedents
    )
    
    return {
        "total_matches": result.total_matches,
        "results": [
            {
                "citation": p.citation,
                "title": p.title,
                "court": p.court,
                "date": p.date,
                "outcome": p.outcome,
                "relevance_score": round(p.relevance_score, 3),
                "summary": p.summary,
            }
            for p in all_precedents[:request.limit]
        ],
    }


@app.get("/stats", response_model=StatsResponse, tags=["System"])
async def get_stats():
    """
    Get system statistics.
    """
    calculator = get_success_calculator()
    matcher = get_precedent_matcher()
    
    return StatsResponse(
        total_cases=len(matcher.cases_index) or calculator.cases_loaded,
        provisions_tracked=len(calculator.provision_stats),
        last_updated=None,  # Could track this
        reporters_covered=["SCMR", "PLD", "CLC", "PCrLJ", "PTD", "PLC", "MLD", "YLR"],
    )


@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "services": {
            "claim_extractor": "ready",
            "outcome_classifier": "ready",
            "precedent_matcher": "ready" if get_precedent_matcher().cases_index else "loading",
            "success_calculator": "ready" if get_success_calculator().cases_loaded > 0 else "loading",
        }
    }


# Main entry point
if __name__ == "__main__":
    import uvicorn
    
    print("Starting Petition Draft Analyzer API...")
    print("API documentation available at: http://localhost:8000/docs")
    
    uvicorn.run(
        "petition_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
