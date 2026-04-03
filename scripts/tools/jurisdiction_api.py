"""
Jurisdiction Comparison API

FastAPI endpoints for multi-jurisdiction legal comparisons across Pakistan.

Endpoints:
- POST /compare - Compare legal question across jurisdictions
- GET /issues - List common legal issues with coverage
- GET /issues/{id} - Detailed comparison for specific issue
- GET /conflicts - List known conflicts between jurisdictions
- GET /jurisdiction/{code}/stats - Statistics for a jurisdiction
"""

import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Import our modules
from comparison_engine import ComparisonEngine
from jurisdiction_mapper import JurisdictionMapper, IssueMapping, MatchType
from jurisdiction_classifier import Jurisdiction

# Initialize FastAPI app
app = FastAPI(
    title="Pakistan Legal Jurisdiction Comparison API",
    description="Compare legal rulings across Pakistan's 7 jurisdictions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global engine instance (lazy loaded)
_engine: Optional[ComparisonEngine] = None
_mapper: Optional[JurisdictionMapper] = None
_initialized = False


def get_engine() -> ComparisonEngine:
    """Get or create the comparison engine"""
    global _engine, _initialized
    if _engine is None:
        data_dir = Path(os.getenv('DATA_DIR', Path(__file__).parent / 'data_v2'))
        _engine = ComparisonEngine(data_dir=data_dir)
    if not _initialized:
        _engine.load_data()
        _initialized = True
    return _engine


def get_mapper() -> JurisdictionMapper:
    """Get or create the jurisdiction mapper"""
    global _mapper
    if _mapper is None:
        engine = get_engine()
        _mapper = JurisdictionMapper(use_semantic=engine.use_semantic)
        # Share data with engine
        for citation, case in engine.cases.items():
            _mapper.process_case(case)
    return _mapper


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class CompareRequest(BaseModel):
    """Request model for comparison endpoint"""
    question: str = Field(..., description="Legal question to compare across jurisdictions")
    jurisdictions: Optional[List[str]] = Field(
        None, 
        description="Specific jurisdictions to include (default: all)"
    )
    limit_per_jurisdiction: int = Field(
        5,
        ge=1,
        le=20,
        description="Maximum cases to consider per jurisdiction"
    )


class JurisdictionHoldingResponse(BaseModel):
    """Response model for a jurisdiction's holding"""
    holding: str
    citation: str
    date: str
    confidence: float
    statutes: List[str]
    judges: List[str]


class CompareResponse(BaseModel):
    """Response model for comparison endpoint"""
    question: str
    timestamp: str
    jurisdictions: Dict[str, JurisdictionHoldingResponse]
    consensus: List[str]
    conflicts: List[str]
    gaps: List[str]
    related_issues: List[str]
    total_cases_found: int


class IssueListItem(BaseModel):
    """Summary of an issue in the list"""
    id: str
    normalized_issue: str
    category: str
    jurisdictions_covered: List[str]
    match_type: str
    has_conflicts: bool


class IssueListResponse(BaseModel):
    """Response for listing issues"""
    total: int
    issues: List[IssueListItem]
    categories: Dict[str, int]


class IssueDetailResponse(BaseModel):
    """Detailed response for a specific issue"""
    issue_id: str
    normalized_issue: str
    category: str
    positions: Dict[str, Dict[str, Any]]
    match_type: str
    consensus_points: List[str]
    conflict_points: List[str]
    gap_jurisdictions: List[str]
    keywords: List[str]


class ConflictResponse(BaseModel):
    """Response for a conflict item"""
    issue_id: str
    normalized_issue: str
    category: str
    jurisdictions_involved: List[str]
    conflict_description: str
    positions: Dict[str, Dict[str, Any]]


class JurisdictionStatsResponse(BaseModel):
    """Response for jurisdiction statistics"""
    jurisdiction: str
    total_cases: int
    categories: Dict[str, int]
    top_statutes: Dict[str, int]
    sample_citations: List[str]


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    loaded: bool
    cases_count: int
    jurisdictions_available: List[str]


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """
    Check API health and data loading status.
    """
    global _engine, _initialized
    
    cases_count = 0
    if _engine and _initialized:
        cases_count = len(_engine.cases)
    
    return HealthResponse(
        status="healthy" if _initialized else "initializing",
        loaded=_initialized,
        cases_count=cases_count,
        jurisdictions_available=[j.value for j in Jurisdiction if j != Jurisdiction.UNKNOWN]
    )


@app.post("/compare", response_model=CompareResponse, tags=["Comparison"])
async def compare_jurisdictions(request: CompareRequest):
    """
    Compare legal rulings across jurisdictions for a given question.
    
    This endpoint searches for relevant cases in each jurisdiction and
    extracts holdings to provide a multi-jurisdiction comparison.
    
    Example questions:
    - "What is the limitation period for breach of contract?"
    - "Is bail allowed in murder cases?"
    - "What constitutes specific performance of contract?"
    
    Returns:
    - Holdings from each jurisdiction that has ruled on the issue
    - Consensus points where jurisdictions agree
    - Conflicts where jurisdictions differ
    - Gaps where no ruling was found
    """
    engine = get_engine()
    
    result = engine.compare(request.question)
    
    # Convert to response format
    jurisdictions_response = {}
    for j, h in result.jurisdictions.items():
        if request.jurisdictions and j not in request.jurisdictions:
            continue
        
        jurisdictions_response[j] = JurisdictionHoldingResponse(
            holding=h.holding,
            citation=h.citation,
            date=h.date,
            confidence=h.confidence,
            statutes=h.statutes,
            judges=h.judges
        )
    
    return CompareResponse(
        question=result.query,
        timestamp=result.timestamp,
        jurisdictions=jurisdictions_response,
        consensus=result.consensus,
        conflicts=result.conflicts,
        gaps=result.gaps,
        related_issues=result.related_issues,
        total_cases_found=result.total_cases_found
    )


@app.get("/issues", response_model=IssueListResponse, tags=["Issues"])
async def list_issues(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=200, description="Maximum issues to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    has_conflicts: Optional[bool] = Query(None, description="Filter to only conflicts")
):
    """
    List common legal issues with jurisdiction coverage.
    
    Shows which issues have been addressed by which jurisdictions,
    helping identify areas of consensus, conflict, and gaps.
    """
    mapper = get_mapper()
    
    # Get all mappings
    all_mappings = mapper.get_issue_mappings(category=category, limit=limit + offset)
    
    # Filter if needed
    if has_conflicts is not None:
        if has_conflicts:
            all_mappings = [m for m in all_mappings if m.match_type == MatchType.CONFLICT]
        else:
            all_mappings = [m for m in all_mappings if m.match_type != MatchType.CONFLICT]
    
    # Apply pagination
    paginated = all_mappings[offset:offset + limit]
    
    # Count categories
    categories = {}
    for m in all_mappings:
        categories[m.category] = categories.get(m.category, 0) + 1
    
    # Build response
    issues = []
    for m in paginated:
        issues.append(IssueListItem(
            id=m.issue_id,
            normalized_issue=m.normalized_issue[:200],
            category=m.category,
            jurisdictions_covered=list(m.positions.keys()),
            match_type=m.match_type.value,
            has_conflicts=m.match_type == MatchType.CONFLICT
        ))
    
    return IssueListResponse(
        total=len(all_mappings),
        issues=issues,
        categories=categories
    )


@app.get("/issues/{issue_id}", response_model=IssueDetailResponse, tags=["Issues"])
async def get_issue_detail(issue_id: str):
    """
    Get detailed comparison for a specific legal issue.
    
    Includes holdings from each jurisdiction, consensus/conflict analysis,
    and identification of jurisdictions with no ruling.
    """
    mapper = get_mapper()
    
    # Find the issue
    all_mappings = mapper.get_issue_mappings(limit=1000)
    
    target_mapping = None
    for m in all_mappings:
        if m.issue_id == issue_id:
            target_mapping = m
            break
    
    if not target_mapping:
        raise HTTPException(status_code=404, detail=f"Issue {issue_id} not found")
    
    # Convert positions to dict
    positions_dict = {}
    for j, p in target_mapping.positions.items():
        positions_dict[j] = {
            'jurisdiction': p.jurisdiction,
            'citation': p.citation,
            'holding': p.holding,
            'confidence': p.confidence,
            'date': p.date,
            'statutes': p.statutes
        }
    
    return IssueDetailResponse(
        issue_id=target_mapping.issue_id,
        normalized_issue=target_mapping.normalized_issue,
        category=target_mapping.category,
        positions=positions_dict,
        match_type=target_mapping.match_type.value,
        consensus_points=target_mapping.consensus_points,
        conflict_points=target_mapping.conflict_points,
        gap_jurisdictions=target_mapping.gap_jurisdictions,
        keywords=target_mapping.keywords
    )


@app.get("/conflicts", response_model=List[ConflictResponse], tags=["Conflicts"])
async def list_conflicts(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=100, description="Maximum conflicts to return")
):
    """
    List known conflicts between jurisdictions.
    
    Returns issues where different jurisdictions have reached
    different conclusions on the same legal question.
    """
    mapper = get_mapper()
    
    conflicts = mapper.get_conflicts(limit=limit)
    
    if category:
        conflicts = [c for c in conflicts if c.category == category]
    
    response = []
    for c in conflicts:
        positions_dict = {}
        for j, p in c.positions.items():
            positions_dict[j] = {
                'citation': p.citation,
                'holding': p.holding[:200] if p.holding else '',
                'confidence': p.confidence
            }
        
        response.append(ConflictResponse(
            issue_id=c.issue_id,
            normalized_issue=c.normalized_issue[:200],
            category=c.category,
            jurisdictions_involved=list(c.positions.keys()),
            conflict_description='; '.join(c.conflict_points) if c.conflict_points else 'Conflict detected',
            positions=positions_dict
        ))
    
    return response


@app.get("/jurisdiction/{code}/stats", response_model=JurisdictionStatsResponse, tags=["Jurisdictions"])
async def get_jurisdiction_stats(code: str):
    """
    Get statistics for a specific jurisdiction.
    
    Available codes:
    - Federal (Supreme Court)
    - Sindh (Sindh High Court)
    - Punjab (Lahore High Court)
    - KPK (Peshawar High Court)
    - Balochistan (Balochistan High Court)
    - Islamabad (Islamabad High Court)
    - AJK (Azad Kashmir High Court)
    - FSC (Federal Shariat Court)
    """
    engine = get_engine()
    
    # Validate jurisdiction code
    valid_codes = [j.value for j in Jurisdiction if j != Jurisdiction.UNKNOWN]
    if code not in valid_codes:
        raise HTTPException(
            status_code=404,
            detail=f"Invalid jurisdiction code. Valid codes: {valid_codes}"
        )
    
    stats = engine.get_jurisdiction_stats(code)
    
    # Get sample citations
    sample_citations = [
        c for c, j in engine.case_jurisdictions.items()
        if j == code
    ][:5]
    
    return JurisdictionStatsResponse(
        jurisdiction=code,
        total_cases=stats['total_cases'],
        categories=stats['categories'],
        top_statutes=stats['top_statutes'],
        sample_citations=sample_citations
    )


@app.get("/jurisdictions", tags=["Jurisdictions"])
async def list_jurisdictions():
    """
    List all available jurisdictions with brief descriptions.
    """
    return {
        "jurisdictions": [
            {"code": "Federal", "name": "Supreme Court of Pakistan", "description": "Apex court with final appellate jurisdiction"},
            {"code": "Sindh", "name": "Sindh High Court", "description": "High Court for Sindh province (Karachi)"},
            {"code": "Punjab", "name": "Lahore High Court", "description": "High Court for Punjab province"},
            {"code": "KPK", "name": "Peshawar High Court", "description": "High Court for Khyber Pakhtunkhwa"},
            {"code": "Balochistan", "name": "Balochistan High Court", "description": "High Court for Balochistan (Quetta)"},
            {"code": "Islamabad", "name": "Islamabad High Court", "description": "High Court for Islamabad Capital Territory"},
            {"code": "AJK", "name": "Azad Kashmir High Court", "description": "High Court for Azad Jammu & Kashmir"},
            {"code": "FSC", "name": "Federal Shariat Court", "description": "Specialized court for Islamic law matters"},
        ]
    }


@app.get("/categories", tags=["Issues"])
async def list_categories():
    """
    List all legal issue categories.
    """
    return {
        "categories": [
            {"name": "limitation", "description": "Limitation periods and time-barred actions"},
            {"name": "contract", "description": "Contract law, breach, specific performance"},
            {"name": "property", "description": "Property rights, land, title disputes"},
            {"name": "constitutional", "description": "Constitutional law and fundamental rights"},
            {"name": "criminal", "description": "Criminal law matters"},
            {"name": "family", "description": "Family law, marriage, divorce, custody"},
            {"name": "tax", "description": "Tax law and revenue matters"},
            {"name": "service", "description": "Service law and employment"},
            {"name": "compensation", "description": "Compensation and damages"},
            {"name": "evidence", "description": "Evidence law and proof"},
            {"name": "jurisdiction", "description": "Jurisdictional matters"},
            {"name": "procedure", "description": "Procedural law (CPC, CrPC)"},
        ]
    }


# ============================================================================
# STARTUP/SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize engine on startup (optional - can be lazy loaded)"""
    # Uncomment to load data on startup:
    # get_engine()
    print("API started. Data will be loaded on first request.")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    print("API shutting down.")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print(f"Starting API server on {host}:{port}")
    print("Documentation available at /docs")
    
    uvicorn.run(
        "jurisdiction_api:app",
        host=host,
        port=port,
        reload=True
    )
