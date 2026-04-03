#!/usr/bin/env python3
"""
FastAPI Timeline Prediction API for Pakistani Legal Cases
Provides endpoints for predicting case durations
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from pathlib import Path
from enum import Enum

# Import our predictor
from timeline_predictor import TimelinePredictor

# ============== Models ==============

class CaseType(str, Enum):
    CONSTITUTIONAL = "Constitutional"
    CRIMINAL = "Criminal"
    CIVIL = "Civil"
    FAMILY = "Family"
    TAX = "Tax"
    LABOUR = "Labour"
    SERVICE = "Service"
    LAND_ACQUISITION = "Land Acquisition"
    BANKING = "Banking"
    CORPORATE = "Corporate"
    ELECTION = "Election"
    ANTI_TERRORISM = "Anti-Terrorism"
    NARCOTICS = "Narcotics"
    SHARIAT = "Shariat"
    APPELLATE = "Appellate"
    GENERAL = "General"

class CourtLevel(str, Enum):
    SUPREME_COURT = "Supreme Court"
    FEDERAL_SHARIAT = "Federal Shariat Court"
    HIGH_COURT = "High Court"
    DISTRICT_COURT = "District Court"
    TRIBUNAL = "Tribunal"

class Jurisdiction(str, Enum):
    PUNJAB = "Punjab"
    SINDH = "Sindh"
    KPK = "KPK"
    BALOCHISTAN = "Balochistan"
    ISLAMABAD = "Islamabad"
    FEDERAL = "Federal"
    AJK = "AJK"
    GILGIT_BALTISTAN = "Gilgit-Baltistan"

class PredictionRequest(BaseModel):
    """Request model for duration prediction"""
    case_type: CaseType = Field(..., description="Type of legal case")
    court: CourtLevel = Field(default=CourtLevel.HIGH_COURT, description="Court level")
    jurisdiction: Jurisdiction = Field(default=Jurisdiction.FEDERAL, description="Jurisdiction/Province")
    
    class Config:
        json_schema_extra = {
            "example": {
                "case_type": "Constitutional",
                "court": "High Court",
                "jurisdiction": "Punjab"
            }
        }

class PredictionResponse(BaseModel):
    """Response model for duration prediction"""
    predicted_days: int = Field(..., description="Predicted duration in days")
    predicted_years: float = Field(..., description="Predicted duration in years")
    confidence_interval: Dict[str, Any] = Field(..., description="50% confidence interval")
    sample_size: int = Field(..., description="Number of similar cases analyzed")
    data_quality: str = Field(..., description="Quality of prediction (high/medium/low/insufficient)")
    human_readable: str = Field(..., description="Human-readable prediction message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "predicted_days": 1460,
                "predicted_years": 4.0,
                "confidence_interval": {
                    "low_days": 730,
                    "high_days": 2190,
                    "low_years": 2.0,
                    "high_years": 6.0
                },
                "sample_size": 45,
                "data_quality": "medium",
                "human_readable": "Similar cases typically take 4.0 years to resolve..."
            }
        }

class StatsResponse(BaseModel):
    """Response model for statistics"""
    total_cases: int
    global_stats: Optional[Dict[str, Any]]
    by_case_type: Dict[str, Any]
    by_court: Dict[str, Any]
    by_jurisdiction: Dict[str, Any]

class CaseTypeStatsResponse(BaseModel):
    """Response model for case type specific statistics"""
    case_type: str
    sample_size: int
    avg_duration_years: float
    median_duration_years: float
    min_duration_days: int
    max_duration_days: int
    p25_days: float
    p50_days: float
    p75_days: float
    p90_days: float

class TrendsResponse(BaseModel):
    """Response model for trends data"""
    trends_by_year: Dict[str, Any]
    summary: Dict[str, Any]

# ============== App Setup ==============

app = FastAPI(
    title="Case Timeline Predictor API",
    description="""
    ## Pakistani Legal Case Duration Prediction API
    
    Predicts how long legal cases will take based on historical data from 
    Pakistani case law (1,729 cases analyzed).
    
    ### Features
    - Duration prediction based on case type, court, and jurisdiction
    - Historical statistics by various groupings
    - Year-over-year trends analysis
    
    ### Data Sources
    - PLD (Pakistan Legal Decisions)
    - CLC (Civil Law Cases)
    - SCMR (Supreme Court Monthly Review)
    - PCrLJ (Pakistan Criminal Law Journal)
    - YLR (Yearly Law Reports)
    - And other reporters
    """,
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

# Initialize predictor
predictor: Optional[TimelinePredictor] = None

@app.on_event("startup")
async def startup_event():
    """Load prediction data on startup"""
    global predictor
    
    script_dir = Path(__file__).parent
    data_dir = script_dir / 'timeline_data'
    
    if not data_dir.exists():
        print("Warning: timeline_data directory not found. Running extraction...")
        try:
            from duration_extractor import main as extract_main
            extract_main()
        except Exception as e:
            print(f"Error running extraction: {e}")
    
    try:
        predictor = TimelinePredictor(str(data_dir))
        print(f"Loaded predictor with {len(predictor.durations)} cases")
    except Exception as e:
        print(f"Error loading predictor: {e}")
        predictor = None

# ============== Endpoints ==============

@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Case Timeline Predictor API",
        "version": "1.0.0",
        "cases_loaded": len(predictor.durations) if predictor else 0
    }

@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
async def predict_duration(request: PredictionRequest):
    """
    Predict case duration based on case type, court level, and jurisdiction.
    
    Returns predicted duration with confidence intervals.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    pred = predictor.predict(
        case_type=request.case_type.value,
        court=request.court.value,
        jurisdiction=request.jurisdiction.value
    )
    
    return PredictionResponse(
        predicted_days=pred.predicted_days,
        predicted_years=pred.predicted_years,
        confidence_interval={
            "low_days": pred.confidence_interval_low_days,
            "high_days": pred.confidence_interval_high_days,
            "low_years": pred.confidence_interval_low_years,
            "high_years": pred.confidence_interval_high_years,
        },
        sample_size=pred.sample_size,
        data_quality=pred.data_quality,
        human_readable=pred.to_human_readable()
    )

@app.get("/predict", response_model=PredictionResponse, tags=["Prediction"])
async def predict_duration_get(
    case_type: CaseType = Query(..., description="Type of legal case"),
    court: CourtLevel = Query(default=CourtLevel.HIGH_COURT, description="Court level"),
    jurisdiction: Jurisdiction = Query(default=Jurisdiction.FEDERAL, description="Jurisdiction")
):
    """
    Predict case duration (GET method for easy testing).
    """
    request = PredictionRequest(
        case_type=case_type,
        court=court,
        jurisdiction=jurisdiction
    )
    return await predict_duration(request)

@app.get("/stats", response_model=StatsResponse, tags=["Statistics"])
async def get_all_stats():
    """
    Get overall duration statistics for all case categories.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    summary = predictor.get_all_stats_summary()
    
    return StatsResponse(
        total_cases=summary['total_cases'],
        global_stats=summary['global_stats'],
        by_case_type=summary['by_case_type'],
        by_court=summary['by_court'],
        by_jurisdiction=summary['by_jurisdiction']
    )

@app.get("/stats/{case_type}", response_model=CaseTypeStatsResponse, tags=["Statistics"])
async def get_case_type_stats(case_type: CaseType):
    """
    Get detailed statistics for a specific case type.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    stats = predictor.get_stats(case_type=case_type.value)
    
    if not stats:
        raise HTTPException(status_code=404, detail=f"No data for case type: {case_type.value}")
    
    return CaseTypeStatsResponse(
        case_type=case_type.value,
        sample_size=stats.sample_size,
        avg_duration_years=round(stats.avg_duration_years, 2),
        median_duration_years=round(stats.median_duration_years, 2),
        min_duration_days=stats.min_duration_days,
        max_duration_days=stats.max_duration_days,
        p25_days=round(stats.p25_days, 0),
        p50_days=round(stats.p50_days, 0),
        p75_days=round(stats.p75_days, 0),
        p90_days=round(stats.p90_days, 0)
    )

@app.get("/stats/{court}/{jurisdiction}", tags=["Statistics"])
async def get_court_jurisdiction_stats(court: CourtLevel, jurisdiction: Jurisdiction):
    """
    Get statistics for a specific court and jurisdiction combination.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    # Get stats for each case type in this court/jurisdiction
    results = {}
    
    for ct in CaseType:
        stats = predictor.get_stats(
            case_type=ct.value,
            court=court.value,
            jurisdiction=jurisdiction.value
        )
        
        if stats and stats.sample_size > 0:
            results[ct.value] = {
                'sample_size': stats.sample_size,
                'avg_years': round(stats.avg_duration_years, 2),
                'median_years': round(stats.median_duration_years, 2)
            }
    
    return {
        "court": court.value,
        "jurisdiction": jurisdiction.value,
        "case_types": results
    }

@app.get("/trends", response_model=TrendsResponse, tags=["Trends"])
async def get_trends():
    """
    Get year-over-year trends in case duration.
    
    Shows whether courts are getting faster or slower over time.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    trends = predictor.get_trends()
    
    # Calculate summary
    years = sorted(trends.keys())
    if len(years) >= 2:
        first_year = years[0]
        last_year = years[-1]
        
        first_avg = trends[first_year]['avg_years']
        last_avg = trends[last_year]['avg_years']
        
        overall_change = round(((last_avg - first_avg) / first_avg) * 100, 1) if first_avg else None
        
        summary = {
            'period': f"{first_year}-{last_year}",
            'overall_change_percent': overall_change,
            'trend': 'slower' if overall_change and overall_change > 0 else 'faster' if overall_change and overall_change < 0 else 'stable',
            'first_year_avg_years': first_avg,
            'last_year_avg_years': last_avg
        }
    else:
        summary = {'note': 'Insufficient data for trend analysis'}
    
    return TrendsResponse(
        trends_by_year=trends,
        summary=summary
    )

@app.get("/rankings/fastest", tags=["Rankings"])
async def get_fastest_case_types():
    """
    Get case types ranked by how fast they resolve (fastest first).
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    summary = predictor.get_all_stats_summary()
    
    # Sort by median years
    rankings = []
    for case_type, data in summary['by_case_type'].items():
        rankings.append({
            'case_type': case_type,
            'median_years': data['median_years'],
            'avg_years': data['avg_years'],
            'sample_size': data['sample_size']
        })
    
    rankings.sort(key=lambda x: x['median_years'])
    
    return {
        'rankings': rankings,
        'fastest': rankings[0] if rankings else None,
        'slowest': rankings[-1] if rankings else None
    }

@app.get("/rankings/courts", tags=["Rankings"])
async def get_court_rankings():
    """
    Get courts ranked by average case duration.
    """
    if not predictor:
        raise HTTPException(status_code=503, detail="Predictor not initialized")
    
    summary = predictor.get_all_stats_summary()
    
    rankings = []
    for court, data in summary['by_court'].items():
        rankings.append({
            'court': court,
            'median_years': data['median_years'],
            'avg_years': data['avg_years'],
            'sample_size': data['sample_size']
        })
    
    rankings.sort(key=lambda x: x['median_years'])
    
    return {
        'rankings': rankings,
        'fastest_court': rankings[0] if rankings else None,
        'slowest_court': rankings[-1] if rankings else None
    }

@app.get("/case-types", tags=["Metadata"])
async def list_case_types():
    """List all available case types"""
    return [ct.value for ct in CaseType]

@app.get("/courts", tags=["Metadata"])
async def list_courts():
    """List all available court levels"""
    return [c.value for c in CourtLevel]

@app.get("/jurisdictions", tags=["Metadata"])
async def list_jurisdictions():
    """List all available jurisdictions"""
    return [j.value for j in Jurisdiction]

# ============== Run ==============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
