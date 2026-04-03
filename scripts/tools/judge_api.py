"""
Judge Intelligence API
=======================
FastAPI endpoints for searching and exploring judge profiles.
"""

import json
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# Load judge profiles on startup
DATA_PATH = Path(__file__).parent / "judge_profiles_full.json"

app = FastAPI(
    title="Judge Intelligence API",
    description="Search and explore Pakistani judge profiles from 1,729 case laws",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global data store
judges_data = {}
judges_by_id = {}
judges_by_name = {}


def load_data():
    """Load judge profiles from JSON file."""
    global judges_data, judges_by_id, judges_by_name
    
    if DATA_PATH.exists():
        with open(DATA_PATH, 'r', encoding='utf-8') as f:
            judges_data = json.load(f)
        
        # Index by ID and name
        for judge in judges_data.get('judges', []):
            judges_by_id[judge['id']] = judge
            judges_by_name[judge['name'].lower()] = judge
        
        print(f"Loaded {len(judges_by_id)} judges")
    else:
        print(f"Warning: Data file not found at {DATA_PATH}")


# Load data on startup
load_data()


# Response models
class JudgeSummary(BaseModel):
    id: int
    name: str
    primary_court: str
    total_cases: int
    cases_as_author: int
    is_chief_justice: bool


class JudgeDetail(BaseModel):
    id: int
    name: str
    primary_court: str
    courts: list[str]
    first_seen: Optional[str]
    last_seen: Optional[str]
    total_cases: int
    cases_as_author: int
    cases_as_bench: int
    is_chief_justice: bool
    avg_judgment_length: float
    practice_areas: dict
    cited_statutes: dict
    co_judges: dict
    cases_by_year: dict
    cases_by_reporter: dict


class JudgeAnalytics(BaseModel):
    id: int
    name: str
    total_cases: int
    active_years: int
    avg_judgment_length: float
    top_practice_areas: list[dict]
    top_co_judges: list[dict]
    top_cited_statutes: list[dict]
    yearly_trend: dict


class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    judges: list[JudgeSummary]


# Endpoints
@app.get("/")
async def root():
    """API root with basic info."""
    return {
        "name": "Judge Intelligence API",
        "version": "1.0.0",
        "total_judges": len(judges_by_id),
        "endpoints": {
            "list_judges": "/judges",
            "get_judge": "/judges/{id}",
            "judge_cases": "/judges/{id}/cases",
            "search_judges": "/judges/search?name=",
            "judge_analytics": "/judges/{id}/analytics",
            "top_judges": "/judges/top",
            "courts": "/courts",
            "practice_areas": "/practice-areas"
        }
    }


@app.get("/judges", response_model=PaginatedResponse)
async def list_judges(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    court: Optional[str] = None,
    min_cases: Optional[int] = None,
    sort_by: str = Query("total_cases", enum=["total_cases", "name", "first_seen"])
):
    """
    List all judges with pagination and filtering.
    
    - **page**: Page number (1-indexed)
    - **page_size**: Number of judges per page (max 100)
    - **court**: Filter by court name (partial match)
    - **min_cases**: Minimum number of cases
    - **sort_by**: Sort field
    """
    judges = list(judges_by_id.values())
    
    # Apply filters
    if court:
        court_lower = court.lower()
        judges = [j for j in judges if court_lower in (j.get('primary_court') or '').lower()]
    
    if min_cases:
        judges = [j for j in judges if j.get('total_cases', 0) >= min_cases]
    
    # Sort
    if sort_by == "total_cases":
        judges.sort(key=lambda x: x.get('total_cases', 0), reverse=True)
    elif sort_by == "name":
        judges.sort(key=lambda x: x.get('name', ''))
    elif sort_by == "first_seen":
        judges.sort(key=lambda x: x.get('first_seen') or '', reverse=True)
    
    # Paginate
    total = len(judges)
    start = (page - 1) * page_size
    end = start + page_size
    page_judges = judges[start:end]
    
    # Convert to summaries
    summaries = [
        JudgeSummary(
            id=j['id'],
            name=j['name'],
            primary_court=j.get('primary_court', ''),
            total_cases=j.get('total_cases', 0),
            cases_as_author=j.get('cases_as_author', 0),
            is_chief_justice=j.get('is_chief_justice', False)
        )
        for j in page_judges
    ]
    
    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        judges=summaries
    )


@app.get("/judges/search")
async def search_judges(
    name: str = Query(..., min_length=2, description="Name to search for"),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Search judges by name (partial match).
    
    - **name**: Name or partial name to search
    - **limit**: Maximum results to return
    """
    name_lower = name.lower()
    matches = []
    
    for judge in judges_by_id.values():
        if name_lower in judge['name'].lower():
            matches.append({
                "id": judge['id'],
                "name": judge['name'],
                "primary_court": judge.get('primary_court', ''),
                "total_cases": judge.get('total_cases', 0),
                "match_score": 1.0 if name_lower == judge['name'].lower() else 0.5
            })
    
    # Sort by match score and case count
    matches.sort(key=lambda x: (-x['match_score'], -x['total_cases']))
    
    return {
        "query": name,
        "results": matches[:limit],
        "total_matches": len(matches)
    }


@app.get("/judges/top")
async def get_top_judges(
    n: int = Query(10, ge=1, le=50),
    by: str = Query("cases", enum=["cases", "authored", "co_judges"])
):
    """
    Get top N judges by various metrics.
    
    - **n**: Number of judges to return
    - **by**: Metric to rank by (cases, authored, co_judges)
    """
    judges = list(judges_by_id.values())
    
    if by == "cases":
        judges.sort(key=lambda x: x.get('total_cases', 0), reverse=True)
    elif by == "authored":
        judges.sort(key=lambda x: x.get('cases_as_author', 0), reverse=True)
    elif by == "co_judges":
        judges.sort(key=lambda x: len(x.get('co_judges', {})), reverse=True)
    
    return {
        "metric": by,
        "judges": [
            {
                "rank": i + 1,
                "id": j['id'],
                "name": j['name'],
                "primary_court": j.get('primary_court', ''),
                "total_cases": j.get('total_cases', 0),
                "cases_as_author": j.get('cases_as_author', 0),
                "co_judges_count": len(j.get('co_judges', {}))
            }
            for i, j in enumerate(judges[:n])
        ]
    }


@app.get("/judges/{judge_id}")
async def get_judge(judge_id: int):
    """
    Get full profile for a specific judge.
    
    - **judge_id**: Judge ID
    """
    if judge_id not in judges_by_id:
        raise HTTPException(status_code=404, detail=f"Judge {judge_id} not found")
    
    return judges_by_id[judge_id]


@app.get("/judges/{judge_id}/cases")
async def get_judge_cases(
    judge_id: int,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """
    Get cases for a specific judge.
    
    - **judge_id**: Judge ID
    - **limit**: Maximum cases to return
    - **offset**: Starting offset for pagination
    """
    if judge_id not in judges_by_id:
        raise HTTPException(status_code=404, detail=f"Judge {judge_id} not found")
    
    judge = judges_by_id[judge_id]
    citations = judge.get('case_citations', [])
    
    return {
        "judge_id": judge_id,
        "judge_name": judge['name'],
        "total_cases": len(citations),
        "offset": offset,
        "limit": limit,
        "cases": citations[offset:offset + limit]
    }


@app.get("/judges/{judge_id}/analytics")
async def get_judge_analytics(judge_id: int):
    """
    Get detailed analytics for a specific judge.
    
    - **judge_id**: Judge ID
    """
    if judge_id not in judges_by_id:
        raise HTTPException(status_code=404, detail=f"Judge {judge_id} not found")
    
    judge = judges_by_id[judge_id]
    
    # Calculate active years
    first = judge.get('first_seen')
    last = judge.get('last_seen')
    active_years = 0
    if first and last:
        try:
            first_year = int(first[:4])
            last_year = int(last[:4])
            active_years = last_year - first_year + 1
        except (ValueError, TypeError):
            pass
    
    # Top practice areas
    practice_areas = judge.get('practice_areas', {})
    top_areas = [
        {"area": k, "cases": v}
        for k, v in sorted(practice_areas.items(), key=lambda x: -x[1])[:5]
    ]
    
    # Top co-judges
    co_judges = judge.get('co_judges', {})
    top_co = [
        {"name": k, "cases_together": v}
        for k, v in sorted(co_judges.items(), key=lambda x: -x[1])[:5]
    ]
    
    # Top cited statutes
    statutes = judge.get('cited_statutes', {})
    top_statutes = [
        {"statute": k, "citations": v}
        for k, v in sorted(statutes.items(), key=lambda x: -x[1])[:5]
    ]
    
    return JudgeAnalytics(
        id=judge['id'],
        name=judge['name'],
        total_cases=judge.get('total_cases', 0),
        active_years=active_years,
        avg_judgment_length=judge.get('avg_judgment_length', 0),
        top_practice_areas=top_areas,
        top_co_judges=top_co,
        top_cited_statutes=top_statutes,
        yearly_trend=judge.get('cases_by_year', {})
    )


@app.get("/courts")
async def list_courts():
    """Get list of all courts with judge counts."""
    court_counts = {}
    
    for judge in judges_by_id.values():
        court = judge.get('primary_court', 'Unknown')
        if court:
            court_counts[court] = court_counts.get(court, 0) + 1
    
    return {
        "total_courts": len(court_counts),
        "courts": [
            {"name": k, "judge_count": v}
            for k, v in sorted(court_counts.items(), key=lambda x: -x[1])
        ]
    }


@app.get("/practice-areas")
async def list_practice_areas():
    """Get list of practice areas with case counts."""
    area_counts = {}
    
    for judge in judges_by_id.values():
        for area, count in judge.get('practice_areas', {}).items():
            area_counts[area] = area_counts.get(area, 0) + count
    
    return {
        "total_areas": len(area_counts),
        "practice_areas": [
            {"name": k, "total_cases": v}
            for k, v in sorted(area_counts.items(), key=lambda x: -x[1])
        ]
    }


@app.get("/stats")
async def get_stats():
    """Get overall statistics about the judge database."""
    total_judges = len(judges_by_id)
    total_cases = sum(j.get('total_cases', 0) for j in judges_by_id.values())
    
    # Chief justices
    chief_justices = sum(1 for j in judges_by_id.values() if j.get('is_chief_justice'))
    
    # Courts
    courts = set(j.get('primary_court') for j in judges_by_id.values() if j.get('primary_court'))
    
    # Practice areas
    all_areas = set()
    for j in judges_by_id.values():
        all_areas.update(j.get('practice_areas', {}).keys())
    
    return {
        "total_judges": total_judges,
        "total_case_associations": total_cases,
        "chief_justices": chief_justices,
        "courts_count": len(courts),
        "practice_areas_count": len(all_areas),
        "data_source": "Pakistani Case Laws 2024",
        "cases_processed": judges_data.get('total_cases_processed', 0)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
