# Judge Intelligence System

A comprehensive system for extracting, profiling, and searching Pakistani judges from case law data.

## Overview

This system processes 1,729 Pakistani case law files to:
- Extract judge names with role identification (author, bench member, chief justice)
- Build detailed profiles including practice areas, co-judge patterns, and cited statutes
- Provide a searchable API for legal research

## Components

### 1. Judge Extractor (`judge_extractor.py`)

Core extraction engine that parses judge names from case files.

**Features:**
- Extracts from both metadata and judgment text
- Handles Pakistani naming patterns:
  - "Mr. Justice [Name]" / "Mrs. Justice [Name]"
  - "[Name], J." / "[Name], CJ" / "[Name], ACJ"
  - "Before: ..." / "Coram: ..." / "Present: ..."
- Role identification (author vs bench member)
- Name normalization for matching

**Usage:**
```python
from judge_extractor import JudgeExtractor, process_case_file

# Process a single case
result = process_case_file("path/to/case.json")
print(result.judges)  # List of JudgeInfo objects
```

### 2. Database Schema (`db_judges.sql`)

PostgreSQL schema for storing judge profiles.

**Tables:**
- `judges` - Master table of judges
- `cases` - Case references
- `judge_cases` - Many-to-many relationship with roles
- `judge_stats` - Aggregated statistics
- `judge_practice_areas` - Practice area breakdown
- `judge_cited_statutes` - Frequently cited statutes
- `judge_co_judges` - Co-occurrence patterns

**Setup:**
```bash
psql -U postgres -d yourdb -f db_judges.sql
```

### 3. Profile Builder (`build_judge_profiles.py`)

Processes all cases and builds comprehensive judge profiles.

**Features:**
- Practice area detection (Constitutional, Criminal, Civil, etc.)
- Statute citation extraction
- Co-judge relationship mapping
- Statistical analysis (judgment length, case counts)

**Usage:**
```bash
# Process all cases
python build_judge_profiles.py

# Limit to first 100 cases (for testing)
python build_judge_profiles.py --limit 100

# Custom output
python build_judge_profiles.py --output my_profiles.json --top 100
```

### 4. Judge API (`judge_api.py`)

FastAPI-based REST API for searching and exploring judge profiles.

**Endpoints:**

| Endpoint | Description |
|----------|-------------|
| `GET /judges` | List all judges with pagination |
| `GET /judges/{id}` | Full judge profile |
| `GET /judges/{id}/cases` | Cases by this judge |
| `GET /judges/{id}/analytics` | Detailed analytics |
| `GET /judges/search?name=` | Search by name |
| `GET /judges/top` | Top judges by metrics |
| `GET /courts` | List all courts |
| `GET /practice-areas` | Practice area distribution |
| `GET /stats` | Overall statistics |

**Start the API:**
```bash
uvicorn judge_api:app --reload --port 8000
```

**Example queries:**
```bash
# List judges
curl http://localhost:8000/judges?page=1&page_size=20

# Search by name
curl http://localhost:8000/judges/search?name=Qazi

# Get judge details
curl http://localhost:8000/judges/1

# Get analytics
curl http://localhost:8000/judges/1/analytics
```

## Output Files

### `judge_profiles.json`
Top 50 judges with full profiles. Compact for quick access.

### `judge_profiles_full.json`
All extracted judges (~413) with complete profiles.

### `judge_extraction_sample.json`
Sample extraction results for verification.

## Data Structure

### Judge Profile
```json
{
  "id": 1,
  "name": "Qazi Faez Isa",
  "primary_court": "Supreme Court of Pakistan",
  "courts": ["Supreme Court of Pakistan"],
  "first_seen": "2023-06-14",
  "last_seen": "2024-01-15",
  "total_cases": 80,
  "cases_as_author": 45,
  "cases_as_bench": 35,
  "is_chief_justice": true,
  "avg_judgment_length": 5234.5,
  "practice_areas": {
    "Constitutional Law": 45,
    "Criminal Law": 20
  },
  "co_judges": {
    "Amin-ud-Din Khan": 25,
    "Athar Minallah": 18
  },
  "cited_statutes": {
    "Constitution of Pakistan": 50,
    "Code of Criminal Procedure": 15
  }
}
```

## Statistics (from 1,729 cases)

- **Total Judges Extracted:** 413
- **Top Judges by Case Count:**
  1. Qazi Faez Isa (Supreme Court) - 80 cases
  2. Syed Hasan Azhar Rizvi (Supreme Court) - 56 cases
  3. Mohammad Karim Khan Agha (Sindh) - 53 cases

- **Practice Area Distribution:**
  - Criminal Law: 3,320 cases
  - Constitutional Law: 1,006 cases
  - Civil Law: 699 cases
  - Tax Law: 296 cases

- **Courts Represented:**
  - Supreme Court of Pakistan
  - Lahore High Court
  - Sindh High Court
  - Peshawar High Court
  - Islamabad High Court
  - Balochistan High Court
  - Federal Shariat Court

## Technical Requirements

- Python 3.10+
- FastAPI (for API)
- PostgreSQL (for database, optional)

**Install dependencies:**
```bash
pip install fastapi uvicorn
```

## Known Limitations

1. **Role Detection:** The author role is determined primarily by who writes the judgment (name at start with "J.---"). Bench composition may have additional judges not captured.

2. **Name Variations:** Some judges may appear under slightly different name spellings. The normalization handles common variations but edge cases may create duplicates.

3. **Practice Area Detection:** Based on keyword matching. Complex cases spanning multiple areas may only be categorized under the primary detected area.

4. **Date Parsing:** Handles common Pakistani date formats but unusual formats may not parse correctly.

## Future Enhancements

1. Add outcome tracking (allowed/dismissed/partly allowed)
2. Implement judge similarity scoring
3. Add temporal analysis of practice area trends
4. Link to full case text for research
5. Add advocate/counsel extraction

## License

Part of the Pakistan Legal Research Platform.
