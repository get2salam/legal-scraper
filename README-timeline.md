# Case Timeline Predictor for Pakistani Legal Cases

A system that predicts how long legal cases will take based on historical data from 1,729 Pakistani case laws.

## Overview

This tool extracts filing and decision dates from Pakistani case law judgments, calculates case durations, and provides predictions for how long similar cases typically take to resolve.

### Key Statistics
- **Total cases analyzed:** 1,729
- **Cases with extractable duration:** 419 (24.2%)
- **Overall median duration:** 4.4 years
- **Fastest case type:** Corporate (0.9 years)
- **Slowest case type:** Civil (9.0 years)

## Files

| File | Description |
|------|-------------|
| `duration_extractor.py` | Extracts filing/decision dates from judgment text |
| `timeline_predictor.py` | Statistical prediction model |
| `timeline_api.py` | FastAPI REST API for predictions |
| `db_timeline.sql` | PostgreSQL schema for persistence |
| `timeline_analysis.json` | Detailed insights report |
| `timeline_data/` | Extracted data (generated) |

## Quick Start

### 1. Extract Duration Data

```bash
cd pakistan-legislation-scraper
python duration_extractor.py
```

This processes all JSON files in `data_v2/` and outputs to `timeline_data/`:
- `case_durations.json` - All cases with extracted metadata
- `valid_durations.json` - Only cases with valid durations
- `extraction_stats.json` - Extraction statistics

### 2. Generate Predictions

```bash
python timeline_predictor.py
```

This computes statistics and generates:
- `timeline_data/prediction_stats.json` - Statistics by category
- `timeline_data/yearly_trends.json` - Year-over-year trends

### 3. Run the API

```bash
pip install fastapi uvicorn
python timeline_api.py
```

Or with uvicorn:
```bash
uvicorn timeline_api:app --reload --port 8000
```

API documentation available at:
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)

## API Endpoints

### Prediction

```bash
# POST request
curl -X POST "http://localhost:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{"case_type": "Constitutional", "court": "High Court", "jurisdiction": "Punjab"}'

# GET request (for testing)
curl "http://localhost:8000/predict?case_type=Criminal&court=High%20Court&jurisdiction=Sindh"
```

Response:
```json
{
  "predicted_days": 880,
  "predicted_years": 2.41,
  "confidence_interval": {
    "low_days": 485,
    "high_days": 887,
    "low_years": 1.33,
    "high_years": 2.43
  },
  "sample_size": 25,
  "data_quality": "medium",
  "human_readable": "Similar cases typically take 2.4 years to resolve..."
}
```

### Statistics

```bash
# Overall stats
curl http://localhost:8000/stats

# Stats by case type
curl http://localhost:8000/stats/Constitutional

# Stats by court and jurisdiction
curl http://localhost:8000/stats/High%20Court/Punjab
```

### Rankings

```bash
# Fastest case types
curl http://localhost:8000/rankings/fastest

# Court rankings
curl http://localhost:8000/rankings/courts
```

### Trends

```bash
curl http://localhost:8000/trends
```

## Key Insights

### By Case Type (Median Duration)

| Rank | Case Type | Median Years | Sample Size |
|------|-----------|--------------|-------------|
| 1 | Corporate | 0.9 | 7 |
| 2 | Narcotics | 1.2 | 1 |
| 3 | Banking | 1.6 | 7 |
| 4 | Family | 2.3 | 38 |
| 5 | Constitutional | 2.4 | 65 |
| ... | ... | ... | ... |
| 13 | Tax | 8.1 | 9 |
| 14 | Land Acquisition | 9.6 | 7 |
| 15 | Civil | 9.0 | 30 |

### By Court Level

| Court | Median Years | Sample Size |
|-------|--------------|-------------|
| Tribunal | 0.7 | 1 |
| District Court | 2.5 | 2 |
| High Court | 3.9 | 319 |
| Supreme Court | 5.0 | 95 |
| Federal Shariat Court | 14.8 | 2 |

### By Jurisdiction

| Jurisdiction | Median Years | Sample Size |
|--------------|--------------|-------------|
| Balochistan | 2.3 | 35 |
| KPK | 2.9 | 14 |
| Federal | 4.0 | 71 |
| Islamabad | 4.3 | 28 |
| Punjab | 4.4 | 148 |
| Sindh | 6.8 | 122 |

## Date Extraction Patterns

The extractor looks for these patterns in judgment text:

1. **FIR Numbers** (Criminal): `FIR No. XX/2022` → Filing year 2022
2. **Explicit dates**: "suit was filed on 18.03.2022"
3. **Document dates**: "petition dated 27.04.2017"
4. **Case references**: "R.F.A No. 53 of 2014"

### Supported Date Formats
- `18.03.2022`, `18/03/2022`, `18-03-2022` (DD.MM.YYYY)
- `2022-03-18` (ISO)
- `18th March, 2022`, `1st January 2024` (Ordinal text)

## PostgreSQL Integration

To use with PostgreSQL:

```bash
# Create database and schema
psql -U postgres -f db_timeline.sql

# Import data
psql -U postgres -c "
  COPY case_durations(case_id, citation, court, jurisdiction, case_type, 
                      filing_date, decision_date, duration_days, 
                      filing_source, extraction_confidence)
  FROM 'timeline_data/case_durations.csv' CSV HEADER;
"

# Refresh statistics
psql -U postgres -c "SELECT refresh_duration_stats();"
```

### Useful Queries

```sql
-- Get prediction for a case
SELECT * FROM predict_duration('Constitutional', 'High Court', 'Punjab');

-- Statistics by case type
SELECT * FROM v_stats_by_case_type;

-- Fastest courts
SELECT * FROM v_stats_by_court ORDER BY avg_days;
```

## Limitations

1. **Extraction Rate:** Only 24.2% of cases had extractable filing dates
2. **Sample Bias:** Data from law reports (appellate cases overrepresented)
3. **Date Estimation:** Many criminal cases use FIR year (not exact filing date)
4. **Small Samples:** Some categories have <10 cases

## Future Improvements

- [ ] Machine learning model for date extraction
- [ ] Include trial court data
- [ ] Track individual judge performance
- [ ] Appeal success rate prediction
- [ ] Integration with court e-filing systems

## Dependencies

```txt
fastapi>=0.100.0
uvicorn>=0.22.0
pydantic>=2.0.0
python-dateutil>=2.8.2
```

Optional for database:
```txt
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0
```

## License

Part of the Pakistan Legislation Scraper project.
