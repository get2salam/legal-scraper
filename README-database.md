# Qanoon Legal Research Platform - Database Setup

PostgreSQL database setup for the Pakistan case law research platform.

## Prerequisites

- PostgreSQL 12+ (with `pg_trgm` extension available)
- Python 3.8+
- psycopg2

## Quick Start

### 1. Install Dependencies

```bash
pip install psycopg2-binary
# or for production:
pip install psycopg2
```

### 2. Create Database

```bash
# Create the database
createdb qanoon

# Or via psql
psql -c "CREATE DATABASE qanoon"
```

### 3. Run Import

```bash
# Using default settings (connects to postgresql://localhost/qanoon)
python import_to_postgres.py

# With custom database URL
python import_to_postgres.py --db-url "postgresql://user:pass@localhost:5432/qanoon"

# Skip schema setup (if tables already exist)
python import_to_postgres.py --no-schema

# Verbose mode
python import_to_postgres.py -v
```

## Database Schema

### Main Table: `cases`

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| citation | VARCHAR(100) | Unique case citation (e.g., "2024 SCMR 150") |
| case_name | VARCHAR(100) | Internal case identifier |
| title | TEXT | Party names/case title |
| court | VARCHAR(255) | Court name |
| case_date | DATE | Parsed judgment date |
| date_raw | VARCHAR(100) | Original date string |
| judges | TEXT[] | Array of judge names |
| headnotes | TEXT | Case summary/headnotes |
| judgment_html | TEXT | Original HTML judgment |
| judgment_clean | TEXT | Plain text for search |
| statutes_cited | TEXT[] | Cited statutes |
| cases_cited | TEXT[] | Cited cases |
| fts_judgment | TSVECTOR | Full-text search vector (auto-generated) |
| fts_headnotes | TSVECTOR | Full-text search vector (auto-generated) |
| fts_combined | TSVECTOR | Weighted combined search vector |

### Indexes

- **B-tree**: citation, case_name, court, case_date
- **GIN (Full-text)**: fts_judgment, fts_headnotes, fts_combined
- **GIN (Array)**: judges, statutes_cited, cases_cited
- **Trigram**: citation (for fuzzy search)

## Search Functions

### Basic Full-Text Search

```sql
-- Search judgments and headnotes
SELECT * FROM search_cases('property transfer', 'combined', 20, 0);

-- Search only headnotes
SELECT * FROM search_cases('murder conviction', 'headnotes', 10, 0);
```

### Advanced Search with Filters

```sql
SELECT * FROM search_cases_advanced(
    query_text := 'constitutional rights',
    filter_court := 'Supreme Court',
    filter_date_from := '2020-01-01',
    filter_date_to := '2024-12-31',
    limit_count := 20
);
```

### Find Cases by Statute

```sql
SELECT * FROM find_cases_by_statute('Contract Act');
SELECT * FROM find_cases_by_statute('Pakistan Penal Code');
```

### Find Cases Citing a Specific Case

```sql
SELECT * FROM find_citing_cases('2024 SCMR 150');
```

### Fuzzy Citation Search

```sql
-- Typo-tolerant search using trigram similarity
SELECT citation, title, court, similarity(citation, '2024 SCR 150') as sim
FROM cases
WHERE citation % '2024 SCR 150'
ORDER BY sim DESC
LIMIT 5;
```

### Search by Judge

```sql
SELECT citation, title, case_date
FROM cases
WHERE 'Justice Faez Isa' = ANY(judges)
ORDER BY case_date DESC;
```

## Statistics Views

```sql
-- Cases by court
SELECT * FROM court_statistics;

-- Cases by year
SELECT * FROM yearly_statistics;
```

## Performance Tips

1. **Use search functions**: The pre-built functions use proper indexes
2. **Limit results**: Always use LIMIT to avoid scanning entire table
3. **Prefer `fts_combined`**: Weighted search gives better relevance
4. **Batch queries**: Use array operators for multiple filters

## Sample Queries

```sql
-- Get recent Supreme Court cases
SELECT citation, title, case_date
FROM cases
WHERE court ILIKE '%supreme court%'
ORDER BY case_date DESC
LIMIT 10;

-- Find cases about specific topic with snippets
SELECT 
    citation,
    ts_headline('english', headnotes, plainto_tsquery('land acquisition')) as snippet
FROM cases
WHERE fts_combined @@ plainto_tsquery('land acquisition')
ORDER BY ts_rank(fts_combined, plainto_tsquery('land acquisition')) DESC
LIMIT 10;

-- Cases with multiple cited statutes
SELECT citation, title, array_length(statutes_cited, 1) as num_statutes
FROM cases
WHERE array_length(statutes_cited, 1) > 3
ORDER BY num_statutes DESC;
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| DATABASE_URL | postgresql://localhost/qanoon | PostgreSQL connection string |

## File Structure

```
├── db_schema.sql           # PostgreSQL schema with indexes & functions
├── import_to_postgres.py   # JSONL to PostgreSQL import script
├── requirements-db.txt     # Python dependencies
├── README-database.md      # This file
└── data_v2/
    └── all_cases.jsonl     # Source data (1,657 cases, 162MB)
```
