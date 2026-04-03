# Pakistan Law Site - Legislation Scraper

A comprehensive scraper for Pakistani statutes and legislation from pakistanlawsite.com (PLS).

## Overview

This scraper extracts:
- **10,918+ statutes** organized alphabetically (A-Z)
- **Section-by-section content** for each statute
- **Case law citations** that reference each statute/section
- **Metadata**: jurisdiction, enactment date, amendments

## Files

| File | Description |
|------|-------------|
| `legislation_scraper.py` | Main scraper with full API support |
| `daily_legislation_scraper.py` | Daily scheduler (one alphabet per day) |
| `statute_case_linker.py` | Extracts statute↔case links |
| `legislation_schema.sql` | PostgreSQL database schema |

## Quick Start

### Test the Scraper

```bash
# Test login and basic functionality
python legislation_scraper.py test

# Check current status
python legislation_scraper.py status
```

### Scrape One Alphabet

```bash
# Scrape all statutes starting with 'A'
python legislation_scraper.py scrape --letter A

# Scrape with limit (for testing)
python legislation_scraper.py scrape --letter A --limit 10
```

### Full Scrape (26 days)

```bash
# Start from beginning
python legislation_scraper.py scrape

# Resume from last position
python legislation_scraper.py resume
```

### Daily Scheduler

```bash
# Run today's alphabet
python daily_legislation_scraper.py run

# Check status
python daily_legislation_scraper.py status

# Reset (start over)
python daily_legislation_scraper.py reset
```

## Data Structure

```
data_v2/
└── legislation/
    ├── A/
    │   ├── Anti_Terrorism_Act_1997.json
    │   └── original/
    │       └── Anti_Terrorism_Act_1997.html
    ├── B/
    │   └── ...
    ├── progress.json
    ├── legislation_index.json
    ├── statute_case_links.jsonl
    └── case_statute_links.jsonl
```

## JSON Schema

Each statute is saved as:

```json
{
  "id": "abc123def456",
  "title": "Anti Terrorism Act 1997",
  "short_title": "Anti Terrorism Act",
  "alphabet": "A",
  "enactment_date": "1997",
  "jurisdiction": "Federal",
  "status": "in_force",
  "sections": [
    {
      "section_id": "124944048",
      "number": "PREAMBLE",
      "title": "Anti Terrorism Act 1997",
      "text": "An Act to provide for...",
      "case_links": [
        {
          "citation": "2024 SCMR 123",
          "year": "2024",
          "reporter": "SCMR",
          "page": "123"
        }
      ]
    },
    {
      "section_id": "124944049",
      "number": "1",
      "title": "Short title and commencement",
      "text": "This Act may be called...",
      "case_links": []
    }
  ],
  "case_links": [
    {
      "citation": "2024 SCMR 123",
      "section": "PREAMBLE",
      "year": "2024",
      "reporter": "SCMR",
      "page": "123"
    }
  ],
  "full_text": "...",
  "scraped_at": "2026-02-06T20:10:00Z",
  "source_url": "https://..."
}
```

## API Endpoints Discovered

| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/Login/StatuecharSearch` | GET | `character` | Get statutes by letter |
| `/Login/GetStatuesSearch` | GET | `caseName` | Get statute sections |
| `/Login/SearchStatueFile` | POST | `caseTypeId` | Get section content |
| `/Login/GetStatuteCaseLaw` | POST | `caseTypeId`, `subTopic` | Get case citations |
| `/Login/GetNewStatue` | POST | `category` | Get new/recent statutes |

## Windows Task Scheduler Setup

1. Open Task Scheduler
2. Create Basic Task: "PLS Legislation Scraper"
3. Trigger: Daily at 10:00 AM
4. Action: Start a program
   - Program: `python`
   - Arguments: `daily_legislation_scraper.py run`
   - Start in: `C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper`

## PLS Operating Hours

The site operates **7 AM - 9 PM Pakistan Time (PKT)**.

The daily scheduler automatically checks operating hours and skips if outside.

Use `--force` to override:
```bash
python daily_legislation_scraper.py run --force
```

## Statute-Case Linking

After scraping, run the linker to extract all case citations:

```bash
python statute_case_linker.py
```

This creates:
- `statute_case_links.jsonl` - statute → cases mapping
- `case_statute_links.jsonl` - case → statutes mapping (reverse)
- `link_statistics.json` - summary statistics

## Database Import

```bash
# Create the schema
psql -U postgres -d pakistan_law -f legislation_schema.sql

# Import data (use import_legislation_to_postgres.py)
python import_legislation_to_postgres.py
```

## Technical Details

### TLS Fingerprinting

Uses `curl_cffi` with Chrome 120 fingerprint to avoid detection:
- Impersonates Chrome browser's TLS handshake
- Matches Chrome's HTTP/2 settings
- Uses realistic headers

### Rate Limiting

- 2-5 second delays between requests
- Extra delays every 5 sections
- 60 second backoff on 403/429 errors
- Human-like jitter in timing

### Progress Tracking

Progress is saved after every 10 statutes:
- `progress.json` - overall progress
- Can resume after interruption
- Skips already-scraped statutes

## Statistics

| Metric | Value |
|--------|-------|
| Total statutes | ~10,918 |
| Alphabets | A-Z (26) |
| Est. sections | ~100,000+ |
| Est. case links | ~500,000+ |
| Scrape time | ~26 days (1 alphabet/day) |

## Jurisdictions Detected

- Federal (Pakistan-wide)
- Punjab
- Sindh  
- KPK (Khyber Pakhtunkhwa)
- Balochistan
- AJK (Azad Jammu & Kashmir)
- Gilgit-Baltistan

## Error Handling

- Automatic retry on network errors
- Re-login after session timeout
- Graceful handling of missing sections
- Progress saved on interrupt (Ctrl+C)

## Requirements

```
curl_cffi>=0.5.0
beautifulsoup4>=4.12.0
python-dotenv>=1.0.0
pytz>=2024.1
```

## Environment Variables

Create `.env` file:
```
PLS_USER=your_username
PLS_PASS=your_password
```

## License

For educational and research purposes only.

## Author

Built as part of the Pakistan Legal Research Pipeline project.
