# Pakistan Legislation Scraper — Claude Code Project Instructions

## ⚠️ INTERNAL ONLY — NEVER PUSH TO GITHUB ⚠️
This repo contains site-specific scraper code for pakistanlawsite.com (PLS).
It must NEVER be pushed to any public repository.

## What This Is
Data collection system for Pakistani legal data: case law, legislation, court judgments, and federal laws. Scrapes from PLS (pakistanlawsite.com) and government sources.

## Tech Stack
- **Python 3.11+** — all scrapers are standalone scripts
- **curl_cffi** — HTTP with Chrome TLS fingerprinting (anti-bot)
- **BeautifulSoup4** — HTML parsing
- No frameworks — pure scripts with stdlib + minimal deps
- Credentials in `.env` (PLS_USER, PLS_PASS)

## Scraper Architecture
```
pls_scraper_v2.py       — Main case law scraper (2015+)
historical_scraper.py   — Pre-2015 case law (fresh login per year)
legislation_scraper.py  — Statutes A-Z from PLS
verify_scraper.py       — Compare PLS index vs local, find gaps
fill_format_gaps.py     — Generate missing formats from existing JSON
rebuild_jsonl_fast.py   — Rebuild JSONL from JSON files
fix_orig_html_v2.py     — Fix JSON-escaped HTML in original files
gen_legislation_formats.py — Generate readable HTML + JSONL for legislation
leg_watchdog.py         — Watchdog: monitors scraper health
_status.py              — Quick status count script
```

## 4 Data Formats (MANDATORY for every case)
1. **JSON** — `data_v2/{REPORTER}/{YEAR}/{CITATION}.json` (structured)
2. **Original HTML** — `data_v2/{REPORTER}/{YEAR}/original/{CITATION}.html` (raw PLS)
3. **Readable HTML** — `data_v2/html/{REPORTER}/{YEAR}/{CITATION}.html` (styled)
4. **JSONL** — `data_v2/{REPORTER}_{YEAR}.jsonl` + `data_v2/all_cases.jsonl`

## Required JSON Fields
Every case law JSON MUST have: `citation`, `reporter`, `year`, `judgment`, `judgment_raw`

## Data Directory
```
data_v2/
├── {REPORTER}/{YEAR}/*.json + original/*.html   # Case law
├── html/{REPORTER}/{YEAR}/*.html                 # Readable HTML
├── legislation/{LETTER}/*.json                   # Statutes
├── legislation/progress.json                     # Completion tracker
├── court_cases/{COURT}/**/*.json                 # Direct court scrapes
├── federal_laws/{acts,ordinances}/*.json         # Government laws
├── analytics/                                    # Citation graph, signals
├── daily_snapshot.json                           # Case law daily totals
├── legislation_snapshot.json                     # Legislation daily totals
└── {REPORTER}_{YEAR}.jsonl                       # Per-reporter JSONL
```

## PLS Operating Constraints — CRITICAL
- **ONE active session per login** — scraper + Chrome browser = session conflict
- **Login needs CSRF token** — must extract from homepage first
- **Session expires SILENTLY** — `_request()` returns None, scraper marks year "complete" with 0 results
- **Always verify "completed" flags** — session death creates false positives in progress.json
- **Fresh login per year** prevents expiry (historical_scraper.py design)
- **Login is flaky** — PLS sometimes needs multiple attempts
- **Human-like timing** — variable delays, reading pauses, random breaks
- **Operating hours**: PLS is most stable 7 AM–9 PM PKT

## Key Constants
- **Reporters**: SCMR, PLD, PCrLJ, MLD, CLC, YLR, PTD, PLC, CLD, GBLR
- **Case law total**: 162,747 JSON files (PLS ceiling — 3,781 ghost entries unfetchable)
- **Legislation target**: 10,915 statutes
- **Years covered**: 1947–2026

## Common Gotchas
- PLS API returns HTML wrapped as JSON string (`"\u003chtml...\u003e"`) — must `json.loads()` to decode
- `str.isupper()` returns False for "PCrLJ" — always use explicit reporter list
- Session death is SILENT — always add `check_session()` before accepting empty results
- Letters with 0 results might be session death, not actually empty (S has ~1,207!)
- `CreationTime` (when scraped) vs `LastWriteTime` (when modified) — use CreationTime for date analysis

## Before Committing
This repo does NOT get pushed to GitHub, but keep code clean:
1. No hardcoded credentials (use .env)
2. All scrapers must save all 4 formats
3. Add `check_session()` to any new scraper
4. Test with a small batch before full runs
