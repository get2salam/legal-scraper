#!/usr/bin/env python3
"""
Supreme Court of Pakistan — Unreported Judgments Scraper
=========================================================

Source: LawGPT API (prod-search-engine.azurewebsites.net)
        Section filter: 'SCP' (Supreme Court Pakistan)

Note: supremecourt.gov.pk is currently under maintenance (all 404s).
      This scraper uses the LawGPT search API which provides SCP section
      cases directly, including full judgment text.

Output: data_v2/SC_UNREPORTED/{YEAR}/
  - {slug}.json           — standard case JSON
  - {slug}_readable.html  — human-readable HTML
  - {slug}_original.html  — raw API response preserved
  - SC_UNREPORTED_{YEAR}.jsonl  — append-only JSONL per year

Progress: data_v2/SC_UNREPORTED/progress.json
Log:      memory/sc-unreported.log (via -X utf8 redirect)

Cap: 500 new cases per run.  Delay: 2-4s between requests.
"""

import sys
import os
import json
import re
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timezone

# ── Encoding setup ────────────────────────────────────────────────────────────
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / 'data_v2' / 'SC_UNREPORTED'
PROG_FILE    = DATA_DIR / 'progress.json'
LOG_DIR      = PROJECT_ROOT.parent.parent / 'memory'   # workspace/memory/
LOG_FILE     = LOG_DIR / 'sc-unreported.log'
MAX_PER_RUN  = 500

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
    ],
)
log = logging.getLogger('sc_unreported')

# ── API constants ─────────────────────────────────────────────────────────────
SEARCH_URL = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'
API_HEADERS = {
    'Content-Type': 'application/json',
    'Accept'      : 'application/json',
    'Origin'      : 'https://platform.lawgpt.pk',
    'Referer'     : 'https://platform.lawgpt.pk/',
    'User-Agent'  : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

# ── Query generation ──────────────────────────────────────────────────────────

def generate_queries() -> list:
    """
    Generate diverse queries to maximise SCP case coverage.
    API returns up to 10 results per query, no real pagination.
    We fire many varied queries and deduplicate by doc_id.
    """
    queries = []
    years = list(range(2026, 2009, -1)) + list(range(2009, 1995, -1)) + \
            list(range(1995, 1980, -1))

    # Core SCP year queries
    for year in years:
        queries.append(f'SCP {year}')
        queries.append(f'Supreme Court {year}')
        queries.append(f'{year} SCP judgment')

    # Case type prefixes — common SC case types
    case_types = [
        'C.A.', 'C.P.', 'Crl.A.', 'C.P.L.A.', 'C.M.A.',
        'H.R.C.', 'S.M.C.', 'C.R.P.', 'Crl.P.L.A.', 'Crl.R.P.',
        'I.C.A.', 'C.U.O.', 'D.S.A.',
    ]
    for ct in case_types:
        for year in years[:15]:  # recent years
            queries.append(f'{ct} {year}')
            queries.append(f'SCP {ct} {year}')

    # Legal terms targeted at SC
    legal_terms = [
        'constitutional', 'fundamental rights', 'Article 184', 'Article 185',
        'suo motu', 'suo-motu', 'human rights',
        'criminal appeal', 'civil appeal', 'leave to appeal',
        'contempt of court', 'bail', 'habeas corpus',
        'federal government', 'provincial government',
        'dismissal', 'acquittal', 'conviction',
        'land acquisition', 'service matters', 'tax',
        'transfer', 'guardianship', 'inheritance',
        'contract', 'property', 'administration',
    ]
    for term in legal_terms:
        for year in years[:12]:
            queries.append(f'SCP {year} {term}')

    # Judge name queries (prominent SC judges)
    judges = [
        'Yahya Afridi', 'Mansoor Ali Shah', 'Syed Mansoor Ali Shah',
        'Isa', 'Bandial', 'Gulzar Ahmed', 'Asif Saeed Khosa',
        'Umar Ata Bandial', 'Qazi Faez Isa', 'Irfan Saadat Khan',
        'Munib Akhtar', 'Jamal Khan Mandokhel', 'Ayesha Malik',
        'Syed Hasan Azhar Rizvi', 'Muhammad Ali Mazhar',
    ]
    for judge in judges:
        queries.append(f'SCP {judge}')
        for year in years[:8]:
            queries.append(f'SCP {judge} {year}')

    # Shuffle and dedup while preserving order
    random.shuffle(queries)
    seen = set()
    result = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            result.append(q)
    return result


# ── HTML builders ─────────────────────────────────────────────────────────────

def build_readable_html(case_data: dict) -> str:
    citation = case_data.get('citation', '')
    title    = case_data.get('title', '')
    date     = case_data.get('date', '')
    result   = case_data.get('result', '')
    judge    = case_data.get('judge', '')
    appeal   = case_data.get('appeal', '')
    judgment = (case_data.get('judgment', '') or '').replace('\n', '<br>\n')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{citation} — Supreme Court of Pakistan</title>
  <style>
    body   {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 24px; color: #1a1a1a; line-height: 1.7; }}
    h1     {{ font-size: 1.3em; border-bottom: 2px solid #003366; padding-bottom: 8px; }}
    h2     {{ font-size: 1.05em; color: #444; margin-top: 4px; }}
    .meta  {{ background: #f0f4f8; padding: 14px 18px; border-radius: 6px; margin: 18px 0; font-size: 0.92em; }}
    .meta dt {{ font-weight: bold; float: left; clear: left; width: 160px; }}
    .meta dd {{ margin-left: 170px; margin-bottom: 4px; }}
    .judgment {{ text-align: justify; margin-top: 24px; }}
  </style>
</head>
<body>
  <h1>{citation}</h1>
  <h2>{title}</h2>
  <dl class="meta">
    <dt>Court:</dt><dd>Supreme Court of Pakistan</dd>
    <dt>Appeal No:</dt><dd>{appeal}</dd>
    <dt>Judge(s):</dt><dd>{judge}</dd>
    <dt>Date:</dt><dd>{date}</dd>
    <dt>Result:</dt><dd>{result}</dd>
  </dl>
  <div class="judgment">{judgment}</div>
</body>
</html>
"""


def build_original_html(item: dict) -> str:
    """Preserve raw API response as HTML."""
    raw_html  = item.get('Case_Description_HTML', '') or item.get('Case_Description', '')
    title     = item.get('Title', '')
    citation  = item.get('Citation_Name', '')
    appeal    = item.get('Appeal', '')
    judge     = item.get('Judge_Name', '')
    date_raw  = item.get('Date_Of_Judgement', '')
    sections  = ', '.join(item.get('Section', []) or [])
    doc_id    = item.get('doc_id', '')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{citation or title} [Original]</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 20px; }}
    .api-meta {{ background: #fff8e1; border: 1px solid #ffc107; padding: 12px; margin-bottom: 20px; font-size: 0.85em; }}
  </style>
</head>
<body>
  <div class="api-meta">
    <strong>Source:</strong> LawGPT API (SCP section)<br>
    <strong>doc_id:</strong> {doc_id}<br>
    <strong>Citation:</strong> {citation}<br>
    <strong>Appeal:</strong> {appeal}<br>
    <strong>Judge:</strong> {judge}<br>
    <strong>Date:</strong> {date_raw}<br>
    <strong>Sections:</strong> {sections}
  </div>
  <h1>{citation or title}</h1>
  {raw_html}
</body>
</html>
"""


# ── Data normalisation ────────────────────────────────────────────────────────

def make_slug(title: str, doc_id: str) -> str:
    clean = re.sub(r'[^\w\s-]', '', title or 'unknown')
    clean = re.sub(r'\s+', '_', clean.strip())[:55]
    clean = clean.strip('_')
    suffix = re.sub(r'[^\w]', '', doc_id)[-8:]
    return f"{clean}_{suffix}" if clean else f"case_{suffix}"


def parse_year(date_str: str, fallback: str = 'unknown') -> str:
    if not date_str:
        return fallback
    m = re.match(r'(\d{4})', date_str)
    return m.group(1) if m else fallback


def normalise_date(date_str: str) -> str:
    """Convert ISO datetime to YYYY-MM-DD."""
    if not date_str:
        return ''
    m = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
    return m.group(1) if m else date_str


def build_case_json(item: dict) -> dict:
    """Convert raw API item to our standard format."""
    doc_id   = item.get('doc_id', '')
    title    = (item.get('Title', '') or '').strip()
    sections = item.get('Section', []) or []
    date_raw = item.get('Date_Of_Judgement', '')
    date     = normalise_date(date_raw)
    year_str = parse_year(date_raw, 'unknown')
    def _str(val):
        if isinstance(val, list):
            return ', '.join(str(v) for v in val)
        return (val or '').strip() if isinstance(val, str) else str(val or '')

    citation  = _str(item.get('Citation_Name', ''))
    appeal    = _str(item.get('Appeal', ''))
    judge     = _str(item.get('Judge_Name', ''))
    result    = _str(item.get('Result', ''))
    desc      = item.get('Case_Description', '') or ''
    desc_html = item.get('Case_Description_HTML', '') or ''
    if isinstance(desc, list):
        desc = ' '.join(str(x) for x in desc)
    if isinstance(desc_html, list):
        desc_html = ' '.join(str(x) for x in desc_html)

    # Build a usable citation if API didn't provide one
    if not citation:
        citation = f"SC_UNREPORTED {year_str} — {appeal[:60]}" if appeal else \
                   f"SC_UNREPORTED {year_str} — {title[:60]}"

    return {
        'citation'     : citation,
        'reporter'     : 'SC_UNREPORTED',
        'year'         : year_str,
        'title'        : title,
        'judgment'     : desc,
        'judgment_raw' : desc_html if desc_html else desc,
        'result'       : result,
        'date'         : date,
        'court'        : 'Supreme Court',
        'case_id'      : doc_id,
        'source'       : 'supremecourt.gov.pk',
        'scraped_at'   : datetime.now(timezone.utc).isoformat(),
        # extra fields (useful for search / display)
        'appeal'       : appeal,
        'judge'        : judge,
        'sections'     : sections,
        'summary'      : (item.get('Short_Summary', '') or '').strip(),
        'statutes'     : item.get('Statute_Collection', []) or [],
    }


# ── File saving ───────────────────────────────────────────────────────────────

def save_case(item: dict, saved_ids: set) -> bool:
    """Save one case in 4 formats.  Returns True if new, False if duplicate."""
    doc_id = item.get('doc_id', '')
    if not doc_id or doc_id in saved_ids:
        return False

    case_data = build_case_json(item)
    year      = case_data['year']
    slug      = make_slug(case_data['title'], doc_id)

    out_dir = DATA_DIR / year
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. JSON
    (out_dir / f'{slug}.json').write_text(
        json.dumps(case_data, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    # 2. Readable HTML
    (out_dir / f'{slug}_readable.html').write_text(
        build_readable_html(case_data), encoding='utf-8',
    )

    # 3. Original HTML (raw API)
    (out_dir / f'{slug}_original.html').write_text(
        build_original_html(item), encoding='utf-8',
    )

    # 4. JSONL — per-year append
    jsonl_path = DATA_DIR / f'SC_UNREPORTED_{year}.jsonl'
    with open(jsonl_path, 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(case_data, ensure_ascii=False) + '\n')

    saved_ids.add(doc_id)
    return True


# ── Progress ──────────────────────────────────────────────────────────────────

def load_progress() -> dict:
    if PROG_FILE.exists():
        try:
            return json.loads(PROG_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    return {
        'saved_ids'    : [],
        'total_saved'  : 0,
        'queries_done' : [],
        'runs'         : [],
        'last_run'     : None,
    }


def save_progress(prog: dict, saved_ids: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prog['saved_ids'] = list(saved_ids)
    prog['total_saved'] = len(saved_ids)
    PROG_FILE.write_text(
        json.dumps(prog, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )


# ── HTTP search ───────────────────────────────────────────────────────────────

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    log.error('curl_cffi not installed. Run: pip install curl_cffi')
    sys.exit(1)


def search(session, query: str) -> list:
    """Fire one API query, return raw item list (all sections)."""
    try:
        resp = session.post(
            SEARCH_URL,
            json={
                'query'   : query,
                'mode'    : 'keyword',
                'page'    : 1,
                'pageSize': 10,
            },
            headers=API_HEADERS,
            timeout=25,
        )
        if resp.status_code != 200:
            log.warning(f'HTTP {resp.status_code} for {query!r}')
            return []
        return resp.json().get('value', []) or []
    except Exception as exc:
        log.error(f'Request error for {query!r}: {exc}')
        return []


def is_scp(item: dict) -> bool:
    """True if this case is from the Supreme Court (SCP section)."""
    sections = set(item.get('Section', []) or [])
    return 'SCP' in sections


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    prog      = load_progress()
    saved_ids = set(prog.get('saved_ids', []))

    session = cffi_requests.Session()
    session.impersonate = 'chrome'

    run_start    = datetime.now(timezone.utc).isoformat()
    saved_this   = 0
    requests_n   = 0
    duplicate_n  = 0
    skipped_n    = 0

    queries  = generate_queries()
    done_set = set(prog.get('queries_done', []))
    queries_done_this_run = []

    log.info('=' * 65)
    log.info('Supreme Court Unreported Judgments Scraper')
    log.info(f'  Source      : LawGPT API (SCP section)')
    log.info(f'  Target dir  : {DATA_DIR}')
    log.info(f'  Already have: {len(saved_ids)} unique cases')
    log.info(f'  Run cap     : {MAX_PER_RUN}')
    log.info(f'  Total queries available: {len(queries)}')
    log.info('=' * 65)

    for query in queries:
        if saved_this >= MAX_PER_RUN:
            log.info(f'Hit cap of {MAX_PER_RUN}. Stopping.')
            break

        if query in done_set:
            skipped_n += 1
            continue

        delay = random.uniform(2.0, 4.0)
        time.sleep(delay)

        items = search(session, query)
        requests_n += 1

        new_this_query = 0
        for item in items:
            if not is_scp(item):
                continue
            doc_id = item.get('doc_id', '')
            if doc_id in saved_ids:
                duplicate_n += 1
                continue
            if save_case(item, saved_ids):
                new_this_query += 1
                saved_this += 1
                case_d = build_case_json(item)
                log.info(
                    f'  SAVED [{prog["total_saved"] + saved_this}] '
                    f'{case_d["year"]} | {case_d["citation"][:70]}'
                )
                if saved_this >= MAX_PER_RUN:
                    break

        if items:
            scp_count = sum(1 for i in items if is_scp(i))
            log.info(
                f'[req {requests_n}] {query!r} '
                f'=> {len(items)} results, {scp_count} SCP, {new_this_query} new'
            )
        else:
            log.debug(f'[req {requests_n}] {query!r} => 0 results')

        done_set.add(query)
        queries_done_this_run.append(query)

        if requests_n % 10 == 0:
            prog['queries_done'] = list(done_set)
            save_progress(prog, saved_ids)
            log.info(f'Progress saved. Total unique cases: {len(saved_ids)}')

    # ── Final save ────────────────────────────────────────────────────────────
    prog['queries_done'] = list(done_set)
    prog['last_run'] = run_start
    prog['runs'].append({
        'start'      : run_start,
        'end'        : datetime.now(timezone.utc).isoformat(),
        'saved'      : saved_this,
        'requests'   : requests_n,
        'duplicates' : duplicate_n,
        'skipped'    : skipped_n,
    })
    save_progress(prog, saved_ids)

    log.info('=' * 65)
    log.info('Run complete')
    log.info(f'  Saved this run : {saved_this}')
    log.info(f'  Total unique   : {len(saved_ids)}')
    log.info(f'  Requests fired : {requests_n}')
    log.info(f'  Duplicates     : {duplicate_n}')
    log.info(f'  Queries skipped: {skipped_n}')
    log.info('=' * 65)

    # Print year breakdown
    if DATA_DIR.exists():
        year_dirs = sorted([d.name for d in DATA_DIR.iterdir()
                            if d.is_dir() and d.name.isdigit()])
        if year_dirs:
            log.info('Year breakdown:')
            for yr in year_dirs:
                count = len(list((DATA_DIR / yr).glob('*.json')))
                log.info(f'  {yr}: {count} cases')


if __name__ == '__main__':
    main()
