"""
Unreported Judgments Scraper — LawGPT Open API
=================================================
Queries the LawGPT search API for cases whose Section field falls in our
'unreported / non-reporter' set (LHC, SHC, IHC, PHC, SCP, C_K, FSC, KLR,
NLR, CLR, PTCL, PCTLR, PLJ, PSC) and saves them to data_v2/UNREPORTED/.

API returns max 10 results per query regardless of pageSize; pagination
does not work. Strategy: fire many diverse queries (year+court combos,
partial titles, keyword phrases) to maximise unique doc_id coverage.

Cap: 300 new saves per run.  Delay: 2-5s between requests.
"""

import sys
import os
import json
import time
import re
import random
import logging
from pathlib import Path
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

from curl_cffi import requests as cffi_requests

# ── Constants ────────────────────────────────────────────────────────────────

SEARCH_URL = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'
DATA_DIR    = Path('data_v2/UNREPORTED')
PROG_FILE   = DATA_DIR / 'progress.json'
MAX_PER_RUN = 300

# Sections that indicate a case IS in a known reporter (exclude these)
KNOWN_REPORTERS = {
    'SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD',
    'GBLR', 'PLCCS', 'PCRLJN', 'YLRN', 'PLCCSN', 'CLCN',
}

# Target sections we actually want
TARGET_SECTIONS = {
    'LHC', 'SHC', 'IHC', 'PHC', 'SCP', 'C_K', 'FSC',
    'KLR', 'NLR', 'CLR', 'PTCL', 'PCTLR', 'PLJ', 'PSC',
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) '
    'Gecko/20100101 Firefox/123.0',
]

# ── Query generation ──────────────────────────────────────────────────────────

def generate_queries():
    """
    Generate a large diverse query list so we hit many different docs.
    API returns up to 10 unique results per query; we cannot paginate.
    """
    queries = []

    # Court abbreviations with common name variants
    court_combos = [
        # (api_section, query_keywords)
        ('LHC', ['LHC', 'Lahore High Court', 'Lahore']),
        ('SHC', ['SHC', 'Sindh High Court', 'Karachi']),
        ('IHC', ['IHC', 'Islamabad High Court']),
        ('PHC', ['PHC', 'Peshawar High Court', 'Peshawar']),
        ('SCP', ['SCP']),
        ('C_K', ['AJK', 'Azad Kashmir', 'AJ&K']),
        ('FSC', ['FSC', 'Federal Shariat Court']),
        ('KLR', ['KLR']),
        ('NLR', ['NLR']),
        ('CLR', ['CLR']),
        ('PTCL', ['PTCL']),
        ('PCTLR', ['PCTLR']),
        ('PLJ', ['PLJ']),
        ('PSC', ['PSC']),
    ]

    years = list(range(2026, 2015, -1)) + list(range(2015, 2005, -1)) + \
            list(range(2005, 1995, -1)) + list(range(1995, 1985, -1)) + \
            list(range(1985, 1970, -1))

    # 1. Year + court keyword combos
    for year in years:
        for _section, kws in court_combos:
            for kw in kws[:2]:  # first two variants
                queries.append(f'{kw} {year}')
                queries.append(f'{year} {kw}')

    # 2. Common legal terms per court per year (catches different result sets)
    legal_terms = [
        'constitutional petition', 'writ petition', 'criminal appeal',
        'civil appeal', 'bail', 'contempt', 'habeas corpus', 'review',
        'revision', 'injunction', 'suit', 'appeal', 'order',
        'judgment', 'acquittal', 'conviction', 'sentence',
        'contract', 'property', 'inheritance', 'custody',
    ]

    for year in years[:12]:  # recent years only for term searches
        for term in legal_terms[:8]:  # top 8 terms
            for _section, kws in court_combos[:6]:  # top courts
                queries.append(f'{kws[0]} {year} {term}')

    # 3. Section-only queries (no year) for broadest sweep
    for _section, kws in court_combos:
        for kw in kws:
            queries.append(kw)

    # 4. Numeric page probe — some judgments have page numbers in citation
    for year in years[:8]:
        for pg in range(1, 200, 25):
            queries.append(f'LHC {year} {pg}')
        for pg in range(1, 100, 20):
            queries.append(f'SHC {year} {pg}')
            queries.append(f'IHC {year} {pg}')
            queries.append(f'PLJ {year} {pg}')

    # Shuffle to get broad coverage quickly
    random.shuffle(queries)

    # De-duplicate while preserving order
    seen = set()
    deduped = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append(q)

    return deduped


# ── File saving ───────────────────────────────────────────────────────────────

def make_slug(title: str, doc_id: str) -> str:
    """URL-safe slug from title + last 8 chars of doc_id."""
    clean = re.sub(r'[^\w\s-]', '', title or 'unknown')
    clean = re.sub(r'\s+', '_', clean.strip())[:50]
    clean = clean.strip('_')
    suffix = re.sub(r'[^\w]', '', doc_id)[-8:]
    return f"{clean}_{suffix}" if clean else f"case_{suffix}"


def build_readable_html(case_data: dict) -> str:
    citation = case_data.get('citation', '')
    title    = case_data.get('title', '')
    court    = case_data.get('court', '')
    date     = case_data.get('date', '')
    result   = case_data.get('result', '')
    judgment = (case_data.get('judgment', '') or '').replace('\n', '<br>\n')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{citation}</title>
  <style>
    body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #222; }}
    h1   {{ font-size: 1.4em; }}
    h2   {{ font-size: 1.1em; color: #444; }}
    .meta {{ background: #f5f5f5; padding: 12px; border-radius: 4px; margin-bottom: 20px; }}
    .judgment {{ line-height: 1.8; }}
  </style>
</head>
<body>
  <h1>{citation}</h1>
  <h2>{title}</h2>
  <div class="meta">
    <strong>Court:</strong> {court}<br>
    <strong>Date:</strong> {date}<br>
    <strong>Result:</strong> {result}
  </div>
  <div class="judgment">{judgment}</div>
</body>
</html>
"""


def build_original_html(item: dict) -> str:
    """Preserve original API response as raw HTML."""
    raw_html = item.get('Case_Description_HTML', '') or item.get('Case_Description', '')
    title = item.get('Title', '')
    citation = item.get('Citation_Name', '')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{citation or title}</title>
</head>
<body>
  <h1>{citation or title}</h1>
  {raw_html}
</body>
</html>
"""


def save_case(item: dict, prog: dict) -> bool:
    """
    Save one case in 4 formats.  Returns True if new, False if already saved.
    """
    doc_id = item.get('doc_id', '')
    if not doc_id or doc_id in prog['saved_ids']:
        return False

    # Extract fields
    title   = item.get('Title', '').strip()
    date    = item.get('Date_Of_Judgement', '') or ''
    year    = item.get('Year', '')
    if not year:
        # derive from date
        m = re.match(r'(\d{4})', date)
        year = m.group(1) if m else 'unknown'

    court       = item.get('Court', '') or ''
    sections    = item.get('Section', []) or []
    reporter    = sections[0] if sections else ''
    result      = item.get('Result', '') or ''
    description = item.get('Case_Description', '') or ''
    citation    = item.get('Citation_Name', '') or f"{year} {reporter} {title[:30]}"

    case_data = {
        'citation'    : citation,
        'reporter'    : reporter,
        'year'        : year,
        'title'       : title,
        'judgment'    : description,
        'judgment_raw': item.get('Case_Description_HTML', description),
        'result'      : result,
        'date'        : date,
        'court'       : court,
        'case_id'     : doc_id,
        'source'      : 'lawgpt_api',
        'scraped_at'  : datetime.now(timezone.utc).isoformat(),
        # extra useful fields
        'sections'    : sections,
        'appeal'      : item.get('Appeal', '') or '',
        'judge'       : item.get('Judge_Name', '') or '',
    }

    # Build output directory
    out_dir = DATA_DIR / reporter / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    slug = make_slug(title, doc_id)

    # 1. JSON
    (out_dir / f"{slug}.json").write_text(
        json.dumps(case_data, ensure_ascii=False, indent=2),
        encoding='utf-8'
    )

    # 2. Readable HTML
    (out_dir / f"{slug}_readable.html").write_text(
        build_readable_html(case_data), encoding='utf-8'
    )

    # 3. Original HTML
    (out_dir / f"{slug}_original.html").write_text(
        build_original_html(item), encoding='utf-8'
    )

    # 4. JSONL (append to COURT_YEAR.jsonl in DATA_DIR root)
    jsonl_name = f"{reporter}_{year}.jsonl"
    jsonl_path = DATA_DIR / jsonl_name
    with open(jsonl_path, 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(case_data, ensure_ascii=False) + '\n')

    prog['saved_ids'].append(doc_id)
    prog['total_saved'] = prog.get('total_saved', 0) + 1

    # Track per-reporter counts
    reporters_map = prog.setdefault('reporters', {})
    reporters_map[reporter] = reporters_map.get(reporter, 0) + 1

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
        'reporters'    : {},
        'runs'         : [],
        'last_run'     : None,
    }


def save_progress(prog: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROG_FILE.write_text(
        json.dumps(prog, ensure_ascii=False, indent=2), encoding='utf-8'
    )


# ── HTTP search ───────────────────────────────────────────────────────────────

def search(session, query: str) -> list[dict]:
    """Fire one API call; return list of raw API items."""
    try:
        resp = session.post(
            SEARCH_URL,
            json={'query': query, 'mode': 'keyword', 'page': 1, 'pageSize': 10},
            headers={
                'Content-Type': 'application/json',
                'User-Agent'  : random.choice(USER_AGENTS),
                'Accept'      : 'application/json',
                'Origin'      : 'https://platform.lawgpt.pk',
                'Referer'     : 'https://platform.lawgpt.pk/',
            },
            timeout=25,
        )
        if resp.status_code != 200:
            log.warning(f"HTTP {resp.status_code} for query: {query!r}")
            return []
        return resp.json().get('value', []) or []
    except Exception as exc:
        log.error(f"Request error for {query!r}: {exc}")
        return []


def is_unreported(item: dict) -> bool:
    """
    True if the case's Section list contains at least one target section
    AND no known reporter section.
    """
    sections = set(item.get('Section', []) or [])
    if not sections:
        return False
    # If it has ANY known reporter, skip it
    if sections & KNOWN_REPORTERS:
        return False
    # Must have at least one target section
    return bool(sections & TARGET_SECTIONS)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prog     = load_progress()
    session  = cffi_requests.Session()
    session.impersonate = 'chrome'

    run_start   = datetime.now(timezone.utc).isoformat()
    saved_this  = 0
    requests_n  = 0
    duplicate_n = 0

    queries        = generate_queries()
    done_set       = set(prog.get('queries_done', []))
    saved_id_set   = set(prog.get('saved_ids', []))

    log.info(
        f"Starting unreported scraper | "
        f"queries: {len(queries)} | "
        f"already saved: {prog['total_saved']} | "
        f"cap: {MAX_PER_RUN}"
    )

    for query in queries:
        if saved_this >= MAX_PER_RUN:
            log.info(f"Hit cap of {MAX_PER_RUN}. Stopping.")
            break

        if query in done_set:
            continue

        # Throttle
        delay = random.uniform(2.0, 5.0)
        time.sleep(delay)

        items = search(session, query)
        requests_n += 1

        new_this_query = 0
        for item in items:
            if not is_unreported(item):
                continue
            doc_id = item.get('doc_id', '')
            if doc_id in saved_id_set:
                duplicate_n += 1
                continue
            if save_case(item, prog):
                saved_id_set.add(doc_id)
                new_this_query += 1
                saved_this += 1
                log.info(
                    f"  SAVED [{prog['total_saved']}] "
                    f"{item.get('Section','?')} | "
                    f"{item.get('Title','?')[:55]}"
                )
                if saved_this >= MAX_PER_RUN:
                    break

        if items:
            log.info(
                f"[{requests_n}] {query!r} → "
                f"{len(items)} results | "
                f"{new_this_query} new saves"
            )

        done_set.add(query)
        prog['queries_done'].append(query)

        # Flush progress every 10 requests
        if requests_n % 10 == 0:
            save_progress(prog)

    # Final flush
    prog['last_run'] = run_start
    prog['runs'].append({
        'start'     : run_start,
        'end'       : datetime.now(timezone.utc).isoformat(),
        'saved'     : saved_this,
        'requests'  : requests_n,
        'duplicates': duplicate_n,
    })
    save_progress(prog)

    log.info(
        f"Run complete | "
        f"saved this run: {saved_this} | "
        f"total ever: {prog['total_saved']} | "
        f"requests: {requests_n} | "
        f"reporters: {prog.get('reporters', {})}"
    )


if __name__ == '__main__':
    main()
