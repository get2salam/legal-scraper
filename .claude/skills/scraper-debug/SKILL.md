---
name: scraper-debug
description: Debug scraper issues — session death, missing data, format gaps, PLS errors. Use when scraper stops, produces empty results, or data integrity checks fail.
---

# Scraper Debug Skill

## Common Failure Modes

### 1. Silent Session Death (MOST COMMON)
**Symptoms**: Scraper marks year/letter "completed" with 0 results
**Cause**: PLS session expired, `_request()` returns None silently
**Fix**: 
- Check `progress.json` — look for completed entries with 0 files
- Re-run with fresh login
- Add `check_session()` call before accepting empty results

### 2. Login Failure
**Symptoms**: Script exits early or gets 403/redirect responses
**Cause**: PLS is flaky, CSRF token mismatch, concurrent session
**Fix**:
- Ensure no other PLS session active (browser, other scraper)
- Extract fresh CSRF token from homepage
- Retry login with delay

### 3. Format Gaps
**Symptoms**: JSON exists but missing original HTML / readable HTML / JSONL
**Cause**: Scraper saved only some formats (older bug, now fixed)
**Fix**: Run `fill_format_gaps.py` or `gen_legislation_formats.py`

### 4. JSON-Escaped HTML
**Symptoms**: Original HTML files contain `\u003c` instead of `<`
**Cause**: Saved `resp.text` instead of `json.loads(resp.text)`
**Fix**: `fix_orig_html_v2.py`

## Verification Commands
```powershell
# Check if scraper is alive
Get-Process python* | Where-Object { $_.Id -eq <PID> }

# Count files per letter
python -c "import os; [print(f'{d}: {len([f for f in os.listdir(os.path.join(\"data_v2/legislation\",d)) if f.endswith(\".json\")])}') for d in sorted(os.listdir('data_v2/legislation')) if os.path.isdir(os.path.join('data_v2/legislation',d)) and len(d)==1]"

# Check progress.json
python -c "import json; print(json.dumps(json.load(open('data_v2/legislation/progress.json')), indent=2))"
```

## PLS Constraints
- ONE session at a time per account
- Login requires CSRF token from homepage
- Session expires silently — always verify
- Human-like delays mandatory (variable 2-10s between requests)
- Ghost cases exist (~3,781) — old internal IDs that API can't serve
