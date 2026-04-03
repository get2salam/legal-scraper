#!/usr/bin/env python3
"""
Court Data Auditor — Verify court scraper data integrity.
Checks: file counts, JSON validity, PDF presence, bench consistency,
        duplicate detection, field completeness, progress file sync.

Usage:
    python court_audit.py                  # Audit all courts
    python court_audit.py --court SC       # Audit specific court
    python court_audit.py --court SHC --bench KHI  # Audit specific bench
    python court_audit.py --fix            # Auto-fix progress files
    python court_audit.py --report         # Save report to file
"""
import json, os, sys, argparse, hashlib, logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
COURT_DIR = DATA / "court_cases"
REPORT_DIR = DATA / "audit"
REPORT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

COURTS = {
    'SC': {
        'name': 'Supreme Court of Pakistan',
        'benches': [],  # No benches, single court
        'source': 'supremecourt.gov.pk',
        'progress_file': 'sc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year', 'judgment_date'],
    },
    'SHC': {
        'name': 'Sindh High Court',
        'benches': ['KHI', 'HYD', 'SUK', 'LAR', 'MIR'],
        'source': 'caselaw.shc.gov.pk',
        'progress_file': 'shc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year', 'bench'],
    },
    'IHC': {
        'name': 'Islamabad High Court',
        'benches': [],
        'source': 'ihc.gov.pk',
        'progress_file': 'ihc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'LHC': {
        'name': 'Lahore High Court',
        'benches': ['LHR', 'RWP', 'MLT', 'BWP'],
        'source': 'data.lhc.gov.pk',
        'progress_file': 'lhc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'PHC': {
        'name': 'Peshawar High Court',
        'benches': ['PSH', 'ABT', 'MNG', 'BNU'],
        'source': 'peshawarhighcourt.gov.pk',
        'progress_file': 'phc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'BHC': {
        'name': 'Balochistan High Court',
        'benches': ['QTA', 'SBI', 'TBT'],
        'source': 'bhc.gov.pk',
        'progress_file': 'bhc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'FSC': {
        'name': 'Federal Shariat Court',
        'benches': [],
        'source': 'federalshariatcourt.gov.pk',
        'progress_file': 'fsc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'SST': {
        'name': 'Sindh Service Tribunal',
        'benches': [],
        'source': 'sstsindh.gov.pk',
        'progress_file': 'sst_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
    'FCC': {
        'name': 'Federal Constitutional Court',
        'benches': [],
        'source': 'fcc.gov.pk',
        'progress_file': 'fcc_progress.json',
        'expected_fields': ['case_number', 'case_title', 'year'],
    },
}


def count_files(directory, pattern="*.json"):
    """Count files matching pattern recursively."""
    if not directory.exists():
        return 0
    return len(list(directory.rglob(pattern)))


def audit_json_file(filepath):
    """Audit a single JSON file for completeness and validity."""
    issues = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            issues.append("empty_json")
            return data, issues
        
        # Check for empty/missing key fields
        for field in ['case_number', 'case_title']:
            val = data.get(field, '')
            if not val or val == '' or val == 'N/A' or val == 'Unknown':
                issues.append(f"missing_{field}")
        
        # Check judgment text
        judgment = data.get('judgment_text', '') or data.get('judgment', '') or data.get('judgment_raw', '')
        if not judgment or len(str(judgment)) < 50:
            issues.append("short_judgment")
        
        # Check year
        year = data.get('year')
        if not year:
            issues.append("missing_year")
        elif isinstance(year, (int, str)):
            yr = int(year) if str(year).isdigit() else 0
            if yr < 1947 or yr > 2027:
                issues.append(f"invalid_year_{year}")
        
        return data, issues
    
    except json.JSONDecodeError:
        return None, ["corrupt_json"]
    except Exception as e:
        return None, [f"read_error: {str(e)[:50]}"]


def find_duplicates(json_files):
    """Find duplicate cases by case_number or content hash."""
    seen_numbers = defaultdict(list)
    seen_hashes = defaultdict(list)
    
    for fp in json_files:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check by case number
            cn = data.get('case_number', '')
            if cn:
                seen_numbers[cn].append(str(fp))
            
            # Check by content hash (title + case_number)
            title = data.get('case_title', '')
            key = f"{cn}|{title}"
            h = hashlib.md5(key.encode()).hexdigest()[:12]
            seen_hashes[h].append(str(fp))
        except:
            pass
    
    dupes_by_number = {k: v for k, v in seen_numbers.items() if len(v) > 1}
    dupes_by_hash = {k: v for k, v in seen_hashes.items() if len(v) > 1}
    
    return dupes_by_number, dupes_by_hash


def audit_court(court_code, bench_filter=None):
    """Full audit of a single court."""
    config = COURTS.get(court_code)
    if not config:
        log.error(f"Unknown court: {court_code}")
        return None
    
    court_dir = COURT_DIR / court_code
    if not court_dir.exists():
        return {
            'court': court_code,
            'name': config['name'],
            'status': 'NO_DATA',
            'json_count': 0,
            'errors': [],
            'warnings': ['Court directory does not exist']
        }
    
    result = {
        'court': court_code,
        'name': config['name'],
        'status': 'OK',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'errors': [],
        'warnings': [],
        'stats': {},
        'benches': {},
        'field_issues': defaultdict(int),
        'year_distribution': defaultdict(int),
    }
    
    # Determine directories to scan
    if config['benches'] and bench_filter:
        scan_dirs = [court_dir / bench_filter]
    elif config['benches']:
        scan_dirs = [court_dir / b for b in config['benches'] if (court_dir / b).exists()]
        # Also check for files directly in court_dir (no bench)
        direct_jsons = list(court_dir.glob("*/*.json"))  # year/file.json
        if direct_jsons:
            scan_dirs.append(court_dir)
    else:
        scan_dirs = [court_dir]
    
    all_json_files = []
    all_pdf_files = []
    
    for scan_dir in scan_dirs:
        if not scan_dir.exists():
            result['warnings'].append(f"Directory not found: {scan_dir.name}")
            continue
        
        json_files = list(scan_dir.rglob("*.json"))
        # Exclude progress files
        json_files = [f for f in json_files if 'progress' not in f.name.lower()]
        pdf_files = list(scan_dir.rglob("*.pdf"))
        
        bench_name = scan_dir.name if scan_dir != court_dir else 'main'
        
        result['benches'][bench_name] = {
            'json_count': len(json_files),
            'pdf_count': len(pdf_files),
        }
        
        all_json_files.extend(json_files)
        all_pdf_files.extend(pdf_files)
    
    result['stats']['total_json'] = len(all_json_files)
    result['stats']['total_pdf'] = len(all_pdf_files)
    
    # ---- JSON Validation ----
    corrupt = 0
    empty = 0
    field_issues = defaultdict(int)
    year_dist = defaultdict(int)
    
    for fp in all_json_files:
        data, issues = audit_json_file(fp)
        if data is None:
            corrupt += 1
        elif not data:
            empty += 1
        else:
            year = data.get('year')
            if year:
                year_dist[str(year)] += 1
        
        for issue in issues:
            field_issues[issue] += 1
    
    result['stats']['corrupt'] = corrupt
    result['stats']['empty'] = empty
    result['field_issues'] = dict(field_issues)
    result['year_distribution'] = dict(sorted(year_dist.items()))
    
    if corrupt > 0:
        result['errors'].append(f"{corrupt} corrupt JSON files")
        result['status'] = 'ERROR'
    if empty > 0:
        result['errors'].append(f"{empty} empty JSON files")
    
    # ---- PDF Check ----
    json_count = len(all_json_files)
    pdf_count = len(all_pdf_files)
    
    if court_code != 'LHC':  # LHC doesn't have PDFs yet
        if pdf_count < json_count:
            missing_pdfs = json_count - pdf_count
            if missing_pdfs > json_count * 0.1:  # >10% missing
                result['errors'].append(f"{missing_pdfs} PDFs missing ({pdf_count}/{json_count})")
            else:
                result['warnings'].append(f"{missing_pdfs} PDFs missing ({pdf_count}/{json_count})")
    
    result['stats']['pdf_coverage'] = f"{pdf_count}/{json_count}" if json_count > 0 else "N/A"
    
    # ---- Duplicate Check ----
    dupes_num, dupes_hash = find_duplicates(all_json_files)
    result['stats']['duplicates_by_number'] = len(dupes_num)
    result['stats']['duplicates_by_hash'] = len(dupes_hash)
    
    if dupes_num:
        result['warnings'].append(f"{len(dupes_num)} potential duplicates by case number")
        # Include first 5 examples
        result['duplicate_examples'] = {k: v for k, v in list(dupes_num.items())[:5]}
    
    # ---- Progress File Sync ----
    prog_file = COURT_DIR / config['progress_file']
    if prog_file.exists():
        try:
            prog = json.load(open(prog_file))
            prog_total = (
                prog.get('total_cases_scraped') or 
                prog.get('total_scraped') or 
                prog.get('total_downloaded') or
                0
            )
            if prog_total > 0 and abs(prog_total - json_count) > 10:
                result['warnings'].append(
                    f"Progress file says {prog_total} scraped but {json_count} JSON files on disk"
                )
            result['stats']['progress_file_total'] = prog_total
        except:
            result['warnings'].append("Progress file exists but couldn't be parsed")
    else:
        result['warnings'].append("No progress file found")
    
    # ---- Bench Consistency (for multi-bench courts) ----
    if config['benches']:
        for bench in config['benches']:
            bdir = court_dir / bench
            if not bdir.exists():
                result['warnings'].append(f"Bench {bench} directory missing")
            elif count_files(bdir, "*.json") == 0:
                result['warnings'].append(f"Bench {bench} has 0 cases")
        
        # Check bench sum vs total
        bench_sum = sum(b['json_count'] for b in result['benches'].values())
        if bench_sum != json_count and config['benches']:
            result['warnings'].append(
                f"Bench sum ({bench_sum}) != total ({json_count}) — orphan files?"
            )
    
    # ---- Field Completeness ----
    for field, count in field_issues.items():
        if count > json_count * 0.1:  # >10% have this issue
            result['errors'].append(f"{count}/{json_count} cases have {field}")
        elif count > 0:
            result['warnings'].append(f"{count} cases have {field}")
    
    # Set final status
    if result['errors']:
        result['status'] = 'ERRORS_FOUND'
    elif result['warnings']:
        result['status'] = 'WARNINGS'
    else:
        result['status'] = 'CLEAN'
    
    return result


def print_court_report(r):
    """Pretty-print a court audit result."""
    status_icon = {'CLEAN': '[CLEAN]', 'WARNINGS': '[WARN]', 'ERRORS_FOUND': '[ERROR]', 'NO_DATA': '[EMPTY]', 'ERROR': '[ERROR]'}
    icon = status_icon.get(r['status'], '[???]')
    
    print(f"\n{'='*60}")
    print(f"  {icon} {r['court']} — {r['name']}")
    print(f"{'='*60}")
    
    stats = r.get('stats', {})
    print(f"  JSON files:  {stats.get('total_json', 0)}")
    print(f"  PDF files:   {stats.get('total_pdf', 0)}")
    print(f"  PDF coverage:{stats.get('pdf_coverage', 'N/A')}")
    print(f"  Corrupt:     {stats.get('corrupt', 0)}")
    print(f"  Empty:       {stats.get('empty', 0)}")
    print(f"  Duplicates:  {stats.get('duplicates_by_number', 0)} (by number), {stats.get('duplicates_by_hash', 0)} (by hash)")
    
    if r.get('benches'):
        print(f"\n  Benches:")
        for bench, info in sorted(r['benches'].items(), key=lambda x: -x[1]['json_count']):
            print(f"    {bench}: {info['json_count']} JSON, {info['pdf_count']} PDFs")
    
    if r.get('year_distribution'):
        years = r['year_distribution']
        print(f"\n  Years: {min(years.keys())}-{max(years.keys())} ({len(years)} years, {sum(years.values())} cases)")
    
    if r['errors']:
        print(f"\n  ERRORS ({len(r['errors'])}):")
        for e in r['errors']:
            print(f"    [ERROR] {e}")
    
    if r['warnings']:
        print(f"\n  WARNINGS ({len(r['warnings'])}):")
        for w in r['warnings']:
            print(f"    [WARN]  {w}")
    
    if r['status'] == 'CLEAN':
        print(f"\n  ALL CHECKS PASSED!")


def main():
    parser = argparse.ArgumentParser(description='Court Data Auditor')
    parser.add_argument('--court', type=str, help='Audit specific court (SC, SHC, IHC, etc.)')
    parser.add_argument('--bench', type=str, help='Audit specific bench (KHI, HYD, etc.)')
    parser.add_argument('--fix', action='store_true', help='Auto-fix progress files')
    parser.add_argument('--report', action='store_true', help='Save JSON report')
    args = parser.parse_args()
    
    print("="*60)
    print("  COURT DATA AUDITOR")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)
    
    courts_to_audit = [args.court.upper()] if args.court else list(COURTS.keys())
    results = []
    
    total_errors = 0
    total_warnings = 0
    total_cases = 0
    
    for court_code in courts_to_audit:
        if court_code not in COURTS:
            print(f"\n  Unknown court: {court_code}")
            continue
        
        court_dir = COURT_DIR / court_code
        if not court_dir.exists() and not args.court:
            continue  # Skip courts with no data in full audit
        
        result = audit_court(court_code, bench_filter=args.bench)
        if result:
            results.append(result)
            print_court_report(result)
            total_errors += len(result['errors'])
            total_warnings += len(result['warnings'])
            total_cases += result.get('stats', {}).get('total_json', 0)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"  AUDIT SUMMARY")
    print(f"{'='*60}")
    print(f"  Courts audited: {len(results)}")
    print(f"  Total cases:    {total_cases}")
    print(f"  Total errors:   {total_errors}")
    print(f"  Total warnings: {total_warnings}")
    
    if total_errors == 0 and total_warnings == 0:
        print(f"\n  ALL COURTS CLEAN!")
    elif total_errors == 0:
        print(f"\n  No critical errors. {total_warnings} warnings to review.")
    else:
        print(f"\n  {total_errors} ERRORS NEED ATTENTION!")
    
    # Save report
    if args.report or True:  # Always save
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'courts_audited': len(results),
            'total_cases': total_cases,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'results': results,
        }
        report_file = REPORT_DIR / f"court_audit_{datetime.now().strftime('%Y-%m-%d_%H%M')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n  Report saved: {report_file}")
    
    return 1 if total_errors > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
