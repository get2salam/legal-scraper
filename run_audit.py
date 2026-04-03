"""Run full quick audit with verbose error trapping."""
import sys
import traceback
from format_auditor import FormatAuditor, DATA_DIR, REPORTERS

print("Starting audit run...")
sys.stdout.flush()

try:
    a = FormatAuditor(fix=True, quick=True)
    reporters = REPORTERS
    
    for reporter in reporters:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            print(f"[SKIP] {reporter} — no directory")
            sys.stdout.flush()
            continue
        
        print(f"[START] {reporter}")
        sys.stdout.flush()
        
        year_dirs = sorted(reporter_dir.iterdir())
        for year_dir in year_dirs:
            if not year_dir.is_dir() or year_dir.name in ('original', 'html'):
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            
            import random
            json_files = list(year_dir.glob('*.json'))
            if not json_files:
                continue
            json_files = random.sample(json_files, min(10, len(json_files)))
            
            for jf in json_files:
                try:
                    a.audit_case(jf)
                except Exception as e:
                    print(f"  ERROR in {jf}: {e}", file=sys.stderr)
                    traceback.print_exc()
        
        print(f"[DONE] {reporter} — total so far: {a.stats['total_cases']}")
        sys.stdout.flush()
    
    print("\n=== FINAL STATS ===")
    s = a.stats
    total = s['total_cases']
    total_checks = total * 4
    total_ok = s['json_ok'] + s['orig_ok'] + s['readable_ok'] + s['jsonl_ok']
    health = (total_ok / total_checks * 100) if total_checks > 0 else 0
    
    print(f"Total cases (quick sample): {total}")
    print(f"JSON: ok={s['json_ok']} bad={s['json_bad']} fixed={s['json_fixed']}")
    print(f"Orig HTML: ok={s['orig_ok']} missing={s['orig_missing']} bad={s['orig_bad_format']} fixed={s['orig_fixed']}")
    print(f"Readable HTML: ok={s['readable_ok']} missing={s['readable_missing']} fixed={s['readable_fixed']}")
    print(f"JSONL: ok={s['jsonl_ok']} missing={s['jsonl_missing']} fixed={s['jsonl_fixed']}")
    print(f"FORMAT HEALTH: {health:.1f}%")
    sys.stdout.flush()

except Exception as e:
    print(f"FATAL ERROR: {e}", file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)
