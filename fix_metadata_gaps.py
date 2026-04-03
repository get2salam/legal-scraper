#!/usr/bin/env python3
"""Fix CLD/GBLR metadata gaps — derive year/reporter/page from filename, add case_title from case_name."""
import json, os, re, sys

DATA_DIR = os.path.join(os.path.dirname(__file__), "data_v2")
REPORTERS = ['SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR']

def fix_reporter(reporter: str) -> dict:
    rp = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(rp):
        return {"reporter": reporter, "total": 0, "fixed": 0, "errors": 0}
    
    total = 0
    fixed = 0
    errors = 0
    
    for year_dir in sorted(os.listdir(rp)):
        yp = os.path.join(rp, year_dir)
        if not os.path.isdir(yp) or year_dir == "original":
            continue
        
        for fn in os.listdir(yp):
            if not fn.endswith(".json"):
                continue
            total += 1
            fp = os.path.join(yp, fn)
            
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                changed = False
                
                # Derive from filename: e.g. 2016_CLD_123.json
                stem = fn.replace(".json", "")
                parts = stem.split("_")
                
                # Fix missing year
                if not data.get("year"):
                    try:
                        data["year"] = int(parts[0]) if parts else int(year_dir)
                    except (ValueError, IndexError):
                        data["year"] = int(year_dir)
                    changed = True
                
                # Fix missing reporter
                if not data.get("reporter"):
                    data["reporter"] = reporter
                    changed = True
                
                # Fix missing page
                if not data.get("page"):
                    # Page is the last numeric part of citation
                    if len(parts) >= 3:
                        data["page"] = parts[-1]
                    elif data.get("citation"):
                        cit_parts = data["citation"].split()
                        if len(cit_parts) >= 3:
                            data["page"] = cit_parts[-1]
                    changed = True
                
                # Add case_title from case_name if missing
                if not data.get("case_title") and data.get("case_name"):
                    data["case_title"] = data["case_name"]
                    changed = True
                
                # Ensure citation exists
                if not data.get("citation"):
                    data["citation"] = f"{year_dir} {reporter} {data.get('page', '')}"
                    changed = True
                
                if changed:
                    with open(fp, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    fixed += 1
                    
            except Exception as e:
                errors += 1
                print(f"  ERROR: {fp}: {e}")
    
    return {"reporter": reporter, "total": total, "fixed": fixed, "errors": errors}

def main():
    print("=" * 60)
    print("METADATA GAP FIXER — CLD/GBLR + Full Scan")
    print("=" * 60)
    
    # Fix known gap reporters first
    results = []
    for rep in REPORTERS:
        print(f"\n📂 Fixing {rep}...")
        r = fix_reporter(rep)
        results.append(r)
        print(f"  Total: {r['total']} | Fixed: {r['fixed']} | Errors: {r['errors']}")
    
    # Verify all reporters are now clean
    print("\n\n📊 Verification scan...")
    for rep in REPORTERS:
        rp = os.path.join(DATA_DIR, rep)
        if not os.path.isdir(rp):
            continue
        missing_count = 0
        total = 0
        for year_dir in os.listdir(rp):
            yp = os.path.join(rp, year_dir)
            if not os.path.isdir(yp) or year_dir == "original":
                continue
            for fn in os.listdir(yp):
                if not fn.endswith(".json"):
                    continue
                total += 1
                fp = os.path.join(yp, fn)
                try:
                    with open(fp, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if not data.get("year") or not data.get("reporter") or not data.get("page"):
                        missing_count += 1
                except:
                    pass
        status = "✅" if missing_count == 0 else f"⚠️ {missing_count} still missing"
        print(f"  {rep}: {total} — {status}")
    
    print("\n" + "=" * 60)
    total_fixed = sum(r["fixed"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    print(f"TOTAL FIXED: {total_fixed} | ERRORS: {total_errors}")
    print("=" * 60)

if __name__ == "__main__":
    main()
