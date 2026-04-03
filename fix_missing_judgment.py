#!/usr/bin/env python3
"""
fix_missing_judgment.py — Fix JSON files missing the 'judgment' field.

For files that have 'judgment_raw' (decoded HTML) but no 'judgment',
copies judgment_raw → judgment. Also regenerates readable HTML for
files flagged as unstyled.

Usage:
    python fix_missing_judgment.py                  # Dry run (report only)
    python fix_missing_judgment.py --fix            # Apply fixes
    python fix_missing_judgment.py --fix --regen    # Also regenerate unstyled readable HTML
"""

import json
import os
import sys
import glob
import time
import argparse
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / 'data_v2'
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

READABLE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{citation}</title>
<style>
    body {{ font-family: Georgia, 'Times New Roman', serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; color: #333; background: #fafafa; }}
    .header {{ border-bottom: 2px solid #2c3e50; padding-bottom: 15px; margin-bottom: 20px; }}
    .citation {{ font-size: 1.4em; font-weight: bold; color: #2c3e50; }}
    .meta {{ color: #666; margin: 5px 0; }}
    .judgment {{ margin-top: 20px; }}
    .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #999; font-size: 0.85em; }}
</style>
</head>
<body>
<div class="header">
    <div class="citation">{citation}</div>
    <div class="meta"><strong>Court:</strong> {court}</div>
    <div class="meta"><strong>Judges:</strong> {judges}</div>
    <div class="meta"><strong>Date:</strong> {date}</div>
</div>
<div class="judgment">
{judgment_html}
</div>
<div class="footer">
    Generated from legal research data • {timestamp}
</div>
</body>
</html>"""


def fix_missing_judgment(do_fix=False, regen_readable=False):
    """Scan all JSON files and fix missing 'judgment' field."""
    start = time.time()
    
    fixed = 0
    already_ok = 0
    no_source = 0
    readable_fixed = 0
    errors = 0
    total = 0
    
    by_reporter = {}
    
    for rep in REPORTERS:
        rep_dir = DATA_DIR / rep
        if not rep_dir.is_dir():
            continue
        
        rep_fixed = 0
        
        for year_dir in sorted(os.listdir(rep_dir)):
            ypath = rep_dir / year_dir
            if not ypath.is_dir() or year_dir == 'original':
                continue
            
            for jf in glob.glob(str(ypath / '*.json')):
                total += 1
                try:
                    with open(jf, encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if 'judgment' in data and data['judgment'] and len(data['judgment'].strip()) > 0:
                        already_ok += 1
                        continue
                    
                    # Try to get judgment from judgment_raw
                    source = None
                    source_key = None
                    for k in ['judgment_raw', 'judgment_html', 'judgment_clean']:
                        if k in data and data[k] and len(str(data[k]).strip()) > 10:
                            source = data[k]
                            source_key = k
                            break
                    
                    if not source:
                        no_source += 1
                        continue
                    
                    if do_fix:
                        data['judgment'] = source
                        with open(jf, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        
                        # Also regenerate readable HTML if requested
                        if regen_readable:
                            citation = data.get('citation', '')
                            readable_path = DATA_DIR / 'html' / rep / year_dir / f"{Path(jf).stem}.html"
                            
                            # Check if readable HTML needs regen (missing or unstyled)
                            needs_regen = False
                            if not readable_path.exists():
                                needs_regen = True
                            else:
                                try:
                                    existing = readable_path.read_text(encoding='utf-8')
                                    if '<style>' not in existing:
                                        needs_regen = True
                                except:
                                    needs_regen = True
                            
                            if needs_regen:
                                readable_path.parent.mkdir(parents=True, exist_ok=True)
                                html_content = READABLE_TEMPLATE.format(
                                    citation=data.get('citation', 'Unknown'),
                                    court=data.get('court', 'Unknown'),
                                    judges=', '.join(data.get('judges', [])) if isinstance(data.get('judges'), list) else str(data.get('judges', '')),
                                    date=data.get('date', data.get('date_decided', 'Unknown')),
                                    judgment_html=data.get('judgment_html', data.get('judgment_raw', '')),
                                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M')
                                )
                                readable_path.write_text(html_content, encoding='utf-8')
                                readable_fixed += 1
                    
                    fixed += 1
                    rep_fixed += 1
                    
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  ERROR: {jf}: {e}")
        
        if rep_fixed > 0:
            by_reporter[rep] = rep_fixed
    
    elapsed = time.time() - start
    
    # Report
    print(f"\n{'='*60}")
    print(f"  Fix Missing Judgment Field — {'DRY RUN' if not do_fix else 'APPLIED'}")
    print(f"{'='*60}")
    print(f"  Total files scanned: {total:,}")
    print(f"  Already OK:          {already_ok:,}")
    print(f"  {'Would fix' if not do_fix else 'Fixed'}:    {fixed:,}")
    print(f"  No source data:      {no_source:,}")
    print(f"  Errors:              {errors}")
    if regen_readable and do_fix:
        print(f"  Readable HTML regen: {readable_fixed:,}")
    print(f"  Elapsed:             {elapsed:.1f}s")
    print()
    
    if by_reporter:
        print("  By reporter:")
        for rep, count in sorted(by_reporter.items(), key=lambda x: -x[1]):
            print(f"    {rep}: {count:,}")
    
    print(f"{'='*60}")
    
    # Save report
    report = {
        'timestamp': datetime.now().isoformat(),
        'mode': 'fix' if do_fix else 'dry_run',
        'total_scanned': total,
        'already_ok': already_ok,
        'fixed': fixed,
        'no_source': no_source,
        'errors': errors,
        'readable_fixed': readable_fixed if regen_readable else 0,
        'by_reporter': by_reporter,
        'elapsed_seconds': round(elapsed, 1)
    }
    
    report_dir = DATA_DIR / 'audit'
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"judgment_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to: {report_path}")
    
    return fixed


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fix JSON files missing judgment field')
    parser.add_argument('--fix', action='store_true', help='Apply fixes (default: dry run)')
    parser.add_argument('--regen', action='store_true', help='Also regenerate unstyled readable HTML')
    args = parser.parse_args()
    
    fix_missing_judgment(do_fix=args.fix, regen_readable=args.regen)
