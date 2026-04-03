#!/usr/bin/env python3
"""
FORMAT AUDITOR — Zero-token format integrity agent.

Checks every PLS case has all 4 formats saved correctly:
  1. JSON  — valid JSON, required fields, judgment_raw is proper HTML
  2. Original HTML — proper HTML (not JSON-encoded), starts with <
  3. Readable HTML — exists, has styling
  4. JSONL — entry exists in reporter JSONL

Auto-fixes:
  - JSON-encoded original HTML → decode to proper HTML
  - JSON-encoded judgment_raw in JSON files → decode
  - Missing readable HTML → regenerate from JSON + original
  - Missing JSONL entries → append

Run: python format_auditor.py [--fix] [--reporter SCMR] [--year 2024]
  --fix     Auto-fix issues (without this, audit-only)
  --reporter  Audit specific reporter only
  --year    Audit specific year only
  --quick   Sample 10 files per year/reporter (fast check)
"""

import json
import glob
import os
import re
import sys
import time
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

DATA_DIR = Path(__file__).parent / 'data_v2'
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

REQUIRED_JSON_FIELDS = ['citation', 'reporter', 'year', 'judgment', 'judgment_raw']

READABLE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{citation}</title>
<style>
body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px;
       line-height: 1.8; color: #e0e0e0; background: #1a1a2e; }}
h1, h2, h3 {{ color: #64ffda; }}
.meta {{ color: #888; margin-bottom: 20px; border-bottom: 1px solid #333; padding-bottom: 10px; }}
.judgment {{ white-space: pre-wrap; }}
.headnotes {{ background: #16213e; padding: 15px; border-radius: 8px; margin: 20px 0; }}
</style>
</head>
<body>
<h1>{citation}</h1>
<div class="meta">
<strong>Court:</strong> {court}<br>
<strong>Judges:</strong> {judges}<br>
<strong>Date:</strong> {date_decided}
</div>
{headnotes_html}
<div class="judgment">{judgment_raw}</div>
</body>
</html>"""


class FormatAuditor:
    def __init__(self, fix=False, quick=False):
        self.fix = fix
        self.quick = quick
        self.stats = {
            'total_cases': 0,
            'json_ok': 0, 'json_bad': 0, 'json_fixed': 0,
            'orig_ok': 0, 'orig_missing': 0, 'orig_bad_format': 0, 'orig_fixed': 0,
            'readable_ok': 0, 'readable_missing': 0, 'readable_fixed': 0,
            'jsonl_ok': 0, 'jsonl_missing': 0, 'jsonl_fixed': 0,
        }
        self.issues = []  # [(severity, reporter, year, citation, issue)]
        self.jsonl_cache = {}  # reporter_year -> set of citations

    def load_jsonl_index(self, reporter, year):
        """Load JSONL file and index citations for fast lookup."""
        key = f"{reporter}_{year}"
        if key in self.jsonl_cache:
            return self.jsonl_cache[key]
        
        jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
        citations = set()
        if jsonl_path.exists():
            try:
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            cit = entry.get('citation', '')
                            if cit:
                                citations.add(cit)
                        except json.JSONDecodeError:
                            pass
            except Exception:
                pass
        
        self.jsonl_cache[key] = citations
        return citations

    def check_json(self, json_path):
        """Check JSON file validity and fields."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            return 'CORRUPT', f'Invalid JSON: {e}'
        except Exception as e:
            return 'ERROR', f'Read error: {e}'
        
        # Check required fields
        missing = [f for f in REQUIRED_JSON_FIELDS if f not in data]
        if missing:
            return 'BAD', f'Missing fields: {missing}'
        
        # Check judgment_raw is proper HTML (not JSON-encoded)
        raw = data.get('judgment_raw', '')
        if raw.startswith('"') and ('\\u003c' in raw or '\\u003e' in raw):
            if self.fix:
                try:
                    decoded = json.loads(raw)
                    if isinstance(decoded, str):
                        data['judgment_raw'] = decoded
                        with open(json_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2, ensure_ascii=False)
                        self.stats['json_fixed'] += 1
                        return 'FIXED', 'judgment_raw was JSON-encoded, decoded'
                except (json.JSONDecodeError, ValueError):
                    pass
            return 'BAD', 'judgment_raw is JSON-encoded (\\u003c escapes)'
        
        # Check judgment is not empty
        if not data.get('judgment', '').strip():
            return 'WARN', 'Empty judgment text'
        
        return 'OK', None

    def check_original_html(self, orig_path):
        """Check original HTML is proper HTML, not JSON-encoded."""
        if not orig_path.exists():
            return 'MISSING', 'Original HTML file missing'
        
        try:
            with open(orig_path, 'r', encoding='utf-8') as f:
                start = f.read(100)
        except Exception as e:
            return 'ERROR', f'Read error: {e}'
        
        if start.startswith('"') and ('\\u003c' in start or 'u003c' in start):
            if self.fix:
                try:
                    with open(orig_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    decoded = json.loads(content)
                    if isinstance(decoded, str) and ('<' in decoded or '&lt;' in decoded):
                        with open(orig_path, 'w', encoding='utf-8') as f:
                            f.write(decoded)
                        self.stats['orig_fixed'] += 1
                        return 'FIXED', 'Decoded JSON-encoded HTML'
                except (json.JSONDecodeError, ValueError):
                    pass
            return 'BAD', 'HTML is JSON-encoded (\\u003c escapes)'
        
        if not (start.strip().startswith('<') or start.strip().startswith('<!') or 
                start.strip().startswith('\ufeff<')):
            return 'WARN', f'Unexpected start: {start[:30]!r}'
        
        return 'OK', None

    def check_readable_html(self, readable_path, json_path, orig_path):
        """Check readable HTML exists and has styling."""
        if not readable_path.exists():
            if self.fix and json_path.exists():
                if self._regenerate_readable(readable_path, json_path, orig_path):
                    self.stats['readable_fixed'] += 1
                    return 'FIXED', 'Regenerated readable HTML'
            return 'MISSING', 'Readable HTML missing'
        
        try:
            with open(readable_path, 'r', encoding='utf-8') as f:
                content = f.read(500)
            if '<style>' not in content and 'style=' not in content:
                return 'WARN', 'No styling found in readable HTML'
        except Exception as e:
            return 'ERROR', f'Read error: {e}'
        
        return 'OK', None

    def _regenerate_readable(self, readable_path, json_path, orig_path):
        """Regenerate readable HTML from JSON + original HTML."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Get judgment_raw - prefer original HTML file if it exists and is valid
            judgment_raw = ''
            if orig_path.exists():
                with open(orig_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                if content.startswith('"'):
                    try:
                        judgment_raw = json.loads(content)
                    except:
                        judgment_raw = data.get('judgment_raw', '')
                else:
                    judgment_raw = content
            else:
                judgment_raw = data.get('judgment_raw', '')
            
            citation = data.get('citation', '')
            court = data.get('court', '')
            judges = data.get('judges', '')
            date_decided = data.get('date_decided', '')
            headnotes = data.get('headnotes', '')
            headnotes_html = f'<div class="headnotes"><h3>Headnotes</h3>{headnotes}</div>' if headnotes else ''
            
            readable = READABLE_TEMPLATE.format(
                citation=citation, court=court, judges=judges,
                date_decided=date_decided, headnotes_html=headnotes_html,
                judgment_raw=judgment_raw,
            )
            
            readable_path.parent.mkdir(parents=True, exist_ok=True)
            with open(readable_path, 'w', encoding='utf-8') as f:
                f.write(readable)
            return True
        except Exception:
            return False

    def check_jsonl(self, reporter, year, citation):
        """Check case exists in JSONL file."""
        citations = self.load_jsonl_index(reporter, year)
        if citation in citations:
            return 'OK', None
        return 'MISSING', f'Not found in {reporter}_{year}.jsonl'

    def append_to_jsonl(self, reporter, year, json_path):
        """Append case to JSONL file."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
            
            # Update cache
            key = f"{reporter}_{year}"
            if key in self.jsonl_cache:
                self.jsonl_cache[key].add(data.get('citation', ''))
            
            self.stats['jsonl_fixed'] += 1
            return True
        except Exception:
            return False

    def audit_case(self, json_path):
        """Audit a single case across all 4 formats."""
        self.stats['total_cases'] += 1
        
        # Parse paths
        json_p = Path(json_path)
        reporter = json_p.parent.name
        if not reporter.isupper() and reporter not in REPORTERS:
            # Year directory - go up one more
            year_str = reporter
            reporter = json_p.parent.parent.name
        else:
            year_str = json_p.parent.name
            reporter = json_p.parent.parent.name
        
        # Correct path parsing: data_v2/REPORTER/YEAR/CITATION.json
        parts = json_p.relative_to(DATA_DIR).parts
        if len(parts) >= 3:
            reporter = parts[0]
            year_str = parts[1]
        
        citation_safe = json_p.stem
        
        try:
            year = int(year_str)
        except ValueError:
            return
        
        orig_path = json_p.parent / 'original' / f'{citation_safe}.html'
        readable_path = DATA_DIR / 'html' / reporter / year_str / f'{citation_safe}.html'
        
        # 1. Check JSON
        status, msg = self.check_json(json_path)
        if status == 'OK':
            self.stats['json_ok'] += 1
        elif status == 'FIXED':
            self.stats['json_ok'] += 1  # Now OK
        else:
            self.stats['json_bad'] += 1
            self.issues.append(('JSON', status, reporter, year, citation_safe, msg))
        
        # 2. Check Original HTML
        status, msg = self.check_original_html(orig_path)
        if status == 'OK':
            self.stats['orig_ok'] += 1
        elif status == 'FIXED':
            self.stats['orig_ok'] += 1
        elif status == 'MISSING':
            self.stats['orig_missing'] += 1
            self.issues.append(('ORIG', status, reporter, year, citation_safe, msg))
        else:
            self.stats['orig_bad_format'] += 1
            self.issues.append(('ORIG', status, reporter, year, citation_safe, msg))
        
        # 3. Check Readable HTML
        status, msg = self.check_readable_html(readable_path, json_p, orig_path)
        if status == 'OK':
            self.stats['readable_ok'] += 1
        elif status == 'FIXED':
            self.stats['readable_ok'] += 1
        else:
            self.stats['readable_missing'] += 1
            self.issues.append(('READ', status, reporter, year, citation_safe, msg))
        
        # 4. Check JSONL
        try:
            with open(json_p, 'r', encoding='utf-8') as f:
                data = json.load(f)
            citation = data.get('citation', '')
        except:
            citation = citation_safe.replace('_', ' ')
        
        status, msg = self.check_jsonl(reporter, year, citation)
        if status == 'OK':
            self.stats['jsonl_ok'] += 1
        else:
            self.stats['jsonl_missing'] += 1
            if self.fix:
                if self.append_to_jsonl(reporter, year, json_p):
                    self.stats['jsonl_ok'] += 1
                    self.stats['jsonl_missing'] -= 1
                else:
                    self.issues.append(('JSONL', status, reporter, year, citation_safe, msg))
            else:
                self.issues.append(('JSONL', status, reporter, year, citation_safe, msg))

    def run(self, reporters=None, years=None):
        """Run full audit."""
        start = time.time()
        reporters = reporters or REPORTERS
        
        print("=" * 70)
        print(f"FORMAT AUDITOR {'(FIX MODE)' if self.fix else '(AUDIT ONLY)'}")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Reporters: {', '.join(reporters)}")
        print(f"Years: {years or 'ALL'}")
        print(f"Quick mode: {self.quick}")
        print("=" * 70)
        
        for reporter in reporters:
            reporter_dir = DATA_DIR / reporter
            if not reporter_dir.exists():
                continue
            
            year_dirs = sorted(reporter_dir.iterdir())
            for year_dir in year_dirs:
                if not year_dir.is_dir() or year_dir.name in ('original', 'html'):
                    continue
                
                try:
                    year = int(year_dir.name)
                except ValueError:
                    continue
                
                if years and year not in years:
                    continue
                
                json_files = list(year_dir.glob('*.json'))
                if not json_files:
                    continue
                
                if self.quick:
                    # Sample up to 10 files
                    import random
                    json_files = random.sample(json_files, min(10, len(json_files)))
                
                for jf in json_files:
                    self.audit_case(jf)
                
                if self.stats['total_cases'] % 10000 == 0 and self.stats['total_cases'] > 0:
                    print(f"  Progress: {self.stats['total_cases']:,} cases audited...")
        
        elapsed = time.time() - start
        self._print_report(elapsed)
        self._save_report(elapsed)

    def _print_report(self, elapsed):
        """Print audit report."""
        s = self.stats
        total = s['total_cases']
        
        print(f"\n{'=' * 70}")
        print(f"AUDIT COMPLETE — {total:,} cases in {elapsed:.0f}s")
        print(f"{'=' * 70}")
        
        print(f"\n📄 JSON Files:")
        print(f"   ✅ OK: {s['json_ok']:,}  ❌ Bad: {s['json_bad']:,}  🔧 Fixed: {s['json_fixed']:,}")
        
        print(f"\n📋 Original HTML:")
        print(f"   ✅ OK: {s['orig_ok']:,}  ❌ Missing: {s['orig_missing']:,}  ⚠️ Bad format: {s['orig_bad_format']:,}  🔧 Fixed: {s['orig_fixed']:,}")
        
        print(f"\n📖 Readable HTML:")
        print(f"   ✅ OK: {s['readable_ok']:,}  ❌ Missing: {s['readable_missing']:,}  🔧 Fixed: {s['readable_fixed']:,}")
        
        print(f"\n📊 JSONL Entries:")
        print(f"   ✅ OK: {s['jsonl_ok']:,}  ❌ Missing: {s['jsonl_missing']:,}  🔧 Fixed: {s['jsonl_fixed']:,}")
        
        # Health score
        total_checks = total * 4
        total_ok = s['json_ok'] + s['orig_ok'] + s['readable_ok'] + s['jsonl_ok']
        health = (total_ok / total_checks * 100) if total_checks > 0 else 0
        
        print(f"\n{'=' * 70}")
        emoji = "🟢" if health >= 99 else "🟡" if health >= 95 else "🔴"
        print(f"{emoji} FORMAT HEALTH: {health:.1f}% ({total_ok:,}/{total_checks:,} checks passed)")
        print(f"{'=' * 70}")
        
        # Top issues by type
        if self.issues:
            issue_counts = defaultdict(int)
            for fmt, severity, reporter, year, cit, msg in self.issues:
                issue_counts[f"{fmt}:{msg[:50]}"] += 1
            
            print(f"\n📋 Top issues:")
            for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1])[:10]:
                print(f"   {count:>6,}x  {issue}")

    def _save_report(self, elapsed):
        """Save audit report to JSON."""
        report_dir = DATA_DIR / 'audit'
        report_dir.mkdir(exist_ok=True)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'mode': 'fix' if self.fix else 'audit',
            'quick': self.quick,
            'elapsed_seconds': round(elapsed, 1),
            'stats': self.stats,
            'issue_count': len(self.issues),
            'top_issues': [],
        }
        
        # Summarize issues
        issue_counts = defaultdict(int)
        for fmt, severity, reporter, year, cit, msg in self.issues:
            issue_counts[f"{fmt}:{severity}:{msg[:80]}"] += 1
        
        report['top_issues'] = [
            {'issue': k, 'count': v}
            for k, v in sorted(issue_counts.items(), key=lambda x: -x[1])[:20]
        ]
        
        # Sample issues for debugging
        report['sample_issues'] = [
            {'format': fmt, 'severity': sev, 'reporter': rep, 'year': yr, 'citation': cit, 'message': msg}
            for fmt, sev, rep, yr, cit, msg in self.issues[:50]
        ]
        
        report_path = report_dir / f'format_audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Report saved: {report_path}")


def main():
    parser = argparse.ArgumentParser(description='Format Auditor — Check all 4 PLS formats')
    parser.add_argument('--fix', action='store_true', help='Auto-fix issues')
    parser.add_argument('--reporter', type=str, help='Audit specific reporter')
    parser.add_argument('--year', type=int, help='Audit specific year')
    parser.add_argument('--quick', action='store_true', help='Quick sample mode (10 per year)')
    args = parser.parse_args()
    
    reporters = [args.reporter] if args.reporter else None
    years = [args.year] if args.year else None
    
    auditor = FormatAuditor(fix=args.fix, quick=args.quick)
    auditor.run(reporters=reporters, years=years)


if __name__ == '__main__':
    main()
