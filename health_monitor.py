#!/usr/bin/env python3
"""
Health Monitor for Qanoon Data Pipeline

Monitors scraper health, data integrity, and sends status reports.
Run via Task Scheduler for automated monitoring.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# Paths
DATA_DIR = Path(__file__).parent / "data_v2"
LOGS_DIR = DATA_DIR / "logs"
HEALTH_LOG = LOGS_DIR / "health_status.json"


def check_case_law_status() -> dict:
    """Check case law scraping status for each reporter."""
    reporters = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR']
    status = {}
    
    for reporter in reporters:
        reporter_dir = DATA_DIR / reporter
        if reporter_dir.exists():
            years = [d.name for d in reporter_dir.iterdir() if d.is_dir() and d.name.isdigit()]
            total_cases = sum(
                len(list((reporter_dir / year).glob('*.json')))
                for year in years
            )
            latest_year = max(years) if years else None
            
            # Check for recent activity
            recent_files = []
            for year_dir in reporter_dir.iterdir():
                if year_dir.is_dir():
                    for f in year_dir.glob('*.json'):
                        mtime = datetime.fromtimestamp(f.stat().st_mtime)
                        if mtime > datetime.now() - timedelta(hours=24):
                            recent_files.append(f.name)
            
            status[reporter] = {
                'total_cases': total_cases,
                'years': len(years),
                'latest_year': latest_year,
                'recent_24h': len(recent_files),
                'healthy': total_cases > 0
            }
        else:
            status[reporter] = {
                'total_cases': 0,
                'years': 0,
                'latest_year': None,
                'recent_24h': 0,
                'healthy': False
            }
    
    return status


def check_legislation_status() -> dict:
    """Check legislation scraping status."""
    legislation_dir = DATA_DIR / "legislation"
    status = {'alphabets': {}, 'total_statutes': 0}
    
    if legislation_dir.exists():
        for letter_dir in sorted(legislation_dir.iterdir()):
            if letter_dir.is_dir() and len(letter_dir.name) == 1:
                count = len(list(letter_dir.glob('*.json')))
                status['alphabets'][letter_dir.name] = count
                status['total_statutes'] += count
    
    # Check progress file
    progress_file = legislation_dir / "progress.json"
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
            status['current_alphabet'] = progress.get('current_alphabet', 'A')
            status['completed_alphabets'] = progress.get('completed_alphabets', [])
    
    return status


def check_chromadb_status() -> dict:
    """Check ChromaDB vector store status."""
    chromadb_dir = DATA_DIR / "chromadb"
    status = {
        'exists': chromadb_dir.exists(),
        'size_mb': 0,
        'healthy': False
    }
    
    if chromadb_dir.exists():
        # Calculate total size
        total_size = sum(f.stat().st_size for f in chromadb_dir.rglob('*') if f.is_file())
        status['size_mb'] = round(total_size / (1024 * 1024), 2)
        status['healthy'] = status['size_mb'] > 10  # At least 10MB expected
    
    return status


def check_disk_space() -> dict:
    """Check available disk space."""
    import shutil
    total, used, free = shutil.disk_usage(DATA_DIR)
    return {
        'total_gb': round(total / (1024**3), 2),
        'used_gb': round(used / (1024**3), 2),
        'free_gb': round(free / (1024**3), 2),
        'percent_used': round(used / total * 100, 1),
        'healthy': free > 10 * (1024**3)  # At least 10GB free
    }


def check_recent_errors() -> list:
    """Check for recent errors in logs."""
    errors = []
    
    # Check orchestrator log
    log_files = list(LOGS_DIR.glob('*.log')) if LOGS_DIR.exists() else []
    
    for log_file in log_files[-5:]:  # Last 5 log files
        try:
            with open(log_file, encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if 'ERROR' in line or 'CRITICAL' in line:
                        errors.append({
                            'file': log_file.name,
                            'message': line.strip()[:200]
                        })
        except Exception:
            pass
    
    return errors[-10:]  # Last 10 errors


def generate_health_report() -> dict:
    """Generate comprehensive health report."""
    report = {
        'timestamp': datetime.now().isoformat(),
        'overall_healthy': True,
        'case_law': check_case_law_status(),
        'legislation': check_legislation_status(),
        'chromadb': check_chromadb_status(),
        'disk': check_disk_space(),
        'recent_errors': check_recent_errors()
    }
    
    # Calculate overall health
    unhealthy_components = []
    
    for reporter, status in report['case_law'].items():
        if not status.get('healthy'):
            unhealthy_components.append(f"CaseLaw-{reporter}")
    
    if not report['chromadb']['healthy']:
        unhealthy_components.append("ChromaDB")
    
    if not report['disk']['healthy']:
        unhealthy_components.append("DiskSpace")
    
    if len(report['recent_errors']) > 5:
        unhealthy_components.append("TooManyErrors")
    
    report['overall_healthy'] = len(unhealthy_components) == 0
    report['unhealthy_components'] = unhealthy_components
    
    return report


def save_report(report: dict):
    """Save health report to file."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(HEALTH_LOG, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    
    # Also save timestamped version
    dated_log = LOGS_DIR / f"health_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(dated_log, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    
    log.info(f"Health report saved to {HEALTH_LOG}")


def print_summary(report: dict):
    """Print human-readable summary."""
    print("\n" + "=" * 60)
    print("  QANOON DATA PIPELINE - HEALTH REPORT")
    print("=" * 60)
    print(f"  Timestamp: {report['timestamp']}")
    print(f"  Overall Status: {'[OK] HEALTHY' if report['overall_healthy'] else '[X] UNHEALTHY'}")
    
    if report['unhealthy_components']:
        print(f"  Issues: {', '.join(report['unhealthy_components'])}")
    
    print("\n--- Case Law ---")
    total_cases = sum(r['total_cases'] for r in report['case_law'].values())
    recent_24h = sum(r['recent_24h'] for r in report['case_law'].values())
    print(f"  Total Cases: {total_cases:,}")
    print(f"  Added (24h): {recent_24h}")
    
    print("\n--- Legislation ---")
    print(f"  Total Statutes: {report['legislation']['total_statutes']:,}")
    print(f"  Current Letter: {report['legislation'].get('current_alphabet', 'N/A')}")
    print(f"  Completed: {len(report['legislation'].get('completed_alphabets', []))}/26")
    
    print("\n--- Storage ---")
    print(f"  ChromaDB: {report['chromadb']['size_mb']} MB")
    print(f"  Disk Free: {report['disk']['free_gb']} GB ({100 - report['disk']['percent_used']:.1f}%)")
    
    if report['recent_errors']:
        print(f"\n--- Recent Errors ({len(report['recent_errors'])}) ---")
        for err in report['recent_errors'][:3]:
            print(f"  • {err['message'][:60]}...")
    
    print("=" * 60 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Health Monitor for Qanoon Pipeline")
    parser.add_argument('--json', action='store_true', help='Output as JSON only')
    parser.add_argument('--save', action='store_true', help='Save report to file')
    args = parser.parse_args()
    
    log.info("Running health check...")
    report = generate_health_report()
    
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_summary(report)
    
    if args.save:
        save_report(report)
    
    # Exit with error code if unhealthy
    sys.exit(0 if report['overall_healthy'] else 1)


if __name__ == "__main__":
    main()
