#!/usr/bin/env python3
"""Analyze today's scraping activity"""
import json
from pathlib import Path
from datetime import datetime, date

today = date.today()
data_dir = Path('data_v2')

today_cases = []
for reporter_dir in data_dir.iterdir():
    if reporter_dir.is_dir() and reporter_dir.name not in ['legislation', 'html', 'backup']:
        for year_dir in reporter_dir.iterdir():
            if year_dir.is_dir():
                for f in year_dir.glob('*.json'):
                    mtime = datetime.fromtimestamp(f.stat().st_mtime)
                    if mtime.date() == today:
                        today_cases.append({
                            'file': f.name,
                            'reporter': reporter_dir.name,
                            'time': mtime.strftime('%H:%M:%S')
                        })

today_cases.sort(key=lambda x: x['time'])

print(f'=== Cases Scraped Today (Feb 7) ===')
print(f'Total: {len(today_cases)} cases')

if today_cases:
    first = today_cases[0]
    last = today_cases[-1]
    print(f"First: {first['time']} - {first['reporter']}")
    print(f"Last: {last['time']} - {last['reporter']}")
    
    first_time = datetime.strptime(first['time'], '%H:%M:%S')
    last_time = datetime.strptime(last['time'], '%H:%M:%S')
    duration = (last_time - first_time).total_seconds() / 3600
    
    if duration > 0:
        rate = len(today_cases) / duration
        print(f'Duration: {duration:.1f} hours')
        print(f'Rate: {rate:.1f} cases/hour')
    
    print(f'\nBy hour:')
    by_hour = {}
    for c in today_cases:
        hour = c['time'][:2]
        by_hour[hour] = by_hour.get(hour, 0) + 1
    
    for hour in sorted(by_hour.keys()):
        print(f'  {hour}:00 - {by_hour[hour]} cases')

# Check logs for errors
log_dir = Path('logs')
if log_dir.exists():
    print(f'\n=== Error Analysis ===')
    for log_file in log_dir.glob('*.log'):
        if log_file.stat().st_mtime > datetime.combine(today, datetime.min.time()).timestamp():
            content = log_file.read_text(errors='ignore')
            errors = [l for l in content.split('\n') if 'error' in l.lower() or '500' in l or '403' in l or '429' in l]
            if errors:
                print(f'\n{log_file.name}:')
                for e in errors[-10:]:  # Last 10 errors
                    print(f'  {e[:100]}')
