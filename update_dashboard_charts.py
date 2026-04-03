#!/usr/bin/env python3
"""Update dashboard with fresh analytics charts and case counts."""
import base64, os, re

dashboard = r'C:\Users\gempo\.openclaw\workspace\dashboard\index.html'
output_dir = r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\analytics\output'

with open(dashboard, 'r', encoding='utf-8') as f:
    html = f.read()

# Update case count references
for old in ['59,541', '59541', '51,594', '51594', '51,058', '51058', '61,806', '61806']:
    html = html.replace(old, '73,578')

for old in ['55.3%', '47.4%', '47.9%', '57.4%']:
    html = html.replace(old, '68.3%')

# Re-embed charts - find all base64 image blocks and replace
chart_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.png')])
print(f"Charts available: {len(chart_files)}")

updated = 0
for chart in chart_files:
    path = os.path.join(output_dir, chart)
    with open(path, 'rb') as f:
        b64_new = base64.b64encode(f.read()).decode()
    
    # Search for this chart's base64 block by looking for its name nearby
    chart_label = chart.replace('.png', '').replace('_', ' ')
    
    # Find position of chart label in html (case insensitive)
    pos = html.lower().find(chart_label.lower())
    if pos == -1:
        # Try with underscores
        pos = html.lower().find(chart.replace('.png', '').lower())
    
    if pos != -1:
        # Find the next base64 image src after this position
        b64_pattern = re.compile(r'data:image/png;base64,([A-Za-z0-9+/=]+)')
        match = b64_pattern.search(html, pos)
        if match and match.start() - pos < 2000:  # within 2000 chars
            html = html[:match.start(1)] + b64_new + html[match.end(1):]
            updated += 1
            print(f"  Updated: {chart}")

with open(dashboard, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"\nUpdated {updated}/{len(chart_files)} charts")
print(f"Dashboard size: {os.path.getsize(dashboard):,} bytes")
