#!/usr/bin/env python3
"""Find JavaScript click handlers for statute rows."""

import re

with open('statuechar_search_response.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Find the script sections
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
print(f"Found {len(scripts)} script sections")

for i, script in enumerate(scripts):
    if 'caseType' in script or 'casetypeid' in script or 'statue' in script.lower():
        print(f"\n=== Script {i} (contains caseType/statue) ===")
        # Print relevant parts
        lines = script.split('\n')
        in_relevant = False
        for line in lines:
            if 'caseType' in line or 'casetypeid' in line or 'statue' in line.lower():
                in_relevant = True
            if in_relevant:
                print(line)
                if line.strip().endswith('}') or line.strip().endswith('});'):
                    if 'function' not in line and 'if' not in line:
                        in_relevant = False
            if len([l for l in script.split('\n') if 'statue' in l.lower()]) > 50:
                # Too many lines, skip
                break

# Also look for onclick attributes
onclick_matches = re.findall(r'onclick="([^"]*statute[^"]*)"', html, re.IGNORECASE)
print(f"\nOnclick attributes with statute: {onclick_matches[:5]}")
