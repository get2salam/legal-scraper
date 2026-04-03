#!/usr/bin/env python3
"""Check for sections with error responses instead of content."""

import json
from pathlib import Path

bad_sections = 0
bad_statutes = []
error_values = ["-1", "1", "", '"-1"', '"1"', '""']

for f in Path("data_v2/legislation/A").glob("*.json"):
    try:
        d = json.load(open(f, encoding="utf-8"))
        statute_bad = 0
        for sec in d.get("sections", []):
            text = sec.get("text", "")
            if text.strip() in error_values or len(text.strip()) < 10:
                bad_sections += 1
                statute_bad += 1
        if statute_bad > 0:
            bad_statutes.append((d.get("title", f.stem)[:50], statute_bad, len(d.get("sections", []))))
    except Exception as e:
        print(f"Error reading {f.name}: {e}")

print(f"Sections with error/empty responses: {bad_sections}")
print(f"Affected statutes: {len(bad_statutes)}")
print()
print("Top 20 affected statutes:")
for title, bad, total in sorted(bad_statutes, key=lambda x: -x[1])[:20]:
    print(f"  {bad}/{total} bad: {title}")
