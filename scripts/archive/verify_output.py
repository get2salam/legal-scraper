#!/usr/bin/env python3
"""Verify the classified database output."""
import json

with open('data_v2/cases_classified.jsonl', 'r', encoding='utf-8') as f:
    case = json.loads(f.readline())

print("=== Sample Case ===")
print(f"Citation: {case.get('citation')}")
print(f"Title: {case.get('title', '')[:60]}")
print(f"Outcome: {case.get('outcome')}")
print(f"Confidence: {case.get('outcome_confidence')}")
print(f"Case Type: {case.get('case_type')}")
print(f"Reporter: {case.get('reporter')}")
print(f"\nProvision Keys: {case.get('provision_keys', [])[:5]}")
print(f"\nClaims Structure: {list(case.get('claims', {}).keys())}")

claims = case.get('claims', {})
print(f"\n  Statutory refs: {len(claims.get('statutory_references', []))}")
print(f"  Constitutional articles: {len(claims.get('constitutional_articles', []))}")
print(f"  Legal principles: {len(claims.get('legal_principles', []))}")
print(f"  Reliefs: {len(claims.get('reliefs_sought', []))}")
