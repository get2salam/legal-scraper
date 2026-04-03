#!/usr/bin/env python3
"""Print consolidated summary of all citator analytics."""
import json, os
from pathlib import Path

ANALYTICS = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\analytics")

def fmt(n):
    return f"{n:,}"

print("=" * 65)
print("  LEXISNEXIS-STYLE CASE CITATOR - COMPLETE RESULTS")
print("=" * 65)

# File sizes
print("\n📁 OUTPUT FILES:")
for f in sorted(ANALYTICS.glob("*.json")):
    size = os.path.getsize(f)
    if size > 1024*1024:
        sz = f"{size/1024/1024:.1f} MB"
    else:
        sz = f"{size/1024:.0f} KB"
    print(f"  {f.name:30s} {sz:>10s}")

# Citation Graph
print("\n📊 CITATION GRAPH:")
with open(ANALYTICS / "citation_graph.json", 'r', encoding='utf-8') as f:
    cg = json.load(f)
total_cit = sum(len(v) for v in cg.values())
print(f"  Cases with citations:    {fmt(len(cg))}")
print(f"  Total citations found:   {fmt(total_cit)}")
print(f"  Avg per citing case:     {total_cit/max(len(cg),1):.1f}")

# Case index
with open(ANALYTICS / "case_index.json", 'r', encoding='utf-8') as f:
    ci = json.load(f)
print(f"  Total cases indexed:     {fmt(len(ci))}")

# Treatment Signals
print("\n🏷️  TREATMENT SIGNALS:")
with open(ANALYTICS / "treatment_signals.json", 'r', encoding='utf-8') as f:
    ts = json.load(f)
total_t = sum(len(v) for v in ts.values())
sigs = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
for v in ts.values():
    for t in v:
        sigs[t.get('signal', 'NEUTRAL')] += 1
print(f"  Unique cited cases:      {fmt(len(ts))}")
print(f"  Total treatment pairs:   {fmt(total_t)}")
print(f"  🔴 RED (negative):       {fmt(sigs['RED'])} ({100*sigs['RED']/total_t:.1f}%)")
print(f"  🟡 YELLOW (cautionary):  {fmt(sigs['YELLOW'])} ({100*sigs['YELLOW']/total_t:.1f}%)")
print(f"  🟢 GREEN (positive):     {fmt(sigs['GREEN'])} ({100*sigs['GREEN']/total_t:.1f}%)")
print(f"  ⚪ NEUTRAL:              {fmt(sigs['NEUTRAL'])} ({100*sigs['NEUTRAL']/total_t:.1f}%)")

# Similar Cases
print("\n🔗 SIMILAR CASES:")
with open(ANALYTICS / "similar_cases.json", 'r', encoding='utf-8') as f:
    sc = json.load(f)
all_scores = [s['score'] for sims in sc.values() for s in sims]
print(f"  Cases with similar:      {fmt(len(sc))}")
print(f"  Total similar pairs:     {fmt(len(all_scores))}")
print(f"  Avg similar per case:    {len(all_scores)/max(len(sc),1):.1f}")
print(f"  Avg similarity score:    {sum(all_scores)/max(len(all_scores),1):.4f}")
print(f"  Max similarity score:    {max(all_scores):.4f}")

# Case Signals
print("\n⚖️  AGGREGATED CASE SIGNALS:")
with open(ANALYTICS / "case_signals.json", 'r', encoding='utf-8') as f:
    cs = json.load(f)
overall = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
sc_red = 0
for v in cs.values():
    overall[v['overall_signal']] += 1
    if v.get('reason') == 'SC_RED':
        sc_red += 1
print(f"  Cases with signals:      {fmt(len(cs))}")
print(f"  🔴 RED (bad law):        {fmt(overall['RED'])} ({100*overall['RED']/len(cs):.1f}%)")
print(f"  🟡 YELLOW (cautionary):  {fmt(overall['YELLOW'])} ({100*overall['YELLOW']/len(cs):.1f}%)")
print(f"  🟢 GREEN (good law):     {fmt(overall['GREEN'])} ({100*overall['GREEN']/len(cs):.1f}%)")
print(f"  ⚪ NEUTRAL:              {fmt(overall['NEUTRAL'])} ({100*overall['NEUTRAL']/len(cs):.1f}%)")
print(f"  SC RED overrides:        {fmt(sc_red)}")

print("\n" + "=" * 65)
print("  ALL 5 SCRIPTS COMPLETED SUCCESSFULLY")
print("=" * 65)
