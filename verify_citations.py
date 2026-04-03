"""Spot-check citation accuracy by reading judgment text and verifying extracted citations."""
import sys, json, re, random
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path

DATA = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
GRAPH = DATA / "analytics" / "citation_graph.json"

# Load existing citation graph
graph = json.load(open(GRAPH, encoding='utf-8'))
print(f"Citation graph: {len(graph)} cases, {sum(len(v) for v in graph.values())} links\n")

# Import the ACTUAL extractor's citation function
sys.path.insert(0, str(Path(__file__).parent))
from citation_extractor import extract_citations, strip_html

# Sample 20 cases that have citations in the graph
cases_with_citations = [(k, v) for k, v in graph.items() if len(v) >= 2]
random.seed(2026)
sample = random.sample(cases_with_citations, min(50, len(cases_with_citations)))

print(f"Verifying {len(sample)} cases...\n")
print("=" * 70)

correct = 0
wrong = 0
missed = 0
total_links = 0
verified_links = 0

for citation, graph_cites in sample:
    # Find the JSON file
    parts = citation.split()
    if len(parts) < 3:
        continue
    year, reporter, page = parts[0], parts[1], parts[2]
    
    # Try to find file
    json_path = DATA / reporter / year / f"{citation.replace(' ', '_')}.json"
    if not json_path.exists():
        # Try PLCCS folder
        json_path = DATA / reporter.replace("(","").replace(")","") / year / f"{citation.replace(' ', '_')}.json"
    
    if not json_path.exists():
        continue
    
    try:
        data = json.load(open(json_path, encoding='utf-8'))
    except:
        continue
    
    # Get judgment text
    judgment = data.get('judgment', '') or data.get('judgment_raw', '') or ''
    if not judgment or len(judgment) < 100:
        continue
    
    # Use the SAME extraction logic as the extractor
    text_clean = strip_html(judgment)
    text_cites = extract_citations(text_clean)
    text_cites.discard(citation)  # Remove self-citation
    
    graph_set = set(graph_cites)
    
    # Compare
    in_both = graph_set & text_cites  # Graph says cited AND text confirms
    in_graph_not_text = graph_set - text_cites  # Graph says cited but text doesn't have it
    in_text_not_graph = text_cites - graph_set  # Text has citation but graph missed it
    
    total_links += len(graph_set)
    verified_links += len(in_both)
    
    status = "PASS" if len(in_graph_not_text) == 0 else "ISSUES"
    
    print(f"\n{citation}")
    print(f"  Graph says: {len(graph_set)} citations")
    print(f"  Text has:   {len(text_cites)} citations")
    print(f"  Confirmed:  {len(in_both)} ({len(in_both)*100//max(len(graph_set),1)}%)")
    
    if in_graph_not_text:
        print(f"  NOT in text: {sorted(in_graph_not_text)[:5]}")
        wrong += len(in_graph_not_text)
    
    if in_text_not_graph:
        print(f"  MISSED by graph: {sorted(in_text_not_graph)[:5]}")
        missed += len(in_text_not_graph)
    
    if len(in_graph_not_text) == 0:
        correct += 1
    
    print(f"  Status: {status}")

print("\n" + "=" * 70)
print(f"VERIFICATION RESULTS")
print(f"=" * 70)
print(f"Cases checked: {len(sample)}")
print(f"Perfect match: {correct}/{len(sample)} ({correct*100//len(sample)}%)")
print(f"Total graph links checked: {total_links}")
print(f"Verified in text: {verified_links} ({verified_links*100//max(total_links,1)}%)")
print(f"Graph links NOT in text: {wrong}")
print(f"Text citations MISSED by graph: {missed}")
print(f"\nACCURACY: {verified_links*100//max(total_links,1)}% of graph citations confirmed in judgment text")
print(f"COVERAGE: {missed} additional citations in text that graph missed")
