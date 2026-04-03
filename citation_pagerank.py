#!/usr/bin/env python3
"""
Citation PageRank — Authority scores for Pakistani legal cases.

Applies Google's PageRank algorithm to the citation graph.
Cases cited by more (and more important) cases get higher scores.

Output:
    - data_v2/analytics/pagerank_scores.json  (all scores)
    - data_v2/analytics/top100_cases.json     (leaderboard)
"""
import json
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

GRAPH_FILE = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\analytics\citation_graph.json")
OUTPUT_DIR = GRAPH_FILE.parent

def load_graph():
    """Load citation graph and build reverse index."""
    graph = json.load(open(GRAPH_FILE, encoding="utf-8"))
    
    # Collect all nodes (both citing and cited)
    all_nodes = set(graph.keys())
    for targets in graph.values():
        all_nodes.update(targets)
    
    # Build reverse graph (who cites this case?)
    reverse = {node: [] for node in all_nodes}
    outgoing_count = {node: 0 for node in all_nodes}
    
    for source, targets in graph.items():
        outgoing_count[source] = len(targets)
        for target in targets:
            if target in reverse:
                reverse[target].append(source)
    
    return all_nodes, graph, reverse, outgoing_count


def pagerank(all_nodes, reverse, outgoing_count, damping=0.85, iterations=50, tolerance=1e-6):
    """Compute PageRank scores."""
    n = len(all_nodes)
    nodes = list(all_nodes)
    node_idx = {node: i for i, node in enumerate(nodes)}
    
    # Initialize scores uniformly
    scores = [1.0 / n] * n
    
    print(f"Computing PageRank: {n:,} nodes, d={damping}, max_iter={iterations}")
    
    for iteration in range(iterations):
        new_scores = [0.0] * n
        
        for i, node in enumerate(nodes):
            rank_sum = 0.0
            for citing_node in reverse.get(node, []):
                j = node_idx.get(citing_node)
                if j is not None and outgoing_count[citing_node] > 0:
                    rank_sum += scores[j] / outgoing_count[citing_node]
            
            new_scores[i] = (1 - damping) / n + damping * rank_sum
        
        # Check convergence
        diff = sum(abs(new_scores[i] - scores[i]) for i in range(n))
        scores = new_scores
        
        if (iteration + 1) % 10 == 0 or diff < tolerance:
            print(f"  Iteration {iteration+1}: diff={diff:.8f}")
        
        if diff < tolerance:
            print(f"  Converged at iteration {iteration+1}")
            break
    
    # Normalize to 0-100 scale
    max_score = max(scores)
    min_score = min(scores)
    score_range = max_score - min_score if max_score > min_score else 1
    
    result = {}
    for i, node in enumerate(nodes):
        normalized = (scores[i] - min_score) / score_range * 100
        result[node] = round(normalized, 2)
    
    return result


def compute_cited_by_counts(reverse):
    """Count how many cases cite each case."""
    return {node: len(citers) for node, citers in reverse.items()}


def main():
    start = time.time()
    
    print("=" * 60)
    print("CITATION PAGERANK")
    print("=" * 60)
    
    print("\nLoading citation graph...")
    all_nodes, graph, reverse, outgoing_count = load_graph()
    print(f"  Nodes: {len(all_nodes):,}")
    print(f"  Edges: {sum(len(v) for v in graph.values()):,}")
    print(f"  Citing cases: {len(graph):,}")
    
    # Compute PageRank
    print()
    scores = pagerank(all_nodes, reverse, outgoing_count)
    
    # Compute cited-by counts
    cited_by = compute_cited_by_counts(reverse)
    
    # Build leaderboard
    top100 = sorted(scores.items(), key=lambda x: -x[1])[:100]
    
    print(f"\n{'=' * 60}")
    print("TOP 20 MOST AUTHORITATIVE CASES")
    print(f"{'=' * 60}")
    for i, (citation, score) in enumerate(top100[:20]):
        cb = cited_by.get(citation, 0)
        print(f"  {i+1:3d}. {citation:30s}  Score: {score:6.2f}  Cited by: {cb:,}")
    
    # Save scores
    print(f"\nSaving scores...")
    
    scores_file = OUTPUT_DIR / "pagerank_scores.json"
    with open(scores_file, "w", encoding="utf-8") as f:
        json.dump(scores, f, ensure_ascii=False)
    print(f"  Scores: {scores_file} ({len(scores):,} entries)")
    
    # Save top 100 leaderboard with metadata
    leaderboard = []
    for rank, (citation, score) in enumerate(top100, 1):
        leaderboard.append({
            "rank": rank,
            "citation": citation,
            "score": score,
            "cited_by_count": cited_by.get(citation, 0),
            "cites_count": len(graph.get(citation, [])),
        })
    
    top_file = OUTPUT_DIR / "top100_cases.json"
    with open(top_file, "w", encoding="utf-8") as f:
        json.dump(leaderboard, f, ensure_ascii=False, indent=2)
    print(f"  Top 100: {top_file}")
    
    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
