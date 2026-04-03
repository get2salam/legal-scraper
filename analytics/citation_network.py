"""
citation_network.py  -  "The Web of Law" (PageRank)
===================================================
Build a citation network from 50,000+ Pakistani court cases,
compute PageRank, identify authoritative cases, and visualize
the structure of legal influence.

Usage:
    python citation_network.py
"""

import matplotlib
matplotlib.use('Agg')

import json
import os
import sys
import re
import time
from collections import defaultdict, Counter
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

try:
    import networkx as nx
except ImportError:
    print("ERROR: networkx not installed. Run: pip install networkx")
    sys.exit(1)

# --- Configuration -----------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent / "data_v2"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORTERS = ["CLC", "CLD", "GBLR", "MLD", "PCrLJ", "PLC", "PLD", "PTD", "SCMR", "YLR"]

# --- Styling -----------------------------------------------------------------

COLORS = {
    'primary':    '#1a5276',
    'secondary':  '#2e86c1',
    'accent':     '#e74c3c',
    'bg':         '#fafafa',
    'grid':       '#e0e0e0',
    'text':       '#2c3e50',
    'gold':       '#f39c12',
    'green':      '#27ae60',
}

plt.rcParams.update({
    'figure.facecolor': COLORS['bg'],
    'axes.facecolor':   '#ffffff',
    'axes.edgecolor':   COLORS['grid'],
    'axes.labelcolor':  COLORS['text'],
    'xtick.color':      COLORS['text'],
    'ytick.color':      COLORS['text'],
    'font.size':        11,
    'axes.titlesize':   14,
    'axes.labelsize':   12,
})


def banner(text):
    """Print a styled banner."""
    width = 72
    print("\n" + "=" * width)
    print(f"  {text}")
    print("=" * width)


def sub_banner(text):
    print(f"\n  -- {text} {'-' * max(1, 60 - len(text))}")


_COURT_RE = re.compile(
    r'(Supreme Court of Pakistan|Federal Shariat Court|'
    r'High Court \(AJ&amp;K\)|Lahore High Court|Sindh High Court|'
    r'Peshawar High Court|Balochistan High Court|Islamabad High Court|'
    r'High Court)',
    re.IGNORECASE,
)

def clean_court(court_str):
    """Extract clean court name from potentially HTML-polluted field."""
    if not court_str:
        return "Unknown"
    # Only look at first 200 chars to avoid regex on massive HTML blobs
    snippet = court_str[:200]
    m = _COURT_RE.search(snippet)
    if m:
        return m.group(1).strip()
    # Fallback: first line, stripped of tags
    first_line = snippet.split('\r')[0].split('\n')[0].strip()
    first_line = re.sub(r'<[^>]+>', '', first_line).strip()[:50]
    return first_line if first_line else "Unknown"


def normalize_citation(cit):
    """Normalize citation string for consistent matching."""
    if not cit:
        return None
    # Remove extra whitespace, normalize
    cit = re.sub(r'\s+', ' ', cit.strip())
    return cit


# --- Phase 1: Load Data -----------------------------------------------------

def load_all_cases():
    """Scan all data_v2/REPORTER/YEAR/*.json files and build lookup."""
    banner("PHASE 1: Loading Case Data")

    cases = {}           # citation -> case metadata (lightweight)
    citation_lookup = {} # citation -> True (for existence checks)
    cases_cited_map = {} # citation -> list of cited citations
    reporter_counts = Counter()
    errors = 0
    total_files = 0

    for reporter in REPORTERS:
        reporter_dir = BASE_DIR / reporter
        if not reporter_dir.exists():
            print(f"  [!] Reporter directory not found: {reporter}")
            continue

        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            for json_file in year_dir.glob("*.json"):
                total_files += 1
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    citation = data.get('citation', '').strip()
                    if not citation:
                        errors += 1
                        continue

                    citation = normalize_citation(citation)
                    court = clean_court(data.get('court', ''))
                    title = (data.get('title', '') or '')[:100]
                    cases_cited = data.get('cases_cited', []) or []
                    date = data.get('date', '')

                    # Store lightweight metadata
                    cases[citation] = {
                        'court': court,
                        'title': title,
                        'date': date,
                        'reporter': reporter,
                    }
                    citation_lookup[citation] = True

                    # Store citations (normalized)
                    normalized_cites = []
                    for c in cases_cited:
                        nc = normalize_citation(c)
                        if nc:
                            normalized_cites.append(nc)
                    cases_cited_map[citation] = normalized_cites

                    reporter_counts[reporter] += 1

                except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                    errors += 1
                    continue

                if total_files % 10000 == 0:
                    print(f"    ... processed {total_files:,} files")

    print(f"\n  [OK] Loaded {len(cases):,} cases from {total_files:,} files")
    print(f"  [X] Skipped {errors:,} files with errors")
    print(f"\n  Cases by reporter:")
    for rep in REPORTERS:
        if reporter_counts[rep] > 0:
            bar = "#" * (reporter_counts[rep] // 200)
            print(f"    {rep:>7}: {reporter_counts[rep]:>6,}  {bar}")

    return cases, citation_lookup, cases_cited_map


# --- Phase 2: Build Graph ---------------------------------------------------

def build_citation_graph(cases, citation_lookup, cases_cited_map):
    """Build directed graph: A -> B means A cites B."""
    banner("PHASE 2: Building Citation Graph")

    G = nx.DiGraph()

    # Add all cases as nodes
    for citation in cases:
        G.add_node(citation)

    # Add edges where both source and target exist in our data
    edge_count = 0
    unresolved = 0
    total_citations = 0

    for source_citation, cited_list in cases_cited_map.items():
        for target_citation in cited_list:
            total_citations += 1
            if target_citation in citation_lookup:
                G.add_edge(source_citation, target_citation)
                edge_count += 1
            else:
                unresolved += 1

    resolution_rate = (edge_count / total_citations * 100) if total_citations > 0 else 0

    print(f"  [OK] Graph built:")
    print(f"    Nodes (cases):      {G.number_of_nodes():>10,}")
    print(f"    Edges (citations):  {G.number_of_edges():>10,}")
    print(f"    Total citations:    {total_citations:>10,}")
    print(f"    Resolved:           {edge_count:>10,}  ({resolution_rate:.1f}%)")
    print(f"    Unresolved:         {unresolved:>10,}  (cited cases not in dataset)")

    return G


# --- Phase 3: PageRank & Centrality -----------------------------------------

def compute_analytics(G, cases):
    """Compute PageRank, in-degree, out-degree, betweenness centrality."""
    banner("PHASE 3: Computing Network Analytics")

    # PageRank
    print("  Computing PageRank...")
    t0 = time.time()
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=100, tol=1e-06)
    print(f"    [OK] PageRank computed in {time.time()-t0:.1f}s")

    # In-degree and out-degree
    print("  Computing degree distributions...")
    in_degrees = dict(G.in_degree())
    out_degrees = dict(G.out_degree())

    # Betweenness centrality (sample for large graphs)
    print("  Computing betweenness centrality (sampled)...")
    t0 = time.time()
    n_nodes = G.number_of_nodes()
    # For large graphs, sample k nodes for approximation
    k_sample = min(500, n_nodes)
    betweenness = nx.betweenness_centrality(G, k=k_sample, normalized=True)
    print(f"    [OK] Betweenness centrality computed in {time.time()-t0:.1f}s (k={k_sample})")

    return pagerank, in_degrees, out_degrees, betweenness


# --- Phase 4: Results & Output -----------------------------------------------

def print_results(G, cases, pagerank, in_degrees, out_degrees, betweenness):
    """Print rich console tables and network statistics."""

    # -- Network Statistics --
    banner("NETWORK STATISTICS")
    n = G.number_of_nodes()
    e = G.number_of_edges()
    density = nx.density(G)
    avg_in = sum(in_degrees.values()) / n if n > 0 else 0

    # Connected components (treat as undirected for this)
    n_weakly = nx.number_weakly_connected_components(G)
    largest_wcc = max(nx.weakly_connected_components(G), key=len)
    largest_wcc_size = len(largest_wcc)

    print(f"  Total nodes (cases):             {n:>10,}")
    print(f"  Total edges (citations):         {e:>10,}")
    print(f"  Average citations per case:      {avg_in:>10.2f}")
    print(f"  Graph density:                   {density:>13.6f}")
    print(f"  Weakly connected components:     {n_weakly:>10,}")
    print(f"  Largest component size:          {largest_wcc_size:>10,}  ({largest_wcc_size/n*100:.1f}%)")

    # Isolates
    isolates = list(nx.isolates(G))
    print(f"  Isolated nodes (no citations):   {len(isolates):>10,}")

    # -- Top 50 by PageRank --
    banner("TOP 50 CASES BY PAGERANK  -  Most Authoritative Cases in Pakistani Law")

    sorted_pr = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:50]

    header = f"  {'Rank':>4}  {'Citation':<25}  {'Court':<35}  {'PageRank':>10}  {'Indeg':>5}  {'Outdeg':>5}"
    print(header)
    print("  " + "-" * len(header.strip()))

    for rank, (cit, pr_score) in enumerate(sorted_pr, 1):
        court = cases.get(cit, {}).get('court', 'Unknown')[:35]
        in_d = in_degrees.get(cit, 0)
        out_d = out_degrees.get(cit, 0)
        print(f"  {rank:>4}  {cit:<25}  {court:<35}  {pr_score:>10.6f}  {in_d:>5}  {out_d:>5}")

    # -- Top 20 Most-Cited (by raw in-degree) --
    banner("TOP 20 MOST-CITED CASES  -  By Raw Citation Count")

    sorted_in = sorted(in_degrees.items(), key=lambda x: x[1], reverse=True)[:20]

    header = f"  {'Rank':>4}  {'Citation':<25}  {'Court':<35}  {'Times Cited':>12}  {'PageRank':>10}"
    print(header)
    print("  " + "-" * len(header.strip()))

    for rank, (cit, in_d) in enumerate(sorted_in, 1):
        court = cases.get(cit, {}).get('court', 'Unknown')[:35]
        pr_score = pagerank.get(cit, 0)
        print(f"  {rank:>4}  {cit:<25}  {court:<35}  {in_d:>12,}  {pr_score:>10.6f}")

    # -- Top 20 Bridge Cases (Betweenness Centrality) --
    banner("TOP 20 BRIDGE CASES  -  Connecting Different Areas of Law")
    print("  (High betweenness centrality = bridges between legal communities)")

    sorted_bc = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:20]

    header = f"  {'Rank':>4}  {'Citation':<25}  {'Court':<35}  {'Betweenness':>12}  {'Indeg':>5}  {'Outdeg':>5}"
    print(header)
    print("  " + "-" * len(header.strip()))

    for rank, (cit, bc_score) in enumerate(sorted_bc, 1):
        court = cases.get(cit, {}).get('court', 'Unknown')[:35]
        in_d = in_degrees.get(cit, 0)
        out_d = out_degrees.get(cit, 0)
        print(f"  {rank:>4}  {cit:<25}  {court:<35}  {bc_score:>12.8f}  {in_d:>5}  {out_d:>5}")

    # -- Reporter Influence --
    banner("REPORTER INFLUENCE ANALYSIS")
    print("  Average PageRank by reporter (which law reports carry more weight?):\n")

    reporter_pr = defaultdict(list)
    for cit, pr_score in pagerank.items():
        rep = cases.get(cit, {}).get('reporter', 'Unknown')
        reporter_pr[rep].append(pr_score)

    reporter_stats = []
    for rep, scores in reporter_pr.items():
        avg = np.mean(scores)
        top = max(scores)
        reporter_stats.append((rep, len(scores), avg, top))

    reporter_stats.sort(key=lambda x: x[2], reverse=True)

    header = f"  {'Reporter':>8}  {'Cases':>7}  {'Avg PageRank':>13}  {'Max PageRank':>13}"
    print(header)
    print("  " + "-" * len(header.strip()))
    for rep, count, avg, top in reporter_stats:
        print(f"  {rep:>8}  {count:>7,}  {avg:>13.8f}  {top:>13.6f}")

    return sorted_pr, sorted_in


# --- Phase 5: Visualizations ------------------------------------------------

def create_visualizations(G, pagerank, in_degrees, sorted_pr, sorted_in, cases):
    """Generate all chart outputs."""
    banner("PHASE 5: Generating Visualizations")

    # -- 1. PageRank Distribution Histogram --
    sub_banner("PageRank Distribution Histogram")

    pr_values = list(pagerank.values())

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Linear scale
    ax = axes[0]
    ax.hist(pr_values, bins=100, color=COLORS['secondary'], edgecolor='white',
            linewidth=0.3, alpha=0.85)
    ax.set_xlabel('PageRank Score')
    ax.set_ylabel('Number of Cases')
    ax.set_title('PageRank Distribution (Linear Scale)')
    ax.axvline(x=np.mean(pr_values), color=COLORS['accent'], linestyle='--',
               linewidth=1.5, label=f'Mean: {np.mean(pr_values):.6f}')
    ax.axvline(x=np.median(pr_values), color=COLORS['gold'], linestyle='--',
               linewidth=1.5, label=f'Median: {np.median(pr_values):.6f}')
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)

    # Log scale
    ax = axes[1]
    # Filter out zeros
    pr_nonzero = [v for v in pr_values if v > 0]
    log_bins = np.logspace(np.log10(min(pr_nonzero)), np.log10(max(pr_nonzero)), 80)
    ax.hist(pr_nonzero, bins=log_bins, color=COLORS['primary'], edgecolor='white',
            linewidth=0.3, alpha=0.85)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('PageRank Score (log)')
    ax.set_ylabel('Number of Cases (log)')
    ax.set_title('PageRank Distribution (Log-Log Scale)')
    ax.grid(True, alpha=0.3, which='both')

    fig.suptitle('PageRank Distribution  -  Pakistani Case Law Network (50K+ Cases)',
                 fontsize=15, fontweight='bold', color=COLORS['text'], y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "pagerank_distribution.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close(fig)
    print(f"    [OK] Saved: {path}")

    # -- 2. Top 30 by PageRank Bar Chart --
    sub_banner("Top 30 PageRank Bar Chart")

    top30 = sorted_pr[:30]
    citations = [c for c, _ in top30]
    scores = [s for _, s in top30]

    # Shorten labels
    labels = []
    for c in citations:
        parts = c.split()
        if len(parts) >= 3:
            labels.append(f"{parts[0]} {parts[1]}\n{parts[2]}")
        else:
            labels.append(c)

    fig, ax = plt.subplots(figsize=(16, 7))

    # Color by reporter
    reporter_colors = {
        'SCMR': '#e74c3c', 'PLD': '#2e86c1', 'CLC': '#27ae60',
        'MLD': '#f39c12', 'PCrLJ': '#8e44ad', 'YLR': '#1abc9c',
        'CLD': '#e67e22', 'PLC': '#3498db', 'PTD': '#95a5a6',
        'GBLR': '#d35400',
    }
    bar_colors = []
    for c in citations:
        rep = cases.get(c, {}).get('reporter', '')
        bar_colors.append(reporter_colors.get(rep, COLORS['secondary']))

    bars = ax.barh(range(len(scores)), scores, color=bar_colors, edgecolor='white',
                   linewidth=0.5, height=0.8)

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel('PageRank Score', fontsize=12)
    ax.set_title('Top 30 Most Authoritative Cases by PageRank',
                 fontsize=14, fontweight='bold', pad=15)
    ax.grid(axis='x', alpha=0.3)

    # Legend for reporters
    legend_handles = []
    for rep, color in sorted(reporter_colors.items()):
        if any(cases.get(c, {}).get('reporter') == rep for c in citations):
            legend_handles.append(plt.Rectangle((0, 0), 1, 1, fc=color, label=rep))
    if legend_handles:
        ax.legend(handles=legend_handles, loc='lower right', fontsize=8,
                  title='Reporter', title_fontsize=9)

    plt.tight_layout()
    path = OUTPUT_DIR / "top_pagerank.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close(fig)
    print(f"    [OK] Saved: {path}")

    # -- 3. Citation Distribution (In-degree, Log-Log) --
    sub_banner("Citation Distribution (Power Law Check)")

    in_deg_values = [d for d in in_degrees.values() if d > 0]
    deg_counts = Counter(in_deg_values)
    degrees = sorted(deg_counts.keys())
    counts = [deg_counts[d] for d in degrees]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # Log-log scatter
    ax = axes[0]
    ax.scatter(degrees, counts, s=15, color=COLORS['primary'], alpha=0.6, edgecolors='none')

    # Power law fit (linear regression on log-log)
    log_deg = np.log10(np.array(degrees, dtype=float))
    log_cnt = np.log10(np.array(counts, dtype=float))
    coeffs = np.polyfit(log_deg, log_cnt, 1)
    fit_x = np.linspace(min(log_deg), max(log_deg), 100)
    fit_y = coeffs[0] * fit_x + coeffs[1]
    ax.plot(10**fit_x, 10**fit_y, color=COLORS['accent'], linewidth=2, linestyle='--',
            label=f'Power law fit: gamma = {-coeffs[0]:.2f}')

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Number of Citations (In-degree)')
    ax.set_ylabel('Number of Cases')
    ax.set_title('Citation Distribution (Log-Log Scale)')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, which='both')

    # CCDF (Complementary Cumulative Distribution)
    ax = axes[1]
    sorted_deg = sorted(in_deg_values, reverse=True)
    n_total = len(sorted_deg)
    ccdf_y = np.arange(1, n_total + 1) / n_total
    ax.plot(sorted_deg, ccdf_y, color=COLORS['secondary'], linewidth=1.5)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('In-degree (k)')
    ax.set_ylabel('P(X >= k)')
    ax.set_title('CCDF of Citation Counts')
    ax.grid(True, alpha=0.3, which='both')

    fig.suptitle('Citation Distribution  -  Power Law Analysis',
                 fontsize=15, fontweight='bold', color=COLORS['text'], y=1.02)
    plt.tight_layout()
    path = OUTPUT_DIR / "citation_distribution.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close(fig)
    print(f"    [OK] Saved: {path}")

    # -- Summary statistics for power law --
    print(f"\n    Power law exponent (gamma):  {-coeffs[0]:.3f}")
    print(f"    R2 of log-log fit:       {np.corrcoef(log_deg, log_cnt)[0,1]**2:.4f}")
    print(f"    Max citations received:  {max(in_deg_values)}")
    print(f"    Median citations:        {np.median(in_deg_values):.0f}")
    print(f"    Mean citations:          {np.mean(in_deg_values):.2f}")


# --- Main --------------------------------------------------------------------

def main():
    print("\n" + "#" * 72)
    print("  [SCALES]  THE WEB OF LAW  -  Pakistani Case Citation Network Analysis")
    print("  [CHART]  PageRank * Centrality * Power Law Distribution")
    print("#" * 72)

    start_time = time.time()

    # Phase 1: Load
    cases, citation_lookup, cases_cited_map = load_all_cases()

    # Phase 2: Build graph
    G = build_citation_graph(cases, citation_lookup, cases_cited_map)

    # Phase 3: Analytics
    pagerank, in_degrees, out_degrees, betweenness = compute_analytics(G, cases)

    # Phase 4: Print results
    sorted_pr, sorted_in = print_results(G, cases, pagerank, in_degrees, out_degrees, betweenness)

    # Phase 5: Visualizations
    create_visualizations(G, pagerank, in_degrees, sorted_pr, sorted_in, cases)

    elapsed = time.time() - start_time
    banner(f"COMPLETE  -  Total time: {elapsed:.1f}s")
    print(f"  Charts saved to: {OUTPUT_DIR}")
    print(f"  Total cases analyzed: {G.number_of_nodes():,}")
    print(f"  Total citation links: {G.number_of_edges():,}\n")


if __name__ == "__main__":
    main()
