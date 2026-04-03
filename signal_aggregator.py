#!/usr/bin/env python3
"""
Signal Aggregator - Script 5/5
For each case, compute overall signal from all treatments it received.
Rules:
- If ANY SC case marks it RED -> case is RED
- Weight by court hierarchy and recency
- Most recent authoritative treatment wins ties
Saves to data_v2/analytics/case_signals.json
"""

import json
import time
import sys
from pathlib import Path

# Import court hierarchy
sys.path.insert(0, str(Path(__file__).parent))
from court_hierarchy import get_hierarchy_weight, is_supreme_court

BASE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
ANALYTICS_DIR = BASE_DIR / "analytics"
TREATMENT_FILE = ANALYTICS_DIR / "treatment_signals.json"
OUTPUT_FILE = ANALYTICS_DIR / "case_signals.json"

def is_sc_citation(citation):
    """Check if a citation is from Supreme Court."""
    parts = citation.split()
    if len(parts) >= 2 and parts[1] == 'SCMR':
        return True
    if parts[0] == 'PLD' and len(parts) >= 3 and parts[2] == 'SC':
        return True
    return False

def compute_treatment_score(treatment, current_year=2025):
    """Compute weighted score for a single treatment."""
    signal = treatment.get('signal', 'NEUTRAL')
    citing_court = treatment.get('citing_court', '')
    citing_year = treatment.get('citing_year', 0)
    
    signal_weights = {'RED': 10, 'YELLOW': 5, 'GREEN': 3, 'NEUTRAL': 1}
    base = signal_weights.get(signal, 1)
    
    # Court weight (0.1 to 1.0)
    court_w = get_hierarchy_weight(citing_court) / 100.0
    if court_w < 0.1:
        court_w = 0.1
    
    # Also check if citing case citation implies SC
    citing_case = treatment.get('citing_case', '')
    if court_w < 1.0 and is_sc_citation(citing_case):
        court_w = 1.0
    
    # Recency weight
    if citing_year and citing_year > 1947:
        years_ago = max(0, current_year - citing_year)
        recency = max(0.3, 1.0 - (years_ago * 0.02))
    else:
        recency = 0.5
    
    return base * court_w * recency

def aggregate_signal(treatments):
    """Aggregate all treatments for a case into overall signal."""
    if not treatments:
        return {
            'overall_signal': 'NEUTRAL',
            'confidence': 0,
            'treatment_count': 0,
            'breakdown': {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
        }
    
    counts = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
    for t in treatments:
        s = t.get('signal', 'NEUTRAL')
        counts[s] = counts.get(s, 0) + 1
    
    # Rule 1: SC RED overrides everything
    for t in treatments:
        if t.get('signal') == 'RED':
            citing_court = t.get('citing_court', '')
            citing_case = t.get('citing_case', '')
            if is_supreme_court(citing_court) or is_sc_citation(citing_case):
                # Find most recent treatment
                best_yr = 0
                best_case = None
                for tt in treatments:
                    yr = tt.get('citing_year', 0)
                    if yr and yr > best_yr:
                        best_yr = yr
                        best_case = tt.get('citing_case')
                return {
                    'overall_signal': 'RED',
                    'confidence': 1.0,
                    'treatment_count': len(treatments),
                    'breakdown': counts,
                    'reason': 'SC_RED',
                    'most_recent': best_case
                }
    
    # Rule 2: Weight by court hierarchy and recency
    weighted = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
    for t in treatments:
        s = t.get('signal', 'NEUTRAL')
        weighted[s] += compute_treatment_score(t)
    
    # Determine winner (exclude NEUTRAL from competition)
    active = {k: v for k, v in weighted.items() if k != 'NEUTRAL' and v > 0}
    
    if not active:
        overall = 'NEUTRAL'
    else:
        overall = max(active, key=active.get)
    
    # RED from any court still significant
    if weighted['RED'] > 0 and counts['RED'] > 0:
        if weighted['RED'] >= weighted.get('GREEN', 0) * 0.5:
            overall = 'RED'
    
    # Confidence
    total_w = sum(weighted.values())
    confidence = weighted.get(overall, 0) / total_w if total_w > 0 and overall != 'NEUTRAL' else 0
    
    # Most recent
    best_yr = 0
    best_case = None
    for t in treatments:
        yr = t.get('citing_year', 0)
        if yr and yr > best_yr:
            best_yr = yr
            best_case = t.get('citing_case')
    
    return {
        'overall_signal': overall,
        'confidence': round(confidence, 3),
        'treatment_count': len(treatments),
        'breakdown': counts,
        'most_recent': best_case
    }

def main():
    print("=" * 60)
    print("SIGNAL AGGREGATOR - Computing Overall Case Signals")
    print("=" * 60)
    
    print(f"Loading treatment signals...")
    with open(TREATMENT_FILE, 'r', encoding='utf-8') as f:
        treatment_signals = json.load(f)
    print(f"  {len(treatment_signals):,} cited cases loaded")
    
    case_signals = {}
    overall_counts = {'RED': 0, 'YELLOW': 0, 'GREEN': 0, 'NEUTRAL': 0}
    processed = 0
    start_time = time.time()
    
    for cited_case, treatments in treatment_signals.items():
        processed += 1
        
        if processed % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"  [{processed:>7,}/{len(treatment_signals):,}] "
                  f"R:{overall_counts['RED']} Y:{overall_counts['YELLOW']} "
                  f"G:{overall_counts['GREEN']} N:{overall_counts['NEUTRAL']}")
        
        result = aggregate_signal(treatments)
        case_signals[cited_case] = result
        overall_counts[result['overall_signal']] += 1
    
    elapsed = time.time() - start_time
    
    print(f"\nSaving case signals ({len(case_signals):,} entries)...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(case_signals, f, ensure_ascii=False)
    
    avg_treatments = sum(v['treatment_count'] for v in case_signals.values()) / max(len(case_signals), 1)
    avg_conf = sum(v['confidence'] for v in case_signals.values()) / max(len(case_signals), 1)
    sc_red = sum(1 for v in case_signals.values() if v.get('reason') == 'SC_RED')
    
    print("\n" + "=" * 60)
    print("SIGNAL AGGREGATOR - SUMMARY")
    print("=" * 60)
    print(f"Cases with signals:        {len(case_signals):,}")
    print(f"Overall breakdown:")
    print(f"  RED (bad law):           {overall_counts['RED']:,}")
    print(f"  YELLOW (cautionary):     {overall_counts['YELLOW']:,}")
    print(f"  GREEN (good law):        {overall_counts['GREEN']:,}")
    print(f"  NEUTRAL:                 {overall_counts['NEUTRAL']:,}")
    print(f"SC RED overrides:          {sc_red:,}")
    print(f"Avg treatments/case:       {avg_treatments:.1f}")
    print(f"Avg confidence:            {avg_conf:.3f}")
    print(f"Time:                      {elapsed:.1f}s")
    print("=" * 60)

if __name__ == '__main__':
    main()
