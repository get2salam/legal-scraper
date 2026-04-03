#!/usr/bin/env python3
"""
Court Hierarchy - Script 4/5
Defines Pakistani court hierarchy for citation analysis.
SC > all HCs > lower courts
FSC parallel to HCs for Shariat matters
HCs are peers (cannot overrule each other)
"""

# Hierarchy weights: higher = more authority
HIERARCHY = {
    'SC': 100, 'Supreme Court': 100, 'Supreme Court of Pakistan': 100,
    'FSC': 70, 'Federal Shariat Court': 70,
    'Lahore': 60, 'Karachi': 60, 'Peshawar': 60, 'Quetta': 60, 'Islamabad': 60,
    'Sindh': 60, 'Balochistan': 60, 'KPK': 60,
    'AJK': 55, 'AJ&K': 55,
    'GBLR': 50,
    'Tribunal': 40,
    'District Court': 30, 'Sessions Court': 30,
}

def get_hierarchy_weight(court):
    """Get hierarchy weight for a court (100=SC, 60=HC, 30=lower, 10=unknown)."""
    if not court:
        return 10
    c = court.strip()
    if c in HIERARCHY:
        return HIERARCHY[c]
    cl = c.lower()
    if 'supreme' in cl:
        return 100
    if 'shariat' in cl or 'fsc' in cl:
        return 70
    if any(x in cl for x in ['lahore', 'karachi', 'peshawar', 'quetta', 'islamabad', 'sindh', 'balochistan', 'high court']):
        return 60
    if 'ajk' in cl or 'aj&k' in cl:
        return 55
    if 'tribunal' in cl:
        return 40
    if any(x in cl for x in ['district', 'sessions', 'civil', 'magistrate']):
        return 30
    return 10

def can_overrule(court_a, court_b):
    """True if court_a can overrule court_b."""
    wa = get_hierarchy_weight(court_a)
    wb = get_hierarchy_weight(court_b)
    if wa == wb:
        return False
    return wa > wb

def is_supreme_court(court):
    """Check if court is Supreme Court."""
    return get_hierarchy_weight(court) >= 100

def is_high_court(court):
    """Check if court is a High Court."""
    return get_hierarchy_weight(court) == 60

def get_court_from_citation(citation):
    """Infer court from citation string."""
    if not citation:
        return None
    parts = citation.split()
    if parts[0] == 'PLD' and len(parts) >= 4:
        return parts[2]
    if len(parts) >= 2 and parts[1] == 'SCMR':
        return 'SC'
    if len(parts) >= 2 and parts[1] == 'GBLR':
        return 'GBLR'
    return None

if __name__ == '__main__':
    print("Court Hierarchy - Self Test")
    print("=" * 50)
    tests = [('SC', 100), ('Lahore', 60), ('FSC', 70), ('AJK', 55), ('Unknown', 10)]
    for court, expected in tests:
        w = get_hierarchy_weight(court)
        ok = "OK" if w == expected else f"FAIL (got {w})"
        print(f"  {court:20s} weight={w:3d} [{ok}]")
    
    pairs = [('SC', 'Lahore', True), ('Lahore', 'Karachi', False), ('Lahore', 'District Court', True)]
    for a, b, expected in pairs:
        result = can_overrule(a, b)
        ok = "OK" if result == expected else "FAIL"
        print(f"  can_overrule({a}, {b}) = {result} [{ok}]")
    print("=" * 50)
