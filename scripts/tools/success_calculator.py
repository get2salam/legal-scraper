#!/usr/bin/env python3
"""
Success Rate Calculator for Pakistani Legal Research Platform
Calculates success rates for specific legal provisions and arguments.

Features:
- Group cases by statutory provision cited
- Calculate win/loss ratios by provision
- Track success by court level
- Historical trend analysis
- Provide actionable insights
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from collections import defaultdict
from datetime import datetime
import statistics


@dataclass
class ProvisionStats:
    """Statistics for a single legal provision."""
    provision: str
    total_cases: int = 0
    allowed: int = 0
    dismissed: int = 0
    partially_allowed: int = 0
    remanded: int = 0
    other: int = 0
    success_rate: float = 0.0
    citations: List[str] = field(default_factory=list)
    
    # By court level
    supreme_court: Dict = field(default_factory=lambda: {'allowed': 0, 'dismissed': 0, 'total': 0})
    high_court: Dict = field(default_factory=lambda: {'allowed': 0, 'dismissed': 0, 'total': 0})
    other_court: Dict = field(default_factory=lambda: {'allowed': 0, 'dismissed': 0, 'total': 0})
    
    # By year
    by_year: Dict[int, Dict] = field(default_factory=dict)
    
    def calculate_success_rate(self):
        """Calculate the success rate."""
        decided = self.allowed + self.dismissed + self.partially_allowed
        if decided > 0:
            # Count allowed fully, partial as 0.5
            success = self.allowed + (self.partially_allowed * 0.5)
            self.success_rate = success / decided
        else:
            self.success_rate = 0.0


@dataclass
class SuccessAnalysis:
    """Complete success analysis for a query."""
    provisions: Dict[str, ProvisionStats]
    overall_success_rate: float
    total_cases_analyzed: int
    strongest_arguments: List[Tuple[str, float]]
    weakest_arguments: List[Tuple[str, float]]
    court_recommendations: Dict[str, str]
    trend_analysis: Dict[str, str]


class SuccessCalculator:
    """
    Calculates success rates for legal provisions.
    """
    
    def __init__(self):
        self.provision_stats: Dict[str, ProvisionStats] = defaultdict(ProvisionStats)
        self.cases_loaded: int = 0
        
    def _get_court_level(self, court: str) -> str:
        """Classify court into hierarchy level."""
        if not court:
            return 'other_court'  # Changed from 'other' to match dataclass field name
        court_lower = court.lower()
        
        if 'supreme' in court_lower or court_lower in ['sc', 'scmr']:
            return 'supreme_court'
        elif 'high' in court_lower or court_lower in ['lahore', 'sindh', 'peshawar', 'balochistan', 'islamabad']:
            return 'high_court'
        else:
            return 'other_court'
    
    def _extract_year(self, citation: str, date: str) -> Optional[int]:
        """Extract year from citation or date."""
        # Try citation first
        match = re.search(r'(\d{4})', citation)
        if match:
            year = int(match.group(1))
            if 1947 <= year <= 2030:
                return year
        
        # Try date
        if date:
            match = re.search(r'(\d{4})', date)
            if match:
                year = int(match.group(1))
                if 1947 <= year <= 2030:
                    return year
        
        return None
    
    def _normalize_provision(self, provision: str) -> str:
        """Normalize provision reference for grouping."""
        # Clean up
        provision = provision.strip().lower()
        
        # Remove common variations
        provision = re.sub(r'\s+', ' ', provision)
        provision = re.sub(r'[,.]$', '', provision)
        
        # Only standardize if not already starting with 'section', 'article', etc.
        if provision.startswith(('s.', 's ', 'sec.', 'sec ')) and not provision.startswith('section'):
            provision = re.sub(r'^s\.?\s*', 'section ', provision)
            provision = re.sub(r'^sec\.?\s*', 'section ', provision)
        
        if provision.startswith(('art.', 'art ')) and not provision.startswith('article'):
            provision = re.sub(r'^art\.?\s*', 'article ', provision)
        
        # Standardize order/rule references
        if provision.startswith(('o.', 'o ')) and not provision.startswith('order'):
            provision = re.sub(r'^o\.?\s*', 'order ', provision)
        if provision.startswith(('r.', 'r ')) and not provision.startswith('rule'):
            provision = re.sub(r'^r\.?\s*', 'rule ', provision)
        
        return provision.title()
    
    def load_cases(self, jsonl_path: Path) -> int:
        """
        Load cases and build provision statistics.
        
        Args:
            jsonl_path: Path to JSONL file with classified cases
            
        Returns:
            Number of cases loaded
        """
        count = 0
        
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                case = json.loads(line)
                self._process_case(case)
                count += 1
        
        # Calculate all success rates
        for stats in self.provision_stats.values():
            stats.calculate_success_rate()
        
        self.cases_loaded = count
        return count
    
    def _process_case(self, case: Dict) -> None:
        """Process a single case and update statistics."""
        citation = case.get('citation', '')
        outcome = case.get('outcome', 'unknown').lower()
        court = case.get('court', '')
        date = case.get('date', '')
        
        # Get provision keys from claims extraction
        provision_keys = case.get('provision_keys', [])
        
        # Also try to extract from claims dict
        if not provision_keys and 'claims' in case:
            claims = case['claims']
            for ref in claims.get('statutory_references', []):
                key = f"Section {ref['section']} {ref.get('normalized_act', ref.get('act', ''))}"
                provision_keys.append(key)
            for art in claims.get('constitutional_articles', []):
                key = f"Article {art['article']} Constitution"
                provision_keys.append(key)
        
        # Fallback: extract provisions from headnotes
        if not provision_keys:
            provision_keys = self._extract_provisions_from_text(
                case.get('headnotes', '') + ' ' + case.get('title', '')
            )
        
        # Get metadata
        court_level = self._get_court_level(court)
        year = self._extract_year(citation, date)
        
        # Update stats for each provision
        for provision in provision_keys:
            provision = self._normalize_provision(provision)
            
            if provision not in self.provision_stats:
                self.provision_stats[provision] = ProvisionStats(provision=provision)
            
            stats = self.provision_stats[provision]
            stats.total_cases += 1
            stats.citations.append(citation)
            
            # Update outcome counts
            if outcome == 'allowed':
                stats.allowed += 1
            elif outcome == 'dismissed':
                stats.dismissed += 1
            elif outcome == 'partially_allowed':
                stats.partially_allowed += 1
            elif outcome == 'remanded':
                stats.remanded += 1
            else:
                stats.other += 1
            
            # Update court-level stats
            court_stats = getattr(stats, court_level, stats.other_court)
            court_stats['total'] += 1
            if outcome == 'allowed':
                court_stats['allowed'] += 1
            elif outcome == 'dismissed':
                court_stats['dismissed'] += 1
            
            # Update yearly stats
            if year:
                if year not in stats.by_year:
                    stats.by_year[year] = {'allowed': 0, 'dismissed': 0, 'total': 0}
                stats.by_year[year]['total'] += 1
                if outcome == 'allowed':
                    stats.by_year[year]['allowed'] += 1
                elif outcome == 'dismissed':
                    stats.by_year[year]['dismissed'] += 1
    
    def _extract_provisions_from_text(self, text: str) -> List[str]:
        """Extract provision references from text (fallback method)."""
        provisions = []
        
        # Section patterns
        section_pattern = re.compile(
            r'(?:section|s\.?|sec\.?)\s*(\d+[\w/-]*)\s*(?:of\s+(?:the\s+)?)?([A-Za-z\s,]+(?:Act|Ordinance|Code))',
            re.IGNORECASE
        )
        
        for match in section_pattern.finditer(text):
            section = match.group(1)
            act = match.group(2).strip()
            provisions.append(f"Section {section} {act}")
        
        # Article patterns
        article_pattern = re.compile(
            r'(?:article|art\.?)\s*(\d+[\w/-]*)\s*(?:of\s+(?:the\s+)?)?constitution',
            re.IGNORECASE
        )
        
        for match in article_pattern.finditer(text):
            article = match.group(1)
            provisions.append(f"Article {article} Constitution")
        
        return provisions
    
    def get_success_rate(self, provision: str) -> Optional[ProvisionStats]:
        """
        Get success rate for a specific provision.
        
        Args:
            provision: The provision to look up
            
        Returns:
            ProvisionStats or None if not found
        """
        provision = self._normalize_provision(provision)
        
        if provision in self.provision_stats:
            return self.provision_stats[provision]
        
        # Try partial match
        for key, stats in self.provision_stats.items():
            if provision in key or key in provision:
                return stats
        
        return None
    
    def analyze_provisions(self, 
                          provisions: List[str],
                          min_sample_size: int = 5) -> SuccessAnalysis:
        """
        Analyze success rates for multiple provisions.
        
        Args:
            provisions: List of provisions to analyze
            min_sample_size: Minimum cases required for reliable stats
            
        Returns:
            SuccessAnalysis with comprehensive statistics
        """
        results = {}
        total_cases = 0
        total_success = 0
        
        for provision in provisions:
            stats = self.get_success_rate(provision)
            if stats:
                results[provision] = stats
                total_cases += stats.total_cases
                total_success += stats.allowed + (stats.partially_allowed * 0.5)
        
        # Calculate overall success rate
        overall_rate = total_success / total_cases if total_cases > 0 else 0.0
        
        # Find strongest and weakest arguments
        rated_provisions = [
            (p, s.success_rate) 
            for p, s in results.items() 
            if s.total_cases >= min_sample_size
        ]
        
        rated_provisions.sort(key=lambda x: -x[1])
        strongest = rated_provisions[:5]
        weakest = list(reversed(rated_provisions[-5:])) if len(rated_provisions) >= 5 else []
        
        # Court recommendations
        court_recs = self._generate_court_recommendations(results)
        
        # Trend analysis
        trends = self._analyze_trends(results)
        
        return SuccessAnalysis(
            provisions=results,
            overall_success_rate=overall_rate,
            total_cases_analyzed=total_cases,
            strongest_arguments=strongest,
            weakest_arguments=weakest,
            court_recommendations=court_recs,
            trend_analysis=trends,
        )
    
    def _generate_court_recommendations(self, 
                                        results: Dict[str, ProvisionStats]) -> Dict[str, str]:
        """Generate recommendations based on court-level success rates."""
        recommendations = {}
        
        for provision, stats in results.items():
            sc_rate = self._calc_court_rate(stats.supreme_court)
            hc_rate = self._calc_court_rate(stats.high_court)
            
            if sc_rate > hc_rate + 0.1 and stats.supreme_court['total'] >= 3:
                recommendations[provision] = f"Higher success at Supreme Court ({sc_rate*100:.0f}% vs {hc_rate*100:.0f}% at HC)"
            elif hc_rate > sc_rate + 0.1 and stats.high_court['total'] >= 3:
                recommendations[provision] = f"Higher success at High Court ({hc_rate*100:.0f}% vs {sc_rate*100:.0f}% at SC)"
        
        return recommendations
    
    def _calc_court_rate(self, court_stats: Dict) -> float:
        """Calculate success rate for a court level."""
        total = court_stats.get('total', 0)
        if total == 0:
            return 0.0
        allowed = court_stats.get('allowed', 0)
        return allowed / total
    
    def _analyze_trends(self, results: Dict[str, ProvisionStats]) -> Dict[str, str]:
        """Analyze temporal trends in success rates."""
        trends = {}
        
        for provision, stats in results.items():
            if len(stats.by_year) < 3:
                continue
            
            years = sorted(stats.by_year.keys())
            if len(years) < 3:
                continue
            
            # Calculate rates for recent years vs older
            recent_years = years[-3:]
            older_years = years[:-3] if len(years) > 3 else years[:2]
            
            recent_rate = self._calc_period_rate(stats.by_year, recent_years)
            older_rate = self._calc_period_rate(stats.by_year, older_years)
            
            if recent_rate > older_rate + 0.1:
                trends[provision] = f"Improving trend: {older_rate*100:.0f}% → {recent_rate*100:.0f}%"
            elif recent_rate < older_rate - 0.1:
                trends[provision] = f"Declining trend: {older_rate*100:.0f}% → {recent_rate*100:.0f}%"
        
        return trends
    
    def _calc_period_rate(self, by_year: Dict, years: List[int]) -> float:
        """Calculate success rate for a period."""
        total = sum(by_year.get(y, {}).get('total', 0) for y in years)
        if total == 0:
            return 0.0
        allowed = sum(by_year.get(y, {}).get('allowed', 0) for y in years)
        return allowed / total
    
    def get_top_provisions(self, 
                          n: int = 20, 
                          min_cases: int = 5,
                          sort_by: str = 'success_rate') -> List[ProvisionStats]:
        """
        Get top provisions by success rate or frequency.
        
        Args:
            n: Number of provisions to return
            min_cases: Minimum cases for inclusion
            sort_by: 'success_rate' or 'total_cases'
            
        Returns:
            List of top provisions
        """
        eligible = [
            s for s in self.provision_stats.values()
            if s.total_cases >= min_cases
        ]
        
        if sort_by == 'success_rate':
            eligible.sort(key=lambda x: -x.success_rate)
        else:
            eligible.sort(key=lambda x: -x.total_cases)
        
        return eligible[:n]
    
    def export_stats(self, output_path: Path) -> None:
        """Export all statistics to JSON."""
        export_data = {
            'total_cases_analyzed': self.cases_loaded,
            'total_provisions_tracked': len(self.provision_stats),
            'provisions': {}
        }
        
        for provision, stats in self.provision_stats.items():
            export_data['provisions'][provision] = {
                'total_cases': stats.total_cases,
                'allowed': stats.allowed,
                'dismissed': stats.dismissed,
                'partially_allowed': stats.partially_allowed,
                'remanded': stats.remanded,
                'success_rate': stats.success_rate,
                'supreme_court': stats.supreme_court,
                'high_court': stats.high_court,
                'by_year': {str(k): v for k, v in stats.by_year.items()},
            }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported stats to {output_path}")


def build_success_database(jsonl_path: Path, 
                           output_path: Path) -> SuccessCalculator:
    """
    Build the success rate database from cases.
    
    Args:
        jsonl_path: Path to classified cases JSONL
        output_path: Path for exported statistics
        
    Returns:
        Populated SuccessCalculator
    """
    calculator = SuccessCalculator()
    
    print(f"Loading cases from {jsonl_path}...")
    count = calculator.load_cases(jsonl_path)
    print(f"Loaded {count} cases, tracking {len(calculator.provision_stats)} provisions")
    
    calculator.export_stats(output_path)
    
    return calculator


def main():
    """CLI interface for success rate calculation."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate success rates for legal provisions')
    parser.add_argument('--input', '-i', type=Path, default=Path('data_v2/cases_classified.jsonl'),
                       help='Input JSONL file with classified cases')
    parser.add_argument('--output', '-o', type=Path, default=Path('data_v2/provision_stats.json'),
                       help='Output JSON file for statistics')
    parser.add_argument('--provision', '-p', type=str,
                       help='Query specific provision')
    parser.add_argument('--top', '-t', type=int, default=20,
                       help='Show top N provisions')
    parser.add_argument('--min-cases', '-m', type=int, default=5,
                       help='Minimum cases for statistics')
    
    args = parser.parse_args()
    
    # Build or load calculator
    calculator = SuccessCalculator()
    
    print(f"Loading cases from {args.input}...")
    count = calculator.load_cases(args.input)
    print(f"Loaded {count} cases, tracking {len(calculator.provision_stats)} provisions\n")
    
    if args.provision:
        # Query specific provision
        stats = calculator.get_success_rate(args.provision)
        if stats:
            print(f"\n{'='*60}")
            print(f"Provision: {stats.provision}")
            print(f"Total Cases: {stats.total_cases}")
            print(f"Success Rate: {stats.success_rate*100:.1f}%")
            print(f"\nOutcomes:")
            print(f"  Allowed: {stats.allowed}")
            print(f"  Dismissed: {stats.dismissed}")
            print(f"  Partially Allowed: {stats.partially_allowed}")
            print(f"  Remanded: {stats.remanded}")
            print(f"\nBy Court Level:")
            print(f"  Supreme Court: {stats.supreme_court['allowed']}/{stats.supreme_court['total']} allowed")
            print(f"  High Court: {stats.high_court['allowed']}/{stats.high_court['total']} allowed")
            
            if stats.by_year:
                print(f"\nBy Year:")
                for year in sorted(stats.by_year.keys())[-5:]:
                    data = stats.by_year[year]
                    rate = data['allowed']/data['total'] if data['total'] > 0 else 0
                    print(f"  {year}: {data['allowed']}/{data['total']} ({rate*100:.0f}%)")
        else:
            print(f"No data found for provision: {args.provision}")
    
    else:
        # Show top provisions
        print(f"\n--- Top {args.top} Provisions by Success Rate ---")
        print(f"(minimum {args.min_cases} cases)\n")
        
        top_provisions = calculator.get_top_provisions(
            n=args.top, 
            min_cases=args.min_cases,
            sort_by='success_rate'
        )
        
        for i, stats in enumerate(top_provisions, 1):
            print(f"{i:2}. {stats.provision}")
            print(f"    Success Rate: {stats.success_rate*100:.1f}% ({stats.allowed}/{stats.total_cases} allowed)")
            print()
        
        # Export stats
        calculator.export_stats(args.output)
        print(f"\nStatistics exported to {args.output}")


if __name__ == '__main__':
    main()
