#!/usr/bin/env python3
"""
Case Timeline Predictor for Pakistani Legal Cases
Predicts case duration based on historical data
"""

import json
import statistics
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import math

@dataclass
class DurationStats:
    """Statistics for a category of cases"""
    sample_size: int
    high_confidence_count: int
    avg_duration_days: float
    median_duration_days: float
    stddev_duration_days: float
    min_duration_days: int
    max_duration_days: int
    p10_days: float
    p25_days: float
    p50_days: float
    p75_days: float
    p90_days: float
    
    @property
    def avg_duration_years(self) -> float:
        return self.avg_duration_days / 365.25
    
    @property
    def median_duration_years(self) -> float:
        return self.median_duration_days / 365.25

@dataclass
class Prediction:
    """Prediction result with confidence intervals"""
    predicted_days: int
    predicted_years: float
    confidence_interval_low_days: int
    confidence_interval_high_days: int
    confidence_interval_low_years: float
    confidence_interval_high_years: float
    sample_size: int
    data_quality: str  # high/medium/low/insufficient
    message: str
    
    def to_human_readable(self) -> str:
        """Generate human-readable prediction message"""
        if self.data_quality == 'insufficient':
            return self.message
        
        years = self.predicted_years
        years_low = self.confidence_interval_low_years
        years_high = self.confidence_interval_high_years
        
        # Format years nicely
        def format_duration(years: float) -> str:
            if years < 1:
                months = round(years * 12)
                return f"{months} month{'s' if months != 1 else ''}"
            elif years < 2:
                return f"{years:.1f} years"
            else:
                return f"{years:.1f} years"
        
        pred_str = format_duration(years)
        low_str = format_duration(years_low)
        high_str = format_duration(years_high)
        
        quality_text = {
            'high': 'Based on substantial historical data',
            'medium': 'Based on moderate historical data',
            'low': 'Based on limited historical data'
        }.get(self.data_quality, '')
        
        return (
            f"Similar cases typically take {pred_str} to resolve.\n"
            f"50% of cases complete between {low_str} and {high_str}.\n"
            f"{quality_text} ({self.sample_size} cases analyzed)."
        )

class TimelinePredictor:
    """
    Predicts case duration based on historical data.
    Uses case_type, court level, and jurisdiction for grouping.
    """
    
    def __init__(self, data_path: Optional[str] = None):
        self.durations: List[Dict] = []
        self.stats_cache: Dict[str, DurationStats] = {}
        self.global_stats: Optional[DurationStats] = None
        
        if data_path:
            self.load_data(data_path)
    
    def load_data(self, data_path: str):
        """Load extracted duration data from JSON file"""
        path = Path(data_path)
        
        # Try to find the data file
        if path.is_file():
            with open(path, 'r', encoding='utf-8') as f:
                self.durations = json.load(f)
        elif path.is_dir():
            # Look for valid_durations.json or case_durations.json
            for fname in ['valid_durations.json', 'case_durations.json']:
                fpath = path / fname
                if fpath.exists():
                    with open(fpath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        # Filter to only include cases with duration
                        self.durations = [d for d in data if d.get('duration_days') is not None]
                    break
        
        if not self.durations:
            raise ValueError(f"No duration data found at {data_path}")
        
        # Precompute statistics
        self._compute_all_stats()
        
        print(f"Loaded {len(self.durations)} cases with duration data")
    
    def _compute_percentile(self, data: List[float], percentile: float) -> float:
        """Compute percentile of a list"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * percentile / 100
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_data[int(k)]
        return sorted_data[int(f)] * (c - k) + sorted_data[int(c)] * (k - f)
    
    def _compute_stats(self, durations: List[int], high_conf_count: int = 0) -> Optional[DurationStats]:
        """Compute statistics for a list of durations"""
        if not durations:
            return None
        
        return DurationStats(
            sample_size=len(durations),
            high_confidence_count=high_conf_count,
            avg_duration_days=statistics.mean(durations),
            median_duration_days=statistics.median(durations),
            stddev_duration_days=statistics.stdev(durations) if len(durations) > 1 else 0,
            min_duration_days=min(durations),
            max_duration_days=max(durations),
            p10_days=self._compute_percentile(durations, 10),
            p25_days=self._compute_percentile(durations, 25),
            p50_days=self._compute_percentile(durations, 50),
            p75_days=self._compute_percentile(durations, 75),
            p90_days=self._compute_percentile(durations, 90),
        )
    
    def _compute_all_stats(self):
        """Pre-compute statistics for all groupings"""
        # Group by various keys
        by_type: Dict[str, List[int]] = defaultdict(list)
        by_type_court: Dict[str, List[int]] = defaultdict(list)
        by_type_court_jur: Dict[str, List[int]] = defaultdict(list)
        by_court: Dict[str, List[int]] = defaultdict(list)
        by_jurisdiction: Dict[str, List[int]] = defaultdict(list)
        all_durations: List[int] = []
        
        high_conf_counts = defaultdict(int)
        
        for case in self.durations:
            duration = case.get('duration_days')
            if duration is None or duration < 0:
                continue
            
            case_type = case.get('case_type', 'General')
            court = case.get('court', 'High Court')
            jurisdiction = case.get('jurisdiction', 'Federal')
            confidence = case.get('extraction_confidence', 'low')
            
            # Track high confidence counts
            is_high_conf = confidence == 'high'
            
            # Key combinations
            key_type = case_type
            key_type_court = f"{case_type}|{court}"
            key_type_court_jur = f"{case_type}|{court}|{jurisdiction}"
            key_court = court
            key_jur = jurisdiction
            
            all_durations.append(duration)
            
            by_type[key_type].append(duration)
            by_type_court[key_type_court].append(duration)
            by_type_court_jur[key_type_court_jur].append(duration)
            by_court[key_court].append(duration)
            by_jurisdiction[key_jur].append(duration)
            
            if is_high_conf:
                high_conf_counts[key_type_court_jur] += 1
        
        # Compute stats for each grouping
        for key, durations in by_type.items():
            self.stats_cache[f"type:{key}"] = self._compute_stats(durations)
        
        for key, durations in by_type_court.items():
            self.stats_cache[f"type_court:{key}"] = self._compute_stats(durations)
        
        for key, durations in by_type_court_jur.items():
            hc = high_conf_counts.get(key, 0)
            self.stats_cache[f"type_court_jur:{key}"] = self._compute_stats(durations, hc)
        
        for key, durations in by_court.items():
            self.stats_cache[f"court:{key}"] = self._compute_stats(durations)
        
        for key, durations in by_jurisdiction.items():
            self.stats_cache[f"jur:{key}"] = self._compute_stats(durations)
        
        # Global stats
        self.global_stats = self._compute_stats(all_durations)
    
    def get_stats(
        self, 
        case_type: Optional[str] = None,
        court: Optional[str] = None,
        jurisdiction: Optional[str] = None
    ) -> Optional[DurationStats]:
        """Get statistics for a given combination"""
        # Try from most specific to least specific
        if case_type and court and jurisdiction:
            key = f"type_court_jur:{case_type}|{court}|{jurisdiction}"
            if key in self.stats_cache:
                return self.stats_cache[key]
        
        if case_type and court:
            key = f"type_court:{case_type}|{court}"
            if key in self.stats_cache:
                return self.stats_cache[key]
        
        if case_type:
            key = f"type:{case_type}"
            if key in self.stats_cache:
                return self.stats_cache[key]
        
        if court:
            key = f"court:{court}"
            if key in self.stats_cache:
                return self.stats_cache[key]
        
        return self.global_stats
    
    def predict(
        self,
        case_type: str,
        court: str = "High Court",
        jurisdiction: str = "Federal"
    ) -> Prediction:
        """
        Predict duration for a case.
        
        Args:
            case_type: Type of case (Constitutional, Criminal, Civil, etc.)
            court: Court level (Supreme Court, High Court, etc.)
            jurisdiction: Province/Territory (Punjab, Sindh, Federal, etc.)
        
        Returns:
            Prediction object with estimated duration and confidence interval
        """
        # Get statistics (tries most specific to least specific)
        stats = self.get_stats(case_type, court, jurisdiction)
        
        if stats is None or stats.sample_size == 0:
            return Prediction(
                predicted_days=0,
                predicted_years=0,
                confidence_interval_low_days=0,
                confidence_interval_high_days=0,
                confidence_interval_low_years=0,
                confidence_interval_high_years=0,
                sample_size=0,
                data_quality='insufficient',
                message=f"Insufficient data for {case_type} cases in {court} ({jurisdiction})"
            )
        
        # Determine data quality
        if stats.sample_size >= 50:
            data_quality = 'high'
        elif stats.sample_size >= 10:
            data_quality = 'medium'
        else:
            data_quality = 'low'
        
        # Use median as prediction (more robust than mean)
        predicted_days = int(stats.median_duration_days)
        predicted_years = round(stats.median_duration_years, 2)
        
        # Use IQR for confidence interval (25th to 75th percentile)
        ci_low = int(stats.p25_days)
        ci_high = int(stats.p75_days)
        
        return Prediction(
            predicted_days=predicted_days,
            predicted_years=predicted_years,
            confidence_interval_low_days=ci_low,
            confidence_interval_high_days=ci_high,
            confidence_interval_low_years=round(ci_low / 365.25, 2),
            confidence_interval_high_years=round(ci_high / 365.25, 2),
            sample_size=stats.sample_size,
            data_quality=data_quality,
            message=f"Based on {stats.sample_size} similar cases"
        )
    
    def get_all_stats_summary(self) -> Dict[str, Any]:
        """Get summary of all precomputed statistics"""
        summary = {
            'total_cases': len(self.durations),
            'global_stats': asdict(self.global_stats) if self.global_stats else None,
            'by_case_type': {},
            'by_court': {},
            'by_jurisdiction': {},
        }
        
        # Collect by case type
        for key, stats in self.stats_cache.items():
            if stats is None:
                continue
            
            if key.startswith('type:'):
                case_type = key[5:]
                summary['by_case_type'][case_type] = {
                    'sample_size': stats.sample_size,
                    'avg_years': round(stats.avg_duration_years, 2),
                    'median_years': round(stats.median_duration_years, 2),
                    'min_days': stats.min_duration_days,
                    'max_days': stats.max_duration_days,
                }
            
            elif key.startswith('court:'):
                court = key[6:]
                summary['by_court'][court] = {
                    'sample_size': stats.sample_size,
                    'avg_years': round(stats.avg_duration_years, 2),
                    'median_years': round(stats.median_duration_years, 2),
                }
            
            elif key.startswith('jur:'):
                jur = key[4:]
                summary['by_jurisdiction'][jur] = {
                    'sample_size': stats.sample_size,
                    'avg_years': round(stats.avg_duration_years, 2),
                    'median_years': round(stats.median_duration_years, 2),
                }
        
        return summary
    
    def get_trends(self) -> Dict[str, Any]:
        """Analyze trends over time"""
        # Group by decision year
        by_year: Dict[int, List[int]] = defaultdict(list)
        
        for case in self.durations:
            decision_date = case.get('decision_date')
            duration = case.get('duration_days')
            
            if decision_date and duration:
                try:
                    year = int(decision_date[:4])
                    if 2010 <= year <= 2025:
                        by_year[year].append(duration)
                except:
                    pass
        
        trends = {}
        prev_avg = None
        
        for year in sorted(by_year.keys()):
            durations = by_year[year]
            avg = statistics.mean(durations)
            median = statistics.median(durations)
            
            yoy_change = None
            if prev_avg:
                yoy_change = round(((avg - prev_avg) / prev_avg) * 100, 1)
            
            trends[year] = {
                'case_count': len(durations),
                'avg_days': round(avg, 0),
                'avg_years': round(avg / 365.25, 2),
                'median_days': round(median, 0),
                'yoy_change_percent': yoy_change
            }
            
            prev_avg = avg
        
        return trends

def main():
    """Main prediction workflow"""
    script_dir = Path(__file__).parent
    data_dir = script_dir / 'timeline_data'
    
    # Check if extraction has been done
    if not data_dir.exists():
        print("No timeline data found. Running extraction first...")
        from duration_extractor import main as extract_main
        extract_main()
    
    # Load predictor
    predictor = TimelinePredictor(str(data_dir))
    
    # Print overall statistics
    print("\n=== Overall Statistics ===")
    if predictor.global_stats:
        gs = predictor.global_stats
        print(f"Total cases analyzed: {gs.sample_size}")
        print(f"Average duration: {gs.avg_duration_years:.2f} years ({int(gs.avg_duration_days)} days)")
        print(f"Median duration: {gs.median_duration_years:.2f} years ({int(gs.median_duration_days)} days)")
        print(f"Range: {gs.min_duration_days} - {gs.max_duration_days} days")
    
    # Sample predictions
    print("\n=== Sample Predictions ===")
    
    test_cases = [
        ("Constitutional", "High Court", "Punjab"),
        ("Criminal", "High Court", "Sindh"),
        ("Civil", "Supreme Court", "Federal"),
        ("Family", "High Court", "Punjab"),
        ("Land Acquisition", "High Court", "Punjab"),
        ("Anti-Terrorism", "High Court", "KPK"),
    ]
    
    for case_type, court, jurisdiction in test_cases:
        pred = predictor.predict(case_type, court, jurisdiction)
        print(f"\n{case_type} case in {court} ({jurisdiction}):")
        print(pred.to_human_readable())
    
    # Get summary
    summary = predictor.get_all_stats_summary()
    
    # Save summary
    output_file = data_dir / 'prediction_stats.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\nPrediction stats saved to: {output_file}")
    
    # Get trends
    trends = predictor.get_trends()
    trends_file = data_dir / 'yearly_trends.json'
    with open(trends_file, 'w', encoding='utf-8') as f:
        json.dump(trends, f, indent=2, ensure_ascii=False)
    
    print(f"Yearly trends saved to: {trends_file}")

if __name__ == '__main__':
    main()
