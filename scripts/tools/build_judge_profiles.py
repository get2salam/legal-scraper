"""
Judge Profile Builder
======================
Processes all case files to build comprehensive judge profiles.
Outputs to JSON and optionally to PostgreSQL.
"""

import json
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Optional
import argparse

from judge_extractor import JudgeExtractor, CaseJudges, JudgeRole


@dataclass
class JudgeProfile:
    """Complete profile for a judge."""
    id: int
    name: str
    primary_court: str
    courts: list = field(default_factory=list)
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    total_cases: int = 0
    cases_as_author: int = 0
    cases_as_bench: int = 0
    is_chief_justice: bool = False
    
    # Case citations
    case_citations: list = field(default_factory=list)
    
    # Statistics
    avg_judgment_length: float = 0
    practice_areas: dict = field(default_factory=dict)
    cited_statutes: dict = field(default_factory=dict)
    co_judges: dict = field(default_factory=dict)
    cases_by_year: dict = field(default_factory=dict)
    cases_by_reporter: dict = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "primary_court": self.primary_court,
            "courts": self.courts,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "total_cases": self.total_cases,
            "cases_as_author": self.cases_as_author,
            "cases_as_bench": self.cases_as_bench,
            "is_chief_justice": self.is_chief_justice,
            "case_citations": self.case_citations[:50],  # Limit for JSON size
            "avg_judgment_length": round(self.avg_judgment_length, 1),
            "practice_areas": dict(sorted(self.practice_areas.items(), key=lambda x: -x[1])[:10]),
            "cited_statutes": dict(sorted(self.cited_statutes.items(), key=lambda x: -x[1])[:20]),
            "co_judges": dict(sorted(self.co_judges.items(), key=lambda x: -x[1])[:15]),
            "cases_by_year": self.cases_by_year,
            "cases_by_reporter": self.cases_by_reporter,
        }


class ProfileBuilder:
    """Builds judge profiles from case files."""
    
    # Common Pakistani statutes patterns
    STATUTE_PATTERNS = [
        r'Constitution of Pakistan[,\s]+Art(?:icle)?\.?\s*(\d+)',
        r'(Code of Civil Procedure|C\.?P\.?C\.?)',
        r'(Code of Criminal Procedure|Cr\.?P\.?C\.?)',
        r'(Pakistan Penal Code|P\.?P\.?C\.?)',
        r'(Qanun-e-Shahadat)',
        r'(Land Acquisition Act)',
        r'(Contract Act)',
        r'(Specific Relief Act)',
        r'(Limitation Act)',
        r'(Registration Act)',
        r'(Transfer of Property Act)',
        r'(Companies Ordinance|Companies Act)',
        r'(Income Tax Ordinance)',
        r'(NAB Ordinance)',
        r'(PEMRA Ordinance)',
        r'(Anti-Terrorism Act)',
        r'(Hudood Ordinance)',
        r'(Family Courts Act)',
        r'(Muslim Family Laws Ordinance)',
    ]
    
    # Practice area detection patterns
    PRACTICE_AREA_PATTERNS = {
        'Constitutional Law': [
            r'Constitution.*Art', r'fundamental right', r'constitutional petition',
            r'writ petition', r'Art\. 199', r'Art\. 184'
        ],
        'Criminal Law': [
            r'P\.?P\.?C\.?', r'murder', r'criminal appeal', r'FIR', r'conviction',
            r'acquittal', r'bail', r'Section 302', r'Section 497'
        ],
        'Civil Law': [
            r'civil suit', r'C\.?P\.?C\.?', r'Order.*Rule', r'decree',
            r'specific performance', r'injunction'
        ],
        'Family Law': [
            r'dissolution of marriage', r'khula', r'divorce', r'maintenance',
            r'custody', r'dower', r'Family Court', r'Muslim Family'
        ],
        'Property Law': [
            r'land.*acquisition', r'mutation', r'revenue', r'pre-emption',
            r'partition', r'Transfer of Property', r'tenancy'
        ],
        'Tax Law': [
            r'Income Tax', r'Sales Tax', r'Customs', r'tax appeal',
            r'FBR', r'Federal Board of Revenue'
        ],
        'Labor Law': [
            r'Industrial Relations', r'employment', r'worker', r'labour',
            r'reinstatement', r'service matter'
        ],
        'Service Law': [
            r'Civil Servant', r'service rules', r'pension', r'government servant',
            r'service tribunal', r'seniority'
        ],
        'Banking Law': [
            r'Banking', r'financial institution', r'loan', r'mortgage',
            r'recovery.*dues'
        ],
        'Election Law': [
            r'election', r'ballot', r'polling', r'returning officer',
            r'nomination', r'Election Commission'
        ],
    }
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.extractor = JudgeExtractor()
        self.judges: dict[str, JudgeProfile] = {}  # normalized_name -> profile
        self.case_judges: dict[str, list[str]] = {}  # citation -> list of judge names
        self.judge_id_counter = 0
        
        # Compile patterns
        self.statute_patterns = [re.compile(p, re.IGNORECASE) for p in self.STATUTE_PATTERNS]
        self.practice_patterns = {
            area: [re.compile(p, re.IGNORECASE) for p in patterns]
            for area, patterns in self.PRACTICE_AREA_PATTERNS.items()
        }
    
    def get_or_create_judge(self, name: str, court: str) -> JudgeProfile:
        """Get existing judge profile or create new one."""
        normalized = name.lower().strip()
        
        if normalized not in self.judges:
            self.judge_id_counter += 1
            self.judges[normalized] = JudgeProfile(
                id=self.judge_id_counter,
                name=name,
                primary_court=court,
                courts=[court] if court else []
            )
        
        profile = self.judges[normalized]
        
        # Update courts list
        if court and court not in profile.courts:
            profile.courts.append(court)
        
        return profile
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format."""
        if not date_str:
            return None
        
        # Clean up the date string
        date_str = date_str.strip()
        date_str = re.sub(r'\r?\n', ' ', date_str)
        
        # Common patterns
        patterns = [
            r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+),?\s+(\d{4})',  # 14th June, 2023
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',  # June 14, 2023
            r'(\d{4})-(\d{2})-(\d{2})',  # 2023-06-14
        ]
        
        months = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11', 'december': '12'
        }
        
        for pattern in patterns:
            match = re.search(pattern, date_str, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    if groups[0].isdigit() and len(groups[0]) == 4:  # YYYY-MM-DD
                        return f"{groups[0]}-{groups[1]}-{groups[2]}"
                    elif groups[2].isdigit() and len(groups[2]) == 4:
                        day = groups[0].zfill(2)
                        month = groups[1].lower()
                        year = groups[2]
                        if month in months:
                            return f"{year}-{months[month]}-{day}"
        
        return None
    
    def parse_citation(self, citation: str) -> dict:
        """Parse citation to extract reporter and year."""
        result = {'reporter': None, 'year': None, 'page': None}
        
        # Pattern: "2024 SCMR 1" or "2024 CLC 125"
        match = re.match(r'(\d{4})\s+(\w+)\s+(\d+)', citation)
        if match:
            result['year'] = int(match.group(1))
            result['reporter'] = match.group(2)
            result['page'] = int(match.group(3))
        
        return result
    
    def extract_statutes(self, text: str) -> dict:
        """Extract cited statutes from judgment text."""
        statutes = defaultdict(int)
        
        if not text:
            return statutes
        
        for pattern in self.statute_patterns:
            matches = pattern.findall(text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                # Normalize statute name
                statute = match.strip()
                if statute:
                    statutes[statute] += 1
        
        return dict(statutes)
    
    def detect_practice_area(self, text: str, headnotes: str = "") -> Optional[str]:
        """Detect the practice area from judgment text and headnotes."""
        combined = (headnotes or "") + " " + (text[:5000] if text else "")
        
        scores = {}
        for area, patterns in self.practice_patterns.items():
            score = 0
            for pattern in patterns:
                if pattern.search(combined):
                    score += 1
            if score > 0:
                scores[area] = score
        
        if scores:
            return max(scores, key=scores.get)
        return None
    
    def count_words(self, text: str) -> int:
        """Count words in judgment text."""
        if not text:
            return 0
        return len(text.split())
    
    def process_case(self, case_data: dict, file_path: Path) -> Optional[CaseJudges]:
        """Process a single case and update judge profiles."""
        citation = case_data.get('citation', '')
        if not citation:
            return None
        
        # Extract judges
        result = self.extractor.extract_from_case(case_data)
        if not result.judges:
            return None
        
        # Parse case metadata
        date_iso = self.parse_date(case_data.get('date', ''))
        citation_info = self.parse_citation(citation)
        judgment_text = case_data.get('judgment_clean', '')
        headnotes = case_data.get('headnotes', '')
        
        # Extract additional info
        word_count = self.count_words(judgment_text)
        practice_area = self.detect_practice_area(judgment_text, headnotes)
        statutes = self.extract_statutes(judgment_text + " " + headnotes)
        
        # Track judges for this case (for co-judge calculation)
        case_judge_names = []
        
        # Update each judge's profile
        for judge_info in result.judges:
            profile = self.get_or_create_judge(judge_info.name, result.court)
            
            # Update basic counts
            profile.total_cases += 1
            if judge_info.role == JudgeRole.AUTHOR:
                profile.cases_as_author += 1
            else:
                profile.cases_as_bench += 1
            
            if judge_info.title == 'CJ':
                profile.is_chief_justice = True
            
            # Update dates
            if date_iso:
                if not profile.first_seen or date_iso < profile.first_seen:
                    profile.first_seen = date_iso
                if not profile.last_seen or date_iso > profile.last_seen:
                    profile.last_seen = date_iso
            
            # Add citation
            profile.case_citations.append(citation)
            
            # Update judgment length average
            if word_count > 0:
                n = profile.total_cases
                profile.avg_judgment_length = (
                    (profile.avg_judgment_length * (n - 1) + word_count) / n
                )
            
            # Update practice areas
            if practice_area:
                profile.practice_areas[practice_area] = (
                    profile.practice_areas.get(practice_area, 0) + 1
                )
            
            # Update cited statutes
            for statute, count in statutes.items():
                profile.cited_statutes[statute] = (
                    profile.cited_statutes.get(statute, 0) + count
                )
            
            # Update cases by year
            if citation_info['year']:
                year = str(citation_info['year'])
                profile.cases_by_year[year] = profile.cases_by_year.get(year, 0) + 1
            
            # Update cases by reporter
            if citation_info['reporter']:
                reporter = citation_info['reporter']
                profile.cases_by_reporter[reporter] = (
                    profile.cases_by_reporter.get(reporter, 0) + 1
                )
            
            case_judge_names.append(judge_info.name)
        
        # Store for co-judge calculation
        self.case_judges[citation] = case_judge_names
        
        return result
    
    def calculate_co_judges(self):
        """Calculate co-judge relationships after processing all cases."""
        for citation, judge_names in self.case_judges.items():
            if len(judge_names) > 1:
                for i, name1 in enumerate(judge_names):
                    normalized1 = name1.lower().strip()
                    if normalized1 in self.judges:
                        for name2 in judge_names[i+1:]:
                            normalized2 = name2.lower().strip()
                            if normalized2 in self.judges:
                                # Update both judges
                                self.judges[normalized1].co_judges[name2] = (
                                    self.judges[normalized1].co_judges.get(name2, 0) + 1
                                )
                                self.judges[normalized2].co_judges[name1] = (
                                    self.judges[normalized2].co_judges.get(name1, 0) + 1
                                )
    
    def process_all(self, limit: Optional[int] = None) -> list[JudgeProfile]:
        """Process all case files and build profiles."""
        # Find all case JSON files
        case_files = list(self.data_dir.glob('**/2024/*.json'))
        
        if limit:
            case_files = case_files[:limit]
        
        total = len(case_files)
        print(f"Processing {total} case files...")
        
        processed = 0
        errors = 0
        
        for i, file_path in enumerate(case_files):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    case_data = json.load(f)
                
                result = self.process_case(case_data, file_path)
                if result:
                    processed += 1
                
                # Progress update
                if (i + 1) % 100 == 0:
                    print(f"  Processed {i + 1}/{total} files, {len(self.judges)} judges found")
                    
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  Error processing {file_path.name}: {e}")
        
        # Calculate co-judges
        print("Calculating co-judge relationships...")
        self.calculate_co_judges()
        
        print(f"\nCompleted: {processed} cases processed, {errors} errors")
        print(f"Found {len(self.judges)} unique judges")
        
        return list(self.judges.values())
    
    def get_top_judges(self, n: int = 50) -> list[JudgeProfile]:
        """Get top N judges by case count."""
        sorted_judges = sorted(
            self.judges.values(),
            key=lambda j: j.total_cases,
            reverse=True
        )
        return sorted_judges[:n]


def main():
    parser = argparse.ArgumentParser(description='Build judge profiles from case files')
    parser.add_argument('--limit', type=int, help='Limit number of cases to process')
    parser.add_argument('--output', type=str, default='judge_profiles.json',
                       help='Output JSON file path')
    parser.add_argument('--top', type=int, default=50,
                       help='Number of top judges to include in output')
    args = parser.parse_args()
    
    data_dir = Path(__file__).parent / 'data_v2'
    
    builder = ProfileBuilder(data_dir)
    profiles = builder.process_all(limit=args.limit)
    
    # Get top judges for output
    top_judges = builder.get_top_judges(args.top)
    
    # Summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    
    total_cases = sum(j.total_cases for j in profiles)
    courts = set(j.primary_court for j in profiles if j.primary_court)
    
    print(f"Total judges: {len(profiles)}")
    print(f"Total case-judge associations: {total_cases}")
    print(f"Courts represented: {len(courts)}")
    print(f"  {', '.join(sorted(courts))}")
    
    print(f"\nTop 10 Most Active Judges:")
    for i, judge in enumerate(top_judges[:10], 1):
        print(f"  {i}. {judge.name} ({judge.primary_court}) - {judge.total_cases} cases")
    
    # Practice area distribution
    all_areas = defaultdict(int)
    for judge in profiles:
        for area, count in judge.practice_areas.items():
            all_areas[area] += count
    
    print(f"\nPractice Area Distribution:")
    for area, count in sorted(all_areas.items(), key=lambda x: -x[1])[:10]:
        print(f"  {area}: {count} cases")
    
    # Save output
    output_path = Path(__file__).parent / args.output
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "total_judges": len(profiles),
        "total_cases_processed": len(builder.case_judges),
        "summary": {
            "courts": list(courts),
            "practice_areas": dict(all_areas)
        },
        "top_judges": [j.to_dict() for j in top_judges]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved {len(top_judges)} judge profiles to {output_path}")
    
    # Also save full profiles
    full_output_path = Path(__file__).parent / 'judge_profiles_full.json'
    full_data = {
        "generated_at": datetime.now().isoformat(),
        "total_judges": len(profiles),
        "judges": [j.to_dict() for j in sorted(profiles, key=lambda x: -x.total_cases)]
    }
    
    with open(full_output_path, 'w', encoding='utf-8') as f:
        json.dump(full_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved all {len(profiles)} judge profiles to {full_output_path}")


if __name__ == '__main__':
    main()
