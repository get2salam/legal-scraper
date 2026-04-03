"""
Legal Issue Extractor for Pakistani Court Cases

Extracts and normalizes legal issues from judgments:
1. Pattern-based extraction (statutes, sections, legal principles)
2. Headnote analysis (pre-summarized legal points)
3. Semantic normalization for comparison across jurisdictions
"""

import re
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import hashlib


@dataclass
class LegalIssue:
    """Represents an extracted legal issue"""
    id: str
    raw_text: str
    normalized_text: str
    category: str  # e.g., 'limitation', 'contract', 'property', 'criminal', etc.
    statutes: List[str] = field(default_factory=list)
    sections: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    confidence: float = 0.0
    source: str = ''  # 'headnote', 'judgment', 'combined'


@dataclass
class ExtractionResult:
    """Result of issue extraction from a case"""
    citation: str
    jurisdiction: str
    issues: List[LegalIssue]
    holding: str  # Main conclusion/ruling
    statutes_cited: List[str]
    cases_cited: List[str]


class LegalIssueExtractor:
    """
    Extract and normalize legal issues from Pakistani court judgments.
    
    Strategies:
    1. Headnote parsing (already summarized in data)
    2. Statute/section pattern matching
    3. Legal principle identification
    4. Keyword-based categorization
    """
    
    # Major Pakistani statutes with abbreviations
    STATUTE_PATTERNS = {
        'Constitution of Pakistan': r'(?:Constitution|Art(?:icle)?\.?\s*\d+)',
        'Contract Act': r'Contract\s+Act.*?(?:1872)?',
        'Limitation Act': r'Limitation\s+Act.*?(?:1908)?',
        'Civil Procedure Code': r'(?:C\.?P\.?C\.?|Civil\s+Procedure\s+Code|Code\s+of\s+Civil\s+Procedure)',
        'Criminal Procedure Code': r'(?:Cr\.?P\.?C\.?|Criminal\s+Procedure\s+Code)',
        'Pakistan Penal Code': r'(?:P\.?P\.?C\.?|Pakistan\s+Penal\s+Code|Penal\s+Code)',
        'Evidence Act': r'(?:Qanun-?e-?Shahadat|Evidence\s+Act|Art(?:icle)?\.?\s*\d+.*?Shahadat)',
        'Land Acquisition Act': r'Land\s+Acquisition\s+Act.*?(?:1894)?',
        'Transfer of Property Act': r'Transfer\s+of\s+Property\s+Act.*?(?:1882)?',
        'Specific Relief Act': r'Specific\s+Relief\s+Act.*?(?:1877)?',
        'Income Tax Ordinance': r'Income\s+Tax\s+(?:Ordinance|Act)',
        'Companies Act': r'Companies\s+(?:Act|Ordinance)',
        'West Pakistan Land Revenue Act': r'(?:West\s+)?Pakistan\s+Land\s+Revenue\s+Act',
        'Registration Act': r'Registration\s+Act.*?(?:1908)?',
        'Stamp Act': r'Stamp\s+Act.*?(?:1899)?',
    }
    
    # Section/article patterns
    SECTION_PATTERN = re.compile(
        r'(?:Sections?|Ss?\.?|Art(?:icle)?s?\.?|Order|Rule|O\.?\s*[IVXLCDM]+|R\.?\s*\d+)\s*'
        r'[\d,\s\-/&and]+(?:\([a-z0-9]+\))?',
        re.IGNORECASE
    )
    
    # Legal issue categories and their keywords
    ISSUE_CATEGORIES = {
        'limitation': [
            'limitation', 'time bar', 'period of limitation', 'limitation act',
            'limitation period', 'barred by time', 'condonation of delay'
        ],
        'contract': [
            'contract', 'breach of contract', 'agreement', 'consideration',
            'specific performance', 'damages', 'privity', 'offer and acceptance'
        ],
        'property': [
            'property', 'land', 'title', 'possession', 'mutation', 'partition',
            'pre-emption', 'easement', 'mortgage', 'lease', 'tenancy'
        ],
        'constitutional': [
            'constitutional', 'fundamental right', 'article 10', 'article 25',
            'writ petition', 'judicial review', 'vires', 'ultra vires'
        ],
        'criminal': [
            'murder', 'qatl', 'robbery', 'theft', 'assault', 'ppc', 'bail',
            'sentence', 'conviction', 'acquittal', 'criminal'
        ],
        'family': [
            'divorce', 'khula', 'talaq', 'maintenance', 'custody', 'dower',
            'mehr', 'nikah', 'marriage', 'dissolution'
        ],
        'tax': [
            'tax', 'income tax', 'sales tax', 'customs', 'duty', 'assessment',
            'return', 'refund', 'exemption'
        ],
        'service': [
            'service', 'employment', 'termination', 'pension', 'seniority',
            'promotion', 'disciplinary', 'civil servant'
        ],
        'compensation': [
            'compensation', 'damages', 'land acquisition', 'valuation',
            'market value', 'compulsory acquisition'
        ],
        'evidence': [
            'evidence', 'witness', 'testimony', 'burden of proof', 'onus',
            'documentary evidence', 'oral evidence', 'admissibility'
        ],
        'jurisdiction': [
            'jurisdiction', 'forum', 'territorial', 'subject matter',
            'appellate', 'revisional', 'writ'
        ],
        'procedure': [
            'procedure', 'cpc', 'crpc', 'plaint', 'written statement',
            'amendment', 'order', 'rule', 'decree', 'execution'
        ],
    }
    
    # Common legal principles
    LEGAL_PRINCIPLES = [
        ('res judicata', 'A matter already adjudicated cannot be relitigated'),
        ('estoppel', 'Party prevented from asserting contrary position'),
        ('specific performance', 'Court order to perform contractual obligation'),
        ('injunction', 'Court order to restrain from doing an act'),
        ('locus standi', 'Legal standing to bring action'),
        ('natural justice', 'Principles of fair procedure'),
        ('audi alteram partem', 'Right to be heard before decision'),
        ('nemo judex', 'No one can be judge in their own cause'),
        ('mandatory provisions', 'Provisions that must be complied with'),
        ('directory provisions', 'Provisions that are merely guiding'),
    ]
    
    def __init__(self):
        self.stats = {
            'cases_processed': 0,
            'issues_extracted': 0,
            'by_category': defaultdict(int),
            'statutes_frequency': defaultdict(int)
        }
        
        # Compile patterns
        self.statute_patterns = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in self.STATUTE_PATTERNS.items()
        }
    
    def generate_issue_id(self, text: str) -> str:
        """Generate unique ID for an issue based on normalized text"""
        normalized = self._normalize_for_id(text)
        return hashlib.md5(normalized.encode()).hexdigest()[:12]
    
    def _normalize_for_id(self, text: str) -> str:
        """Normalize text for ID generation"""
        text = text.lower()
        text = re.sub(r'[^\w\s]', '', text)
        text = ' '.join(text.split())
        return text[:200]
    
    def extract_statutes(self, text: str) -> List[str]:
        """Extract statute references from text"""
        statutes = []
        for name, pattern in self.statute_patterns.items():
            if pattern.search(text):
                statutes.append(name)
                self.stats['statutes_frequency'][name] += 1
        return statutes
    
    def extract_sections(self, text: str) -> List[str]:
        """Extract section/article references from text"""
        matches = self.SECTION_PATTERN.findall(text)
        # Clean and deduplicate
        sections = []
        for match in matches:
            cleaned = re.sub(r'\s+', ' ', match.strip())
            if cleaned and len(cleaned) < 50:
                sections.append(cleaned)
        return list(set(sections))
    
    def categorize_issue(self, text: str) -> Tuple[str, float]:
        """
        Categorize a legal issue based on keywords.
        Returns (category, confidence)
        """
        text_lower = text.lower()
        scores = {}
        
        for category, keywords in self.ISSUE_CATEGORIES.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                scores[category] = score
        
        if not scores:
            return ('general', 0.3)
        
        # Get highest scoring category
        best_category = max(scores, key=scores.get)
        max_score = scores[best_category]
        confidence = min(0.95, 0.5 + (max_score * 0.15))
        
        return (best_category, confidence)
    
    def extract_from_headnote(self, headnote: str) -> List[LegalIssue]:
        """
        Extract legal issues from headnote.
        
        Headnotes typically contain structured legal points like:
        "(a) Contract Act---S. 10---Agreement---Void contract..."
        """
        issues = []
        
        if not headnote:
            return issues
        
        # Split on common headnote separators
        # Look for patterns like "(a)", "(b)", "(1)", etc.
        parts = re.split(r'\n\s*\([a-z0-9]+\)\s*', headnote)
        
        for part in parts:
            part = part.strip()
            if len(part) < 20:
                continue
            
            # Extract first sentence as the issue summary
            first_sentence = part.split('---')[0] if '---' in part else part[:500]
            
            # Get statutes and sections
            statutes = self.extract_statutes(part)
            sections = self.extract_sections(part)
            
            # Categorize
            category, confidence = self.categorize_issue(part)
            
            # Normalize text
            normalized = self._normalize_issue_text(first_sentence)
            
            issue = LegalIssue(
                id=self.generate_issue_id(normalized),
                raw_text=first_sentence[:500],
                normalized_text=normalized,
                category=category,
                statutes=statutes,
                sections=sections,
                keywords=self._extract_keywords(part),
                confidence=confidence,
                source='headnote'
            )
            issues.append(issue)
        
        return issues
    
    def extract_from_judgment(self, judgment: str) -> List[LegalIssue]:
        """
        Extract legal issues from the judgment text.
        
        Looks for:
        1. Questions of law
        2. Points for determination
        3. Legal principle discussions
        """
        issues = []
        
        if not judgment:
            return issues
        
        # Pattern for "question of law" sections
        question_patterns = [
            r'(?:question|point)s?\s+(?:of|for)\s+(?:law|determination)[:\-]?\s*([^.]+\.)',
            r'(?:issue|matter)\s+(?:is|was|for\s+consideration)[:\-]?\s*([^.]+\.)',
            r'(?:whether)[^.]+\?',
        ]
        
        for pattern in question_patterns:
            matches = re.findall(pattern, judgment, re.IGNORECASE)
            for match in matches[:3]:  # Limit to avoid too many
                match = match.strip()
                if len(match) < 20 or len(match) > 500:
                    continue
                
                category, confidence = self.categorize_issue(match)
                normalized = self._normalize_issue_text(match)
                
                issue = LegalIssue(
                    id=self.generate_issue_id(normalized),
                    raw_text=match,
                    normalized_text=normalized,
                    category=category,
                    statutes=self.extract_statutes(match),
                    sections=self.extract_sections(match),
                    keywords=self._extract_keywords(match),
                    confidence=confidence * 0.8,  # Lower confidence for judgment extraction
                    source='judgment'
                )
                issues.append(issue)
        
        return issues
    
    def _normalize_issue_text(self, text: str) -> str:
        """
        Normalize legal issue text for comparison.
        
        - Lowercase
        - Remove case-specific references
        - Standardize terminology
        """
        text = text.lower()
        
        # Remove case references
        text = re.sub(r'\d{4}\s+[A-Z]+\s+\d+', '', text)
        
        # Remove party names (usually in caps or specific patterns)
        text = re.sub(r'(?:petitioner|respondent|appellant|plaintiff|defendant)s?', '', text)
        
        # Standardize common terms
        replacements = {
            'ss.': 'section',
            's.': 'section',
            'art.': 'article',
            'arts.': 'article',
            'o.': 'order',
            'r.': 'rule',
            'cl.': 'clause',
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        return text[:300]
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract significant legal keywords from text"""
        keywords = set()
        text_lower = text.lower()
        
        # Check against all category keywords
        for category, kw_list in self.ISSUE_CATEGORIES.items():
            for kw in kw_list:
                if kw in text_lower:
                    keywords.add(kw)
        
        # Check for legal principles
        for principle, _ in self.LEGAL_PRINCIPLES:
            if principle in text_lower:
                keywords.add(principle)
        
        return list(keywords)[:10]
    
    def extract_holding(self, judgment: str, headnote: str = '') -> str:
        """
        Extract the main holding/ruling from the case.
        
        Looks for conclusion/order sections.
        """
        # Try headnote first (often contains holding)
        if headnote:
            # Look for typical holding patterns
            holding_match = re.search(
                r'(?:held|held that|court held|ruled|ordered)[:\-,]?\s*([^.]+\.)',
                headnote,
                re.IGNORECASE
            )
            if holding_match:
                return holding_match.group(1).strip()[:500]
        
        if not judgment:
            return ''
        
        # Look in judgment for order/decree section
        order_patterns = [
            r'(?:ORDER|DECREE|JUDGMENT)\s*\n(.+?)(?:\n\s*\n|$)',
            r'(?:accordingly|therefore|in view of the above)[,\s]+(?:the\s+)?(?:petition|appeal|case)[^.]+\.',
        ]
        
        for pattern in order_patterns:
            match = re.search(pattern, judgment, re.IGNORECASE)
            if match:
                holding = match.group(0 if len(match.groups()) == 0 else 1)
                return holding.strip()[:500]
        
        # Fallback: last paragraph often contains ruling
        paragraphs = judgment.split('\n\n')
        for para in reversed(paragraphs[-5:]):
            if any(word in para.lower() for word in ['dismissed', 'allowed', 'disposed', 'granted', 'rejected']):
                return para.strip()[:500]
        
        return ''
    
    def extract(self, case_data: Dict, jurisdiction: str = '') -> ExtractionResult:
        """
        Extract all legal issues from a case.
        
        Args:
            case_data: Dictionary with case information
            jurisdiction: Jurisdiction of the case
            
        Returns:
            ExtractionResult with all extracted information
        """
        citation = case_data.get('citation', '')
        headnote = case_data.get('headnotes', '')
        judgment = case_data.get('judgment_clean', '')
        
        # Extract issues from both sources
        headnote_issues = self.extract_from_headnote(headnote)
        judgment_issues = self.extract_from_judgment(judgment)
        
        # Combine and deduplicate
        all_issues = self._deduplicate_issues(headnote_issues + judgment_issues)
        
        # Extract holding
        holding = self.extract_holding(judgment, headnote)
        
        # Get cited statutes and cases
        statutes_cited = case_data.get('statutes_cited', [])
        if not statutes_cited:
            statutes_cited = self.extract_statutes(headnote + '\n' + judgment)
        
        cases_cited = case_data.get('cases_cited', [])
        
        # Update stats
        self.stats['cases_processed'] += 1
        self.stats['issues_extracted'] += len(all_issues)
        for issue in all_issues:
            self.stats['by_category'][issue.category] += 1
        
        return ExtractionResult(
            citation=citation,
            jurisdiction=jurisdiction,
            issues=all_issues,
            holding=holding,
            statutes_cited=statutes_cited if isinstance(statutes_cited, list) else [statutes_cited],
            cases_cited=cases_cited if isinstance(cases_cited, list) else [cases_cited]
        )
    
    def _deduplicate_issues(self, issues: List[LegalIssue]) -> List[LegalIssue]:
        """Remove duplicate issues based on normalized text similarity"""
        seen_ids = set()
        unique_issues = []
        
        for issue in issues:
            if issue.id not in seen_ids:
                seen_ids.add(issue.id)
                unique_issues.append(issue)
        
        return unique_issues
    
    def get_stats(self) -> Dict:
        """Get extraction statistics"""
        return {
            'cases_processed': self.stats['cases_processed'],
            'issues_extracted': self.stats['issues_extracted'],
            'by_category': dict(self.stats['by_category']),
            'statutes_frequency': dict(self.stats['statutes_frequency'])
        }


def main():
    """Demonstrate issue extraction"""
    import sys
    from jurisdiction_classifier import load_cases_from_directory
    
    extractor = LegalIssueExtractor()
    
    data_dir = Path(__file__).parent / 'data_v2'
    
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)
    
    print(f"Loading cases from {data_dir}...")
    cases = load_cases_from_directory(data_dir)
    print(f"Loaded {len(cases)} cases")
    
    # Extract issues from all cases
    print("\nExtracting legal issues...")
    results = []
    
    for i, case in enumerate(cases):
        if i % 100 == 0:
            print(f"  Processing case {i+1}/{len(cases)}...")
        
        result = extractor.extract(case)
        results.append(result)
    
    # Print statistics
    stats = extractor.get_stats()
    print("\n" + "="*60)
    print("ISSUE EXTRACTION STATISTICS")
    print("="*60)
    
    print(f"\nCases processed: {stats['cases_processed']}")
    print(f"Total issues extracted: {stats['issues_extracted']}")
    print(f"Average issues per case: {stats['issues_extracted']/max(1,stats['cases_processed']):.1f}")
    
    print("\nIssues by Category:")
    for cat, count in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
        pct = 100 * count / max(1, stats['issues_extracted'])
        print(f"  {cat:20} {count:6} ({pct:5.1f}%)")
    
    print("\nMost Cited Statutes:")
    for statute, count in sorted(stats['statutes_frequency'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {statute:35} {count:6}")
    
    # Show sample extractions
    print("\n" + "="*60)
    print("SAMPLE EXTRACTIONS")
    print("="*60)
    
    for result in results[:3]:
        print(f"\nCitation: {result.citation}")
        print(f"Issues found: {len(result.issues)}")
        for issue in result.issues[:2]:
            print(f"  Category: {issue.category}")
            print(f"  Text: {issue.normalized_text[:100]}...")
            print(f"  Statutes: {', '.join(issue.statutes) or 'None'}")
            print(f"  Confidence: {issue.confidence:.2f}")
            print()
        if result.holding:
            print(f"  Holding: {result.holding[:200]}...")
    
    # Save results
    output_file = data_dir / 'issue_extraction_results.json'
    output_data = {
        'stats': stats,
        'extractions': [
            {
                'citation': r.citation,
                'jurisdiction': r.jurisdiction,
                'issues': [asdict(i) for i in r.issues],
                'holding': r.holding,
                'statutes_cited': r.statutes_cited,
                'cases_cited': r.cases_cited
            }
            for r in results[:500]  # Limit output size
        ]
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nExtraction results saved to: {output_file}")
    
    return results


if __name__ == '__main__':
    main()
