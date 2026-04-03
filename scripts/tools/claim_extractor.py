#!/usr/bin/env python3
"""
Legal Claim Extractor for Pakistani Legal Research Platform
Extracts legal claims, statutory references, and arguments from petition text.

Features:
- Extract statutory references (Section X of Act Y)
- Identify legal principles and doctrines
- Extract relief sought
- Identify cause of action
- Support for both English and Urdu text
"""

import re
import json
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, asdict, field
from collections import defaultdict


# Common Pakistani statutes and their abbreviations
STATUTE_ALIASES = {
    'cpc': 'Code of Civil Procedure, 1908',
    'code of civil procedure': 'Code of Civil Procedure, 1908',
    'c.p.c': 'Code of Civil Procedure, 1908',
    'crpc': 'Code of Criminal Procedure, 1898',
    'cr.p.c': 'Code of Criminal Procedure, 1898',
    'code of criminal procedure': 'Code of Criminal Procedure, 1898',
    'ppc': 'Pakistan Penal Code, 1860',
    'p.p.c': 'Pakistan Penal Code, 1860',
    'pakistan penal code': 'Pakistan Penal Code, 1860',
    'qso': 'Qanun-e-Shahadat Order, 1984',
    'qanun-e-shahadat': 'Qanun-e-Shahadat Order, 1984',
    'evidence act': 'Qanun-e-Shahadat Order, 1984',
    'constitution': 'Constitution of Pakistan, 1973',
    'tpa': 'Transfer of Property Act, 1882',
    'transfer of property act': 'Transfer of Property Act, 1882',
    'contract act': 'Contract Act, 1872',
    'specific relief act': 'Specific Relief Act, 1877',
    'sra': 'Specific Relief Act, 1877',
    'limitation act': 'Limitation Act, 1908',
    'registration act': 'Registration Act, 1908',
    'stamp act': 'Stamp Act, 1899',
    'court fees act': 'Court Fees Act, 1870',
    'land acquisition act': 'Land Acquisition Act, 1894',
    'rent restriction ordinance': 'Punjab Rented Premises Act, 2009',
    'mflo': 'Muslim Family Laws Ordinance, 1961',
    'muslim family laws ordinance': 'Muslim Family Laws Ordinance, 1961',
    'dissolution of muslim marriages act': 'Dissolution of Muslim Marriages Act, 1939',
    'guardian and wards act': 'Guardian and Wards Act, 1890',
    'companies act': 'Companies Act, 2017',
    'income tax ordinance': 'Income Tax Ordinance, 2001',
    'sales tax act': 'Sales Tax Act, 1990',
    'customs act': 'Customs Act, 1969',
    'nab ordinance': 'National Accountability Ordinance, 1999',
    'nab': 'National Accountability Ordinance, 1999',
    'cnsa': 'Control of Narcotic Substances Act, 1997',
    'anti-terrorism act': 'Anti-Terrorism Act, 1997',
    'ata': 'Anti-Terrorism Act, 1997',
    'civil servants act': 'Civil Servants Act, 1973',
    'provincial civil servants act': 'Punjab Civil Servants Act, 1974',
    'estacode': 'Establishment Code',
    'peca': 'Prevention of Electronic Crimes Act, 2016',
}

# Constitutional articles commonly cited
CONSTITUTIONAL_ARTICLES = {
    '4': 'Right of individuals to be dealt with in accordance with law',
    '9': 'Security of person',
    '10': 'Safeguards as to arrest and detention',
    '10A': 'Right to fair trial',
    '14': 'Inviolability of dignity of man',
    '18': 'Freedom of trade, business or profession',
    '19': 'Freedom of speech',
    '19A': 'Right to information',
    '20': 'Freedom to profess religion',
    '22': 'Safeguards as to educational institutions',
    '23': 'Right to acquire property',
    '24': 'Protection of property rights',
    '25': 'Equality of citizens',
    '25A': 'Right to education',
    '184': 'Original jurisdiction of Supreme Court',
    '185': 'Appellate jurisdiction of Supreme Court',
    '186': 'Advisory jurisdiction of Supreme Court',
    '187': 'Issue of orders by Supreme Court',
    '188': 'Review jurisdiction of Supreme Court',
    '199': 'Jurisdiction of High Court',
    '203C': 'Federal Shariat Court - Constitution',
    '203D': 'Powers of Federal Shariat Court',
    '212': 'Service Tribunals',
}

# Common legal principles/doctrines
LEGAL_PRINCIPLES = [
    ('res judicata', 'Res Judicata - matter already judged'),
    ('res sub judice', 'Res Sub Judice - matter pending in another court'),
    ('estoppel', 'Estoppel - prevented from denying'),
    ('promissory estoppel', 'Promissory Estoppel'),
    ('locus standi', 'Locus Standi - right to bring action'),
    ('limitation', 'Limitation - time bar'),
    ('adverse possession', 'Adverse Possession'),
    ('specific performance', 'Specific Performance of Contract'),
    ('injunction', 'Injunction'),
    ('declaration', 'Declaratory Relief'),
    ('restitution', 'Restitution'),
    ('quantum meruit', 'Quantum Meruit'),
    ('lis pendens', 'Lis Pendens'),
    ('ultra vires', 'Ultra Vires - beyond powers'),
    ('natural justice', 'Principles of Natural Justice'),
    ('audi alteram partem', 'Audi Alteram Partem - hear the other side'),
    ('nemo judex', 'Nemo Judex in Causa Sua - no one judge in their own case'),
    ('mala fide', 'Mala Fide - bad faith'),
    ('bona fide', 'Bona Fide - good faith'),
    ('ex parte', 'Ex Parte - one-sided'),
    ('prima facie', 'Prima Facie - at first impression'),
    ('burden of proof', 'Burden of Proof'),
    ('preponderance of evidence', 'Preponderance of Evidence'),
    ('beyond reasonable doubt', 'Beyond Reasonable Doubt'),
    ('maintainability', 'Maintainability of suit/petition'),
    ('territorial jurisdiction', 'Territorial Jurisdiction'),
    ('pecuniary jurisdiction', 'Pecuniary Jurisdiction'),
    ('subject matter jurisdiction', 'Subject Matter Jurisdiction'),
    ('cause of action', 'Cause of Action'),
    ('accrual of cause of action', 'Accrual of Cause of Action'),
    ('alternate remedy', 'Availability of Alternate Remedy'),
    ('exhaustion of remedies', 'Exhaustion of Remedies'),
    ('mandatory injunction', 'Mandatory Injunction'),
    ('prohibitory injunction', 'Prohibitory Injunction'),
    ('interlocutory injunction', 'Interlocutory Injunction'),
    ('perpetual injunction', 'Perpetual Injunction'),
    ('balance of convenience', 'Balance of Convenience'),
    ('irreparable injury', 'Irreparable Injury'),
    ('prima facie case', 'Prima Facie Case'),
    ('triable issue', 'Triable Issue'),
    ('pre-emption', 'Pre-emption'),
    ('partition', 'Partition'),
    ('joint holding', 'Joint Holding'),
    ('tenancy', 'Tenancy'),
    ('eviction', 'Eviction'),
    ('default', 'Default'),
    ('arrears of rent', 'Arrears of Rent'),
    ('personal need', 'Personal Need (bona fide)'),
    ('willful default', 'Willful Default'),
    ('sub-letting', 'Sub-letting'),
    ('fundamental rights', 'Fundamental Rights'),
    ('vested rights', 'Vested Rights'),
    ('legitimate expectation', 'Legitimate Expectation'),
    ('proportionality', 'Proportionality'),
    ('reasonableness', 'Reasonableness'),
    ('arbitrariness', 'Arbitrariness'),
    ('discrimination', 'Discrimination'),
    ('equal protection', 'Equal Protection'),
    ('due process', 'Due Process'),
]

# Relief types commonly sought
RELIEF_PATTERNS = [
    (r'declaration\s+(?:that|to\s+the\s+effect)', 'Declaration'),
    (r'decree\s+for\s+(?:specific\s+)?performance', 'Specific Performance'),
    (r'permanent\s+(?:prohibitory\s+)?injunction', 'Permanent Injunction'),
    (r'mandatory\s+injunction', 'Mandatory Injunction'),
    (r'temporary\s+injunction', 'Temporary Injunction'),
    (r'restraining\s+order', 'Restraining Order'),
    (r'damages\s+(?:to\s+the\s+tune|amounting)', 'Damages'),
    (r'compensation', 'Compensation'),
    (r'recovery\s+of\s+(?:possession|amount|money|rent)', 'Recovery'),
    (r'ejectment', 'Ejectment'),
    (r'possession\s+of\s+(?:suit\s+)?(?:property|land|premises)', 'Possession'),
    (r'partition\s+of\s+(?:suit\s+)?(?:property|land)', 'Partition'),
    (r'pre[\s-]?emption', 'Pre-emption'),
    (r'cancellation\s+of\s+(?:sale\s+)?deed', 'Cancellation of Deed'),
    (r'setting?\s+aside\s+(?:of\s+)?(?:impugned\s+)?(?:order|judgment|decree)', 'Setting Aside'),
    (r'quash(?:ing|ment)?\s+(?:of\s+)?(?:impugned\s+)?(?:order|judgment|fir|proceedings)', 'Quashing'),
    (r'writ\s+of\s+mandamus', 'Writ of Mandamus'),
    (r'writ\s+of\s+certiorari', 'Writ of Certiorari'),
    (r'writ\s+of\s+prohibition', 'Writ of Prohibition'),
    (r'writ\s+of\s+habeas\s+corpus', 'Writ of Habeas Corpus'),
    (r'writ\s+of\s+quo\s+warranto', 'Writ of Quo Warranto'),
    (r'grant(?:ing)?\s+(?:of\s+)?bail', 'Bail'),
    (r'confirmation\s+of\s+bail', 'Bail Confirmation'),
    (r'cancellation\s+of\s+bail', 'Bail Cancellation'),
    (r'reduction\s+(?:of\s+)?(?:in\s+)?sentence', 'Sentence Reduction'),
    (r'acquittal', 'Acquittal'),
    (r'custody\s+(?:of\s+)?(?:minor|child)', 'Custody'),
    (r'maintenance', 'Maintenance'),
    (r'dissolution\s+of\s+marriage', 'Dissolution of Marriage'),
    (r'restitution\s+of\s+conjugal\s+rights', 'Restitution of Conjugal Rights'),
    (r'dower|mehr|mahr', 'Dower/Mehr'),
    (r'reinstatement', 'Reinstatement'),
    (r'back\s+benefits', 'Back Benefits'),
    (r'service\s+benefits', 'Service Benefits'),
    (r'pension', 'Pension'),
    (r'refund', 'Refund'),
    (r'directions?\s+(?:to|for)', 'Directions'),
    (r'costs?\s+of\s+(?:the\s+)?(?:suit|petition|proceedings)', 'Costs'),
]


@dataclass
class StatutoryReference:
    """A reference to a statutory provision."""
    section: str
    act: str
    normalized_act: Optional[str] = None
    subsection: Optional[str] = None
    clause: Optional[str] = None
    full_reference: str = ""


@dataclass
class LegalPrinciple:
    """A legal principle or doctrine identified in text."""
    principle: str
    description: str
    context: Optional[str] = None


@dataclass
class Relief:
    """Relief sought in petition."""
    relief_type: str
    description: Optional[str] = None


@dataclass
class ExtractedClaims:
    """Complete extraction result."""
    statutory_references: List[StatutoryReference] = field(default_factory=list)
    constitutional_articles: List[Dict[str, str]] = field(default_factory=list)
    legal_principles: List[LegalPrinciple] = field(default_factory=list)
    reliefs_sought: List[Relief] = field(default_factory=list)
    cause_of_action: Optional[str] = None
    parties: Dict[str, List[str]] = field(default_factory=dict)
    key_facts: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'statutory_references': [asdict(r) for r in self.statutory_references],
            'constitutional_articles': self.constitutional_articles,
            'legal_principles': [asdict(p) for p in self.legal_principles],
            'reliefs_sought': [asdict(r) for r in self.reliefs_sought],
            'cause_of_action': self.cause_of_action,
            'parties': self.parties,
            'key_facts': self.key_facts,
        }
    
    def get_provision_keys(self) -> List[str]:
        """Get list of provision keys for success rate lookup."""
        keys = []
        
        for ref in self.statutory_references:
            act_short = ref.normalized_act or ref.act
            key = f"Section {ref.section} {act_short}"
            keys.append(key)
        
        for art in self.constitutional_articles:
            key = f"Article {art['article']} Constitution"
            keys.append(key)
        
        return keys


class ClaimExtractor:
    """
    Extracts legal claims from petition/judgment text.
    """
    
    def __init__(self):
        self.statute_aliases = STATUTE_ALIASES
        self.constitutional_articles = CONSTITUTIONAL_ARTICLES
        self.legal_principles = LEGAL_PRINCIPLES
        self.relief_patterns = RELIEF_PATTERNS
        
        # Compile regex patterns
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for efficiency."""
        # Section patterns - captures section number and act name
        self.section_patterns = [
            # Section 12 of the Code of Civil Procedure
            re.compile(
                r'(?:section|s\.?|sec\.?)\s*(\d+[\w/-]*)'
                r'(?:\s*(?:\(([^)]+)\)|\[([^\]]+)\]))?'  # Optional subsection
                r'(?:\s+(?:of|under)\s+(?:the\s+)?)?'
                r'([A-Z][A-Za-z\s,]+(?:Act|Ordinance|Order|Code|Rules?|Regulation|Law|Constitution)[,\s]*(?:\d{4})?)?',
                re.IGNORECASE
            ),
            # S. 302 PPC or Section 302, PPC
            re.compile(
                r'(?:section|s\.?|sec\.?)\s*(\d+[\w/-]*)'
                r'(?:\s*(?:\(([^)]+)\)|\[([^\]]+)\]))?'
                r'[,\s]+(?:of\s+(?:the\s+)?)?'
                r'([A-Z]{2,}[A-Za-z]*\.?)',
                re.IGNORECASE
            ),
        ]
        
        # Article patterns for Constitution
        self.article_pattern = re.compile(
            r'(?:article|art\.?)\s*(\d+[\w/-]*)'
            r'(?:\s*(?:\(([^)]+)\)|\[([^\]]+)\]))?'
            r'(?:\s+(?:of|under)\s+(?:the\s+)?)?'
            r'(?:Constitution(?:\s+of\s+(?:Pakistan|Islamic\s+Republic))?(?:[,\s]*\d{4})?)?',
            re.IGNORECASE
        )
        
        # Order and Rule patterns (for CPC)
        self.order_rule_pattern = re.compile(
            r'(?:order|o\.?)\s*([IVXLCDM]+|\d+)'
            r'[,\s]*(?:rule|r\.?)\s*(\d+[\w/-]*)'
            r'(?:\s+(?:of|under)\s+(?:the\s+)?)?'
            r'([A-Za-z\s,]+(?:Code|Act|Rules?)?(?:[,\s]*\d{4})?)?',
            re.IGNORECASE
        )
    
    def _normalize_act_name(self, act: str) -> Optional[str]:
        """Normalize act name to canonical form."""
        if not act:
            return None
        
        act_lower = act.lower().strip().rstrip(',.')
        
        # Check aliases
        for alias, full_name in self.statute_aliases.items():
            if alias in act_lower or act_lower in alias:
                return full_name
        
        # Check for common patterns
        if 'civil' in act_lower and 'procedure' in act_lower:
            return 'Code of Civil Procedure, 1908'
        if 'criminal' in act_lower and 'procedure' in act_lower:
            return 'Code of Criminal Procedure, 1898'
        if 'penal' in act_lower:
            return 'Pakistan Penal Code, 1860'
        if 'constitution' in act_lower:
            return 'Constitution of Pakistan, 1973'
        
        return act.strip()
    
    def extract_statutory_references(self, text: str) -> List[StatutoryReference]:
        """Extract statutory references from text."""
        references = []
        seen = set()
        
        for pattern in self.section_patterns:
            for match in pattern.finditer(text):
                section = match.group(1)
                subsection = match.group(2) or match.group(3)
                act = match.group(4) if len(match.groups()) >= 4 else None
                
                # Create unique key
                key = (section, act or '', subsection or '')
                if key in seen:
                    continue
                seen.add(key)
                
                ref = StatutoryReference(
                    section=section,
                    act=act.strip() if act else 'Unknown',
                    normalized_act=self._normalize_act_name(act) if act else None,
                    subsection=subsection,
                    full_reference=match.group(0).strip()
                )
                references.append(ref)
        
        # Also extract Order/Rule references
        for match in self.order_rule_pattern.finditer(text):
            order = match.group(1)
            rule = match.group(2)
            act = match.group(3) if len(match.groups()) >= 3 else 'CPC'
            
            key = (f"O.{order} R.{rule}", act or 'CPC', '')
            if key in seen:
                continue
            seen.add(key)
            
            ref = StatutoryReference(
                section=f"Order {order}, Rule {rule}",
                act=act.strip() if act else 'Code of Civil Procedure, 1908',
                normalized_act='Code of Civil Procedure, 1908',
                full_reference=match.group(0).strip()
            )
            references.append(ref)
        
        return references
    
    def extract_constitutional_articles(self, text: str) -> List[Dict[str, str]]:
        """Extract constitutional article references."""
        articles = []
        seen = set()
        
        for match in self.article_pattern.finditer(text):
            article_num = match.group(1)
            clause = match.group(2) or match.group(3)
            
            if article_num in seen:
                continue
            seen.add(article_num)
            
            description = self.constitutional_articles.get(article_num, '')
            
            articles.append({
                'article': article_num,
                'clause': clause,
                'description': description,
                'full_reference': match.group(0).strip()
            })
        
        return articles
    
    def extract_legal_principles(self, text: str) -> List[LegalPrinciple]:
        """Extract legal principles and doctrines from text."""
        principles = []
        text_lower = text.lower()
        seen = set()
        
        for term, description in self.legal_principles:
            if term.lower() in text_lower and term not in seen:
                seen.add(term)
                
                # Try to get context
                idx = text_lower.find(term.lower())
                context_start = max(0, idx - 100)
                context_end = min(len(text), idx + len(term) + 100)
                context = text[context_start:context_end].strip()
                
                principles.append(LegalPrinciple(
                    principle=term,
                    description=description,
                    context=context
                ))
        
        return principles
    
    def extract_reliefs_sought(self, text: str) -> List[Relief]:
        """Extract reliefs sought from petition text."""
        reliefs = []
        text_lower = text.lower()
        seen = set()
        
        for pattern, relief_type in self.relief_patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                if relief_type not in seen:
                    seen.add(relief_type)
                    reliefs.append(Relief(
                        relief_type=relief_type,
                        description=match.group(0).strip()
                    ))
        
        return reliefs
    
    def extract_cause_of_action(self, text: str) -> Optional[str]:
        """Attempt to identify the cause of action."""
        # Patterns that often indicate cause of action
        coa_patterns = [
            r'cause\s+of\s+action\s+(?:is|arises|arose)\s+([^.]+)',
            r'grievance\s+(?:of\s+the\s+)?(?:petitioner|appellant|plaintiff)\s+(?:is|was)\s+([^.]+)',
            r'(?:petitioner|appellant|plaintiff)\s+(?:is|was)\s+aggrieved\s+(?:by|of)\s+([^.]+)',
            r'dispute\s+(?:arises|arose)\s+(?:out\s+of|from)\s+([^.]+)',
        ]
        
        for pattern in coa_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()[:200]  # Limit length
        
        return None
    
    def extract_parties(self, text: str) -> Dict[str, List[str]]:
        """Extract party names from text."""
        parties = {
            'petitioners': [],
            'respondents': [],
            'appellants': [],
            'plaintiffs': [],
            'defendants': [],
        }
        
        # Patterns to find party names
        party_patterns = [
            (r'(?:petitioner|appellant|plaintiff)[s]?\s*[-:]\s*([^\n]+)', 'petitioners'),
            (r'(?:respondent|defendant)[s]?\s*[-:]\s*([^\n]+)', 'respondents'),
            (r'([^\n]+)\s+(?:v\.?|vs\.?|versus)\s+([^\n]+)', None),  # Special handling
        ]
        
        for pattern, party_type in party_patterns[:-1]:
            match = re.search(pattern, text[:2000], re.IGNORECASE)
            if match:
                names = match.group(1).strip()
                parties[party_type] = [n.strip() for n in re.split(r'[,&]|and', names) if n.strip()]
        
        # Handle "X v. Y" pattern
        vs_match = re.search(party_patterns[-1][0], text[:500], re.IGNORECASE)
        if vs_match:
            parties['petitioners'] = parties.get('petitioners') or [vs_match.group(1).strip()]
            parties['respondents'] = parties.get('respondents') or [vs_match.group(2).strip()]
        
        return {k: v for k, v in parties.items() if v}
    
    def extract(self, text: str, extract_all: bool = True) -> ExtractedClaims:
        """
        Extract all legal claims from text.
        
        Args:
            text: Petition or judgment text
            extract_all: Whether to extract all types or just main claims
            
        Returns:
            ExtractedClaims object with all extracted information
        """
        if not text:
            return ExtractedClaims()
        
        claims = ExtractedClaims(
            statutory_references=self.extract_statutory_references(text),
            constitutional_articles=self.extract_constitutional_articles(text),
            legal_principles=self.extract_legal_principles(text),
            reliefs_sought=self.extract_reliefs_sought(text),
        )
        
        if extract_all:
            claims.cause_of_action = self.extract_cause_of_action(text)
            claims.parties = self.extract_parties(text)
        
        return claims
    
    def extract_from_case(self, case: Dict) -> ExtractedClaims:
        """Extract claims from a case dictionary."""
        # Combine relevant fields
        text_parts = [
            case.get('title', ''),
            case.get('headnotes', ''),
            case.get('judgment_clean', '')[:10000],  # First 10k chars of judgment
        ]
        text = '\n\n'.join(p for p in text_parts if p)
        
        return self.extract(text)


def extract_all_cases(jsonl_path: Path, output_path: Path) -> None:
    """
    Extract claims from all cases in a JSONL file.
    
    Args:
        jsonl_path: Path to input JSONL
        output_path: Path to output JSONL with extractions
    """
    extractor = ClaimExtractor()
    
    with open(jsonl_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            if not line.strip():
                continue
            
            case = json.loads(line)
            claims = extractor.extract_from_case(case)
            
            # Add extractions to case
            case['claims'] = claims.to_dict()
            case['provision_keys'] = claims.get_provision_keys()
            
            outfile.write(json.dumps(case, ensure_ascii=False) + '\n')
    
    print(f"Extraction complete. Output: {output_path}")


def main():
    """CLI interface for claim extraction."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract legal claims from cases')
    parser.add_argument('--input', '-i', type=Path, default=Path('data_v2/all_cases.jsonl'),
                       help='Input JSONL file')
    parser.add_argument('--output', '-o', type=Path, default=Path('data_v2/cases_claims.jsonl'),
                       help='Output JSONL file')
    parser.add_argument('--test', '-t', action='store_true',
                       help='Run on first 10 cases only')
    parser.add_argument('--text', type=str,
                       help='Extract from provided text')
    
    args = parser.parse_args()
    
    extractor = ClaimExtractor()
    
    if args.text:
        # Extract from provided text
        claims = extractor.extract(args.text)
        print(json.dumps(claims.to_dict(), indent=2, ensure_ascii=False))
        return
    
    if args.test:
        # Test mode
        print("Testing on first 10 cases...\n")
        
        with open(args.input, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 10:
                    break
                
                if not line.strip():
                    continue
                
                case = json.loads(line)
                claims = extractor.extract_from_case(case)
                
                print(f"\n{'='*60}")
                print(f"Case: {case.get('citation', 'Unknown')}")
                print(f"Title: {case.get('title', 'Unknown')[:80]}")
                print(f"\nStatutory References ({len(claims.statutory_references)}):")
                for ref in claims.statutory_references[:5]:
                    print(f"  - Section {ref.section} of {ref.act}")
                
                print(f"\nConstitutional Articles ({len(claims.constitutional_articles)}):")
                for art in claims.constitutional_articles[:5]:
                    print(f"  - Article {art['article']}: {art['description']}")
                
                print(f"\nLegal Principles ({len(claims.legal_principles)}):")
                for prin in claims.legal_principles[:5]:
                    print(f"  - {prin.principle}: {prin.description}")
                
                print(f"\nReliefs ({len(claims.reliefs_sought)}):")
                for relief in claims.reliefs_sought[:5]:
                    print(f"  - {relief.relief_type}")
                
                if claims.cause_of_action:
                    print(f"\nCause of Action: {claims.cause_of_action[:100]}...")
    
    else:
        # Full extraction
        print(f"Extracting claims from {args.input}...")
        extract_all_cases(args.input, args.output)


if __name__ == '__main__':
    main()
