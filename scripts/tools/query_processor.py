#!/usr/bin/env python3
"""
Query Processor for Qanoon AI Research Copilot
Implements query understanding, expansion, and rewriting for legal search.
"""

import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class QueryIntent(Enum):
    """Types of legal research queries."""
    QUESTION = "question"  # Direct legal question
    RESEARCH = "research"  # Research task (find cases about...)
    COMPARISON = "comparison"  # Compare cases/laws
    DEFINITION = "definition"  # What is...
    PRECEDENT = "precedent"  # Find precedents for...
    PROCEDURE = "procedure"  # How to...
    CITATION = "citation"  # Find specific case by citation


@dataclass
class LegalEntity:
    """Represents an extracted legal entity."""
    text: str
    entity_type: str  # 'statute', 'case', 'concept', 'court', 'section', 'party'
    normalized: str
    confidence: float = 1.0


@dataclass
class ProcessedQuery:
    """Result of query processing."""
    original_query: str
    intent: QueryIntent
    entities: List[LegalEntity]
    expanded_queries: List[str]
    primary_query: str
    filters: Dict[str, str] = field(default_factory=dict)
    keywords: List[str] = field(default_factory=list)
    practice_areas: List[str] = field(default_factory=list)
    

class LegalEntityExtractor:
    """Extract legal entities from query text."""
    
    # Pakistani statute patterns
    STATUTE_PATTERNS = [
        (r'\b(Constitution\s+of\s+Pakistan)', 'Constitution of Pakistan'),
        (r'\b(Article\s+\d+(?:\([a-z0-9]+\))?)', None),  # Article 199, Article 17(2)
        (r'\b(Section\s+\d+[A-Z]?(?:\([a-z0-9]+\))?)', None),  # Section 302, Section 489-F
        (r'\b(PPC|Pakistan\s+Penal\s+Code)', 'Pakistan Penal Code'),
        (r'\b(Cr\.?P\.?C\.?|Criminal\s+Procedure\s+Code)', 'Criminal Procedure Code'),
        (r'\b(CPC|Civil\s+Procedure\s+Code)', 'Civil Procedure Code'),
        (r'\b(Contract\s+Act)', 'Contract Act'),
        (r'\b(Transfer\s+of\s+Property\s+Act)', 'Transfer of Property Act'),
        (r'\b(Specific\s+Relief\s+Act)', 'Specific Relief Act'),
        (r'\b(Qanun-?e-?Shahadat|Evidence\s+Act)', 'Qanun-e-Shahadat'),
        (r'\b(Family\s+Courts?\s+Act)', 'Family Courts Act'),
        (r'\b(Rent\s+Restriction\s+(?:Act|Ordinance))', 'Rent Restriction'),
        (r'\b(Income\s+Tax\s+Ordinance)', 'Income Tax Ordinance'),
        (r'\b(Companies\s+(?:Act|Ordinance))', 'Companies Act'),
        (r'\b(NIRC|National\s+Industrial\s+Relations\s+Commission)', 'NIRC'),
        (r'\b(NAB|National\s+Accountability\s+Bureau)', 'NAB'),
        (r'\b(SECP|Securities\s+and\s+Exchange\s+Commission)', 'SECP'),
    ]
    
    # Case citation patterns
    CITATION_PATTERNS = [
        r'\b(\d{4}\s+SCMR\s+\d+)',
        r'\b(\d{4}\s+PLD\s+\w+\s+\d+)',
        r'\b(\d{4}\s+CLC\s+\d+)',
        r'\b(\d{4}\s+MLD\s+\d+)',
        r'\b(\d{4}\s+YLR\s+\d+)',
        r'\b(\d{4}\s+PCr\.?LJ\s+\d+)',
        r'\b(\d{4}\s+PTD\s+\d+)',
        r'\b(\d{4}\s+PLC\s+\d+)',
        r'\b(PLD\s+\d{4}\s+\w+\s+\d+)',
    ]
    
    # Court names
    COURT_PATTERNS = [
        (r'\b(Supreme\s+Court(?:\s+of\s+Pakistan)?)', 'Supreme Court'),
        (r'\b(Federal\s+Shariat\s+Court)', 'Federal Shariat Court'),
        (r'\b(High\s+Court)', None),  # Keep as-is to capture Lahore High Court, etc.
        (r'\b(LHC|Lahore\s+High\s+Court)', 'Lahore High Court'),
        (r'\b(SHC|Sindh\s+High\s+Court)', 'Sindh High Court'),
        (r'\b(PHC|Peshawar\s+High\s+Court)', 'Peshawar High Court'),
        (r'\b(BHC|Balochistan\s+High\s+Court)', 'Balochistan High Court'),
        (r'\b(IHC|Islamabad\s+High\s+Court)', 'Islamabad High Court'),
        (r'\b(Sessions?\s+Court)', 'Sessions Court'),
        (r'\b(District\s+Court)', 'District Court'),
        (r'\b(Civil\s+Judge)', 'Civil Court'),
        (r'\b(Family\s+Court)', 'Family Court'),
        (r'\b(Banking\s+Court)', 'Banking Court'),
        (r'\b(Labour\s+Court|Labor\s+Court)', 'Labour Court'),
        (r'\b(Service\s+Tribunal)', 'Service Tribunal'),
    ]
    
    # Legal concepts
    CONCEPT_PATTERNS = [
        'bail', 'habeas corpus', 'writ', 'injunction', 'stay order',
        'specific performance', 'damages', 'compensation', 'restitution',
        'eviction', 'possession', 'ownership', 'title', 'mutation',
        'maintenance', 'custody', 'divorce', 'khula', 'talaq', 'dower', 'mehr',
        'murder', 'qatl', 'robbery', 'theft', 'dacoity', 'kidnapping',
        'defamation', 'fraud', 'cheating', 'forgery', 'corruption',
        'termination', 'reinstatement', 'seniority', 'promotion',
        'tax assessment', 'tax recovery', 'customs duty',
        'constitutional petition', 'appeal', 'revision', 'review',
        'limitation', 'res judicata', 'estoppel', 'locus standi',
        'burden of proof', 'preponderance', 'beyond reasonable doubt',
        'acquittal', 'conviction', 'sentence', 'remission',
    ]
    
    def extract(self, text: str) -> List[LegalEntity]:
        """Extract all legal entities from text."""
        entities = []
        text_upper = text.upper()
        
        # Extract statutes
        for pattern, normalized in self.STATUTE_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                entities.append(LegalEntity(
                    text=match.group(1),
                    entity_type='statute',
                    normalized=normalized or match.group(1),
                    confidence=0.9
                ))
        
        # Extract case citations
        for pattern in self.CITATION_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                citation = re.sub(r'\s+', ' ', match.group(1).upper())
                entities.append(LegalEntity(
                    text=match.group(1),
                    entity_type='case',
                    normalized=citation,
                    confidence=0.95
                ))
        
        # Extract courts
        for pattern, normalized in self.COURT_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                entities.append(LegalEntity(
                    text=match.group(1),
                    entity_type='court',
                    normalized=normalized or match.group(1),
                    confidence=0.85
                ))
        
        # Extract legal concepts
        text_lower = text.lower()
        for concept in self.CONCEPT_PATTERNS:
            if concept in text_lower:
                entities.append(LegalEntity(
                    text=concept,
                    entity_type='concept',
                    normalized=concept,
                    confidence=0.7
                ))
        
        return entities


class IntentDetector:
    """Detect the intent of a legal query."""
    
    QUESTION_STARTERS = [
        r'^(what|who|when|where|why|how|is|are|can|could|should|would|does|do|will)\b',
        r'\?$',
    ]
    
    COMPARISON_PATTERNS = [
        r'\b(compare|comparison|difference|between|versus|vs\.?)\b',
        r'\b(similar\s+to|different\s+from|distinguish)\b',
    ]
    
    DEFINITION_PATTERNS = [
        r'^what\s+is\b',
        r'^define\b',
        r'^meaning\s+of\b',
        r'^definition\s+of\b',
        r'\bwhat\s+(?:does|do)\s+.+\s+mean\b',
    ]
    
    PRECEDENT_PATTERNS = [
        r'\b(precedent|leading\s+case|landmark\s+case|authority)\b',
        r'\b(find\s+cases?\s+(?:about|on|regarding|relating))\b',
    ]
    
    PROCEDURE_PATTERNS = [
        r'^how\s+(?:to|do|can|should)\b',
        r'\b(procedure|process|steps|requirements)\b',
        r'\b(file|filing|submit|apply)\b',
    ]
    
    CITATION_PATTERNS = [
        r'\b\d{4}\s+(?:SCMR|PLD|CLC|MLD|YLR|PCrLJ|PTD|PLC)\b',
    ]
    
    def detect(self, query: str, entities: List[LegalEntity]) -> QueryIntent:
        """Detect the intent of a query."""
        query_lower = query.lower()
        
        # Check for specific citation lookup
        for pattern in self.CITATION_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return QueryIntent.CITATION
        
        # Check for definition
        for pattern in self.DEFINITION_PATTERNS:
            if re.search(pattern, query_lower):
                return QueryIntent.DEFINITION
        
        # Check for comparison
        for pattern in self.COMPARISON_PATTERNS:
            if re.search(pattern, query_lower):
                return QueryIntent.COMPARISON
        
        # Check for precedent search
        for pattern in self.PRECEDENT_PATTERNS:
            if re.search(pattern, query_lower):
                return QueryIntent.PRECEDENT
        
        # Check for procedure
        for pattern in self.PROCEDURE_PATTERNS:
            if re.search(pattern, query_lower):
                return QueryIntent.PROCEDURE
        
        # Check for question
        for pattern in self.QUESTION_STARTERS:
            if re.search(pattern, query_lower):
                return QueryIntent.QUESTION
        
        # Default to research
        return QueryIntent.RESEARCH


class QueryExpander:
    """Expand queries with synonyms and related terms."""
    
    LEGAL_SYNONYMS = {
        'murder': ['homicide', 'killing', 'qatl', 'section 302', 'culpable homicide'],
        'theft': ['stealing', 'larceny', 'section 379', 'dishonest taking'],
        'robbery': ['dacoity', 'section 392', 'section 395', 'armed robbery'],
        'bail': ['pre-arrest bail', 'post-arrest bail', 'interim bail', 'regular bail'],
        'divorce': ['dissolution of marriage', 'talaq', 'khula', 'judicial separation'],
        'maintenance': ['nafaqa', 'alimony', 'financial support', 'spousal support'],
        'custody': ['guardianship', 'hizanat', 'child custody', 'visitation'],
        'eviction': ['ejectment', 'dispossession', 'removal of tenant'],
        'landlord': ['lessor', 'property owner', 'house owner'],
        'tenant': ['lessee', 'occupant', 'renter'],
        'contract': ['agreement', 'deed', 'covenant'],
        'damages': ['compensation', 'restitution', 'monetary relief'],
        'injunction': ['stay order', 'restraining order', 'prohibitory order'],
        'appeal': ['appellate', 'revision', 'review'],
        'writ': ['constitutional petition', 'article 199', 'certiorari', 'mandamus'],
        'fraud': ['cheating', 'misrepresentation', 'deceit', 'section 420'],
        'termination': ['dismissal', 'removal from service', 'discharge'],
        'property': ['land', 'immovable property', 'real estate'],
        'inheritance': ['succession', 'wirasat', 'estate', 'testate', 'intestate'],
        'defamation': ['libel', 'slander', 'section 499', 'section 500'],
        'tax': ['taxation', 'revenue', 'fiscal', 'duty'],
    }
    
    PRACTICE_AREA_KEYWORDS = {
        'constitutional': ['constitution', 'fundamental rights', 'writ', 'article 199', 'article 184'],
        'criminal': ['murder', 'theft', 'robbery', 'bail', 'fir', 'section 302', 'ppc'],
        'civil': ['contract', 'damages', 'specific performance', 'suit', 'decree'],
        'property': ['land', 'tenant', 'landlord', 'eviction', 'mutation', 'property'],
        'family': ['divorce', 'custody', 'maintenance', 'khula', 'dower', 'mehr'],
        'tax': ['income tax', 'sales tax', 'customs', 'ptd', 'taxation'],
        'labor': ['termination', 'employment', 'wages', 'nirc', 'workman'],
        'corporate': ['company', 'shareholder', 'director', 'secp', 'winding up'],
        'banking': ['bank', 'loan', 'mortgage', 'recovery', 'cheque'],
        'administrative': ['service tribunal', 'civil servant', 'seniority', 'promotion'],
    }
    
    def expand(self, query: str, entities: List[LegalEntity], 
               max_variants: int = 5) -> List[str]:
        """Generate query variants for better retrieval."""
        variants = [query]
        query_lower = query.lower()
        
        # Add synonym-based variants
        for term, synonyms in self.LEGAL_SYNONYMS.items():
            if term in query_lower:
                for synonym in synonyms[:2]:  # Limit synonyms
                    variant = re.sub(rf'\b{term}\b', synonym, query_lower, flags=re.IGNORECASE)
                    if variant != query_lower and variant not in variants:
                        variants.append(variant)
                        if len(variants) >= max_variants:
                            break
            if len(variants) >= max_variants:
                break
        
        # Add entity-focused variant
        if entities:
            entity_terms = [e.normalized for e in entities if e.entity_type in ('statute', 'concept')]
            if entity_terms:
                entity_query = ' '.join(entity_terms[:3])
                if entity_query not in variants:
                    variants.append(entity_query)
        
        # Add structured legal query variant
        concepts = [e.normalized for e in entities if e.entity_type == 'concept']
        statutes = [e.normalized for e in entities if e.entity_type == 'statute']
        if concepts or statutes:
            structured = f"{' '.join(concepts[:2])} {' '.join(statutes[:2])} Pakistani law"
            if structured.strip() != 'Pakistani law' and structured not in variants:
                variants.append(structured.strip())
        
        return variants[:max_variants]
    
    def detect_practice_areas(self, query: str, entities: List[LegalEntity]) -> List[str]:
        """Detect relevant practice areas for the query."""
        query_lower = query.lower()
        areas = []
        
        for area, keywords in self.PRACTICE_AREA_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in query_lower)
            # Also check entities
            for entity in entities:
                if any(kw in entity.normalized.lower() for kw in keywords):
                    score += 1
            if score >= 1:
                areas.append((area, score))
        
        areas.sort(key=lambda x: x[1], reverse=True)
        return [area for area, _ in areas[:3]]


class QueryRewriter:
    """Rewrite queries for optimal retrieval."""
    
    def rewrite(self, query: str, intent: QueryIntent, 
                entities: List[LegalEntity]) -> str:
        """Rewrite query for better retrieval."""
        rewritten = query
        
        # Remove question words for research intents
        if intent in (QueryIntent.RESEARCH, QueryIntent.PRECEDENT):
            rewritten = re.sub(r'^(what|who|when|where|why|how|can|could|should|would|is|are|does|do)\s+', '', 
                              rewritten, flags=re.IGNORECASE)
            rewritten = re.sub(r'\?$', '', rewritten)
        
        # Add "Pakistan" context if not present
        if 'pakistan' not in rewritten.lower() and 'pld' not in rewritten.lower():
            rewritten = f"{rewritten} Pakistan law"
        
        # Expand abbreviations
        abbreviations = {
            r'\bPPC\b': 'Pakistan Penal Code',
            r'\bCrPC\b': 'Criminal Procedure Code',
            r'\bCPC\b': 'Civil Procedure Code',
            r'\bFIR\b': 'First Information Report',
            r'\bNAB\b': 'National Accountability Bureau',
        }
        for abbrev, expansion in abbreviations.items():
            if re.search(abbrev, rewritten, re.IGNORECASE):
                # Keep both abbreviation and expansion
                rewritten = re.sub(abbrev, f'{expansion}', rewritten, flags=re.IGNORECASE)
        
        return rewritten.strip()


class QueryProcessor:
    """Main query processor combining all components."""
    
    def __init__(self):
        self.entity_extractor = LegalEntityExtractor()
        self.intent_detector = IntentDetector()
        self.query_expander = QueryExpander()
        self.query_rewriter = QueryRewriter()
    
    def process(self, query: str) -> ProcessedQuery:
        """Process a natural language legal query."""
        # Clean query
        query = query.strip()
        
        # Extract entities
        entities = self.entity_extractor.extract(query)
        
        # Detect intent
        intent = self.intent_detector.detect(query, entities)
        
        # Expand query
        expanded_queries = self.query_expander.expand(query, entities)
        
        # Detect practice areas
        practice_areas = self.query_expander.detect_practice_areas(query, entities)
        
        # Rewrite primary query
        primary_query = self.query_rewriter.rewrite(query, intent, entities)
        
        # Build filters based on entities
        filters = {}
        courts = [e.normalized for e in entities if e.entity_type == 'court']
        if courts:
            filters['court'] = courts[0]
        
        # Extract keywords (non-stopwords)
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 
                     'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
                     'would', 'could', 'should', 'can', 'may', 'might', 'must',
                     'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from',
                     'about', 'into', 'through', 'during', 'before', 'after',
                     'above', 'below', 'between', 'under', 'and', 'or', 'but',
                     'if', 'then', 'so', 'what', 'when', 'where', 'why', 'how',
                     'who', 'which', 'this', 'that', 'these', 'those', 'it'}
        
        words = re.findall(r'\b\w+\b', query.lower())
        keywords = [w for w in words if w not in stopwords and len(w) > 2]
        
        return ProcessedQuery(
            original_query=query,
            intent=intent,
            entities=entities,
            expanded_queries=expanded_queries,
            primary_query=primary_query,
            filters=filters,
            keywords=keywords,
            practice_areas=practice_areas
        )
    
    def get_search_queries(self, processed: ProcessedQuery) -> List[str]:
        """Get the list of queries to execute for search."""
        queries = [processed.primary_query]
        
        # Add expanded queries
        for eq in processed.expanded_queries:
            if eq not in queries:
                queries.append(eq)
        
        return queries[:5]  # Limit to 5 queries


def main():
    """Test the query processor."""
    processor = QueryProcessor()
    
    test_queries = [
        "Can a landlord evict a tenant without notice?",
        "What is the punishment for murder under Section 302 PPC?",
        "Find cases about bail in NAB cases",
        "2024 SCMR 456",
        "How to file a constitutional petition in Lahore High Court?",
        "Compare divorce laws for Muslims and non-Muslims",
        "What are the grounds for khula in Pakistan?",
        "Specific performance of contract requirements",
        "Supreme Court precedents on fundamental rights",
        "Tax evasion penalty under Income Tax Ordinance",
    ]
    
    print("=" * 70)
    print("Query Processor Test")
    print("=" * 70)
    
    for query in test_queries:
        print(f"\n{'='*70}")
        print(f"Query: {query}")
        print("-" * 70)
        
        result = processor.process(query)
        
        print(f"Intent: {result.intent.value}")
        print(f"Practice Areas: {', '.join(result.practice_areas) if result.practice_areas else 'General'}")
        
        if result.entities:
            print("Entities:")
            for entity in result.entities:
                print(f"  - [{entity.entity_type}] {entity.text} → {entity.normalized}")
        
        print(f"Primary Query: {result.primary_query}")
        print(f"Expanded Queries:")
        for eq in result.expanded_queries:
            print(f"  - {eq}")
        
        if result.filters:
            print(f"Filters: {result.filters}")
        
        print(f"Keywords: {', '.join(result.keywords)}")


if __name__ == "__main__":
    main()
