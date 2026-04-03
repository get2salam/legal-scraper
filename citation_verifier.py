#!/usr/bin/env python3
"""
Citation Verifier for Qanoon AI Research Copilot
Prevents hallucination by verifying all citations exist in the database.
"""

import re
import json
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field
from pathlib import Path
from difflib import SequenceMatcher

import chromadb
from chromadb.config import Settings

from enhanced_vectorstore import CHROMADB_PATH, COLLECTION_NAME


@dataclass
class VerificationResult:
    """Result of citation verification."""
    citation: str
    exists: bool
    confidence: float
    matched_citation: Optional[str] = None
    source_text: Optional[str] = None
    warning: Optional[str] = None


@dataclass
class ClaimVerification:
    """Result of claim verification against passages."""
    claim: str
    supported: bool
    support_strength: float  # 0-1
    supporting_passages: List[str]
    warning: Optional[str] = None


@dataclass
class AnswerVerification:
    """Complete verification of an answer."""
    is_valid: bool
    overall_confidence: float
    citation_results: List[VerificationResult]
    claim_results: List[ClaimVerification]
    unsupported_claims: List[str]
    hallucinated_citations: List[str]
    warnings: List[str]
    
    def to_dict(self) -> Dict:
        return {
            'is_valid': self.is_valid,
            'overall_confidence': self.overall_confidence,
            'citation_results': [
                {
                    'citation': r.citation,
                    'exists': r.exists,
                    'confidence': r.confidence,
                    'matched_citation': r.matched_citation,
                    'warning': r.warning
                }
                for r in self.citation_results
            ],
            'unsupported_claims': self.unsupported_claims,
            'hallucinated_citations': self.hallucinated_citations,
            'warnings': self.warnings
        }


class CitationDatabase:
    """Interface to verify citations exist in the database."""
    
    # Pakistani citation patterns
    CITATION_PATTERNS = [
        r'(\d{4})\s+(SCMR)\s+(\d+)',
        r'(\d{4})\s+(PLD)\s+(\w+)\s+(\d+)',
        r'(\d{4})\s+(CLC)\s+(\d+)',
        r'(\d{4})\s+(MLD)\s+(\d+)',
        r'(\d{4})\s+(YLR)\s+(\d+)',
        r'(\d{4})\s+(PCr\.?LJ)\s+(\d+)',
        r'(\d{4})\s+(PTD)\s+(\d+)',
        r'(\d{4})\s+(PLC)\s+(\d+)',
        r'(PLD)\s+(\d{4})\s+(\w+)\s+(\d+)',
    ]
    
    def __init__(self, chromadb_path: Path = CHROMADB_PATH,
                 collection_name: str = COLLECTION_NAME):
        self.client = None
        self.collection = None
        self._citation_cache: Set[str] = set()
        self._initialized = False
        self.chromadb_path = chromadb_path
        self.collection_name = collection_name
    
    def _initialize(self):
        """Lazy initialization of database connection."""
        if self._initialized:
            return
        
        try:
            self.client = chromadb.PersistentClient(
                path=str(self.chromadb_path),
                settings=Settings(anonymized_telemetry=False)
            )
            self.collection = self.client.get_collection(self.collection_name)
            
            # Build citation cache from all documents
            # Get all unique citations
            all_docs = self.collection.get(include=["metadatas"])
            for meta in all_docs['metadatas']:
                if meta and meta.get('citation'):
                    # Normalize and cache
                    citation = meta['citation'].strip().upper()
                    citation = re.sub(r'\s+', ' ', citation)
                    self._citation_cache.add(citation)
            
            self._initialized = True
            print(f"✅ Citation database initialized with {len(self._citation_cache)} unique citations")
            
        except Exception as e:
            print(f"⚠️ Could not initialize citation database: {e}")
            self._initialized = True  # Prevent retry
    
    def normalize_citation(self, citation: str) -> str:
        """Normalize a citation string for comparison."""
        citation = citation.strip().upper()
        citation = re.sub(r'\s+', ' ', citation)
        # Standardize reporter names
        citation = re.sub(r'PCR\.?L\.?J\.?', 'PCRLJ', citation)
        return citation
    
    def citation_exists(self, citation: str) -> Tuple[bool, Optional[str], float]:
        """
        Check if a citation exists in the database.
        
        Returns:
            (exists, matched_citation, confidence)
        """
        self._initialize()
        
        normalized = self.normalize_citation(citation)
        
        # Exact match
        if normalized in self._citation_cache:
            return True, normalized, 1.0
        
        # Fuzzy match
        best_match = None
        best_score = 0.0
        
        for cached_citation in self._citation_cache:
            # Quick prefix check for efficiency
            if normalized[:4] != cached_citation[:4]:
                continue
            
            # Calculate similarity
            score = SequenceMatcher(None, normalized, cached_citation).ratio()
            if score > best_score and score > 0.85:
                best_score = score
                best_match = cached_citation
        
        if best_match:
            return True, best_match, best_score
        
        return False, None, 0.0
    
    def get_citation_text(self, citation: str) -> Optional[str]:
        """Get the text associated with a citation."""
        self._initialize()
        
        if not self.collection:
            return None
        
        normalized = self.normalize_citation(citation)
        
        # Search for documents with this citation
        results = self.collection.get(
            where={"citation": {"$contains": citation.split()[0]}},
            include=["documents", "metadatas"],
            limit=5
        )
        
        for doc, meta in zip(results['documents'], results['metadatas']):
            if meta and self.normalize_citation(meta.get('citation', '')) == normalized:
                return doc
        
        return None


class ClaimExtractor:
    """Extract verifiable claims from text."""
    
    # Patterns that indicate a claim is being made
    CLAIM_INDICATORS = [
        r'held that\b',
        r'ruled that\b',
        r'stated that\b',
        r'observed that\b',
        r'decided that\b',
        r'concluded that\b',
        r'the court\b.*\bfound\b',
        r'according to\b',
        r'it was held\b',
        r'the law is\b',
        r'under.*law\b',
        r'provides that\b',
        r'requires that\b',
        r'mandates that\b',
    ]
    
    def extract_claims(self, text: str) -> List[str]:
        """Extract individual claims from answer text."""
        claims = []
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # Check if sentence makes a legal claim
            is_claim = False
            for pattern in self.CLAIM_INDICATORS:
                if re.search(pattern, sentence, re.IGNORECASE):
                    is_claim = True
                    break
            
            # Also treat sentences with citations as claims
            citation_pattern = r'\b\d{4}\s+(?:SCMR|PLD|CLC|MLD|YLR|PCrLJ|PTD|PLC)\b'
            if re.search(citation_pattern, sentence):
                is_claim = True
            
            if is_claim:
                claims.append(sentence)
        
        return claims


class CitationExtractor:
    """Extract citations from text."""
    
    PATTERNS = [
        r'\b(\d{4}\s+SCMR\s+\d+)\b',
        r'\b(\d{4}\s+PLD\s+\w+\s+\d+)\b',
        r'\b(\d{4}\s+CLC\s+\d+)\b',
        r'\b(\d{4}\s+MLD\s+\d+)\b',
        r'\b(\d{4}\s+YLR\s+\d+)\b',
        r'\b(\d{4}\s+PCr\.?LJ\s+\d+)\b',
        r'\b(\d{4}\s+PTD\s+\d+)\b',
        r'\b(\d{4}\s+PLC\s+\d+)\b',
        r'\b(PLD\s+\d{4}\s+\w+\s+\d+)\b',
    ]
    
    def extract(self, text: str) -> List[str]:
        """Extract all citations from text."""
        citations = []
        
        for pattern in self.PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            citations.extend(matches)
        
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for c in citations:
            normalized = re.sub(r'\s+', ' ', c.upper())
            if normalized not in seen:
                seen.add(normalized)
                unique.append(c)
        
        return unique


class PassageGrounder:
    """Verify claims are grounded in retrieved passages."""
    
    def __init__(self, similarity_threshold: float = 0.3):
        self.threshold = similarity_threshold
    
    def check_grounding(self, claim: str, passages: List[str]) -> Tuple[bool, float, List[str]]:
        """
        Check if a claim is grounded in the passages.
        
        Returns:
            (is_grounded, confidence, supporting_passages)
        """
        if not passages:
            return False, 0.0, []
        
        claim_lower = claim.lower()
        claim_words = set(re.findall(r'\b\w+\b', claim_lower))
        
        # Remove common words
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'that', 'this', 
                     'of', 'in', 'to', 'for', 'on', 'with', 'as', 'by', 'at', 'it',
                     'and', 'or', 'but', 'be', 'have', 'has', 'had', 'do', 'does'}
        claim_words = claim_words - stopwords
        
        supporting = []
        best_score = 0.0
        
        for passage in passages:
            passage_lower = passage.lower()
            passage_words = set(re.findall(r'\b\w+\b', passage_lower))
            
            # Calculate word overlap
            if not claim_words:
                continue
            
            overlap = claim_words & passage_words
            score = len(overlap) / len(claim_words)
            
            # Check for key phrase matches
            # Extract key phrases (3-grams) from claim
            words = claim_lower.split()
            for i in range(len(words) - 2):
                phrase = ' '.join(words[i:i+3])
                if len(phrase) > 10 and phrase in passage_lower:
                    score += 0.2
            
            if score > best_score:
                best_score = score
            
            if score >= self.threshold:
                supporting.append(passage[:200] + '...')
        
        is_grounded = best_score >= self.threshold
        return is_grounded, min(1.0, best_score), supporting


class CitationVerifier:
    """Main citation verification class."""
    
    def __init__(self, chromadb_path: Path = CHROMADB_PATH,
                 collection_name: str = COLLECTION_NAME):
        self.citation_db = CitationDatabase(chromadb_path, collection_name)
        self.citation_extractor = CitationExtractor()
        self.claim_extractor = ClaimExtractor()
        self.passage_grounder = PassageGrounder()
    
    def verify_citation(self, citation: str) -> VerificationResult:
        """Verify a single citation exists in database."""
        exists, matched, confidence = self.citation_db.citation_exists(citation)
        
        warning = None
        if exists and matched != self.citation_db.normalize_citation(citation):
            warning = f"Citation format differs from database: {matched}"
        
        source_text = None
        if exists:
            source_text = self.citation_db.get_citation_text(citation)
        
        return VerificationResult(
            citation=citation,
            exists=exists,
            confidence=confidence,
            matched_citation=matched,
            source_text=source_text[:500] if source_text else None,
            warning=warning
        )
    
    def verify_citations(self, citations: List[str]) -> List[VerificationResult]:
        """Verify multiple citations."""
        return [self.verify_citation(c) for c in citations]
    
    def verify_answer(self, answer: str, 
                      retrieved_passages: List[str]) -> AnswerVerification:
        """
        Fully verify an answer for hallucinations.
        
        Args:
            answer: The generated answer text
            retrieved_passages: List of passage texts used for generation
        
        Returns:
            AnswerVerification with detailed results
        """
        warnings = []
        
        # 1. Extract and verify citations
        extracted_citations = self.citation_extractor.extract(answer)
        citation_results = self.verify_citations(extracted_citations)
        
        hallucinated_citations = [
            r.citation for r in citation_results if not r.exists
        ]
        
        if hallucinated_citations:
            warnings.append(f"Found {len(hallucinated_citations)} potentially hallucinated citations")
        
        # 2. Extract and verify claims
        claims = self.claim_extractor.extract_claims(answer)
        claim_results = []
        unsupported_claims = []
        
        for claim in claims:
            is_grounded, strength, supporting = self.passage_grounder.check_grounding(
                claim, retrieved_passages
            )
            
            result = ClaimVerification(
                claim=claim,
                supported=is_grounded,
                support_strength=strength,
                supporting_passages=supporting,
                warning=None if is_grounded else "Claim may not be fully supported by passages"
            )
            claim_results.append(result)
            
            if not is_grounded:
                unsupported_claims.append(claim)
        
        if unsupported_claims:
            warnings.append(f"Found {len(unsupported_claims)} potentially unsupported claims")
        
        # 3. Calculate overall confidence
        citation_score = 1.0
        if extracted_citations:
            verified_count = sum(1 for r in citation_results if r.exists)
            citation_score = verified_count / len(extracted_citations)
        
        claim_score = 1.0
        if claims:
            supported_count = sum(1 for r in claim_results if r.supported)
            claim_score = supported_count / len(claims)
        
        # Weight: citations more important than claims
        overall_confidence = (citation_score * 0.6) + (claim_score * 0.4)
        
        # Answer is valid if no hallucinated citations and most claims supported
        is_valid = (len(hallucinated_citations) == 0 and 
                   (len(unsupported_claims) / max(1, len(claims))) < 0.3)
        
        return AnswerVerification(
            is_valid=is_valid,
            overall_confidence=round(overall_confidence, 2),
            citation_results=citation_results,
            claim_results=claim_results,
            unsupported_claims=unsupported_claims,
            hallucinated_citations=hallucinated_citations,
            warnings=warnings
        )
    
    def get_confidence_level(self, confidence: float) -> str:
        """Get human-readable confidence level."""
        if confidence >= 0.9:
            return "HIGH"
        elif confidence >= 0.7:
            return "MEDIUM"
        elif confidence >= 0.5:
            return "LOW"
        else:
            return "VERY LOW"


class AnswerSanitizer:
    """Sanitize answers to remove or flag unverified content."""
    
    def __init__(self, verifier: CitationVerifier):
        self.verifier = verifier
    
    def sanitize(self, answer: str, retrieved_passages: List[str],
                 mode: str = 'flag') -> Tuple[str, Dict]:
        """
        Sanitize an answer.
        
        Args:
            answer: The generated answer
            retrieved_passages: Passages used for generation
            mode: 'flag' to add warnings, 'remove' to remove unverified content
        
        Returns:
            (sanitized_answer, verification_report)
        """
        verification = self.verifier.verify_answer(answer, retrieved_passages)
        
        sanitized = answer
        
        if mode == 'flag':
            # Add warnings for hallucinated citations
            for bad_citation in verification.hallucinated_citations:
                sanitized = sanitized.replace(
                    bad_citation,
                    f"[UNVERIFIED: {bad_citation}]"
                )
            
            # Add disclaimer if confidence is low
            if verification.overall_confidence < 0.7:
                sanitized += "\n\n⚠️ *Note: Some claims in this answer may not be fully verified. Please cross-reference with original sources.*"
        
        elif mode == 'remove':
            # Remove sentences with hallucinated citations
            for bad_citation in verification.hallucinated_citations:
                pattern = rf'[^.]*{re.escape(bad_citation)}[^.]*\.'
                sanitized = re.sub(pattern, '', sanitized)
            
            sanitized = re.sub(r'\s+', ' ', sanitized).strip()
        
        return sanitized, verification.to_dict()


def main():
    """Test the citation verifier."""
    print("=" * 70)
    print("Citation Verifier Test")
    print("=" * 70)
    
    # Check if database exists
    if not CHROMADB_PATH.exists():
        print(f"\n❌ Database not found at {CHROMADB_PATH}")
        print("Please run: python enhanced_vectorstore.py --force")
        return
    
    verifier = CitationVerifier()
    
    # Test citation verification
    test_citations = [
        "2024 PLD 1",
        "2024 SCMR 456",
        "2023 CLC 123",
        "1999 SCMR 999",  # May not exist
        "2024 PLD SC 100",
    ]
    
    print("\n📚 Testing Citation Verification:")
    print("-" * 50)
    
    for citation in test_citations:
        result = verifier.verify_citation(citation)
        status = "✅" if result.exists else "❌"
        print(f"{status} {citation}")
        print(f"   Exists: {result.exists}, Confidence: {result.confidence:.2f}")
        if result.matched_citation:
            print(f"   Matched: {result.matched_citation}")
        if result.warning:
            print(f"   ⚠️ {result.warning}")
        print()
    
    # Test answer verification
    test_answer = """
    Under Pakistani law, landlords cannot evict tenants without proper notice. 
    The Supreme Court in 2024 PLD 1 held that tenants have fundamental rights to shelter.
    Additionally, in 2023 SCMR 999 (which may not exist), it was observed that 
    eviction procedures must follow due process.
    """
    
    test_passages = [
        "The right to shelter is fundamental... landlords must provide proper notice",
        "Eviction procedures require due process under the law",
        "Tenants cannot be removed arbitrarily without court order"
    ]
    
    print("\n📝 Testing Answer Verification:")
    print("-" * 50)
    print(f"Answer: {test_answer[:100]}...")
    
    verification = verifier.verify_answer(test_answer, test_passages)
    
    print(f"\n✅ Valid: {verification.is_valid}")
    print(f"📊 Confidence: {verification.overall_confidence} ({verifier.get_confidence_level(verification.overall_confidence)})")
    
    if verification.hallucinated_citations:
        print(f"\n⚠️ Potentially Hallucinated Citations:")
        for c in verification.hallucinated_citations:
            print(f"   - {c}")
    
    if verification.unsupported_claims:
        print(f"\n⚠️ Potentially Unsupported Claims:")
        for c in verification.unsupported_claims[:3]:
            print(f"   - {c[:80]}...")
    
    if verification.warnings:
        print(f"\n⚠️ Warnings:")
        for w in verification.warnings:
            print(f"   - {w}")


if __name__ == "__main__":
    main()
