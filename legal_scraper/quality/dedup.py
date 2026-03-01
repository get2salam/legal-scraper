"""
Near-duplicate detection for scraped legal documents.

Uses content fingerprinting (SimHash) to efficiently detect near-duplicate
cases even when there are minor formatting or OCR differences.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class DuplicatePair:
    """A pair of near-duplicate documents."""

    id_a: str
    id_b: str
    similarity: float
    method: str

    def __str__(self) -> str:
        return f"{self.id_a} <-> {self.id_b} ({self.similarity:.1%} similar, {self.method})"


class ContentFingerprinter:
    """
    Generate content fingerprints for efficient duplicate detection.

    Uses SimHash (Charikar's technique) which preserves locality:
    similar documents produce similar hashes, enabling fast nearest-
    neighbor search with Hamming distance.

    Usage:
        fp = ContentFingerprinter()
        hash_a = fp.fingerprint("This is document A text...")
        hash_b = fp.fingerprint("This is document A text with typo...")
        similarity = fp.similarity(hash_a, hash_b)  # ~0.95
    """

    def __init__(
        self,
        hash_bits: int = 128,
        ngram_size: int = 3,
        normalize: bool = True,
    ):
        """
        Args:
            hash_bits: Number of bits in the fingerprint (64 or 128)
            ngram_size: Size of character n-grams for shingling
            normalize: Whether to normalize text before fingerprinting
        """
        if hash_bits not in (64, 128):
            raise ValueError("hash_bits must be 64 or 128")
        self.hash_bits = hash_bits
        self.ngram_size = ngram_size
        self.normalize = normalize

    def fingerprint(self, text: str) -> int:
        """
        Compute SimHash fingerprint of text.

        Args:
            text: Document text

        Returns:
            Integer fingerprint
        """
        if self.normalize:
            text = self._normalize(text)

        if not text:
            return 0

        # Generate shingles (character n-grams)
        shingles = self._shingle(text)

        if not shingles:
            return 0

        # SimHash: accumulate weighted bit vectors
        vector = [0] * self.hash_bits

        for shingle in shingles:
            h = self._hash_shingle(shingle)
            for i in range(self.hash_bits):
                if h & (1 << i):
                    vector[i] += 1
                else:
                    vector[i] -= 1

        # Convert to binary fingerprint
        fingerprint = 0
        for i in range(self.hash_bits):
            if vector[i] > 0:
                fingerprint |= 1 << i

        return fingerprint

    def similarity(self, hash_a: int, hash_b: int) -> float:
        """
        Compute similarity between two fingerprints.

        Uses normalized Hamming distance.

        Args:
            hash_a: First fingerprint
            hash_b: Second fingerprint

        Returns:
            Similarity score (0.0 to 1.0)
        """
        if hash_a == 0 and hash_b == 0:
            return 1.0
        if hash_a == 0 or hash_b == 0:
            return 0.0

        # Count differing bits (Hamming distance)
        xor = hash_a ^ hash_b
        differing_bits = bin(xor).count("1")

        return 1.0 - (differing_bits / self.hash_bits)

    def _normalize(self, text: str) -> str:
        """Normalize text for fingerprinting."""
        text = text.lower()
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text)
        # Remove punctuation (keep alphanumeric + spaces)
        text = re.sub(r"[^\w\s]", "", text)
        return text.strip()

    def _shingle(self, text: str) -> list[str]:
        """Generate character n-grams."""
        if len(text) < self.ngram_size:
            return [text]
        return [text[i : i + self.ngram_size] for i in range(len(text) - self.ngram_size + 1)]

    def _hash_shingle(self, shingle: str) -> int:
        """Hash a single shingle to hash_bits bits."""
        h = hashlib.md5(shingle.encode("utf-8")).hexdigest()
        return int(h, 16) % (2**self.hash_bits)


class DuplicateDetector:
    """
    Detect near-duplicate cases in a collection.

    Supports both exact and near-duplicate detection using
    content fingerprinting, citation matching, and title similarity.

    Usage:
        detector = DuplicateDetector(threshold=0.85)
        detector.add("case_1", case_1_dict)
        detector.add("case_2", case_2_dict)
        duplicates = detector.find_duplicates()
    """

    def __init__(
        self,
        threshold: float = 0.85,
        hash_bits: int = 128,
        ngram_size: int = 3,
    ):
        """
        Args:
            threshold: Similarity threshold for duplicate detection (0-1)
            hash_bits: Bits for SimHash fingerprint
            ngram_size: Character n-gram size
        """
        self.threshold = threshold
        self.fingerprinter = ContentFingerprinter(hash_bits, ngram_size)
        self._fingerprints: dict[str, int] = {}
        self._exact_hashes: dict[str, str] = {}  # exact content hash -> id
        self._cases: dict[str, dict] = {}

    def add(self, case_id: str, case: dict, text_field: str = "text"):
        """
        Add a case to the detection index.

        Args:
            case_id: Unique case identifier
            case: Case data dict
            text_field: Key containing the case text
        """
        text = case.get(text_field, "")
        self._cases[case_id] = case

        # Exact content hash
        if text:
            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            self._exact_hashes[case_id] = content_hash

        # SimHash fingerprint
        self._fingerprints[case_id] = self.fingerprinter.fingerprint(text)

    def add_batch(self, cases: list[dict], id_field: str = "id", text_field: str = "text"):
        """Add multiple cases at once."""
        for case in cases:
            case_id = case.get(id_field)
            if case_id:
                self.add(str(case_id), case, text_field)

    def find_duplicates(self) -> list[DuplicatePair]:
        """
        Find all duplicate pairs in the index.

        Returns:
            List of DuplicatePair objects, sorted by similarity (desc)
        """
        duplicates: list[DuplicatePair] = []
        seen: set[tuple[str, str]] = set()

        # Phase 1: Exact duplicates (O(n) via hash grouping)
        hash_groups: dict[str, list[str]] = defaultdict(list)
        for case_id, content_hash in self._exact_hashes.items():
            hash_groups[content_hash].append(case_id)

        for group in hash_groups.values():
            if len(group) > 1:
                for i in range(len(group)):
                    for j in range(i + 1, len(group)):
                        pair_key = (
                            min(group[i], group[j]),
                            max(group[i], group[j]),
                        )
                        if pair_key not in seen:
                            seen.add(pair_key)
                            duplicates.append(
                                DuplicatePair(
                                    id_a=group[i],
                                    id_b=group[j],
                                    similarity=1.0,
                                    method="exact_hash",
                                )
                            )

        # Phase 2: Near-duplicates via SimHash comparison
        # For small-medium datasets, pairwise is fine.
        # For large datasets (>100K), use LSH bucketing.
        ids = list(self._fingerprints.keys())
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                pair_key = (min(ids[i], ids[j]), max(ids[i], ids[j]))
                if pair_key in seen:
                    continue

                sim = self.fingerprinter.similarity(
                    self._fingerprints[ids[i]],
                    self._fingerprints[ids[j]],
                )
                if sim >= self.threshold:
                    seen.add(pair_key)
                    duplicates.append(
                        DuplicatePair(
                            id_a=ids[i],
                            id_b=ids[j],
                            similarity=sim,
                            method="simhash",
                        )
                    )

        duplicates.sort(key=lambda d: d.similarity, reverse=True)
        return duplicates

    def find_similar(self, case_id: str, top_k: int = 10) -> list[DuplicatePair]:
        """
        Find most similar cases to a given case.

        Args:
            case_id: Target case ID
            top_k: Number of results

        Returns:
            List of DuplicatePair objects
        """
        if case_id not in self._fingerprints:
            raise KeyError(f"Case {case_id} not in index")

        target_fp = self._fingerprints[case_id]
        similarities: list[DuplicatePair] = []

        for other_id, other_fp in self._fingerprints.items():
            if other_id == case_id:
                continue
            sim = self.fingerprinter.similarity(target_fp, other_fp)
            similarities.append(
                DuplicatePair(
                    id_a=case_id,
                    id_b=other_id,
                    similarity=sim,
                    method="simhash",
                )
            )

        similarities.sort(key=lambda d: d.similarity, reverse=True)
        return similarities[:top_k]

    def stats(self) -> dict:
        """Get index statistics."""
        # Count exact duplicate groups
        hash_groups: dict[str, list[str]] = defaultdict(list)
        for case_id, content_hash in self._exact_hashes.items():
            hash_groups[content_hash].append(case_id)
        exact_groups = sum(1 for g in hash_groups.values() if len(g) > 1)

        return {
            "total_indexed": len(self._fingerprints),
            "exact_duplicate_groups": exact_groups,
            "threshold": self.threshold,
            "hash_bits": self.fingerprinter.hash_bits,
        }

    def clear(self):
        """Clear the detection index."""
        self._fingerprints.clear()
        self._exact_hashes.clear()
        self._cases.clear()
