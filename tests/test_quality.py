"""
Tests for the data quality module.

Covers validation, deduplication, and quality reporting.
"""

import json
import tempfile
from pathlib import Path

import pytest

from legal_scraper.quality.validator import (
    CaseValidator,
    FieldSpec,
    Severity,
    ValidationResult,
    check_text_quality,
    validate_case,
)
from legal_scraper.quality.dedup import (
    ContentFingerprinter,
    DuplicateDetector,
    DuplicatePair,
)
from legal_scraper.quality.reporter import (
    QualityReport,
    QualityReporter,
    generate_quality_report,
)


# ─── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def valid_case():
    """A fully valid case dict."""
    return {
        "id": "2024-SC-001",
        "title": "Smith v. State Department of Revenue",
        "citation": "2024 SC 445",
        "court": "Supreme Court",
        "date": "2024-03-15",
        "year": 2024,
        "judges": ["Justice A. Roberts", "Justice B. Chen"],
        "text": (
            "This is the full text of the judgment. It contains the facts, "
            "arguments, and reasoning of the court in sufficient detail to "
            "constitute a complete legal document. The petitioner filed a "
            "constitutional petition under Article 199 challenging the "
            "impugned order dated 2024-01-10. The respondent appeared and "
            "contested the petition on merits. After hearing both sides, "
            "we find that the petition has merit and is allowed."
        ),
        "headnotes": "Constitutional law — Judicial review — Standard of review",
        "statutes_cited": ["Constitution Article 199", "Civil Procedure Code Section 9"],
    }


@pytest.fixture
def minimal_case():
    """A case with only required fields."""
    return {
        "id": "2023-HC-100",
        "title": "Doe v. City Planning Authority",
        "text": (
            "The petitioner has challenged the decision of the planning "
            "authority. After careful examination of the record, the court "
            "finds the petition has no merit. The decision is affirmed."
        ),
    }


@pytest.fixture
def invalid_case():
    """A case with multiple validation issues."""
    return {
        "id": "",
        "title": "Ab",
        "text": "Short",
        "citation": "no-digits-here",
        "year": "not-a-number",
    }


@pytest.fixture
def sample_cases(valid_case, minimal_case, invalid_case):
    """Collection of varied cases for batch testing."""
    return [valid_case, minimal_case, invalid_case]


# ─── Validator Tests ────────────────────────────────────────────────────


class TestCaseValidator:
    """Tests for CaseValidator."""

    def test_valid_case_passes(self, valid_case):
        result = validate_case(valid_case)
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.completeness_score > 0.8

    def test_minimal_case_passes(self, minimal_case):
        result = validate_case(minimal_case)
        assert result.valid is True
        # Completeness lower due to missing optional fields
        assert 0.3 < result.completeness_score < 0.9

    def test_invalid_case_fails(self, invalid_case):
        result = validate_case(invalid_case)
        assert result.valid is False
        assert len(result.errors) > 0

    def test_missing_required_field(self):
        case = {"title": "Test Case", "text": "Some text " * 50}
        result = validate_case(case)
        assert result.valid is False
        id_errors = [e for e in result.errors if e.field == "id"]
        assert len(id_errors) == 1
        assert "missing" in id_errors[0].message.lower()

    def test_empty_required_field(self):
        case = {"id": "", "title": "Test", "text": "x" * 200}
        result = validate_case(case)
        assert result.valid is False

    def test_type_mismatch_warning(self):
        case = {
            "id": "test-1",
            "title": "Valid Title Here",
            "text": "x" * 200,
            "judges": 12345,  # Should be list or str
        }
        result = validate_case(case)
        judge_issues = [i for i in result.issues if i.field == "judges"]
        assert len(judge_issues) > 0
        assert any("type" in str(i.message).lower() for i in judge_issues)

    def test_strict_mode(self):
        case = {
            "id": "test-1",
            "title": "Valid Title",
            "text": "x" * 200,
            "citation": "no-digits",  # Pattern mismatch
        }
        normal = validate_case(case, strict=False)
        strict = validate_case(case, strict=True)

        # Strict mode should have more (or equal) errors
        assert len(strict.errors) >= len(normal.errors)

    def test_text_too_short(self):
        case = {"id": "test-1", "title": "Valid Title", "text": "Too short"}
        result = validate_case(case)
        text_issues = [i for i in result.issues if i.field == "text"]
        assert any("short" in str(i.message).lower() for i in text_issues)

    def test_citation_pattern(self):
        case = {
            "id": "test-1",
            "title": "Valid Title",
            "text": "x" * 200,
            "citation": "2024 SC 445",
        }
        result = validate_case(case)
        citation_issues = [
            i for i in result.issues
            if i.field == "citation" and i.severity != Severity.INFO
        ]
        assert len(citation_issues) == 0

    def test_unknown_fields_info(self, valid_case):
        valid_case["custom_field"] = "something"
        result = validate_case(valid_case)
        info_issues = [
            i for i in result.issues
            if i.field == "custom_field" and i.severity == Severity.INFO
        ]
        assert len(info_issues) == 1

    def test_internal_fields_ignored(self, valid_case):
        valid_case["_scraped_at"] = "2024-01-01"
        valid_case["_internal_note"] = "test"
        result = validate_case(valid_case)
        internal_issues = [
            i for i in result.issues
            if i.field.startswith("_")
        ]
        assert len(internal_issues) == 0

    def test_completeness_score_range(self, sample_cases):
        validator = CaseValidator()
        for case in sample_cases:
            result = validator.validate(case)
            assert 0.0 <= result.completeness_score <= 1.0

    def test_field_scores_present(self, valid_case):
        result = validate_case(valid_case)
        assert "id" in result.field_scores
        assert "title" in result.field_scores
        assert "text" in result.field_scores

    def test_batch_validate(self, sample_cases):
        validator = CaseValidator()
        results = validator.validate_batch(sample_cases)
        assert len(results) == 3
        assert all(isinstance(r, ValidationResult) for r in results)

    def test_custom_field_spec(self):
        validator = CaseValidator()
        validator.add_field(
            FieldSpec(
                name="jurisdiction",
                required=True,
                field_type=str,
                min_length=2,
                weight=0.8,
            )
        )
        case = {"id": "test", "title": "Title", "text": "x" * 200}
        result = validator.validate(case)
        assert any(
            i.field == "jurisdiction" and i.severity == Severity.ERROR
            for i in result.issues
        )

    def test_remove_field_spec(self):
        validator = CaseValidator()
        validator.remove_field("headnotes")
        case = {"id": "test", "title": "Title", "text": "x" * 200}
        result = validator.validate(case)
        assert "headnotes" not in result.field_scores

    def test_to_dict(self, valid_case):
        result = validate_case(valid_case)
        d = result.to_dict()
        assert "case_id" in d
        assert "valid" in d
        assert "completeness_score" in d
        assert isinstance(d["issues"], list)


class TestCheckTextQuality:
    """Tests for check_text_quality function."""

    def test_empty_text(self):
        result = check_text_quality("")
        assert result["quality"] == "empty"
        assert result["score"] == 0.0

    def test_good_text(self):
        text = "This is a well-formed legal document. " * 50
        result = check_text_quality(text)
        assert result["quality_score"] > 0.7
        assert result["word_count"] > 200
        assert result["sentence_count"] > 0

    def test_short_text_penalty(self):
        result = check_text_quality("Very short text here.")
        assert result["quality_score"] < 0.7

    def test_html_artifact_detection(self):
        text = "Normal text <div class='content'>with HTML tags</div> inside. " * 10
        result = check_text_quality(text)
        assert result["artifacts"]["html_tags"] is True

    def test_excessive_whitespace_detection(self):
        text = "Normal text" + " " * 20 + "with gaps. " * 20
        result = check_text_quality(text)
        assert result["artifacts"]["excessive_whitespace"] is True

    def test_truncated_detection(self):
        text = "This document was cut off mid sentence..."
        result = check_text_quality(text)
        assert result["artifacts"]["truncated"] is True

    def test_boilerplate_detection(self):
        text = (
            "All rights reserved. Copyright 2024. Terms of use apply. "
            "Disclaimer: this content is provided as-is. Click here for more. "
        ) * 5
        result = check_text_quality(text)
        assert result["artifacts"]["boilerplate_heavy"] is True


# ─── Deduplication Tests ────────────────────────────────────────────────


class TestContentFingerprinter:
    """Tests for SimHash fingerprinting."""

    def test_identical_texts(self):
        fp = ContentFingerprinter()
        text = "The court hereby grants the motion for summary judgment."
        h1 = fp.fingerprint(text)
        h2 = fp.fingerprint(text)
        assert h1 == h2
        assert fp.similarity(h1, h2) == 1.0

    def test_similar_texts_high_similarity(self):
        fp = ContentFingerprinter()
        text1 = (
            "The Supreme Court held that the defendant's rights were "
            "violated under the Fourth Amendment to the Constitution."
        )
        text2 = (
            "The Supreme Court held that the defendant's rights were "
            "violated under the Fourth Ammendment to the Constitution."  # typo
        )
        h1 = fp.fingerprint(text1)
        h2 = fp.fingerprint(text2)
        similarity = fp.similarity(h1, h2)
        assert similarity > 0.7

    def test_different_texts_low_similarity(self):
        fp = ContentFingerprinter()
        text1 = "Criminal law case about theft and burglary at night."
        text2 = "Tax dispute regarding corporate income assessment rates."
        h1 = fp.fingerprint(text1)
        h2 = fp.fingerprint(text2)
        similarity = fp.similarity(h1, h2)
        assert similarity < 0.9

    def test_empty_text(self):
        fp = ContentFingerprinter()
        h = fp.fingerprint("")
        assert h == 0

    def test_normalization(self):
        fp = ContentFingerprinter(normalize=True)
        h1 = fp.fingerprint("  Hello   WORLD!  ")
        h2 = fp.fingerprint("hello world")
        assert h1 == h2

    def test_no_normalization(self):
        fp = ContentFingerprinter(normalize=False)
        h1 = fp.fingerprint("Hello World")
        h2 = fp.fingerprint("hello world")
        # Without normalization, these should differ
        assert h1 != h2

    def test_64_bit_hash(self):
        fp = ContentFingerprinter(hash_bits=64)
        h = fp.fingerprint("Test document content for fingerprinting")
        assert isinstance(h, int)
        assert h < 2**64

    def test_128_bit_hash(self):
        fp = ContentFingerprinter(hash_bits=128)
        h = fp.fingerprint("Test document content for fingerprinting")
        assert isinstance(h, int)
        assert h < 2**128

    def test_invalid_hash_bits(self):
        with pytest.raises(ValueError):
            ContentFingerprinter(hash_bits=256)

    def test_both_zero_similarity(self):
        fp = ContentFingerprinter()
        assert fp.similarity(0, 0) == 1.0

    def test_one_zero_similarity(self):
        fp = ContentFingerprinter()
        h = fp.fingerprint("Some text")
        assert fp.similarity(0, h) == 0.0
        assert fp.similarity(h, 0) == 0.0


class TestDuplicateDetector:
    """Tests for DuplicateDetector."""

    def test_exact_duplicates(self):
        detector = DuplicateDetector()
        text = "Identical case text for both documents. " * 20
        detector.add("case-1", {"id": "case-1", "text": text})
        detector.add("case-2", {"id": "case-2", "text": text})

        dups = detector.find_duplicates()
        assert len(dups) >= 1
        exact = [d for d in dups if d.method == "exact_hash"]
        assert len(exact) == 1
        assert exact[0].similarity == 1.0

    def test_near_duplicates(self):
        detector = DuplicateDetector(threshold=0.7)
        base = "The court considered the evidence and found the petition meritorious. " * 20
        detector.add("case-1", {"id": "case-1", "text": base})
        detector.add(
            "case-2",
            {"id": "case-2", "text": base.replace("meritorious", "without merit")},
        )
        detector.add(
            "case-3",
            {"id": "case-3", "text": "Completely different tax case about corporate rates. " * 20},
        )

        dups = detector.find_duplicates()
        # case-1 and case-2 should be detected
        pair_ids = {(d.id_a, d.id_b) for d in dups}
        has_near_dup = any(
            ("case-1" in ids and "case-2" in ids) for ids in [
                (d.id_a, d.id_b) for d in dups
            ]
        )
        assert has_near_dup

    def test_no_duplicates(self):
        detector = DuplicateDetector(threshold=0.95)
        detector.add("case-1", {"text": "Criminal law and penalty assessment case. " * 30})
        detector.add("case-2", {"text": "Environmental protection regulatory matter. " * 30})

        dups = detector.find_duplicates()
        assert len(dups) == 0

    def test_add_batch(self):
        detector = DuplicateDetector()
        cases = [
            {"id": f"case-{i}", "text": f"Case text number {i}. " * 20}
            for i in range(5)
        ]
        detector.add_batch(cases)
        assert detector.stats()["total_indexed"] == 5

    def test_find_similar(self):
        detector = DuplicateDetector()
        base = "Constitutional rights under due process clause. " * 20
        detector.add("target", {"text": base})
        detector.add("similar", {"text": base + " Extra sentence."})
        detector.add("different", {"text": "Tax law about corporate entities. " * 20})

        results = detector.find_similar("target", top_k=2)
        assert len(results) == 2
        assert results[0].id_b == "similar"
        assert results[0].similarity > results[1].similarity

    def test_find_similar_missing_case(self):
        detector = DuplicateDetector()
        with pytest.raises(KeyError):
            detector.find_similar("nonexistent")

    def test_stats(self):
        detector = DuplicateDetector(threshold=0.9)
        text = "Identical. " * 50
        detector.add("a", {"text": text})
        detector.add("b", {"text": text})
        detector.add("c", {"text": "Different text entirely. " * 50})

        stats = detector.stats()
        assert stats["total_indexed"] == 3
        assert stats["exact_duplicate_groups"] == 1
        assert stats["threshold"] == 0.9

    def test_clear(self):
        detector = DuplicateDetector()
        detector.add("case-1", {"text": "Some text"})
        assert detector.stats()["total_indexed"] == 1
        detector.clear()
        assert detector.stats()["total_indexed"] == 0

    def test_custom_text_field(self):
        detector = DuplicateDetector()
        text = "Same content in judgment field. " * 20
        detector.add("a", {"judgment": text}, text_field="judgment")
        detector.add("b", {"judgment": text}, text_field="judgment")

        dups = detector.find_duplicates()
        assert len(dups) >= 1


# ─── Reporter Tests ─────────────────────────────────────────────────────


class TestQualityReporter:
    """Tests for QualityReporter."""

    def test_analyze_mixed_quality(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)

        assert report.total_cases == 3
        assert report.valid_cases >= 1
        assert report.invalid_cases >= 1
        assert 0 <= report.avg_completeness <= 1.0
        assert len(report.field_completeness) > 0

    def test_analyze_empty(self):
        reporter = QualityReporter()
        report = reporter.analyze([])
        assert report.total_cases == 0
        assert report.pass_rate == 0.0

    def test_report_to_dict(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)
        d = report.to_dict()

        assert "summary" in d
        assert "field_completeness" in d
        assert "top_issues" in d
        assert d["summary"]["total_cases"] == 3

    def test_report_to_json(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)
        j = report.to_json()

        parsed = json.loads(j)
        assert parsed["summary"]["total_cases"] == 3

    def test_report_save_load(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)

        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w"
        ) as f:
            report.save(f.name)
            saved_path = Path(f.name)

        try:
            with open(saved_path) as f:
                loaded = json.load(f)
            assert loaded["summary"]["total_cases"] == 3
        finally:
            saved_path.unlink()

    def test_summary_text(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)
        text = report.summary_text()

        assert "Quality Report" in text
        assert "Total cases" in text
        assert "Field Completeness" in text

    def test_completeness_distribution(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)

        total_in_buckets = sum(report.completeness_distribution.values())
        assert total_in_buckets == len(sample_cases)

    def test_severity_counts(self, sample_cases):
        reporter = QualityReporter()
        report = reporter.analyze(sample_cases)

        assert "error" in report.severity_counts or "warning" in report.severity_counts

    def test_pass_rate(self, valid_case):
        reporter = QualityReporter()
        cases = [valid_case.copy() for _ in range(10)]
        report = reporter.analyze(cases)
        assert report.pass_rate == 1.0

    def test_compare_reports(self):
        reporter = QualityReporter()

        before_cases = [
            {"id": f"c{i}", "title": f"Case {i}", "text": "Short"}
            for i in range(5)
        ]
        after_cases = [
            {
                "id": f"c{i}",
                "title": f"Case {i}",
                "text": "Much longer text content " * 50,
                "court": "Supreme Court",
                "year": 2024,
            }
            for i in range(10)
        ]

        report_before = reporter.analyze(before_cases)
        report_after = reporter.analyze(after_cases)
        comparison = reporter.compare_reports(report_before, report_after)

        assert comparison["cases"]["delta"] == 5
        assert comparison["completeness"]["delta"] > 0
        assert "field_changes" in comparison


class TestGenerateQualityReport:
    """Tests for generate_quality_report convenience function."""

    def test_from_directory(self, valid_case, minimal_case):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write some case files
            for i, case in enumerate([valid_case, minimal_case]):
                path = Path(tmpdir) / f"case_{i}.json"
                with open(path, "w") as f:
                    json.dump(case, f)

            report = generate_quality_report(tmpdir)
            assert report.total_cases == 2
            assert report.valid_cases >= 1

    def test_from_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report = generate_quality_report(tmpdir)
            assert report.total_cases == 0

    def test_with_output_file(self, valid_case):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write case
            case_path = Path(tmpdir) / "case.json"
            with open(case_path, "w") as f:
                json.dump(valid_case, f)

            # Generate with output
            output = Path(tmpdir) / "reports" / "quality.json"
            report = generate_quality_report(tmpdir, output=output)

            assert output.exists()
            with open(output) as f:
                loaded = json.load(f)
            assert loaded["summary"]["total_cases"] == 1

    def test_handles_corrupt_json(self, valid_case):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Valid file
            with open(Path(tmpdir) / "good.json", "w") as f:
                json.dump(valid_case, f)

            # Corrupt file
            with open(Path(tmpdir) / "bad.json", "w") as f:
                f.write("{invalid json content")

            report = generate_quality_report(tmpdir)
            assert report.total_cases == 1  # Only the valid one
