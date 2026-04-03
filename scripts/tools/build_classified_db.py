#!/usr/bin/env python3
"""
Build the classified cases database from individual reporter JSON files.
Processes CLC, MLD, PCrLJ, PLC, PLD, PTD, SCMR, YLR directories.
"""

import json
from pathlib import Path
from collections import Counter
from outcome_classifier import OutcomeClassifier
from claim_extractor import ClaimExtractor

DATA_DIR = Path("data_v2")
REPORTERS = ["CLC", "MLD", "PCrLJ", "PLC", "PLD", "PTD", "SCMR", "YLR"]
OUTPUT_FILE = DATA_DIR / "cases_classified.jsonl"


def find_json_files() -> list:
    """Find all individual JSON case files."""
    files = []
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if reporter_dir.exists():
            files.extend(reporter_dir.rglob("*.json"))
    return files


def build_database():
    """Build the classified cases database."""
    print("Loading classifiers...", flush=True)
    classifier = OutcomeClassifier()
    print("Outcome classifier loaded", flush=True)
    extractor = ClaimExtractor()
    print("Claim extractor loaded", flush=True)
    
    files = find_json_files()
    print(f"Found {len(files)} case files to process", flush=True)
    
    outcome_counts = Counter()
    processed = 0
    errors = 0
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        for i, filepath in enumerate(files):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    case = json.load(f)
                
                # Classify outcome
                result = classifier.classify(
                    judgment_text=case.get('judgment_clean', ''),
                    title=case.get('title', ''),
                    headnotes=case.get('headnotes', ''),
                    judgment_html=case.get('judgment', '')
                )
                
                # Extract claims
                claims = extractor.extract_from_case(case)
                
                # Add classification results
                case['outcome'] = result.outcome.value
                case['outcome_confidence'] = result.confidence
                case['outcome_patterns'] = result.matched_patterns
                case['case_type'] = result.case_type
                case['provision_keys'] = claims.get_provision_keys()
                case['claims'] = claims.to_dict()
                
                # Add reporter info
                case['reporter'] = filepath.parent.parent.name
                
                outcome_counts[result.outcome.value] += 1
                processed += 1
                
                outfile.write(json.dumps(case, ensure_ascii=False) + '\n')
                
            except Exception as e:
                errors += 1
                print(f"Error processing {filepath}: {e}")
            
            if (i + 1) % 200 == 0:
                print(f"Processed {i + 1}/{len(files)} files...")
    
    print(f"\n=== Build Complete ===")
    print(f"Total processed: {processed}")
    print(f"Errors: {errors}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"\n--- Outcome Distribution ---")
    total = sum(outcome_counts.values())
    for outcome, count in sorted(outcome_counts.items(), key=lambda x: -x[1]):
        print(f"  {outcome}: {count} ({count/total*100:.1f}%)")


if __name__ == '__main__':
    build_database()
