#!/usr/bin/env python3
"""
Legal Glossary Extractor
========================
Extract and define common legal terms found in Pakistani case law.
"""

import json
import re
from collections import Counter
from pathlib import Path

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"

# Common Pakistani/Commonwealth legal terms with definitions
LEGAL_TERMS = {
    # Latin legal maxims
    "ratio decidendi": "The legal principle or reasoning that forms the basis of a court's decision",
    "obiter dictum": "Remarks or observations made by a judge that are not essential to the decision",
    "stare decisis": "The doctrine of following precedent; 'to stand by things decided'",
    "res judicata": "A matter already judged; prevents re-litigation of the same issue",
    "ultra vires": "Beyond the legal power or authority of a body",
    "intra vires": "Within the legal power or authority of a body",
    "prima facie": "At first appearance; sufficient to establish a fact unless rebutted",
    "inter alia": "Among other things",
    "ipso facto": "By the fact itself; by that very fact",
    "mala fide": "In bad faith; with dishonest intent",
    "bona fide": "In good faith; genuine",
    "ex parte": "On behalf of one party only; without notice to other party",
    "suo motu": "On its own motion; court acting without application from parties",
    "locus standi": "The right to bring an action or be heard in court",
    "mens rea": "Guilty mind; criminal intent required for most crimes",
    "actus reus": "Guilty act; the physical element of a crime",
    
    # Pakistani constitutional terms
    "fundamental rights": "Basic rights guaranteed under Part II of the Constitution",
    "writ petition": "Application to High Court under Article 199 for enforcement of rights",
    "constitutional petition": "Application challenging validity of law or government action",
    "intra-court appeal": "Appeal within the same court (typically High Court)",
    "leave to appeal": "Permission required to appeal to Supreme Court under Article 185",
    "civil petition for leave to appeal": "Application seeking Supreme Court's permission to hear appeal",
    "human rights case": "Case brought under Supreme Court's original jurisdiction (Article 184(3))",
    
    # Common legal procedures
    "remand": "Sending a case back to lower court for reconsideration",
    "stay order": "Court order temporarily stopping an action or proceeding",
    "interim relief": "Temporary remedy granted pending final decision",
    "ad interim": "In the meantime; temporary order",
    "decree": "Formal court judgment in civil matters",
    "judgment": "Court's reasoned decision on the merits",
    "order": "Court's direction on procedural or interlocutory matters",
    "conviction": "Finding of guilt in criminal case",
    "acquittal": "Finding of not guilty; discharge of accused",
    "bail": "Temporary release of accused pending trial",
    "anticipatory bail": "Bail granted before arrest in anticipation of accusation",
    "cognizable offence": "Offence for which police can arrest without warrant",
    "non-cognizable": "Offence requiring court permission for arrest",
    "FIR": "First Information Report - initial crime report to police",
    "challan": "Police report/charge sheet submitted to court",
    
    # Pakistani specific
    "National Accountability Bureau": "Anti-corruption body investigating public officials",
    "NAB": "National Accountability Bureau",
    "Anti-Terrorism Court": "Special court for terrorism-related offences",
    "ATC": "Anti-Terrorism Court",
    "Federal Shariat Court": "Court determining if laws conform to Islamic injunctions",
    "Qanun-e-Shahadat": "Law of Evidence Order, 1984 (Islamic evidence law)",
    "hudood": "Islamic criminal punishments for specific offences",
    "qisas": "Islamic law of retaliation (eye for an eye)",
    "diyat": "Blood money; compensation for homicide or injury",
    "tazir": "Discretionary punishment under Islamic law",
    "fasakh": "Judicial dissolution of marriage",
    "khula": "Wife-initiated divorce in exchange for consideration",
    "talaq": "Husband's right to divorce",
}


def extract_terms_from_text(text):
    """Find legal terms in text."""
    if not text:
        return []
    
    found_terms = []
    text_lower = text.lower()
    
    for term in LEGAL_TERMS.keys():
        # Check for term (case insensitive, word boundary)
        pattern = r'\b' + re.escape(term.lower()) + r'\b'
        if re.search(pattern, text_lower):
            found_terms.append(term)
    
    return found_terms


def analyze_term_frequency(cases):
    """Analyze frequency of legal terms across all cases."""
    term_counts = Counter()
    term_examples = {}  # Store example cases for each term
    
    for case in cases:
        case_id = case.get("id", "")
        title = case.get("title", "")
        judgment = case.get("judgment", case.get("text", ""))
        headnotes = case.get("headnotes", "")
        
        full_text = f"{headnotes}\n{judgment}"
        found = extract_terms_from_text(full_text)
        
        for term in found:
            term_counts[term] += 1
            if term not in term_examples:
                term_examples[term] = {"case_id": case_id, "title": title}
    
    return term_counts, term_examples


def load_all_cases():
    """Load all cases from JSONL files."""
    cases = []
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        cases.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return cases


def generate_glossary(term_counts, term_examples):
    """Generate a formatted glossary."""
    print("=" * 70)
    print("  PAKISTANI LEGAL GLOSSARY")
    print("  (Terms extracted from case law)")
    print("=" * 70)
    
    # Group by category
    categories = {
        "Latin Maxims": ["ratio decidendi", "obiter dictum", "stare decisis", "res judicata",
                        "ultra vires", "intra vires", "prima facie", "inter alia", "ipso facto",
                        "mala fide", "bona fide", "ex parte", "suo motu", "locus standi",
                        "mens rea", "actus reus"],
        "Constitutional Law": ["fundamental rights", "writ petition", "constitutional petition",
                              "intra-court appeal", "leave to appeal", "civil petition for leave to appeal",
                              "human rights case"],
        "Procedure": ["remand", "stay order", "interim relief", "ad interim", "decree",
                     "judgment", "order", "conviction", "acquittal", "bail", "anticipatory bail",
                     "cognizable offence", "non-cognizable", "FIR", "challan"],
        "Islamic Law": ["Qanun-e-Shahadat", "hudood", "qisas", "diyat", "tazir",
                       "fasakh", "khula", "talaq"],
        "Institutions": ["National Accountability Bureau", "NAB", "Anti-Terrorism Court",
                        "ATC", "Federal Shariat Court"],
    }
    
    for category, terms in categories.items():
        found_in_category = [(t, term_counts.get(t, 0)) for t in terms if term_counts.get(t, 0) > 0]
        
        if found_in_category:
            print(f"\n{'=' * 70}")
            print(f"  {category.upper()}")
            print("=" * 70)
            
            for term, count in sorted(found_in_category, key=lambda x: x[1], reverse=True):
                definition = LEGAL_TERMS.get(term, "")
                example = term_examples.get(term, {})
                
                print(f"\n  {term.upper()} ({count} occurrences)")
                print(f"  Definition: {definition}")
                if example:
                    print(f"  Example: {example.get('title', 'N/A')}")
    
    print("\n" + "=" * 70)


def save_glossary_json(term_counts, term_examples):
    """Save glossary as JSON."""
    glossary = []
    for term, count in term_counts.most_common():
        glossary.append({
            "term": term,
            "definition": LEGAL_TERMS.get(term, ""),
            "frequency": count,
            "example_case": term_examples.get(term, {}),
        })
    
    output_path = DATA_DIR / "legal_glossary.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(glossary, f, indent=2, ensure_ascii=False)
    
    print(f"\nGlossary saved to: {output_path}")


def main():
    print("Loading cases...")
    cases = load_all_cases()
    
    if not cases:
        print("No cases found!")
        return
    
    print(f"Loaded {len(cases)} cases. Extracting legal terms...")
    
    term_counts, term_examples = analyze_term_frequency(cases)
    
    print(f"Found {len(term_counts)} unique legal terms\n")
    
    generate_glossary(term_counts, term_examples)
    save_glossary_json(term_counts, term_examples)


if __name__ == "__main__":
    main()
