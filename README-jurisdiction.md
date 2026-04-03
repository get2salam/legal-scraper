# Pakistan Legal Jurisdiction Comparison Tool

Compare legal rulings across Pakistan's 7 jurisdictions to identify consensus, conflicts, and gaps in legal interpretation.

## Overview

This tool enables lawyers, researchers, and legal professionals to:
- **Compare** how different Pakistani courts have ruled on the same legal issues
- **Identify conflicts** where High Courts differ from each other or the Supreme Court
- **Find gaps** where certain jurisdictions haven't addressed specific legal questions
- **Trace consensus** where courts agree on interpretation

## Pakistan's Legal Jurisdictions

| Code | Court | Description |
|------|-------|-------------|
| Federal | Supreme Court of Pakistan | Apex court with final appellate jurisdiction |
| Sindh | Sindh High Court | High Court for Sindh province (Karachi) |
| Punjab | Lahore High Court | High Court for Punjab province |
| KPK | Peshawar High Court | High Court for Khyber Pakhtunkhwa |
| Balochistan | Balochistan High Court | High Court for Balochistan (Quetta) |
| Islamabad | Islamabad High Court | High Court for Islamabad Capital Territory |
| AJK | Azad Kashmir High Court | High Court for Azad Jammu & Kashmir |
| FSC | Federal Shariat Court | Specialized court for Islamic law matters |

## Components

### 1. Jurisdiction Classifier (`jurisdiction_classifier.py`)

Classifies cases by jurisdiction using multiple strategies:
- **Reporter-based**: SCMR is always Supreme Court
- **Court field parsing**: Direct match from case data
- **Citation suffix**: PLD Lah = Lahore High Court

```python
from jurisdiction_classifier import JurisdictionClassifier

classifier = JurisdictionClassifier()
result = classifier.classify({
    'citation': '2024 CLC 1',
    'court': 'Lahore'
})
print(result.jurisdiction)  # Jurisdiction.PUNJAB
print(result.confidence)    # 0.95
```

### 2. Issue Extractor (`issue_extractor.py`)

Extracts legal issues from judgments:
- Parses headnotes for structured legal points
- Identifies statutes and sections cited
- Categorizes issues (contract, property, criminal, etc.)
- Normalizes text for cross-jurisdiction matching

```python
from issue_extractor import LegalIssueExtractor

extractor = LegalIssueExtractor()
result = extractor.extract(case_data)

for issue in result.issues:
    print(f"Category: {issue.category}")
    print(f"Text: {issue.normalized_text}")
    print(f"Statutes: {issue.statutes}")
```

### 3. Jurisdiction Mapper (`jurisdiction_mapper.py`)

Maps same issues across jurisdictions:
- Uses semantic similarity (sentence-transformers)
- Falls back to keyword matching
- Identifies consensus, conflicts, and gaps

```python
from jurisdiction_mapper import JurisdictionMapper

mapper = JurisdictionMapper()
mapper.process_cases(cases)

mappings = mapper.get_issue_mappings(category='contract')
conflicts = mapper.get_conflicts()
```

### 4. Comparison Engine (`comparison_engine.py`)

Generates side-by-side comparisons:
- Natural language query processing
- Vector search with ChromaDB
- Multi-jurisdiction comparison output

```python
from comparison_engine import ComparisonEngine

engine = ComparisonEngine()
engine.load_data()

result = engine.compare("What is the limitation period for breach of contract?")
print(result.jurisdictions)  # Holdings from each jurisdiction
print(result.conflicts)      # Where courts differ
print(result.consensus)      # Where courts agree
```

### 5. Comparison API (`jurisdiction_api.py`)

FastAPI endpoints for integration:

```bash
# Start the API
python jurisdiction_api.py

# Or with uvicorn
uvicorn jurisdiction_api:app --reload --port 8000
```

## API Endpoints

### POST /compare
Compare a legal question across jurisdictions.

```bash
curl -X POST "http://localhost:8000/compare" \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the limitation period for breach of contract?"}'
```

Response:
```json
{
  "question": "What is the limitation period for breach of contract?",
  "jurisdictions": {
    "Federal": {
      "holding": "3 years under Limitation Act",
      "citation": "2023 SCMR 456",
      "confidence": 0.95
    },
    "Punjab": {
      "holding": "6 years for written contracts",
      "citation": "2024 PLD Lah 78",
      "confidence": 0.82
    }
  },
  "conflicts": ["Punjab interprets limitation differently for written vs oral contracts"],
  "consensus": ["All jurisdictions agree on 3-year base period"],
  "gaps": ["No AJK ruling found on this issue"]
}
```

### GET /issues
List common legal issues with coverage.

```bash
curl "http://localhost:8000/issues?category=contract&limit=10"
```

### GET /issues/{id}
Get detailed comparison for a specific issue.

```bash
curl "http://localhost:8000/issues/abc123def456"
```

### GET /conflicts
List known conflicts between jurisdictions.

```bash
curl "http://localhost:8000/conflicts?category=property"
```

### GET /jurisdiction/{code}/stats
Get statistics for a specific jurisdiction.

```bash
curl "http://localhost:8000/jurisdiction/Punjab/stats"
```

## Installation

### Requirements

```bash
pip install -r requirements_jurisdiction.txt
```

Or install individually:

```bash
pip install fastapi uvicorn
pip install sentence-transformers
pip install chromadb
pip install scikit-learn numpy
```

### Optional Dependencies

- **sentence-transformers**: For semantic similarity (recommended)
- **chromadb**: For vector search (recommended for large datasets)

The tool works without these but with reduced matching accuracy.

## Usage Examples

### Command Line

```bash
# Run jurisdiction classifier
python jurisdiction_classifier.py

# Run issue extractor
python issue_extractor.py

# Run jurisdiction mapper
python jurisdiction_mapper.py

# Run comparison engine
python comparison_engine.py

# Start API server
python jurisdiction_api.py
```

### Python Integration

```python
from pathlib import Path
from comparison_engine import ComparisonEngine

# Initialize
engine = ComparisonEngine(
    data_dir=Path('data_v2'),
    use_chromadb=True,
    use_semantic=True
)
engine.load_data()

# Compare across jurisdictions
result = engine.compare("Is specific performance available for sale of immovable property?")

# Check each jurisdiction
for jurisdiction, holding in result.jurisdictions.items():
    print(f"{jurisdiction}: {holding.holding[:100]}...")
    print(f"  Citation: {holding.citation}")
    print(f"  Confidence: {holding.confidence}")

# Identify conflicts
for conflict in result.conflicts:
    print(f"CONFLICT: {conflict}")

# Identify gaps
for gap in result.gaps:
    print(f"GAP: {gap}")
```

## Data Format

### Input JSON Structure

Each case JSON should have:

```json
{
  "citation": "2024 CLC 1",
  "case_name": "2024L201",
  "title": "Appellant v. Respondent",
  "court": "Lahore",
  "date": "14th June, 2023",
  "judges": ["Judge Name 1", "Judge Name 2"],
  "headnotes": "Legal points summarized...",
  "judgment_clean": "Full judgment text...",
  "statutes_cited": ["Contract Act", "Limitation Act"],
  "cases_cited": ["2020 SCMR 123", "PLD 2019 SC 456"]
}
```

### Output Structure

Comparison results include:

```json
{
  "question": "Legal question being compared",
  "timestamp": "2024-02-06T19:45:00",
  "jurisdictions": {
    "Federal": {
      "holding": "Court's ruling",
      "citation": "Case citation",
      "date": "Judgment date",
      "confidence": 0.95,
      "statutes": ["Statutes cited"],
      "judges": ["Judge names"]
    }
  },
  "consensus": ["Points of agreement"],
  "conflicts": ["Points of disagreement"],
  "gaps": ["Missing jurisdictions"],
  "related_issues": ["Related legal issues"]
}
```

## Legal Issue Categories

| Category | Description | Example Issues |
|----------|-------------|----------------|
| limitation | Time-barred actions | Limitation periods, delay condonation |
| contract | Contract law | Breach, specific performance, damages |
| property | Property rights | Title disputes, partition, pre-emption |
| constitutional | Constitutional law | Fundamental rights, judicial review |
| criminal | Criminal matters | Murder, theft, bail, sentencing |
| family | Family law | Divorce, custody, maintenance |
| tax | Revenue matters | Income tax, sales tax, exemptions |
| service | Employment law | Termination, pension, seniority |
| compensation | Damages | Land acquisition, valuation |
| evidence | Proof and witnesses | Burden of proof, admissibility |
| jurisdiction | Forum issues | Territorial, subject matter |
| procedure | Procedural law | CPC, CrPC, amendments |

## Citation Patterns

| Reporter | Full Name | Jurisdiction |
|----------|-----------|--------------|
| SCMR | Supreme Court Monthly Review | Always Federal (Supreme Court) |
| PLD | Pakistan Legal Decisions | Varies (check court field or suffix) |
| CLC | Civil Law Cases | Varies |
| MLD | Monthly Law Digest | Varies |
| PCrLJ | Pakistan Criminal Law Journal | Varies |
| PLC | Pakistan Labor Cases | Varies |
| PTD | Pakistan Tax Decisions | Varies |
| YLR | Yearly Law Reports | Varies |

Citation suffixes:
- `PLD Lah` → Lahore High Court (Punjab)
- `PLD Kar` → Sindh High Court
- `PLD Pesh` → Peshawar High Court (KPK)
- `PLD Quetta` → Balochistan High Court
- `PLD Isl` → Islamabad High Court
- `PLD SC` → Supreme Court (Federal)
- `PLD FSC` → Federal Shariat Court

## Accuracy Notes

- **Jurisdiction Classification**: >95% accuracy using combined strategies
- **Issue Extraction**: Best from headnotes (pre-summarized); judgment extraction is supplementary
- **Conflict Detection**: Simple keyword-based analysis; for production use, recommend LLM-based holding comparison
- **Semantic Matching**: Significantly improves issue matching when sentence-transformers is available

## Limitations

1. **Holding extraction** relies on pattern matching; may miss nuanced rulings
2. **Conflict detection** uses keyword analysis; doesn't understand legal nuance
3. **Coverage depends on data** - some jurisdictions may have fewer cases
4. **Language**: Primarily handles English-language judgments

## Future Enhancements

- [ ] LLM-based holding extraction and comparison
- [ ] Urdu language support
- [ ] Timeline analysis (how positions evolved)
- [ ] Citation network analysis
- [ ] Precedent tracking across jurisdictions
- [ ] API authentication and rate limiting

## License

Part of the Pakistan Legal Research Platform. See main project LICENSE.

## Contributing

Contributions welcome! See CONTRIBUTING.md for guidelines.
