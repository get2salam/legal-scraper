# Petition Draft Analyzer for Pakistan Legal Research

A comprehensive system for analyzing draft petitions and finding supporting or contrary precedents from Pakistani case law (1,700+ cases).

## Overview

This tool helps lawyers and legal researchers:
- Extract legal claims and statutory references from draft petitions
- Find relevant precedent cases (supporting and contrary)
- Calculate success rates for specific legal provisions
- Get actionable recommendations based on historical case outcomes

## Components

### 1. Outcome Classifier (`outcome_classifier.py`)
Classifies case outcomes from judgment text:
- **ALLOWED** - Petition/appeal successful
- **DISMISSED** - Petition/appeal failed
- **PARTIALLY_ALLOWED** - Mixed outcome
- **REMANDED** - Sent back to lower court
- **WITHDRAWN** - Case withdrawn
- **DISPOSED** - Ambiguous outcome

```bash
# Test on sample cases
python outcome_classifier.py --test

# Classify all cases
python outcome_classifier.py -i data_v2/all_cases.jsonl -o data_v2/cases_classified.jsonl
```

### 2. Claim Extractor (`claim_extractor.py`)
Extracts legal elements from petition text:
- Statutory references (Section X of Act Y)
- Constitutional articles (Article 199, etc.)
- Legal principles (res judicata, estoppel, etc.)
- Relief sought (declaration, injunction, etc.)
- Cause of action

```bash
# Test extraction
python claim_extractor.py --test

# Extract from specific text
python claim_extractor.py --text "The petitioner seeks declaration under Section 12 CPC..."
```

### 3. Precedent Matcher (`precedent_matcher.py`)
Finds relevant cases using semantic similarity:
- Embeds text using sentence-transformers
- Searches via ChromaDB vector database
- Filters by court level, outcome, date
- Returns supporting and contrary precedents

```bash
# Initialize ChromaDB (first time)
python precedent_matcher.py --init -i data_v2/cases_classified.jsonl

# Search for precedents
python precedent_matcher.py --query "writ petition challenging termination from service"
```

### 4. Success Calculator (`success_calculator.py`)
Calculates win/loss ratios for legal provisions:
- Groups cases by statutory provision
- Tracks success by court level (Supreme, High Court, etc.)
- Analyzes temporal trends
- Exports statistics for quick lookup

```bash
# Build statistics database
python success_calculator.py -i data_v2/cases_classified.jsonl -o data_v2/provision_stats.json

# Query specific provision
python success_calculator.py --provision "Section 12 CPC"
```

### 5. Petition API (`petition_api.py`)
FastAPI REST endpoints:
- `POST /analyze` - Analyze petition text
- `POST /analyze/file` - Analyze PDF/DOCX file
- `GET /provisions/{section}` - Get success rate for provision
- `POST /precedents/search` - Search precedents
- `GET /provisions` - List all tracked provisions
- `GET /stats` - System statistics

```bash
# Start API server
python petition_api.py
# Or with uvicorn
uvicorn petition_api:app --reload --port 8000

# API docs at http://localhost:8000/docs
```

## Quick Start

### 1. Install Dependencies
```bash
pip install sentence-transformers chromadb fastapi uvicorn pdfplumber python-docx
```

### 2. Prepare Data
```bash
# First, classify all cases
python outcome_classifier.py -i data_v2/all_cases.jsonl -o data_v2/cases_classified.jsonl

# Build success rate database
python success_calculator.py -i data_v2/cases_classified.jsonl

# Initialize ChromaDB for semantic search
python precedent_matcher.py --init -i data_v2/cases_classified.jsonl
```

### 3. Start API
```bash
python petition_api.py
```

### 4. Analyze a Petition
```bash
curl -X POST "http://localhost:8000/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "The petitioner, a government servant, challenges his termination under Article 199 of the Constitution. The impugned order violates principles of natural justice as no show cause notice was issued before termination. The petitioner seeks reinstatement with back benefits.",
    "include_precedents": true,
    "include_success_rates": true
  }'
```

## API Response Format

```json
{
  "claims_extracted": [
    "Article 199 - Jurisdiction of High Court",
    "natural justice"
  ],
  "statutory_references": [...],
  "constitutional_articles": [
    {"article": "199", "description": "Jurisdiction of High Court"}
  ],
  "legal_principles": [
    {"principle": "natural justice", "description": "Principles of Natural Justice"}
  ],
  "reliefs_sought": ["Reinstatement", "Back Benefits"],
  "supporting_precedents": [
    {
      "citation": "2024 SCMR 123",
      "title": "Government Servant v. Province",
      "court": "Supreme Court",
      "outcome": "allowed",
      "relevance_score": 0.89,
      "summary": "Termination without show cause notice held illegal..."
    }
  ],
  "contrary_precedents": [...],
  "success_analysis": {
    "Article 199 Constitution": {
      "success_rate": 0.72,
      "sample_size": 45,
      "allowed": 32,
      "dismissed": 13
    }
  },
  "recommendation": "Strong argument based on Article 199 (success rate: 72%)",
  "processing_time_ms": 245.5
}
```

## Supported Statutes

The system recognizes references to major Pakistani legislation:
- Constitution of Pakistan, 1973
- Code of Civil Procedure (CPC), 1908
- Code of Criminal Procedure (CrPC), 1898
- Pakistan Penal Code (PPC), 1860
- Qanun-e-Shahadat Order, 1984
- Specific Relief Act, 1877
- Transfer of Property Act, 1882
- Land Acquisition Act, 1894
- Muslim Family Laws Ordinance, 1961
- And many more...

## Data Sources

Covers cases from major law reporters:
- **SCMR** - Supreme Court Monthly Review
- **PLD** - Pakistan Legal Decisions
- **CLC** - Civil Law Cases
- **PCrLJ** - Pakistan Criminal Law Journal
- **PTD** - Pakistan Tax Decisions
- **PLC** - Pakistan Labour Cases
- **MLD** - Monthly Law Digest
- **YLR** - Yearly Law Reports

## Technical Details

### Outcome Classification
Uses pattern matching on the final section of judgments. Patterns include:
- "petition is allowed/dismissed"
- "appeal succeeds/fails"
- "writ is granted/refused"
- And 50+ more patterns with confidence scores

### Semantic Search
- Model: `all-MiniLM-L6-v2` (384 dimensions)
- Vector DB: ChromaDB with cosine similarity
- Chunks: Case headnotes and key portions

### Success Rate Calculation
Groups cases by:
- Statutory provision (e.g., "Section 12 CPC")
- Constitutional article (e.g., "Article 199")
- Tracks outcomes by court level and year

## Performance

- Claim extraction: <100ms
- Outcome classification: <50ms
- Precedent search: <2s (with ChromaDB)
- Full analysis: <5s

## File Structure

```
pakistan-legislation-scraper/
├── data_v2/
│   ├── all_cases.jsonl          # Raw case data
│   ├── cases_classified.jsonl   # Cases with outcomes
│   ├── provision_stats.json     # Pre-computed success rates
│   └── chromadb/                # Vector embeddings
├── outcome_classifier.py        # Case outcome classification
├── claim_extractor.py          # Legal claim extraction
├── precedent_matcher.py        # Semantic precedent search
├── success_calculator.py       # Success rate calculation
├── petition_api.py             # FastAPI endpoints
└── README-petition.md          # This documentation
```

## Contributing

To improve the system:
1. Add more outcome patterns in `outcome_classifier.py`
2. Expand statute aliases in `claim_extractor.py`
3. Improve embedding quality with domain-specific models
4. Add Urdu language support for claim extraction

## License

MIT License - See LICENSE file for details.
