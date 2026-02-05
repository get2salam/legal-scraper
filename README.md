# Pakistan Legal Data Scraper 🇵🇰⚖️

A comprehensive Python toolkit for collecting Pakistani legal data — legislation, case law, and judicial precedents from multiple official sources.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Purpose

Data collection pipeline for **Qanoon.com** — an AI-powered legal research platform for Pakistani lawyers. This toolkit enables:

- **Legislation Scraping** — Federal and provincial statutes
- **Case Law Collection** — Reported judgments from all major law reports
- **Semantic Search** — AI-powered search across the entire corpus

## 📚 Data Sources

### Legislation
| Source | URL | Coverage |
|--------|-----|----------|
| Pakistan Code | pakistancode.gov.pk | 1,000+ federal acts |
| National Assembly | na.gov.pk | Recent parliamentary acts |

### Case Law (Authenticated)
| Source | Books | Years |
|--------|-------|-------|
| PakistanLawSite.com | PLD, SCMR, MLD, PCrLJ, CLC, YLR, PTD, CLD, GBLR | 2000-2025 |

**Law Reports Covered:**
- **PLD** — Pakistan Legal Decisions (Supreme Court & High Courts)
- **SCMR** — Supreme Court Monthly Review
- **MLD** — Monthly Law Digest
- **PCrLJ** — Pakistan Criminal Law Journal
- **CLC** — Civil Law Cases
- **YLR** — Yearly Law Reporter
- **PTD** — Pakistan Tax Decisions
- **CLD** — Company Law Decisions
- **GBLR** — Gilgit-Baltistan Law Reports

## ✨ Features

- **🔐 Authenticated Scraping** — Session management with login support
- **⏱️ Rate Limiting** — Configurable delays, daily limits, automatic breaks
- **🕐 Smart Scheduling** — Operates during business hours (PKT) to mimic human usage
- **📊 Progress Tracking** — Resume from where you left off
- **📄 Dual Format** — Headnotes + full judgments
- **🔍 Section Parsing** — Break acts into searchable sections
- **🧠 Embeddings** — Generate vectors for semantic search

## 🛠️ Installation

```bash
# Clone
git clone https://github.com/get2salam/pakistan-legislation-scraper.git
cd pakistan-legislation-scraper

# Virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Dependencies
pip install -r requirements.txt

# For local embeddings (free)
pip install sentence-transformers
```

## ⚙️ Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials (for authenticated sources)
PLS_USER=your_username
PLS_PASS=your_password
```

## 📖 Usage

### Case Law Scraper (PakistanLawSite)

```bash
# Check status
python pls_scraper.py status

# Enumerate cases for a book/year
python pls_scraper.py enumerate --book PLD --year 2025

# Fetch full case documents
python pls_scraper.py fetch-cases --limit 50

# Search by keyword
python pls_scraper.py search --keyword "constitutional petition"

# Overnight batch scrape
python overnight_scrape.py
```

### Legislation Scraper (Pakistan Code)

```bash
# List all federal laws
python scraper.py list

# Download PDFs with rate limiting
python daily_scraper.py pakistan-code --limit 20

# Extract text from PDFs
python scraper.py extract
```

### Processing Pipeline

```bash
# Parse acts into sections
python section_parser.py

# Generate embeddings
python embedding_generator.py --provider local

# Search
python simple_search.py --query "punishment for murder"
```

## 📁 Output Structure

```
data/
├── pakistanlawsite/
│   ├── jsonl/
│   │   ├── cases_PLD_2025.jsonl    # Case law by book/year
│   │   ├── cases_SCMR_2025.jsonl
│   │   └── ...
│   ├── cases/                       # Full judgment HTML
│   ├── headnotes/                   # Case headnotes
│   └── pls_progress.json           # Progress tracker
├── raw/
│   ├── laws.json                    # Legislation metadata
│   └── pdfs/                        # Downloaded PDFs
├── processed/
│   └── parsed_acts.json            # Sectioned legislation
└── embeddings/
    └── legal_embeddings.json       # Vector embeddings
```

## 📊 Data Format (JSONL)

Each case is stored as a JSON line:

```json
{
  "case_id": "2025PLD1",
  "title": "2025 PLD 1",
  "book": "PLD",
  "year": 2025,
  "headnotes": "...",
  "full_text": "...",
  "fetched_at": "2025-02-04T12:00:00Z"
}
```

## 🔧 Rate Limiting

Built-in safeguards to respect source servers:

| Setting | Default | Description |
|---------|---------|-------------|
| `DAILY_REQUEST_LIMIT` | 500 | Max requests per day |
| `MIN_DELAY_SECONDS` | 8 | Minimum delay between requests |
| `MAX_DELAY_SECONDS` | 15 | Maximum delay between requests |
| `BREAK_AFTER_REQUESTS` | 30 | Take break after N requests |
| `BREAK_DURATION_SECONDS` | 120 | Break duration (2 min) |

## 🧠 Embedding Options

| Provider | Cost | Model |
|----------|------|-------|
| `local` | Free | sentence-transformers/all-MiniLM-L6-v2 |
| `openai` | $0.02/1M tokens | text-embedding-3-small |
| `google` | Free tier | text-embedding-004 |

## ⚠️ Legal Notice

- This tool is for authorized users with valid subscriptions
- Respects rate limits and operates during business hours
- Use responsibly for research and legal practice

## 🔜 Roadmap

- [ ] Provincial legislation (Punjab, Sindh, KPK, Balochistan)
- [ ] Supreme Court website direct scraping
- [ ] High Court judgment archives
- [ ] Federal Shariat Court decisions
- [ ] Elasticsearch integration
- [ ] REST API

## 👨‍💻 Author

**Abdul Salam**
- MS in Artificial Intelligence (University of Stirling)
- LLM in Commercial Law | LLB
- Building AI tools for the legal industry

[LinkedIn](https://linkedin.com/in/abdulsalam-ai) | [GitHub](https://github.com/get2salam)

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

*Part of the [Qanoon.com](https://qanoon.com) project — AI-powered legal research for Pakistan* 🇵🇰
