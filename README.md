# Legal Scraper ⚖️

A comprehensive Python toolkit for collecting legal case law and legislation from official sources. Designed to be jurisdiction-agnostic and extensible.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Features

- **Authenticated Scraping** — Session management with login support
- **Rate Limiting** — Configurable delays, daily limits, automatic breaks
- **Smart Scheduling** — Operates during business hours to mimic human usage
- **Progress Tracking** — Resume from where you left off
- **Dual Format Output** — Both JSONL (batch) and JSON (individual) files
- **Section Parsing** — Break legislation into searchable sections
- **Embedding Generation** — Generate vectors for semantic search

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Legal Scraper                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │   Scraper   │───▶│  Processor  │───▶│   Output       │  │
│  │   Module    │    │   Pipeline  │    │   (JSON/JSONL) │  │
│  └─────────────┘    └─────────────┘    └────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# Clone
git clone https://github.com/get2salam/legal-scraper.git
cd legal-scraper

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env with your source credentials

# Run scraper
python scraper.py status
python scraper.py enumerate --book PLD --year 2025
python scraper.py fetch-cases --limit 50
```

## 📁 Output Structure

```
data/
├── jsonl/
│   ├── cases_2025.jsonl      # Batch format
│   ├── cases_2024.jsonl
│   └── ...
├── cases/                     # Individual JSON files
│   ├── case_001.json
│   └── ...
└── progress.json             # Progress tracker
```

## 📊 Data Format

Each case is stored as JSON:

```json
{
  "id": "2025001",
  "title": "Case Title",
  "book": "LAW_REPORT",
  "year": 2025,
  "court": "Supreme Court",
  "judges": ["Judge A", "Judge B"],
  "headnotes": "Summary of legal principles...",
  "judgment": "Full text of the judgment...",
  "scraped_at": "2025-02-05T12:00:00Z"
}
```

## 🔧 Configuration

### Rate Limiting

| Setting | Default | Description |
|---------|---------|-------------|
| `DAILY_REQUEST_LIMIT` | 500 | Max requests per day |
| `MIN_DELAY_SECONDS` | 8 | Minimum delay between requests |
| `MAX_DELAY_SECONDS` | 15 | Maximum delay |
| `BREAK_AFTER_REQUESTS` | 30 | Take break after N requests |
| `BREAK_DURATION_SECONDS` | 120 | Break duration |

### Environment Variables

```bash
# .env file
SOURCE_USER=your_username
SOURCE_PASS=your_password
```

## 🛠️ Analytics Tools

| Script | Purpose |
|--------|---------|
| `analytics.py` | Case law statistics |
| `citation_extractor.py` | Build citation networks |
| `legal_glossary.py` | Extract legal terms |
| `sync_formats.py` | Sync JSON ↔ JSONL |

## 🧠 Embedding Support

Generate embeddings for semantic search:

```bash
# Local embeddings (free)
pip install sentence-transformers
python embedding_generator.py --provider local

# Or use cloud providers
python embedding_generator.py --provider openai
```

## 📚 Supported Features

- **Case Law** — Judgments, headnotes, citations
- **Legislation** — Acts, ordinances, regulations
- **Search** — Keyword and citation search
- **Filters** — By court, year, topic

## 🔜 Roadmap

- [ ] Multi-jurisdiction support
- [ ] Elasticsearch integration
- [ ] REST API
- [ ] Web interface

## 👨‍💻 Author

**Abdul Salam**
- MS in Artificial Intelligence
- LLM in Commercial Law | LLB

[LinkedIn](https://linkedin.com/in/abdulsalam-ai) | [GitHub](https://github.com/get2salam)

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

*Part of the Qanoon.com project — AI-powered legal research*
