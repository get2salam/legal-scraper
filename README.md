# Legal Scraper 📚⚖️

A modular Python framework for scraping legal databases, case law, and legislation. Built with extensibility in mind — add adapters for any legal data source.

## Features

- 🔌 **Pluggable adapters** — Add support for any legal database
- ⏱️ **Human-like timing** — Variable delays to avoid detection
- 💾 **Dual format output** — JSON for inspection, JSONL for processing
- 📊 **Built-in analytics** — Citation extraction, statistics, glossary building
- 🔄 **Resume support** — Pick up where you left off
- 🛡️ **Rate limiting** — Configurable daily limits and breaks

## Installation

```bash
git clone https://github.com/get2salam/legal-scraper.git
cd legal-scraper
pip install -r requirements.txt
cp .env.example .env  # Configure your credentials
```

## Quick Start

```python
from legal_scraper import Scraper, BaseAdapter

# Use a built-in adapter
scraper = Scraper(adapter="example")
cases = scraper.search("constitutional petition")

# Or create your own adapter
class MyDatabaseAdapter(BaseAdapter):
    def authenticate(self):
        # Your login logic
        pass
    
    def search(self, query):
        # Your search logic
        pass
    
    def fetch_case(self, case_id):
        # Your fetch logic
        pass
```

## Architecture

```
legal-scraper/
├── adapters/           # Database-specific adapters
│   ├── base.py         # Abstract base adapter
│   └── example.py      # Example adapter template
├── core/
│   ├── scraper.py      # Main scraper engine
│   ├── timing.py       # Human-like delay logic
│   └── storage.py      # JSON/JSONL persistence
├── analytics/
│   ├── citations.py    # Citation extraction
│   ├── stats.py        # Case statistics
│   └── glossary.py     # Legal term extraction
└── cli.py              # Command-line interface
```

## Creating an Adapter

```python
from legal_scraper.adapters.base import BaseAdapter

class MyLegalDBAdapter(BaseAdapter):
    """Adapter for MyLegalDB.com"""
    
    BASE_URL = "https://example-legal-db.com"
    
    def authenticate(self) -> bool:
        """Login and establish session."""
        # Implement authentication
        return True
    
    def search(self, query: str, **kwargs) -> list[dict]:
        """Search for cases matching query."""
        # Implement search
        return []
    
    def fetch_case(self, case_id: str) -> dict:
        """Fetch full case details."""
        # Implement case fetching
        return {}
    
    def enumerate_by_year(self, year: int) -> list[str]:
        """List all case IDs for a given year."""
        # Implement enumeration
        return []
```

## CLI Usage

```bash
# Check scraper status
python cli.py status

# Search for cases
python cli.py search --adapter example --query "murder appeal"

# Fetch cases by year
python cli.py enumerate --adapter example --year 2024

# Run analytics on collected data
python cli.py analyze --type citations
python cli.py analyze --type stats
python cli.py analyze --type glossary
```

## Configuration

Create a `.env` file:

```env
# Adapter credentials (adapter-specific)
ADAPTER_USER=your_username
ADAPTER_PASS=your_password

# Rate limiting
DAILY_REQUEST_LIMIT=500
MIN_DELAY_SECONDS=5
MAX_DELAY_SECONDS=20

# Output
DATA_DIR=data
OUTPUT_FORMAT=both  # json, jsonl, or both
```

## Human-Like Timing

The scraper uses variable delays to mimic human browsing:

- Base delay: 5-20 seconds (randomized)
- "Reading" pauses: 12% chance of 30-90 second pause
- Breaks: Every 22-38 requests, pause for 1.5-3 minutes
- All intervals are randomized, not fixed

## Analytics

### Citation Extraction

```python
from legal_scraper.analytics import extract_citations

citations = extract_citations(case_text)
# Returns: ['Article 14', 'Section 302', '2023 SC 445']
```

### Statistics

```python
from legal_scraper.analytics import generate_stats

stats = generate_stats(cases_dir="data/cases")
# Returns: avg length, citation counts, court distribution, etc.
```

## Contributing

1. Fork the repository
2. Create an adapter for your legal database
3. Add tests
4. Submit a PR

## License

MIT License — see [LICENSE](LICENSE)

## Disclaimer

This tool is for **authorized access only**. Always:
- Respect robots.txt and terms of service
- Use with valid credentials/subscriptions
- Don't overload servers
- Comply with local laws regarding web scraping

---

Built with ⚖️ by [Abdul Salam](https://github.com/get2salam)
