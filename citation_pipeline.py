#!/usr/bin/env python3
"""
Citation Pipeline - Orchestrates legislation → case law → verification
=======================================================================
Three-stage pipeline that mimics PLS functionality:
1. Legislation Scraper: Gets statutes + extracts case citations
2. Linked Cases Scraper: Fetches only cited cases (any year)
3. Link Generator + Verifier: Creates working links, tests them

Usage:
    python citation_pipeline.py run          # Run full pipeline
    python citation_pipeline.py extract      # Extract citations from legislation
    python citation_pipeline.py fetch        # Fetch missing cited cases
    python citation_pipeline.py generate     # Generate HTML links
    python citation_pipeline.py verify       # Verify all links work
    python citation_pipeline.py status       # Show pipeline status
"""

import json
import os
import re
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
PIPELINE_DIR = DATA_DIR / "pipeline"
QUEUE_FILE = PIPELINE_DIR / "citation_queue.json"
STATUS_FILE = PIPELINE_DIR / "pipeline_status.json"
VERIFIED_FILE = PIPELINE_DIR / "verified_links.json"
BROKEN_FILE = PIPELINE_DIR / "broken_links.json"

# Ensure directories exist
PIPELINE_DIR.mkdir(parents=True, exist_ok=True)


class CitationQueue:
    """Manages the queue of citations to fetch."""
    
    def __init__(self):
        self.queue_file = QUEUE_FILE
        self.load()
    
    def load(self):
        """Load queue from disk."""
        if self.queue_file.exists():
            data = json.loads(self.queue_file.read_text(encoding='utf-8'))
            self.pending = set(data.get('pending', []))
            self.fetched = set(data.get('fetched', []))
            self.failed = set(data.get('failed', []))
            self.sources = data.get('sources', {})  # citation -> [statute_ids]
        else:
            self.pending = set()
            self.fetched = set()
            self.failed = set()
            self.sources = {}
    
    def save(self):
        """Save queue to disk."""
        data = {
            'pending': list(self.pending),
            'fetched': list(self.fetched),
            'failed': list(self.failed),
            'sources': self.sources,
            'updated': datetime.now().isoformat()
        }
        self.queue_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def add(self, citation: str, source_statute: str):
        """Add a citation to the queue."""
        normalized = self.normalize_citation(citation)
        if normalized and normalized not in self.fetched:
            self.pending.add(normalized)
            if normalized not in self.sources:
                self.sources[normalized] = []
            if source_statute not in self.sources[normalized]:
                self.sources[normalized].append(source_statute)
    
    def mark_fetched(self, citation: str):
        """Mark a citation as successfully fetched."""
        normalized = self.normalize_citation(citation)
        self.pending.discard(normalized)
        self.failed.discard(normalized)
        self.fetched.add(normalized)
    
    def mark_failed(self, citation: str):
        """Mark a citation as failed to fetch."""
        normalized = self.normalize_citation(citation)
        self.pending.discard(normalized)
        self.failed.add(normalized)
    
    @staticmethod
    def normalize_citation(citation: str) -> Optional[str]:
        """Normalize citation format: '1986 PLD 29' -> '1986_PLD_29'"""
        if not citation:
            return None
        # Clean up the citation
        citation = citation.strip().replace(',', '').replace('.', '')
        # Extract year, reporter, page
        match = re.match(r'(\d{4})\s+([A-Z]+)\s+(\d+)', citation)
        if match:
            year, reporter, page = match.groups()
            return f"{year}_{reporter}_{page}"
        return None
    
    def get_pending(self, limit: int = 100) -> List[str]:
        """Get pending citations to fetch."""
        return list(self.pending)[:limit]
    
    def stats(self) -> Dict:
        """Get queue statistics."""
        return {
            'pending': len(self.pending),
            'fetched': len(self.fetched),
            'failed': len(self.failed),
            'total_unique': len(self.pending | self.fetched | self.failed)
        }


class CitationExtractor:
    """Extracts case citations from legislation JSON files."""
    
    def __init__(self):
        self.queue = CitationQueue()
    
    def extract_from_all_legislation(self) -> Dict:
        """Extract citations from all legislation files."""
        stats = {'statutes': 0, 'sections': 0, 'citations': 0, 'unique': 0}
        
        for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
            if not letter_dir.is_dir() or letter_dir.name == 'html':
                continue
            
            for json_file in letter_dir.glob('*.json'):
                self.extract_from_statute(json_file, stats)
        
        stats['unique'] = len(self.queue.pending | self.queue.fetched)
        self.queue.save()
        return stats
    
    def extract_from_statute(self, json_path: Path, stats: Dict):
        """Extract citations from a single statute."""
        try:
            data = json.loads(json_path.read_text(encoding='utf-8'))
            statute_id = data.get('id', json_path.stem)
            stats['statutes'] += 1
            
            for section in data.get('sections', []):
                stats['sections'] += 1
                for link in section.get('case_links', []):
                    citation = link.get('citation', '')
                    if citation:
                        self.queue.add(citation, statute_id)
                        stats['citations'] += 1
        except Exception as e:
            logger.warning(f"Error processing {json_path}: {e}")


class LinkedCaseFetcher:
    """Fetches cases from the citation queue."""
    
    def __init__(self):
        self.queue = CitationQueue()
    
    def case_exists(self, citation: str) -> bool:
        """Check if we already have this case."""
        normalized = CitationQueue.normalize_citation(citation)
        if not normalized:
            return False
        
        # Parse citation
        parts = normalized.split('_')
        if len(parts) != 3:
            return False
        
        year, reporter, page = parts
        
        # Check if JSON file exists
        case_path = DATA_DIR / reporter / year / f"{normalized}.json"
        return case_path.exists()
    
    def fetch_pending(self, limit: int = 50) -> Dict:
        """Fetch pending cases."""
        stats = {'checked': 0, 'already_have': 0, 'fetched': 0, 'failed': 0}
        
        pending = self.queue.get_pending(limit)
        
        for citation in pending:
            stats['checked'] += 1
            
            # Check if we already have it
            if self.case_exists(citation):
                self.queue.mark_fetched(citation)
                stats['already_have'] += 1
                continue
            
            # Parse citation for fetching
            parts = citation.split('_')
            if len(parts) != 3:
                self.queue.mark_failed(citation)
                stats['failed'] += 1
                continue
            
            year, reporter, page = parts
            
            # Add to fetch list (actual fetching done by linked_cases_scraper.py)
            logger.info(f"Need to fetch: {year} {reporter} {page}")
            # We'll mark as pending still - the scraper will handle it
        
        self.queue.save()
        return stats
    
    def generate_fetch_list(self) -> Path:
        """Generate a list of cases to fetch for the scraper."""
        fetch_list = []
        
        for citation in self.queue.pending:
            if not self.case_exists(citation):
                parts = citation.split('_')
                if len(parts) == 3:
                    year, reporter, page = parts
                    fetch_list.append({
                        'year': int(year),
                        'reporter': reporter,
                        'page': int(page),
                        'citation': f"{year} {reporter} {page}"
                    })
        
        # Sort by year (newest first) then reporter
        fetch_list.sort(key=lambda x: (-x['year'], x['reporter'], x['page']))
        
        output_file = PIPELINE_DIR / "cases_to_fetch.json"
        output_file.write_text(json.dumps(fetch_list, indent=2), encoding='utf-8')
        
        logger.info(f"Generated fetch list: {len(fetch_list)} cases -> {output_file}")
        return output_file


class LinkGenerator:
    """Generates clickable HTML links in legislation files."""
    
    def __init__(self):
        self.queue = CitationQueue()
        self.generated = 0
        self.broken = 0
    
    def get_case_path(self, citation: str) -> Optional[str]:
        """Get the relative path to a case file."""
        normalized = CitationQueue.normalize_citation(citation)
        if not normalized:
            return None
        
        parts = normalized.split('_')
        if len(parts) != 3:
            return None
        
        year, reporter, page = parts
        
        # Check JSON exists
        case_json = DATA_DIR / reporter / year / f"{normalized}.json"
        if not case_json.exists():
            return None
        
        # Check HTML exists (or will generate)
        case_html = DATA_DIR / "html" / reporter / year / f"{normalized}.html"
        if case_html.exists():
            return f"../../html/{reporter}/{year}/{normalized}.html"
        
        # Fallback to JSON path (can still be rendered)
        return f"../../{reporter}/{year}/{normalized}.json"
    
    def update_legislation_links(self) -> Dict:
        """Update all legislation files with working links."""
        stats = {'statutes': 0, 'links_updated': 0, 'links_broken': 0}
        
        for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
            if not letter_dir.is_dir() or letter_dir.name == 'html':
                continue
            
            for json_file in letter_dir.glob('*.json'):
                self.update_statute_links(json_file, stats)
        
        return stats
    
    def update_statute_links(self, json_path: Path, stats: Dict):
        """Update links in a single statute file."""
        try:
            data = json.loads(json_path.read_text(encoding='utf-8'))
            stats['statutes'] += 1
            modified = False
            
            for section in data.get('sections', []):
                for link in section.get('case_links', []):
                    citation = link.get('citation', '')
                    if citation:
                        path = self.get_case_path(citation)
                        if path:
                            link['url'] = path
                            link['available'] = True
                            stats['links_updated'] += 1
                            modified = True
                        else:
                            link['url'] = ''
                            link['available'] = False
                            stats['links_broken'] += 1
            
            if modified:
                json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
        except Exception as e:
            logger.warning(f"Error updating {json_path}: {e}")


class LinkVerifier:
    """Verifies all case links in legislation work."""
    
    def verify_all(self) -> Dict:
        """Verify all links in legislation files."""
        verified = []
        broken = []
        
        for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
            if not letter_dir.is_dir() or letter_dir.name == 'html':
                continue
            
            for json_file in letter_dir.glob('*.json'):
                self.verify_statute(json_file, verified, broken)
        
        # Save results
        VERIFIED_FILE.write_text(json.dumps({
            'count': len(verified),
            'links': verified[:100],  # Sample
            'verified_at': datetime.now().isoformat()
        }, indent=2), encoding='utf-8')
        
        BROKEN_FILE.write_text(json.dumps({
            'count': len(broken),
            'links': broken,
            'verified_at': datetime.now().isoformat()
        }, indent=2), encoding='utf-8')
        
        return {
            'verified': len(verified),
            'broken': len(broken),
            'total': len(verified) + len(broken),
            'success_rate': f"{len(verified) / (len(verified) + len(broken)) * 100:.1f}%" if (verified or broken) else "N/A"
        }
    
    def verify_statute(self, json_path: Path, verified: List, broken: List):
        """Verify links in a single statute."""
        try:
            data = json.loads(json_path.read_text(encoding='utf-8'))
            statute_title = data.get('title', json_path.stem)
            
            for section in data.get('sections', []):
                section_num = section.get('number', '?')
                for link in section.get('case_links', []):
                    citation = link.get('citation', '')
                    url = link.get('url', '')
                    
                    if url and url != '#':
                        # Check if target exists
                        target_path = LEGISLATION_DIR / json_path.parent.name / url
                        if target_path.exists() or (DATA_DIR / url.lstrip('../')).exists():
                            verified.append({
                                'statute': statute_title,
                                'section': section_num,
                                'citation': citation,
                                'url': url
                            })
                        else:
                            broken.append({
                                'statute': statute_title,
                                'section': section_num,
                                'citation': citation,
                                'url': url,
                                'reason': 'target_not_found'
                            })
                    elif citation:
                        broken.append({
                            'statute': statute_title,
                            'section': section_num,
                            'citation': citation,
                            'url': url,
                            'reason': 'no_url'
                        })
        except Exception as e:
            logger.warning(f"Error verifying {json_path}: {e}")


def get_pipeline_status() -> Dict:
    """Get overall pipeline status."""
    queue = CitationQueue()
    queue_stats = queue.stats()
    
    # Count legislation
    leg_count = sum(1 for d in LEGISLATION_DIR.iterdir() if d.is_dir() and d.name != 'html'
                    for f in d.glob('*.json'))
    
    # Count cases
    case_count = sum(1 for reporter_dir in DATA_DIR.iterdir() 
                     if reporter_dir.is_dir() and reporter_dir.name not in ['legislation', 'html', 'pipeline', 'audit', 'backup']
                     for year_dir in reporter_dir.iterdir() if year_dir.is_dir()
                     for f in year_dir.glob('*.json'))
    
    return {
        'legislation_statutes': leg_count,
        'cases_scraped': case_count,
        'citation_queue': queue_stats,
        'pipeline_dir': str(PIPELINE_DIR),
        'updated': datetime.now().isoformat()
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    command = sys.argv[1].lower()
    
    if command == 'status':
        status = get_pipeline_status()
        print("\n📊 PIPELINE STATUS")
        print("=" * 50)
        print(f"📜 Legislation statutes: {status['legislation_statutes']}")
        print(f"📁 Cases scraped: {status['cases_scraped']}")
        print(f"\n🔗 Citation Queue:")
        print(f"   Pending: {status['citation_queue']['pending']}")
        print(f"   Fetched: {status['citation_queue']['fetched']}")
        print(f"   Failed: {status['citation_queue']['failed']}")
        print(f"   Total unique: {status['citation_queue']['total_unique']}")
    
    elif command == 'extract':
        logger.info("Extracting citations from legislation...")
        extractor = CitationExtractor()
        stats = extractor.extract_from_all_legislation()
        print(f"\n✅ Extracted citations:")
        print(f"   Statutes processed: {stats['statutes']}")
        print(f"   Sections scanned: {stats['sections']}")
        print(f"   Citations found: {stats['citations']}")
        print(f"   Unique citations: {stats['unique']}")
    
    elif command == 'fetch':
        logger.info("Checking pending cases...")
        fetcher = LinkedCaseFetcher()
        
        # First, mark existing cases as fetched
        stats = fetcher.fetch_pending(limit=1000)
        print(f"\n📊 Queue check:")
        print(f"   Already have: {stats['already_have']}")
        
        # Generate fetch list for scraper
        fetch_file = fetcher.generate_fetch_list()
        print(f"   Fetch list: {fetch_file}")
        
        # Show queue status
        queue_stats = fetcher.queue.stats()
        print(f"\n📋 Queue status:")
        print(f"   Still need: {queue_stats['pending']}")
    
    elif command == 'generate':
        logger.info("Generating case links in legislation...")
        generator = LinkGenerator()
        stats = generator.update_legislation_links()
        print(f"\n✅ Link generation:")
        print(f"   Statutes updated: {stats['statutes']}")
        print(f"   Links working: {stats['links_updated']}")
        print(f"   Links broken: {stats['links_broken']}")
    
    elif command == 'verify':
        logger.info("Verifying all links...")
        verifier = LinkVerifier()
        stats = verifier.verify_all()
        print(f"\n✅ Verification complete:")
        print(f"   Working links: {stats['verified']}")
        print(f"   Broken links: {stats['broken']}")
        print(f"   Success rate: {stats['success_rate']}")
    
    elif command == 'run':
        logger.info("Running full pipeline...")
        
        # Step 1: Extract citations
        print("\n📜 Step 1: Extracting citations...")
        extractor = CitationExtractor()
        extract_stats = extractor.extract_from_all_legislation()
        print(f"   Found {extract_stats['citations']} citations ({extract_stats['unique']} unique)")
        
        # Step 2: Check what we have / need
        print("\n📥 Step 2: Checking case availability...")
        fetcher = LinkedCaseFetcher()
        fetch_stats = fetcher.fetch_pending(limit=10000)
        fetch_file = fetcher.generate_fetch_list()
        queue_stats = fetcher.queue.stats()
        print(f"   Already have: {fetch_stats['already_have']}")
        print(f"   Need to fetch: {queue_stats['pending']}")
        
        # Step 3: Generate links for what we have
        print("\n🔗 Step 3: Generating links...")
        generator = LinkGenerator()
        gen_stats = generator.update_legislation_links()
        print(f"   Working links: {gen_stats['links_updated']}")
        print(f"   Missing cases: {gen_stats['links_broken']}")
        
        # Step 4: Verify
        print("\n✅ Step 4: Verifying...")
        verifier = LinkVerifier()
        verify_stats = verifier.verify_all()
        print(f"   Success rate: {verify_stats['success_rate']}")
        
        # Summary
        print("\n" + "=" * 50)
        print("📊 PIPELINE COMPLETE")
        print("=" * 50)
        if queue_stats['pending'] > 0:
            print(f"\n⚠️  {queue_stats['pending']} cases need fetching!")
            print(f"   Run: python linked_cases_scraper.py --from-file {fetch_file}")
    
    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == '__main__':
    main()
