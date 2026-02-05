#!/usr/bin/env python3
"""
Daily Safe Scraper for Pakistani Legal Data
- Respects rate limits
- Takes breaks
- Tracks progress
- Won't get us banned
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rotating user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

class SafeScraper:
    """Rate-limited scraper that won't get us banned"""
    
    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = Path(progress_file)
        self.progress = self._load_progress()
        self.session = requests.Session()
        self.request_count = 0
        
    def _load_progress(self) -> dict:
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                return json.load(f)
        return {}
    
    def _save_progress(self):
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    
    def _safe_delay(self):
        """Random delay between requests"""
        settings = self.progress.get('settings', {})
        min_delay = settings.get('min_delay_seconds', 3)
        max_delay = settings.get('max_delay_seconds', 8)
        delay = random.uniform(min_delay, max_delay)
        logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)
    
    def _maybe_take_break(self):
        """Take a break every N requests"""
        self.request_count += 1
        settings = self.progress.get('settings', {})
        break_after = settings.get('break_after_requests', 50)
        break_duration = settings.get('break_duration_minutes', 5)
        
        if self.request_count > 0 and self.request_count % break_after == 0:
            logger.info(f"Taking a {break_duration} minute break after {self.request_count} requests...")
            time.sleep(break_duration * 60)
    
    def fetch(self, url: str, max_retries: int = 3) -> requests.Response:
        """Fetch URL with rate limiting and retries"""
        self._safe_delay()
        self._maybe_take_break()
        
        for attempt in range(max_retries):
            try:
                self.session.headers.update(self._get_headers())
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 429:  # Rate limited
                    logger.warning("Rate limited! Waiting 5 minutes...")
                    time.sleep(300)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10 * (attempt + 1))  # Exponential backoff
        
        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")
    
    def download_pdf(self, url: str, output_path: Path) -> bool:
        """Download a PDF file"""
        if output_path.exists():
            logger.debug(f"Already exists: {output_path}")
            return True
        
        try:
            response = self.fetch(url)
            
            if 'application/pdf' in response.headers.get('content-type', ''):
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Downloaded: {output_path.name}")
                return True
            else:
                logger.warning(f"Not a PDF: {url}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return False
    
    def run_daily_pakistan_code(self, limit: int = None):
        """Run daily scrape of Pakistan Code PDFs"""
        source = 'pakistan_code'
        daily_limit = limit or self.progress.get(source, {}).get('daily_limit', 20)
        
        # Load laws
        laws_file = Path("data/raw/laws.json")
        if not laws_file.exists():
            logger.error("No laws.json found. Run scraper.py list first.")
            return
        
        with open(laws_file) as f:
            laws = json.load(f)
        
        # Find laws without downloaded PDFs
        pdf_dir = Path("data/raw/pdfs")
        pdf_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded = 0
        for law in tqdm(laws, desc="Checking laws"):
            if downloaded >= daily_limit:
                logger.info(f"Daily limit ({daily_limit}) reached. Stopping.")
                break
            
            # Check if already downloaded
            safe_title = ''.join(c for c in law.get('title', 'unknown')[:50] if c.isalnum() or c in ' _-')
            safe_title = safe_title.strip().replace(' ', '_')
            year = law.get('year', 'unknown')
            filename = f"{year}_{safe_title}.pdf"
            filepath = pdf_dir / filename
            
            if filepath.exists():
                continue
            
            # Try to find and download PDF
            page_url = law.get('url')
            if not page_url:
                continue
            
            try:
                # Fetch the law page
                response = self.fetch(page_url)
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find PDF link
                pdf_link = soup.find('a', href=lambda x: x and '.pdf' in x.lower())
                if pdf_link:
                    pdf_url = pdf_link.get('href')
                    if not pdf_url.startswith('http'):
                        from urllib.parse import urljoin
                        pdf_url = urljoin(page_url, pdf_url)
                    
                    if self.download_pdf(pdf_url, filepath):
                        downloaded += 1
                        law['local_pdf'] = str(filepath)
            
            except Exception as e:
                logger.warning(f"Error processing {law.get('title', 'unknown')[:30]}: {e}")
        
        # Update progress
        self.progress[source]['pdfs_downloaded'] = sum(
            1 for f in pdf_dir.glob('*.pdf')
        )
        self.progress[source]['last_run'] = datetime.now().isoformat()
        
        # Log the daily run
        self.progress.setdefault('daily_logs', []).append({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': source,
            'downloaded': downloaded,
            'total_pdfs': self.progress[source]['pdfs_downloaded']
        })
        
        self._save_progress()
        
        # Save updated laws
        with open(laws_file, 'w', encoding='utf-8') as f:
            json.dump(laws, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Daily scrape complete. Downloaded {downloaded} PDFs.")
        logger.info(f"Total PDFs: {self.progress[source]['pdfs_downloaded']}")
    
    def show_status(self):
        """Show current progress"""
        print("\n" + "="*50)
        print("SCRAPING PROGRESS")
        print("="*50)
        
        for source in ['pakistan_code', 'na_acts', 'pakistanlawsite']:
            data = self.progress.get(source, {})
            print(f"\n{source}:")
            print(f"  Total discovered: {data.get('total_discovered') or data.get('total_estimated') or 'Unknown'}")
            print(f"  PDFs downloaded:  {data.get('pdfs_downloaded', 0)}")
            print(f"  Texts extracted:  {data.get('texts_extracted', 0)}")
            print(f"  Last run:         {data.get('last_run', 'Never')}")
            print(f"  Daily limit:      {data.get('daily_limit', 'N/A')}")
        
        print("\n" + "="*50)


def main():
    parser = argparse.ArgumentParser(description='Daily Safe Scraper')
    parser.add_argument('action', choices=['status', 'pakistan-code', 'na', 'all'],
                       help='Action to perform')
    parser.add_argument('--limit', type=int, help='Override daily limit')
    
    args = parser.parse_args()
    
    scraper = SafeScraper()
    
    if args.action == 'status':
        scraper.show_status()
    
    elif args.action == 'pakistan-code':
        scraper.run_daily_pakistan_code(limit=args.limit)
    
    elif args.action == 'na':
        logger.info("NA scraping not yet implemented")
    
    elif args.action == 'all':
        logger.info("Running all daily scrapes...")
        scraper.run_daily_pakistan_code(limit=args.limit)


if __name__ == '__main__':
    main()
