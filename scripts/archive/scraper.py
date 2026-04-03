#!/usr/bin/env python3
"""
Pakistan Legislation Scraper
Scrapes federal laws from pakistancode.gov.pk and na.gov.pk
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data/logs/scraper.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Constants
PAKISTAN_CODE_BASE = "https://pakistancode.gov.pk"
PAKISTAN_CODE_LAWS_LIST = f"{PAKISTAN_CODE_BASE}/english/sHyuRiF.php"
NA_BASE = "https://na.gov.pk"
NA_ACTS = f"{NA_BASE}/en/acts.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests


class PakistanCodeScraper:
    """Scraper for pakistancode.gov.pk"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.pdf_dir = self.raw_dir / "pdfs"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.data_dir / "logs"
        
        # Create directories
        for dir_path in [self.raw_dir, self.pdf_dir, self.processed_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch(self, url: str) -> requests.Response:
        """Fetch URL with retry logic"""
        logger.debug(f"Fetching: {url}")
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY)  # Be nice to the server
        return response
    
    def scrape_law_list(self) -> List[Dict]:
        """Scrape the list of laws from Pakistan Code"""
        logger.info("Scraping law list from Pakistan Code...")
        
        response = self._fetch(PAKISTAN_CODE_LAWS_LIST)
        soup = BeautifulSoup(response.text, 'lxml')
        
        laws = []
        
        # Find all law entries - they appear to be in various formats
        # Look for links containing law information
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Skip navigation links
            if not text or len(text) < 10:
                continue
            
            # Check if this looks like a law link
            if 'UY2F' in href or 'pdffiles' in href:
                law_entry = {
                    'title': text,
                    'url': urljoin(PAKISTAN_CODE_BASE + '/english/', href),
                    'source': 'pakistan_code',
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Try to extract year from title
                year_match = re.search(r'\b(19|20)\d{2}\b', text)
                if year_match:
                    law_entry['year'] = int(year_match.group())
                
                # Try to extract act number
                act_match = re.search(r'Act\s+No\.?\s*([IVXLCDM]+|\d+)', text, re.I)
                if act_match:
                    law_entry['act_number'] = act_match.group(1)
                
                laws.append(law_entry)
        
        # Remove duplicates based on URL
        seen_urls = set()
        unique_laws = []
        for law in laws:
            if law['url'] not in seen_urls:
                seen_urls.add(law['url'])
                unique_laws.append(law)
        
        logger.info(f"Found {len(unique_laws)} unique laws")
        
        # Save to file
        output_file = self.raw_dir / "laws.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_laws, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved law list to {output_file}")
        return unique_laws
    
    def scrape_na_acts(self, year: Optional[int] = None) -> List[Dict]:
        """Scrape Acts from National Assembly website"""
        logger.info("Scraping acts from National Assembly...")
        
        response = self._fetch(NA_ACTS)
        soup = BeautifulSoup(response.text, 'lxml')
        
        acts = []
        
        # Find all table rows with act information
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                # Try to find PDF link
                pdf_link = row.find('a', href=lambda x: x and '.pdf' in x.lower())
                if pdf_link:
                    href = pdf_link.get('href', '')
                    text = pdf_link.get_text(strip=True) or row.get_text(strip=True)
                    
                    # Get date if available
                    date_text = cells[0].get_text(strip=True) if cells else ''
                    
                    act_entry = {
                        'title': text[:500],  # Truncate very long titles
                        'pdf_url': urljoin(NA_BASE, href),
                        'date': date_text,
                        'source': 'national_assembly',
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    # Extract year
                    year_match = re.search(r'\b(19|20)\d{2}\b', text)
                    if year_match:
                        act_entry['year'] = int(year_match.group())
                    
                    acts.append(act_entry)
        
        logger.info(f"Found {len(acts)} acts from National Assembly")
        
        # Save to file
        output_file = self.raw_dir / "na_acts.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(acts, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved NA acts to {output_file}")
        return acts
    
    def download_pdfs(self, laws: List[Dict], limit: Optional[int] = None) -> List[Dict]:
        """Download PDF files for laws"""
        logger.info(f"Downloading PDFs (limit: {limit or 'all'})...")
        
        if limit:
            laws = laws[:limit]
        
        downloaded = []
        for law in tqdm(laws, desc="Downloading PDFs"):
            # Determine PDF URL
            pdf_url = law.get('pdf_url') or law.get('url')
            if not pdf_url:
                continue
            
            # Check if URL is a PDF
            if not pdf_url.lower().endswith('.pdf'):
                # Try to fetch the page and find PDF link
                try:
                    response = self._fetch(pdf_url)
                    soup = BeautifulSoup(response.text, 'lxml')
                    pdf_link = soup.find('a', href=lambda x: x and '.pdf' in x.lower())
                    if pdf_link:
                        pdf_url = urljoin(pdf_url, pdf_link.get('href'))
                    else:
                        logger.debug(f"No PDF found for: {law.get('title', 'Unknown')[:50]}")
                        continue
                except Exception as e:
                    logger.warning(f"Error fetching page: {e}")
                    continue
            
            # Generate filename
            safe_title = re.sub(r'[^\w\s-]', '', law.get('title', 'unknown')[:50])
            safe_title = safe_title.strip().replace(' ', '_')
            year = law.get('year', 'unknown')
            filename = f"{year}_{safe_title}.pdf"
            filepath = self.pdf_dir / filename
            
            # Skip if already downloaded
            if filepath.exists():
                logger.debug(f"Already exists: {filename}")
                law['local_pdf'] = str(filepath)
                downloaded.append(law)
                continue
            
            # Download PDF
            try:
                response = self._fetch(pdf_url)
                if response.headers.get('content-type', '').startswith('application/pdf'):
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"Downloaded: {filename}")
                    law['local_pdf'] = str(filepath)
                    downloaded.append(law)
                else:
                    logger.warning(f"Not a PDF: {pdf_url}")
            except Exception as e:
                logger.warning(f"Failed to download {pdf_url}: {e}")
        
        logger.info(f"Downloaded {len(downloaded)} PDFs")
        return downloaded
    
    def extract_text_from_pdfs(self, laws: List[Dict]) -> List[Dict]:
        """Extract text from downloaded PDFs"""
        try:
            import pdfplumber
        except ImportError:
            logger.error("pdfplumber not installed. Run: pip install pdfplumber")
            return laws
        
        logger.info("Extracting text from PDFs...")
        
        for law in tqdm(laws, desc="Extracting text"):
            pdf_path = law.get('local_pdf')
            if not pdf_path or not Path(pdf_path).exists():
                continue
            
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    text_parts = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            text_parts.append(text)
                    
                    full_text = '\n\n'.join(text_parts)
                    law['full_text'] = full_text
                    law['page_count'] = len(pdf.pages)
                    law['char_count'] = len(full_text)
                    
                    logger.debug(f"Extracted {len(full_text)} chars from {pdf_path}")
            except Exception as e:
                logger.warning(f"Error extracting text from {pdf_path}: {e}")
                law['extraction_error'] = str(e)
        
        # Save processed data
        output_file = self.processed_dir / "laws_with_text.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(laws, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved processed laws to {output_file}")
        return laws


def main():
    parser = argparse.ArgumentParser(description='Pakistan Legislation Scraper')
    parser.add_argument('action', choices=['list', 'download', 'extract', 'na', 'all'],
                       help='Action to perform')
    parser.add_argument('--limit', type=int, help='Limit number of items to process')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    
    args = parser.parse_args()
    
    scraper = PakistanCodeScraper(data_dir=args.data_dir)
    
    if args.action == 'list':
        scraper.scrape_law_list()
    
    elif args.action == 'na':
        scraper.scrape_na_acts()
    
    elif args.action == 'download':
        # Load existing law list or scrape
        laws_file = Path(args.data_dir) / "raw" / "laws.json"
        if laws_file.exists():
            with open(laws_file) as f:
                laws = json.load(f)
        else:
            laws = scraper.scrape_law_list()
        
        scraper.download_pdfs(laws, limit=args.limit)
    
    elif args.action == 'extract':
        # Load laws with PDFs
        laws_file = Path(args.data_dir) / "raw" / "laws.json"
        if laws_file.exists():
            with open(laws_file) as f:
                laws = json.load(f)
            scraper.extract_text_from_pdfs(laws)
        else:
            logger.error("No laws.json found. Run 'list' and 'download' first.")
    
    elif args.action == 'all':
        logger.info("Running full pipeline...")
        
        # 1. Scrape Pakistan Code
        laws = scraper.scrape_law_list()
        
        # 2. Scrape National Assembly
        na_acts = scraper.scrape_na_acts()
        
        # 3. Combine and deduplicate
        all_laws = laws + na_acts
        
        # 4. Download PDFs
        all_laws = scraper.download_pdfs(all_laws, limit=args.limit)
        
        # 5. Extract text
        all_laws = scraper.extract_text_from_pdfs(all_laws)
        
        logger.info(f"Pipeline complete. Processed {len(all_laws)} laws.")


if __name__ == '__main__':
    main()
