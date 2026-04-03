#!/usr/bin/env python3
"""
Dedicated SCMR 2023 scraper - fills the gap
Uses existing PLSScraperV2 infrastructure
"""

import sys
import logging
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from pls_scraper_v2 import PLSScraperV2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler('logs/scmr_2023_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Scrape SCMR 2023"""
    logger.info("=" * 60)
    logger.info("SCMR 2023 DEDICATED SCRAPER")
    logger.info("=" * 60)
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize scraper
    scraper = PLSScraperV2()
    
    # Login
    if not scraper.login():
        logger.error("Failed to login to PLS")
        return
    
    logger.info("Logged in successfully, starting SCMR 2023...")
    
    # Scrape SCMR 2023
    count = scraper.scrape_reporter_year("SCMR", 2023)
    
    logger.info("=" * 60)
    logger.info(f"SCMR 2023 COMPLETE - {count} cases scraped")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
