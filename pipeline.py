#!/usr/bin/env python3
"""
Full Data Pipeline for Pakistani Legal Data
Runs: Scrape → Extract → Parse → Embed → Index

Run this daily to incrementally build the legal database.
"""

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_command(cmd: list, description: str) -> bool:
    """Run a command and return success status"""
    logger.info(f"\n{'='*60}")
    logger.info(f"STEP: {description}")
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info('='*60)
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False
        )
        logger.info(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ {description} failed with code {e.returncode}")
        return False
    except FileNotFoundError:
        logger.error(f"✗ Command not found: {cmd[0]}")
        return False


def check_progress():
    """Load and display current progress"""
    progress_file = Path("progress.json")
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
        
        print("\n" + "="*60)
        print("CURRENT PROGRESS")
        print("="*60)
        
        for source in ['pakistan_code', 'na_acts']:
            data = progress.get(source, {})
            print(f"\n{source}:")
            print(f"  Discovered: {data.get('total_discovered', 0)}")
            print(f"  Downloaded: {data.get('pdfs_downloaded', 0)}")
            print(f"  Extracted:  {data.get('texts_extracted', 0)}")
        print("="*60)
        return progress
    return {}


def count_pdfs():
    """Count downloaded PDFs"""
    pdf_dir = Path("data/raw/pdfs")
    if pdf_dir.exists():
        return len(list(pdf_dir.glob("*.pdf")))
    return 0


def count_embeddings():
    """Count generated embeddings"""
    emb_file = Path("data/embeddings/legal_embeddings.json")
    if emb_file.exists():
        with open(emb_file) as f:
            data = json.load(f)
        return len(data.get('documents', []))
    return 0


def run_pipeline(
    skip_scrape: bool = False,
    skip_extract: bool = False,
    skip_parse: bool = False,
    skip_embed: bool = False,
    scrape_limit: int = 20,
    embed_provider: str = "local"
):
    """Run the full data pipeline"""
    
    start_time = datetime.now()
    results = {}
    
    print("\n" + "🚀"*30)
    print("PAKISTANI LEGAL DATA PIPELINE")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀"*30)
    
    # Check initial progress
    initial_progress = check_progress()
    initial_pdfs = count_pdfs()
    initial_embeddings = count_embeddings()
    
    # Step 1: Scrape
    if not skip_scrape:
        success = run_command(
            [sys.executable, "daily_scraper.py", "pakistan-code", "--limit", str(scrape_limit)],
            "Scraping Pakistan Code PDFs"
        )
        results['scrape'] = success
    
    # Step 2: Extract text from PDFs
    if not skip_extract:
        success = run_command(
            [sys.executable, "scraper.py", "extract"],
            "Extracting text from PDFs"
        )
        results['extract'] = success
    
    # Step 3: Parse into sections
    if not skip_parse:
        success = run_command(
            [sys.executable, "section_parser.py"],
            "Parsing acts into sections"
        )
        results['parse'] = success
    
    # Step 4: Generate embeddings
    if not skip_embed:
        success = run_command(
            [sys.executable, "embedding_generator.py", "--provider", embed_provider],
            f"Generating embeddings ({embed_provider})"
        )
        results['embed'] = success
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    final_pdfs = count_pdfs()
    final_embeddings = count_embeddings()
    
    print("\n" + "="*60)
    print("PIPELINE SUMMARY")
    print("="*60)
    print(f"\nDuration: {duration}")
    print(f"\nPDFs: {initial_pdfs} → {final_pdfs} (+{final_pdfs - initial_pdfs})")
    print(f"Embeddings: {initial_embeddings} → {final_embeddings} (+{final_embeddings - initial_embeddings})")
    
    print("\nStep Results:")
    for step, success in results.items():
        status = "✓" if success else "✗"
        print(f"  {status} {step}")
    
    print("\n" + "="*60)
    
    # Update log
    log_file = Path("data/logs/pipeline.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(log_file, 'a') as f:
        f.write(f"\n{start_time.isoformat()} - Pipeline run\n")
        f.write(f"  Duration: {duration}\n")
        f.write(f"  PDFs: {initial_pdfs} → {final_pdfs}\n")
        f.write(f"  Embeddings: {initial_embeddings} → {final_embeddings}\n")
        for step, success in results.items():
            f.write(f"  {step}: {'OK' if success else 'FAILED'}\n")
    
    return all(results.values())


def main():
    parser = argparse.ArgumentParser(description='Run the full data pipeline')
    parser.add_argument('--skip-scrape', action='store_true',
                       help='Skip the scraping step')
    parser.add_argument('--skip-extract', action='store_true',
                       help='Skip the text extraction step')
    parser.add_argument('--skip-parse', action='store_true',
                       help='Skip the section parsing step')
    parser.add_argument('--skip-embed', action='store_true',
                       help='Skip the embedding generation step')
    parser.add_argument('--scrape-limit', type=int, default=20,
                       help='Number of PDFs to scrape (default: 20)')
    parser.add_argument('--embed-provider', choices=['local', 'openai', 'google'],
                       default='local', help='Embedding provider (default: local)')
    parser.add_argument('--status', action='store_true',
                       help='Just show current progress')
    
    args = parser.parse_args()
    
    if args.status:
        check_progress()
        print(f"\nPDFs downloaded: {count_pdfs()}")
        print(f"Embeddings generated: {count_embeddings()}")
        return
    
    success = run_pipeline(
        skip_scrape=args.skip_scrape,
        skip_extract=args.skip_extract,
        skip_parse=args.skip_parse,
        skip_embed=args.skip_embed,
        scrape_limit=args.scrape_limit,
        embed_provider=args.embed_provider
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
