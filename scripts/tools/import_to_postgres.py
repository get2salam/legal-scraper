#!/usr/bin/env python3
"""
Qanoon Legal Research Platform - PostgreSQL Import Script

Imports case data from JSONL file into PostgreSQL database with:
- HTML to plain text conversion for full-text search
- Date parsing from various formats
- Batch inserts for performance
- Progress tracking and error handling

Usage:
    python import_to_postgres.py [--db-url URL] [--input FILE] [--batch-size N]
    
Environment variables:
    DATABASE_URL: PostgreSQL connection string (default: postgresql://localhost/qanoon)
"""

import json
import re
import os
import sys
import argparse
import logging
from datetime import datetime
from html.parser import HTMLParser
from typing import Optional, List, Dict, Any
from io import StringIO

import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HTMLStripper(HTMLParser):
    """Strip HTML tags and decode entities to plain text."""
    
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()
    
    def handle_data(self, data):
        self.text.write(data)
    
    def get_data(self):
        return self.text.getvalue()


def strip_html(html: str) -> str:
    """Convert HTML to plain text."""
    if not html:
        return ""
    
    # Handle escaped unicode
    try:
        html = html.encode('utf-8').decode('unicode_escape')
    except:
        pass
    
    # Remove script and style tags with content
    html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    
    # Strip HTML tags
    stripper = HTMLStripper()
    try:
        stripper.feed(html)
        text = stripper.get_data()
    except:
        # Fallback: simple regex strip
        text = re.sub(r'<[^>]+>', ' ', html)
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string in various formats.
    
    Examples:
        "14th June, 2023" -> 2023-06-14
        "10th November, 2023" -> 2023-11-10
        "2023-06-14" -> 2023-06-14
    """
    if not date_str:
        return None
    
    # Remove ordinal suffixes (1st, 2nd, 3rd, 4th, etc.)
    date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str, flags=re.IGNORECASE)
    
    # Try various formats
    formats = [
        '%d %B, %Y',      # "14 June, 2023"
        '%d %B %Y',       # "14 June 2023"
        '%B %d, %Y',      # "June 14, 2023"
        '%B %d %Y',       # "June 14 2023"
        '%Y-%m-%d',       # "2023-06-14"
        '%d-%m-%Y',       # "14-06-2023"
        '%d/%m/%Y',       # "14/06/2023"
        '%Y/%m/%d',       # "2023/06/14"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    # Try to extract year at minimum
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        logger.debug(f"Could only extract year from date: {date_str}")
        # Return January 1st of that year as fallback
        try:
            return datetime(int(year_match.group()), 1, 1)
        except:
            pass
    
    logger.debug(f"Could not parse date: {date_str}")
    return None


def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse ISO timestamp string."""
    if not ts_str:
        return None
    
    formats = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    
    return None


def clean_court_name(court: str) -> str:
    """Clean court name by removing HTML artifacts."""
    if not court:
        return ""
    
    # Remove any HTML that might be in court field
    court = strip_html(court)
    
    # Remove common artifacts
    court = re.sub(r'\[?o:p\]?', '', court, flags=re.IGNORECASE)
    court = re.sub(r'\\u003[co].*', '', court)
    
    # Normalize
    court = re.sub(r'\s+', ' ', court).strip()
    
    # Remove trailing brackets/junk
    court = re.sub(r'[\]\[]+$', '', court).strip()
    
    return court


def process_case(case: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single case record for database insertion."""
    
    # Extract and clean judgment
    judgment_html = case.get('judgment', '')
    
    # Handle double-encoded JSON strings
    if isinstance(judgment_html, str) and judgment_html.startswith('"'):
        try:
            judgment_html = json.loads(judgment_html)
        except:
            pass
    
    judgment_clean = strip_html(judgment_html)
    
    # Parse date
    date_raw = case.get('date', '')
    case_date = parse_date(date_raw)
    
    # Clean court name
    court = clean_court_name(case.get('court', ''))
    
    # Parse timestamp
    fetched_at = parse_timestamp(case.get('fetched_at', ''))
    cleaned_at = parse_timestamp(case.get('cleaned_at', ''))
    
    # Ensure arrays are lists
    judges = case.get('judges', [])
    if not isinstance(judges, list):
        judges = [judges] if judges else []
    judges = [j for j in judges if j]  # Remove empty strings
    
    statutes_cited = case.get('statutes_cited', [])
    if not isinstance(statutes_cited, list):
        statutes_cited = [statutes_cited] if statutes_cited else []
    statutes_cited = [s for s in statutes_cited if s]
    
    cases_cited = case.get('cases_cited', [])
    if not isinstance(cases_cited, list):
        cases_cited = [cases_cited] if cases_cited else []
    cases_cited = [c for c in cases_cited if c]
    
    # Clean headnotes
    headnotes = case.get('headnotes', '')
    if headnotes:
        headnotes = strip_html(headnotes)
    
    return {
        'citation': case.get('citation', ''),
        'case_name': case.get('case_name', ''),
        'title': case.get('title', ''),
        'court': court,
        'case_date': case_date,
        'date_raw': date_raw,
        'judges': judges,
        'headnotes': headnotes,
        'judgment_html': judgment_html,
        'judgment_clean': judgment_clean,
        'statutes_cited': statutes_cited,
        'cases_cited': cases_cited,
        'fetched_at': fetched_at,
        'cleaned_at': cleaned_at,
    }


def create_connection(db_url: str):
    """Create database connection."""
    try:
        conn = psycopg2.connect(db_url)
        conn.set_client_encoding('UTF8')
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def setup_database(conn, schema_file: str = 'db_schema.sql'):
    """Run schema setup if schema file exists."""
    if os.path.exists(schema_file):
        logger.info(f"Running schema setup from {schema_file}")
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        logger.info("Schema setup complete")
    else:
        logger.warning(f"Schema file not found: {schema_file}")


def insert_batch(conn, cases: List[Dict[str, Any]]) -> int:
    """Insert a batch of cases using execute_values for performance."""
    if not cases:
        return 0
    
    insert_sql = """
        INSERT INTO cases (
            citation, case_name, title, court, case_date, date_raw,
            judges, headnotes, judgment_html, judgment_clean,
            statutes_cited, cases_cited, fetched_at, cleaned_at
        ) VALUES %s
        ON CONFLICT (citation) DO UPDATE SET
            case_name = EXCLUDED.case_name,
            title = EXCLUDED.title,
            court = EXCLUDED.court,
            case_date = EXCLUDED.case_date,
            date_raw = EXCLUDED.date_raw,
            judges = EXCLUDED.judges,
            headnotes = EXCLUDED.headnotes,
            judgment_html = EXCLUDED.judgment_html,
            judgment_clean = EXCLUDED.judgment_clean,
            statutes_cited = EXCLUDED.statutes_cited,
            cases_cited = EXCLUDED.cases_cited,
            fetched_at = EXCLUDED.fetched_at,
            cleaned_at = EXCLUDED.cleaned_at,
            updated_at = NOW()
    """
    
    # Prepare values
    values = []
    for case in cases:
        values.append((
            case['citation'],
            case['case_name'],
            case['title'],
            case['court'],
            case['case_date'],
            case['date_raw'],
            case['judges'],
            case['headnotes'],
            case['judgment_html'],
            case['judgment_clean'],
            case['statutes_cited'],
            case['cases_cited'],
            case['fetched_at'],
            case['cleaned_at'],
        ))
    
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values)
    
    return len(cases)


def import_jsonl(
    input_file: str,
    db_url: str,
    batch_size: int = 100,
    setup_schema: bool = True,
    schema_file: str = 'db_schema.sql'
) -> Dict[str, int]:
    """
    Import JSONL file to PostgreSQL.
    
    Returns dict with stats: total, imported, errors
    """
    stats = {'total': 0, 'imported': 0, 'errors': 0, 'skipped': 0}
    
    logger.info(f"Connecting to database...")
    conn = create_connection(db_url)
    
    try:
        if setup_schema:
            setup_database(conn, schema_file)
        
        logger.info(f"Reading from {input_file}")
        batch = []
        
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                stats['total'] += 1
                
                try:
                    # Parse JSON
                    case = json.loads(line.strip())
                    
                    # Skip if no citation
                    if not case.get('citation'):
                        logger.warning(f"Line {line_num}: Missing citation, skipping")
                        stats['skipped'] += 1
                        continue
                    
                    # Process case
                    processed = process_case(case)
                    batch.append(processed)
                    
                    # Insert batch when full
                    if len(batch) >= batch_size:
                        inserted = insert_batch(conn, batch)
                        conn.commit()
                        stats['imported'] += inserted
                        logger.info(f"Imported {stats['imported']}/{stats['total']} cases...")
                        batch = []
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Line {line_num}: JSON parse error - {e}")
                    stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Line {line_num}: Error processing - {e}")
                    stats['errors'] += 1
        
        # Insert remaining batch
        if batch:
            inserted = insert_batch(conn, batch)
            conn.commit()
            stats['imported'] += inserted
        
        logger.info(f"Import complete: {stats['imported']} imported, {stats['errors']} errors, {stats['skipped']} skipped")
        
        # Print some stats
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cases")
            total = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM cases WHERE case_date IS NOT NULL")
            with_dates = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM cases WHERE array_length(judges, 1) > 0")
            with_judges = cur.fetchone()[0]
            
            logger.info(f"Database stats: {total} total cases, {with_dates} with parsed dates, {with_judges} with judges")
        
    finally:
        conn.close()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='Import legal cases to PostgreSQL')
    parser.add_argument(
        '--db-url', '-d',
        default=os.environ.get('DATABASE_URL', 'postgresql://localhost/qanoon'),
        help='PostgreSQL connection URL (default: postgresql://localhost/qanoon)'
    )
    parser.add_argument(
        '--input', '-i',
        default='data_v2/all_cases.jsonl',
        help='Input JSONL file (default: data_v2/all_cases.jsonl)'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=100,
        help='Batch size for inserts (default: 100)'
    )
    parser.add_argument(
        '--no-schema',
        action='store_true',
        help='Skip schema setup (assume tables exist)'
    )
    parser.add_argument(
        '--schema-file', '-s',
        default='db_schema.sql',
        help='Schema SQL file (default: db_schema.sql)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)
    
    # Run import
    try:
        stats = import_jsonl(
            input_file=args.input,
            db_url=args.db_url,
            batch_size=args.batch_size,
            setup_schema=not args.no_schema,
            schema_file=args.schema_file
        )
        
        if stats['errors'] > 0:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
