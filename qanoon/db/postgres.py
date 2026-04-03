"""
PostgreSQL Importer
===================

Imports case data from JSONL to PostgreSQL with:
- HTML to plain text conversion
- Date parsing from various formats
- Batch inserts for performance
- Full-text search support

Example:
    from qanoon.db import PostgresImporter
    
    importer = PostgresImporter()
    stats = importer.import_jsonl("data_v2/all_cases.jsonl")
"""

import os
import re
import json
import logging
from datetime import datetime
from html.parser import HTMLParser
from typing import Optional, List, Dict, Any
from io import StringIO
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

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


def strip_html(html_content: str) -> str:
    """Convert HTML to plain text."""
    if not html_content:
        return ""
    
    try:
        html_content = html_content.encode('utf-8').decode('unicode_escape')
    except:
        pass
    
    html_content = re.sub(r'<script[^>]*>.*?</script>', ' ', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', ' ', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    stripper = HTMLStripper()
    try:
        stripper.feed(html_content)
        text = stripper.get_data()
    except:
        text = re.sub(r'<[^>]+>', ' ', html_content)
    
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string in various formats."""
    if not date_str:
        return None
    
    date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str, flags=re.IGNORECASE)
    
    formats = [
        '%d %B, %Y',
        '%d %B %Y',
        '%B %d, %Y',
        '%B %d %Y',
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        try:
            return datetime(int(year_match.group()), 1, 1)
        except:
            pass
    
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
    
    court = strip_html(court)
    court = re.sub(r'\[?o:p\]?', '', court, flags=re.IGNORECASE)
    court = re.sub(r'\\u003[co].*', '', court)
    court = re.sub(r'\s+', ' ', court).strip()
    court = re.sub(r'[\]\[]+$', '', court).strip()
    
    return court


def process_case(case: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single case record for database insertion."""
    judgment_html = case.get('judgment', '')
    
    if isinstance(judgment_html, str) and judgment_html.startswith('"'):
        try:
            judgment_html = json.loads(judgment_html)
        except:
            pass
    
    judgment_clean = strip_html(judgment_html)
    case_date = parse_date(case.get('date', ''))
    court = clean_court_name(case.get('court', ''))
    fetched_at = parse_timestamp(case.get('fetched_at', ''))
    cleaned_at = parse_timestamp(case.get('cleaned_at', ''))
    
    judges = case.get('judges', [])
    if not isinstance(judges, list):
        judges = [judges] if judges else []
    judges = [j for j in judges if j]
    
    statutes_cited = case.get('statutes_cited', [])
    if not isinstance(statutes_cited, list):
        statutes_cited = [statutes_cited] if statutes_cited else []
    statutes_cited = [s for s in statutes_cited if s]
    
    cases_cited = case.get('cases_cited', [])
    if not isinstance(cases_cited, list):
        cases_cited = [cases_cited] if cases_cited else []
    cases_cited = [c for c in cases_cited if c]
    
    headnotes = case.get('headnotes', '')
    if headnotes:
        headnotes = strip_html(headnotes)
    
    return {
        'citation': case.get('citation', ''),
        'case_name': case.get('case_name', ''),
        'title': case.get('title', ''),
        'court': court,
        'case_date': case_date,
        'date_raw': case.get('date', ''),
        'judges': judges,
        'headnotes': headnotes,
        'judgment_html': judgment_html,
        'judgment_clean': judgment_clean,
        'statutes_cited': statutes_cited,
        'cases_cited': cases_cited,
        'fetched_at': fetched_at,
        'cleaned_at': cleaned_at,
    }


class PostgresImporter:
    """
    Imports legal cases to PostgreSQL.
    
    Args:
        db_url: PostgreSQL connection URL
        batch_size: Number of cases per batch insert
    """
    
    def __init__(
        self,
        db_url: str = None,
        batch_size: int = 100
    ):
        self.db_url = db_url or os.environ.get('DATABASE_URL', 'postgresql://localhost/qanoon')
        self.batch_size = batch_size
        self.conn = None
    
    def connect(self):
        """Create database connection."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.set_client_encoding('UTF8')
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def setup_schema(self, schema_file: str = None):
        """Run schema setup from SQL file."""
        if schema_file and Path(schema_file).exists():
            logger.info(f"Running schema setup from {schema_file}")
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            with self.conn.cursor() as cur:
                cur.execute(schema_sql)
            self.conn.commit()
            logger.info("Schema setup complete")
    
    def insert_batch(self, cases: List[Dict[str, Any]]) -> int:
        """Insert a batch of cases using execute_values."""
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
        
        with self.conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        
        return len(cases)
    
    def import_jsonl(
        self,
        input_file: str,
        setup_schema: bool = True,
        schema_file: str = None
    ) -> Dict[str, int]:
        """
        Import JSONL file to PostgreSQL.
        
        Returns dict with stats: total, imported, errors
        """
        stats = {'total': 0, 'imported': 0, 'errors': 0, 'skipped': 0}
        
        logger.info(f"Connecting to database...")
        if not self.connect():
            raise Exception("Failed to connect to database")
        
        try:
            if setup_schema and schema_file:
                self.setup_schema(schema_file)
            
            logger.info(f"Reading from {input_file}")
            batch = []
            
            with open(input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    stats['total'] += 1
                    
                    try:
                        case = json.loads(line.strip())
                        
                        if not case.get('citation'):
                            logger.warning(f"Line {line_num}: Missing citation, skipping")
                            stats['skipped'] += 1
                            continue
                        
                        processed = process_case(case)
                        batch.append(processed)
                        
                        if len(batch) >= self.batch_size:
                            inserted = self.insert_batch(batch)
                            self.conn.commit()
                            stats['imported'] += inserted
                            logger.info(f"Imported {stats['imported']}/{stats['total']} cases...")
                            batch = []
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Line {line_num}: JSON parse error - {e}")
                        stats['errors'] += 1
                    except Exception as e:
                        logger.error(f"Line {line_num}: Error processing - {e}")
                        stats['errors'] += 1
            
            if batch:
                inserted = self.insert_batch(batch)
                self.conn.commit()
                stats['imported'] += inserted
            
            logger.info(f"Import complete: {stats['imported']} imported, {stats['errors']} errors")
            
        finally:
            self.close()
        
        return stats
