#!/usr/bin/env python3
"""
Data Cleaner for PLS Scraper
=============================
Cleans raw HTML from scraped cases and extracts structured data.
Can be run standalone or integrated with the scraper.
"""

import re
import json
import html
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
import logging

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.CLEANER, "data_cleaner")
    HAS_STATUS_REPORTER = True
except ImportError:
    _status_reporter = None
    HAS_STATUS_REPORTER = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2"
HTML_DIR = DATA_DIR / "html"
BACKUP_DIR = DATA_DIR / "backup"

# Import HTML generator (lazy import to avoid circular deps)
_html_generator = None

def get_html_generator():
    """Lazy import of HTML generator module."""
    global _html_generator
    if _html_generator is None:
        try:
            from generate_html import generate_html_for_case
            _html_generator = generate_html_for_case
        except ImportError:
            logger.warning("HTML generator not available")
            _html_generator = lambda *args, **kwargs: None
    return _html_generator


import shutil
from datetime import date


def create_backup(data_dir: Path = DATA_DIR, backup_dir: Path = BACKUP_DIR) -> Path:
    """Create a timestamped backup of all JSON files before cleaning."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    backup_path = backup_dir / timestamp
    
    logger.info(f"Creating backup at {backup_path}...")
    
    # Count files to backup
    json_files = list(data_dir.rglob("*.json"))
    json_files = [f for f in json_files if f.name != 'progress.json' 
                  and 'html' not in str(f) 
                  and 'backup' not in str(f)
                  and 'audit' not in str(f)]
    
    if not json_files:
        logger.warning("No files to backup")
        return backup_path
    
    backup_path.mkdir(parents=True, exist_ok=True)
    
    backed_up = 0
    for filepath in json_files:
        # Preserve directory structure
        rel_path = filepath.relative_to(data_dir)
        dest_path = backup_path / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(filepath, dest_path)
        backed_up += 1
    
    logger.info(f"Backed up {backed_up} files to {backup_path}")
    
    # Also backup JSONL files
    jsonl_files = list(data_dir.glob("*.jsonl"))
    for jsonl_file in jsonl_files:
        shutil.copy2(jsonl_file, backup_path / jsonl_file.name)
    
    if jsonl_files:
        logger.info(f"Backed up {len(jsonl_files)} JSONL files")
    
    # Write backup manifest
    manifest = {
        'timestamp': timestamp,
        'files_backed_up': backed_up,
        'jsonl_backed_up': len(jsonl_files),
        'source_dir': str(data_dir),
        'backup_dir': str(backup_path)
    }
    with open(backup_path / 'manifest.json', 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    
    return backup_path


def restore_from_backup(backup_path: Path, data_dir: Path = DATA_DIR) -> int:
    """Restore files from a backup directory."""
    if not backup_path.exists():
        logger.error(f"Backup path does not exist: {backup_path}")
        return 0
    
    logger.info(f"Restoring from {backup_path}...")
    
    restored = 0
    for filepath in backup_path.rglob("*.json"):
        if filepath.name == 'manifest.json':
            continue
        
        rel_path = filepath.relative_to(backup_path)
        dest_path = data_dir / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(filepath, dest_path)
        restored += 1
    
    # Restore JSONL files
    for jsonl_file in backup_path.glob("*.jsonl"):
        shutil.copy2(jsonl_file, data_dir / jsonl_file.name)
    
    logger.info(f"Restored {restored} files from backup")
    return restored


def list_backups(backup_dir: Path = BACKUP_DIR) -> list:
    """List available backups."""
    if not backup_dir.exists():
        return []
    
    backups = []
    for d in sorted(backup_dir.iterdir(), reverse=True):
        if d.is_dir():
            manifest_path = d / 'manifest.json'
            if manifest_path.exists():
                with open(manifest_path) as f:
                    manifest = json.load(f)
                backups.append(manifest)
            else:
                backups.append({'timestamp': d.name, 'files_backed_up': '?'})
    
    return backups


def unescape_html(text: str) -> str:
    """Unescape HTML entities and unicode escapes."""
    if not text:
        return ""
    
    # Handle unicode escapes like \u003c
    try:
        # If it's a JSON-encoded string, decode it
        if '\\u' in text:
            text = text.encode().decode('unicode_escape')
    except:
        pass
    
    # Handle HTML entities
    text = html.unescape(text)
    
    return text


def strip_html_tags(html_content: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    if not html_content:
        return ""
    
    # Unescape first
    html_content = unescape_html(html_content)
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for element in soup(['script', 'style', 'head', 'meta']):
        element.decompose()
    
    # Get text with proper spacing
    text = soup.get_text(separator='\n', strip=True)
    
    # Clean up multiple newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Clean up multiple spaces
    text = re.sub(r' {2,}', ' ', text)
    
    # Remove MS Office artifacts
    text = re.sub(r'\[if[^\]]*\].*?\[endif\]', '', text, flags=re.DOTALL)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    
    return text.strip()


def extract_court(text: str) -> str:
    """Extract court name from case text."""
    if not text:
        return ""
    
    text = strip_html_tags(text)
    
    court_patterns = [
        r'\[(Supreme Court of Pakistan)\]',
        r'\[(High Court[^]]*)\]',
        r'\[(Federal Shariat Court)\]',
        r'\[(Sindh|Punjab|Lahore|Peshawar|Balochistan|Islamabad)[^\]]*\]',
        r'(Supreme Court of Pakistan)',
        r'(High Court of [A-Za-z]+)',
        r'(Federal Shariat Court)',
        r'(Customs Appellate Tribunal[^,\n]*)',
        r'(Income Tax Appellate Tribunal[^,\n]*)',
    ]
    
    for pattern in court_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return ""


def extract_date(text: str) -> str:
    """Extract decision date from case text."""
    if not text:
        return ""
    
    text = strip_html_tags(text)
    
    # Pattern: "decided on 1st November, 2023" or "Decided on 22nd March, 2022"
    date_patterns = [
        r'decided on\s+(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})',
        r'Date of (?:hearing|decision|order)[:\s]+(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})',
        r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),?\s+\d{4})',
        r'(\d{1,2}[./]\d{1,2}[./]\d{4})',
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return ""


def extract_judges(text: str) -> List[str]:
    """Extract judge names from case text."""
    if not text:
        return []
    
    text = strip_html_tags(text)
    
    judges = []
    
    # Look for "Present:" or "Before:" section
    present_match = re.search(
        r'(?:Present|Before)[:\s]+([^\n]+(?:JJ?\.?|Chief Justice|C\.J\.))',
        text,
        re.IGNORECASE
    )
    
    if present_match:
        judge_text = present_match.group(1)
        
        # Split by comma or 'and'
        parts = re.split(r',\s*|\s+and\s+', judge_text)
        
        for part in parts:
            # Clean up each judge name
            name = re.sub(r'\s*(JJ?\.?|Chief Justice|C\.J\.)\s*$', '', part).strip()
            name = re.sub(r'^(Mr\.|Mrs\.|Justice)\s*', '', name).strip()
            
            if name and len(name) > 2 and not name.isupper():
                judges.append(name)
    
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for j in judges:
        if j.lower() not in seen:
            seen.add(j.lower())
            unique.append(j)
    
    return unique


def extract_headnotes(text: str, citation: str = "") -> str:
    """Extract headnotes section from case text."""
    if not text:
        return ""
    
    text = strip_html_tags(text)
    
    # Headnotes are usually between the citation header and "JUDGMENT" or "ORDER"
    # They often start with statute references like "Constitution of Pakistan---"
    
    # Try to find headnotes section
    headnote_start = None
    headnote_end = None
    
    # Find where headnotes start (after citation, before judgment)
    lines = text.split('\n')
    in_headnotes = False
    headnote_lines = []
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Headnotes typically start with statute references
        if re.match(r'^[A-Z][a-z]+.*---$', line_stripped) or \
           re.match(r'^----.*---', line_stripped):
            in_headnotes = True
        
        # Headnotes end at JUDGMENT or ORDER
        if re.match(r'^(JUDGMENT|ORDER|J\s*U\s*D\s*G\s*M\s*E\s*N\s*T)$', line_stripped, re.IGNORECASE):
            break
        
        # Also end at counsel listings
        if 'for Appellant' in line_stripped or 'for Petitioner' in line_stripped:
            break
        
        if in_headnotes:
            headnote_lines.append(line)
    
    headnotes = '\n'.join(headnote_lines).strip()
    
    # Limit length
    if len(headnotes) > 5000:
        headnotes = headnotes[:5000] + "..."
    
    return headnotes


def extract_judgment(text: str) -> str:
    """Extract the main judgment text."""
    if not text:
        return ""
    
    text = strip_html_tags(text)
    
    # Find where judgment starts
    judgment_start = None
    
    patterns = [
        r'\n(JUDGMENT)\n',
        r'\n(ORDER)\n',
        r'\n(J\s*U\s*D\s*G\s*M\s*E\s*N\s*T)\n',
        r'\n(O\s*R\s*D\s*E\s*R)\n',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            judgment_start = match.start()
            break
    
    if judgment_start:
        judgment = text[judgment_start:].strip()
    else:
        # Fallback: return full text
        judgment = text
    
    return judgment


def extract_title(text: str, citation: str = "") -> str:
    """Extract case title (parties)."""
    if not text:
        return ""
    
    text = strip_html_tags(text)
    
    # Look for "Versus" pattern
    versus_match = re.search(
        r'([A-Z][A-Za-z\s.,]+(?:---)?)\s*[Vv]ersus\s*([A-Z][A-Za-z\s.,]+)',
        text
    )
    
    if versus_match:
        petitioner = versus_match.group(1).strip().rstrip('-')
        respondent = versus_match.group(2).strip()
        return f"{petitioner} v. {respondent}"
    
    return ""


def clean_case(case_data: dict) -> dict:
    """Clean a single case's data."""
    
    # Get raw content for parsing (check both original and cleaned field names)
    raw_judgment = case_data.get('judgment', '') or case_data.get('judgment_raw', '') or case_data.get('text', '')
    
    # Safety check: if no raw judgment found, skip cleaning to preserve existing data
    if not raw_judgment:
        return case_data  # Return unchanged
    
    raw_court = case_data.get('court', '')
    
    # Unescape and clean the raw judgment
    clean_text = strip_html_tags(raw_judgment)
    
    # Create proper HTML (decoded, renderable in browser)
    judgment_html = unescape_html(raw_judgment)
    
    # Extract clean fields
    cleaned = {
        'citation': case_data.get('citation', ''),
        'case_name': case_data.get('case_name', ''),
        'title': extract_title(clean_text, case_data.get('citation', '')) or case_data.get('title', ''),
        'court': extract_court(raw_court + ' ' + clean_text) or case_data.get('court', ''),
        'date': extract_date(clean_text) or case_data.get('date', ''),
        'judges': extract_judges(clean_text) or case_data.get('judges', []),
        'headnotes': extract_headnotes(clean_text, case_data.get('citation', '')),
        'judgment_clean': extract_judgment(clean_text),
        'judgment_html': judgment_html,  # Decoded HTML (renderable)
        'judgment_raw': raw_judgment,  # Keep escaped raw for reference
        'statutes_cited': case_data.get('statutes_cited', []),
        'cases_cited': case_data.get('cases_cited', []),
        'fetched_at': case_data.get('fetched_at', ''),
        'cleaned_at': datetime.now().isoformat(),
    }
    
    # Clean up judges list
    if isinstance(cleaned['judges'], list):
        cleaned['judges'] = [j for j in cleaned['judges'] if j and len(j) > 2]
        # Deduplicate
        seen = set()
        unique_judges = []
        for j in cleaned['judges']:
            if j.lower() not in seen:
                seen.add(j.lower())
                unique_judges.append(j)
        cleaned['judges'] = unique_judges
    
    return cleaned


def process_file(filepath: Path, overwrite: bool = True, generate_html: bool = True) -> bool:
    """Process and clean a single JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Skip if already cleaned
        if data.get('cleaned_at') and not overwrite:
            return True
        
        cleaned = clean_case(data)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False)
        
        # Generate HTML file for this case
        if generate_html:
            try:
                html_generator = get_html_generator()
                html_generator(cleaned, HTML_DIR)
            except Exception as e:
                logger.warning(f"Could not generate HTML for {filepath.name}: {e}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {filepath}: {e}")
        return False


def process_all(data_dir: Path = DATA_DIR, overwrite: bool = False, generate_html: bool = True, backup: bool = True):
    """Process all JSON files in data directory."""
    logger.info(f"Processing files in {data_dir}")
    
    # Create backup before cleaning (safety first!)
    if backup:
        try:
            backup_path = create_backup(data_dir)
            logger.info(f"Backup created: {backup_path}")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            logger.error("Aborting cleaning to prevent data loss. Use --no-backup to skip.")
            return 0, 0, 0
    
    # Report status to orchestrator
    if HAS_STATUS_REPORTER and _status_reporter:
        _status_reporter.start(task="Cleaning all cases")
    
    json_files = list(data_dir.rglob("*.json"))
    json_files = [f for f in json_files if f.name != 'progress.json' and 'html' not in f.parts]
    
    logger.info(f"Found {len(json_files)} files to process")
    
    success = 0
    failed = 0
    skipped = 0
    total = len(json_files)
    
    for i, filepath in enumerate(json_files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data.get('cleaned_at') and not overwrite:
                skipped += 1
                continue
            
            if process_file(filepath, overwrite, generate_html):
                success += 1
            else:
                failed += 1
            
            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{total} ({success} cleaned, {skipped} skipped, {failed} failed)")
                # Update status for orchestrator
                if HAS_STATUS_REPORTER and _status_reporter:
                    _status_reporter.progress_update(i + 1, total, f"{success} cleaned, {skipped} skipped")
        
        except Exception as e:
            logger.error(f"Error with {filepath}: {e}")
            failed += 1
    
    logger.info(f"Complete: {success} cleaned, {skipped} skipped, {failed} failed")
    
    # Report completion
    if HAS_STATUS_REPORTER and _status_reporter:
        _status_reporter.complete(success=failed == 0, message=f"{success} cleaned, {skipped} skipped, {failed} failed")
    return success, skipped, failed


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean scraped case data")
    parser.add_argument("command", choices=["clean", "clean-one", "test", "list-backups", "restore"], 
                        help="Command to run")
    parser.add_argument("--file", "-f", help="Single file to clean")
    parser.add_argument("--overwrite", "-o", action="store_true",
                        help="Overwrite already cleaned files")
    parser.add_argument("--no-html", action="store_true",
                        help="Skip HTML generation")
    parser.add_argument("--no-backup", action="store_true",
                        help="Skip backup before cleaning (dangerous!)")
    parser.add_argument("--backup-path", help="Backup path for restore command")
    
    args = parser.parse_args()
    
    if args.command == "clean":
        process_all(DATA_DIR, args.overwrite, generate_html=not args.no_html, backup=not args.no_backup)
    
    elif args.command == "list-backups":
        backups = list_backups()
        if not backups:
            print("No backups found")
        else:
            print(f"\n{'='*60}")
            print(f"{'AVAILABLE BACKUPS':^60}")
            print(f"{'='*60}")
            for b in backups:
                print(f"  {b['timestamp']} - {b.get('files_backed_up', '?')} files")
            print(f"\nBackup location: {BACKUP_DIR}")
            print(f"To restore: python data_cleaner.py restore --backup-path {BACKUP_DIR}/<timestamp>")
    
    elif args.command == "restore":
        if not args.backup_path:
            print("Please specify --backup-path")
            print("Use 'python data_cleaner.py list-backups' to see available backups")
            return
        restored = restore_from_backup(Path(args.backup_path))
        print(f"Restored {restored} files")
    
    elif args.command == "clean-one":
        if not args.file:
            print("Please specify --file")
            return
        process_file(Path(args.file), overwrite=True, generate_html=not args.no_html)
        print(f"Cleaned: {args.file}")
    
    elif args.command == "test":
        # Test on first file
        files = list(DATA_DIR.rglob("*.json"))
        files = [f for f in files if f.name != 'progress.json']
        if files:
            test_file = files[0]
            print(f"Testing on: {test_file}")
            
            with open(test_file, 'r', encoding='utf-8') as f:
                original = json.load(f)
            
            cleaned = clean_case(original)
            
            print("\n=== ORIGINAL ===")
            print(f"Court: {original.get('court', '')[:100]}...")
            print(f"Date: {original.get('date', '')}")
            print(f"Judges: {original.get('judges', [])[:5]}")
            
            print("\n=== CLEANED ===")
            print(f"Court: {cleaned['court']}")
            print(f"Date: {cleaned['date']}")
            print(f"Judges: {cleaned['judges']}")
            print(f"Title: {cleaned['title']}")
            print(f"Headnotes: {cleaned['headnotes'][:200]}...")
            print(f"Judgment (first 500 chars): {cleaned['judgment_clean'][:500]}...")


if __name__ == "__main__":
    main()
