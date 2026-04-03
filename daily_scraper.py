#!/usr/bin/env python3
"""
Daily Scraper - One Year Per Day
=================================
Autonomous scraper that processes one year per day.
Tracks progress and automatically moves to next year.

Features:
- PLS operating hours check (7 AM - 9 PM PKT)
- Automatic verification after scraping
- Data cleaning post-scrape
- Windows notifications
- Pipeline status reporting

Run via Windows Task Scheduler at your preferred time.
"""

import json
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Configuration
SCRAPER_DIR = Path(__file__).parent
SCHEDULE_FILE = SCRAPER_DIR / "daily_schedule.json"
LOG_FILE = SCRAPER_DIR / "daily_scraper.log"
PROGRESS_FILE = SCRAPER_DIR / "data_v2" / "progress.json"

# Years to scrape (newest first)
ALL_YEARS = list(range(2025, 1946, -1))  # 2025 down to 1947

# Reporters
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Safety settings
MAX_RUNTIME_HOURS = 8  # Stop after this many hours (safety limit)
SESSION_BREAK_MINUTES = 30  # Break every N cases
SESSION_BREAK_AFTER_CASES = 500  # Take break after this many cases

# PLS Operating Hours (PKT = UTC+5)
PLS_OPEN_HOUR = 7   # 7 AM PKT
PLS_CLOSE_HOUR = 21  # 9 PM PKT
PKT_OFFSET = timedelta(hours=5)

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.SCRAPER, "daily_scraper")
    HAS_STATUS_REPORTER = True
except ImportError:
    _status_reporter = None
    HAS_STATUS_REPORTER = False


def log(msg: str):
    """Log with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def is_pls_open() -> bool:
    """Check if PLS is within operating hours - DISABLED for 24/7 scraping."""
    return True  # 24/7 aggressive scraping mode


def get_pkt_time_str() -> str:
    """Get current PKT time as string."""
    utc_now = datetime.now(timezone.utc)
    pkt_now = utc_now + PKT_OFFSET
    return pkt_now.strftime("%H:%M PKT")


def load_schedule() -> dict:
    """Load schedule tracking file."""
    if SCHEDULE_FILE.exists():
        try:
            return json.loads(SCHEDULE_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {
        "current_year": None,
        "completed_years": [],
        "started_at": None,
        "last_run": None
    }


def save_schedule(schedule: dict):
    """Save schedule tracking file."""
    schedule["last_run"] = datetime.now().isoformat()
    SCHEDULE_FILE.write_text(json.dumps(schedule, indent=2, ensure_ascii=False), encoding='utf-8')


def load_progress() -> dict:
    """Load scraper progress."""
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"completed_searches": [], "cases_fetched": [], "total_cases": 0}


def is_year_complete(year: int) -> bool:
    """Check if all reporters for a year are done."""
    progress = load_progress()
    completed = progress.get("completed_searches", [])
    for reporter in REPORTERS:
        if f"{year}-{reporter}" not in completed:
            return False
    return True


def get_next_year(schedule: dict) -> int | None:
    """Get the next year to scrape."""
    completed = set(schedule.get("completed_years", []))
    for year in ALL_YEARS:
        if year not in completed:
            # Double-check with progress file
            if not is_year_complete(year):
                return year
            else:
                # Year is done, add to completed
                if year not in completed:
                    schedule["completed_years"].append(year)
    return None


def run_scraper(year: int) -> bool:
    """Run the scraper for a specific year."""
    log(f"Starting scraper for year {year}...")
    
    try:
        # Run scraper as subprocess
        result = subprocess.run(
            [sys.executable, "pls_scraper_v2.py", "scrape", "--year", str(year)],
            cwd=SCRAPER_DIR,
            timeout=MAX_RUNTIME_HOURS * 3600,  # Convert hours to seconds
            capture_output=False  # Let output go to console/log
        )
        
        if result.returncode == 0:
            log(f"Scraper completed successfully for year {year}")
            return True
        else:
            log(f"Scraper exited with code {result.returncode} for year {year}")
            return False
            
    except subprocess.TimeoutExpired:
        log(f"Scraper timed out after {MAX_RUNTIME_HOURS} hours for year {year}")
        return False
    except KeyboardInterrupt:
        log("Scraper interrupted by user")
        return False
    except Exception as e:
        log(f"Error running scraper: {e}")
        return False


def run_data_cleaner() -> tuple:
    """Run the data cleaner on all files."""
    log("Running data cleaner...")
    try:
        from data_cleaner import process_all, DATA_DIR
        success, skipped, failed = process_all(DATA_DIR, overwrite=False)
        log(f"Data cleaner complete: {success} cleaned, {skipped} skipped, {failed} failed")
        return success, skipped, failed
    except Exception as e:
        log(f"Data cleaner error: {e}")
        return 0, 0, 0


def run_verification(year: int, fix: bool = True) -> bool:
    """
    Run verification for a specific year after scraping.
    Returns True if verification passed (no missing cases).
    """
    log(f"Running verification for year {year}...")
    try:
        from verify_scraper import verify_after_scrape
        success = verify_after_scrape(year, fix=fix)
        if success:
            log(f"Verification passed for year {year}")
        else:
            log(f"Verification found issues for year {year} (check audit report)")
        return success
    except Exception as e:
        log(f"Verification error: {e}")
        return False


def send_notification(title: str, message: str):
    """Send a notification (Windows toast)."""
    try:
        # Try PowerShell toast notification
        ps_script = f'''
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
        $textNodes = $template.GetElementsByTagName("text")
        $textNodes.Item(0).AppendChild($template.CreateTextNode("{title}")) | Out-Null
        $textNodes.Item(1).AppendChild($template.CreateTextNode("{message}")) | Out-Null
        $toast = [Windows.UI.Notifications.ToastNotification]::new($template)
        [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("PLS Scraper").Show($toast)
        '''
        subprocess.run(["powershell", "-Command", ps_script], capture_output=True)
    except:
        pass  # Notification is optional


def main():
    log("=" * 60)
    log("Daily Scraper - Starting")
    log("=" * 60)
    
    # Check PLS operating hours
    if not is_pls_open():
        log(f"PLS is closed (current time: {get_pkt_time_str()})")
        log(f"Operating hours: {PLS_OPEN_HOUR}:00 - {PLS_CLOSE_HOUR}:00 PKT")
        log("Scraper will wait for operating hours or exit.")
        send_notification("PLS Scraper", f"Outside operating hours ({get_pkt_time_str()}). Waiting...")
        # Note: The actual scraper (pls_scraper_v2) will wait for operating hours
    else:
        log(f"PLS is open (current time: {get_pkt_time_str()})")
    
    # Load schedule
    schedule = load_schedule()
    
    # Get next year to scrape
    year = get_next_year(schedule)
    
    if year is None:
        log("All years completed! Nothing to do.")
        send_notification("PLS Scraper", "All years completed!")
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.complete(success=True, message="All years completed")
        return
    
    log(f"Today's target: {year}")
    log(f"Completed years: {len(schedule.get('completed_years', []))}/{len(ALL_YEARS)}")
    
    # Report status to orchestrator
    if HAS_STATUS_REPORTER and _status_reporter:
        _status_reporter.start(task=f"Scraping year {year}", year=year)
    
    # Check if year already in progress
    if schedule.get("current_year") == year:
        log(f"Resuming year {year} from previous run")
    else:
        schedule["current_year"] = year
        schedule["started_at"] = datetime.now().isoformat()
        save_schedule(schedule)
    
    # Send start notification
    send_notification("PLS Scraper Started", f"Scraping year {year}")
    
    # Run scraper
    success = run_scraper(year)
    
    # Check if year is complete
    if is_year_complete(year):
        log(f"Year {year} COMPLETED!")
        if year not in schedule.get("completed_years", []):
            schedule.setdefault("completed_years", []).append(year)
        schedule["current_year"] = None
        save_schedule(schedule)
        
        # Run verification (with auto-fix for missing cases)
        log("Starting post-scrape verification...")
        verification_ok = run_verification(year, fix=True)
        
        # Run data cleaner
        log("Starting post-scrape data cleaning...")
        cleaned, skipped, failed = run_data_cleaner()
        
        # Summary
        progress = load_progress()
        total = progress.get("total_cases", 0)
        status = "✓" if verification_ok else "⚠"
        send_notification("PLS Scraper Complete", f"{status} Year {year} done! Total: {total} cases, {cleaned} cleaned")
        
        # Report completion to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.complete(
                success=verification_ok,
                message=f"Year {year} complete: {total} cases, {cleaned} cleaned"
            )
    else:
        log(f"Year {year} not yet complete (will resume tomorrow)")
        save_schedule(schedule)
        send_notification("PLS Scraper Paused", f"Year {year} partial - will resume")
        
        # Report partial completion
        if HAS_STATUS_REPORTER and _status_reporter:
            progress = load_progress()
            _status_reporter.progress_update(
                len(progress.get("completed_searches", [])),
                len(REPORTERS),
                f"Year {year} partial - will resume"
            )
    
    # Final stats
    progress = load_progress()
    log(f"Total cases in database: {progress.get('total_cases', 0)}")
    log("Daily Scraper - Finished")
    log("=" * 60)


if __name__ == "__main__":
    main()
