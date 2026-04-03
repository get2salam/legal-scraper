#!/usr/bin/env python3
"""
Qanoon Pipeline Orchestrator
=============================
Autonomous supervisor for the Pakistan legislation scraping pipeline.
Monitors work, executes jobs, handles errors - no human intervention needed.

Pipeline Order:
    Scrape Year → Verify → Fix Missing → Re-verify → Clean → Generate HTML → Update JSONL

Modes:
    python orchestrator.py --daemon        # Run as daemon, check every 30 min
    python orchestrator.py --run-pending   # One-shot, run all pending work
    python orchestrator.py --status        # Show current status
    python orchestrator.py --force-fix     # Force fix missing cases now
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
import subprocess
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import threading

# ==============================================================================
# Configuration
# ==============================================================================

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
LOGS_DIR = DATA_DIR / "logs"
STATE_FILE = DATA_DIR / "orchestrator_state.json"
SCHEDULE_FILE = SCRIPT_DIR / "daily_schedule.json"
PROGRESS_FILE = DATA_DIR / "progress.json"

# Scripts to orchestrate
SCRAPER_SCRIPT = SCRIPT_DIR / "pls_scraper_v2.py"
VERIFY_SCRIPT = SCRIPT_DIR / "verify_scraper.py"
CLEANER_SCRIPT = SCRIPT_DIR / "data_cleaner.py"
HTML_SCRIPT = SCRIPT_DIR / "generate_html.py"
JSONL_SCRIPT = SCRIPT_DIR / "convert_to_jsonl.py"

# Timing
CHECK_INTERVAL_MINUTES = 30
PLS_OPERATING_HOURS = (7, 21)  # 7AM - 9PM PKT (UTC+5)
PKT_OFFSET_HOURS = 5

# Reporters
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Job settings
MAX_FIX_ATTEMPTS = 3
JOB_TIMEOUT_HOURS = 8  # Max time for a single job

# Ensure directories exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# Logging Setup
# ==============================================================================

LOG_FILE = LOGS_DIR / "orchestrator.log"

def setup_logging():
    """Configure logging to both file and console."""
    logger = logging.getLogger("orchestrator")
    logger.setLevel(logging.DEBUG)
    
    # File handler - detailed logs
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_fmt)
    
    # Console handler - info and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_fmt)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ==============================================================================
# Windows Notifications
# ==============================================================================

def send_notification(title: str, message: str, level: str = "info"):
    """Send Windows toast notification."""
    try:
        # Try winotify first (cleaner API)
        from winotify import Notification, audio
        toast = Notification(
            app_id="Qanoon Pipeline",
            title=title,
            msg=message,
            duration="long"
        )
        if level == "error":
            toast.set_audio(audio.Reminder, loop=False)
        toast.show()
        return True
    except ImportError:
        pass
    
    try:
        # Fallback to win10toast
        from win10toast import ToastNotifier
        toaster = ToastNotifier()
        toaster.show_toast(
            title,
            message,
            duration=10,
            threaded=True
        )
        return True
    except ImportError:
        pass
    
    try:
        # Last resort: PowerShell
        ps_script = f'''
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null
        $template = @"
        <toast>
            <visual>
                <binding template="ToastText02">
                    <text id="1">{title}</text>
                    <text id="2">{message}</text>
                </binding>
            </visual>
        </toast>
"@
        $xml = New-Object Windows.Data.Xml.Dom.XmlDocument
        $xml.LoadXml($template)
        $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
        [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Qanoon").Show($toast)
        '''
        subprocess.run(
            ["powershell", "-Command", ps_script],
            capture_output=True,
            timeout=10
        )
        return True
    except Exception:
        pass
    
    # If all else fails, log it
    logger.info(f"[!] Notification: {title} - {message}")
    return False

# ==============================================================================
# Data Classes
# ==============================================================================

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class JobType(str, Enum):
    SCRAPE = "scrape"
    VERIFY = "verify"
    FIX = "fix"
    CLEAN = "clean"
    HTML = "html"
    JSONL = "jsonl"

@dataclass
class Job:
    """Represents a pipeline job."""
    id: str
    type: JobType
    year: Optional[int] = None
    reporter: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0
    pid: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "type": self.type.value if isinstance(self.type, JobType) else self.type,
            "year": self.year,
            "reporter": self.reporter,
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "attempts": self.attempts,
            "pid": self.pid
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Job":
        return cls(
            id=data["id"],
            type=JobType(data["type"]) if data.get("type") else JobType.SCRAPE,
            year=data.get("year"),
            reporter=data.get("reporter"),
            status=JobStatus(data["status"]) if data.get("status") else JobStatus.PENDING,
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            error=data.get("error"),
            attempts=data.get("attempts", 0),
            pid=data.get("pid")
        )

@dataclass
class OrchestratorState:
    """Persistent state for the orchestrator."""
    running: bool = False
    current_job: Optional[Dict] = None
    job_history: List[Dict] = field(default_factory=list)
    last_check: Optional[str] = None
    last_successful_run: Dict[str, str] = field(default_factory=dict)  # year -> timestamp
    fix_attempts: Dict[str, int] = field(default_factory=dict)  # "year-reporter" -> attempts
    started_at: Optional[str] = None
    pid: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return {
            "running": self.running,
            "current_job": self.current_job,
            "job_history": self.job_history[-100:],  # Keep last 100 jobs
            "last_check": self.last_check,
            "last_successful_run": self.last_successful_run,
            "fix_attempts": self.fix_attempts,
            "started_at": self.started_at,
            "pid": self.pid
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "OrchestratorState":
        return cls(
            running=data.get("running", False),
            current_job=data.get("current_job"),
            job_history=data.get("job_history", []),
            last_check=data.get("last_check"),
            last_successful_run=data.get("last_successful_run", {}),
            fix_attempts=data.get("fix_attempts", {}),
            started_at=data.get("started_at"),
            pid=data.get("pid")
        )

# ==============================================================================
# State Management
# ==============================================================================

def load_state() -> OrchestratorState:
    """Load orchestrator state from file."""
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return OrchestratorState.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
    return OrchestratorState()

def save_state(state: OrchestratorState):
    """Save orchestrator state to file."""
    try:
        STATE_FILE.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_schedule() -> Dict:
    """Load daily schedule."""
    if SCHEDULE_FILE.exists():
        try:
            return json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {"current_year": None, "completed_years": []}

def load_progress() -> Dict:
    """Load scraper progress."""
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {"completed_searches": [], "cases_fetched": []}

# ==============================================================================
# Time & Hours Checking
# ==============================================================================

def get_pkt_time() -> datetime:
    """Get current time in Pakistan (PKT = UTC+5)."""
    from datetime import timezone
    utc_now = datetime.now(timezone.utc)
    pkt = timezone(timedelta(hours=PKT_OFFSET_HOURS))
    return utc_now.astimezone(pkt)

def is_within_operating_hours() -> bool:
    """Check if we're within PLS operating hours (7AM-9PM PKT)."""
    pkt = get_pkt_time()
    hour = pkt.hour
    return PLS_OPERATING_HOURS[0] <= hour < PLS_OPERATING_HOURS[1]

def get_next_operating_window() -> datetime:
    """Get next time PLS will be operational."""
    pkt = get_pkt_time()
    hour = pkt.hour
    
    if hour < PLS_OPERATING_HOURS[0]:
        # Before opening - wait until 7AM today
        return pkt.replace(hour=PLS_OPERATING_HOURS[0], minute=0, second=0, microsecond=0)
    elif hour >= PLS_OPERATING_HOURS[1]:
        # After closing - wait until 7AM tomorrow
        tomorrow = pkt + timedelta(days=1)
        return tomorrow.replace(hour=PLS_OPERATING_HOURS[0], minute=0, second=0, microsecond=0)
    else:
        # We're in operating hours
        return pkt

# ==============================================================================
# Work Detection
# ==============================================================================

def get_latest_verification() -> Optional[Dict]:
    """Get the most recent verification report."""
    if not AUDIT_DIR.exists():
        return None
    
    audit_files = sorted(AUDIT_DIR.glob("*_verification.json"), reverse=True)
    if not audit_files:
        return None
    
    try:
        return json.loads(audit_files[0].read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read verification report: {e}")
        return None

def get_missing_cases() -> Dict[int, Dict[str, List[Dict]]]:
    """
    Get all missing cases from verification reports.
    Returns: {year: {reporter: [missing_cases]}}
    """
    verification = get_latest_verification()
    if not verification:
        return {}
    
    missing = {}
    for result in verification.get("results", []):
        if result.get("missing_cases"):
            year = result["year"]
            reporter = result["reporter"]
            
            if year not in missing:
                missing[year] = {}
            missing[year][reporter] = result["missing_cases"]
    
    return missing

def get_incomplete_years() -> List[int]:
    """Get years that haven't been fully scraped."""
    schedule = load_schedule()
    progress = load_progress()
    
    completed_years = set(schedule.get("completed_years", []))
    current_year = schedule.get("current_year")
    
    # Check which years have incomplete reporters
    incomplete = []
    
    completed_searches = set(progress.get("completed_searches", []))
    
    # Check current year if set
    if current_year and current_year not in completed_years:
        for reporter in REPORTERS:
            if f"{current_year}-{reporter}" not in completed_searches:
                if current_year not in incomplete:
                    incomplete.append(current_year)
                break
    
    return incomplete

def is_scraper_running() -> bool:
    """Check if scraper is currently running."""
    # Check by process name
    try:
        result = subprocess.run(
            ["powershell", "-Command", 
             "Get-Process python -ErrorAction SilentlyContinue | " +
             "Where-Object {$_.CommandLine -like '*pls_scraper*' -or $_.CommandLine -like '*verify_scraper*'}"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return bool(result.stdout.strip())
    except:
        pass
    
    # Fallback: check state file
    state = load_state()
    if state.current_job and state.current_job.get("status") == "running":
        # Verify the PID is still alive
        pid = state.current_job.get("pid")
        if pid:
            try:
                import psutil
                return psutil.pid_exists(pid)
            except ImportError:
                # Without psutil, trust the state
                return True
    
    return False

def needs_verification(year: int) -> bool:
    """Check if a year needs verification."""
    verification = get_latest_verification()
    if not verification:
        return True
    
    # Check if verification is recent (within last 24 hours)
    generated_at = verification.get("generated_at", "")
    if generated_at:
        try:
            gen_time = datetime.fromisoformat(generated_at)
            if datetime.now() - gen_time > timedelta(hours=24):
                return True
        except:
            pass
    
    # Check if this year was verified
    for result in verification.get("results", []):
        if result.get("year") == year:
            return False
    
    return True

def needs_cleaning(year: int) -> bool:
    """Check if scraped data needs cleaning."""
    # Look for raw HTML files that haven't been cleaned
    reporter_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir() and d.name in REPORTERS]
    
    for reporter_dir in reporter_dirs:
        # Check if there are cases without cleaned judgment text
        json_files = list(reporter_dir.glob(f"{year}_*.json"))
        for jf in json_files[:5]:  # Sample check
            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
                judgment = data.get("judgment", "")
                # If judgment contains raw HTML, needs cleaning
                if "<" in judgment and ">" in judgment:
                    return True
            except:
                pass
    
    return False

# ==============================================================================
# Job Execution
# ==============================================================================

def run_command(cmd: List[str], job: Job, timeout_hours: int = JOB_TIMEOUT_HOURS) -> Tuple[bool, str]:
    """
    Run a command and track its execution.
    Returns: (success, output/error)
    """
    logger.info(f"Executing: {' '.join(cmd)}")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(SCRIPT_DIR),
            bufsize=1
        )
        
        job.pid = process.pid
        
        # Stream output to log
        output_lines = []
        start_time = time.time()
        timeout_seconds = timeout_hours * 3600
        
        while True:
            # Check timeout
            if time.time() - start_time > timeout_seconds:
                process.kill()
                return False, f"Job timed out after {timeout_hours} hours"
            
            # Read output
            line = process.stdout.readline()
            if line:
                line = line.rstrip()
                output_lines.append(line)
                logger.debug(f"  {line}")
            
            # Check if process finished
            if process.poll() is not None:
                # Read remaining output
                remaining = process.stdout.read()
                if remaining:
                    output_lines.extend(remaining.rstrip().split('\n'))
                break
            
            time.sleep(0.1)
        
        output = '\n'.join(output_lines[-50:])  # Last 50 lines
        
        if process.returncode == 0:
            return True, output
        else:
            return False, f"Exit code {process.returncode}\n{output}"
            
    except Exception as e:
        return False, f"Exception: {str(e)}\n{traceback.format_exc()}"

def run_scrape_year(year: int) -> Tuple[bool, str]:
    """Run scraper for a specific year."""
    job = Job(
        id=f"scrape_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.SCRAPE,
        year=year
    )
    
    cmd = [sys.executable, str(SCRAPER_SCRIPT), "scrape", "--year", str(year)]
    return run_command(cmd, job)

def run_verification(year: int) -> Tuple[bool, str]:
    """Run verification for a specific year."""
    job = Job(
        id=f"verify_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.VERIFY,
        year=year
    )
    
    cmd = [sys.executable, str(VERIFY_SCRIPT), "--year", str(year)]
    return run_command(cmd, job)

def run_fix_missing(year: int) -> Tuple[bool, str]:
    """Fix missing cases for a specific year."""
    job = Job(
        id=f"fix_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.FIX,
        year=year
    )
    
    cmd = [sys.executable, str(VERIFY_SCRIPT), "--year", str(year), "--fix"]
    return run_command(cmd, job)

def run_cleaner() -> Tuple[bool, str]:
    """Run data cleaner on all data."""
    job = Job(
        id=f"clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.CLEAN
    )
    
    cmd = [sys.executable, str(CLEANER_SCRIPT)]
    return run_command(cmd, job, timeout_hours=2)

def run_html_generator() -> Tuple[bool, str]:
    """Run HTML generator."""
    job = Job(
        id=f"html_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.HTML
    )
    
    cmd = [sys.executable, str(HTML_SCRIPT)]
    return run_command(cmd, job, timeout_hours=2)

def run_jsonl_converter() -> Tuple[bool, str]:
    """Run JSONL converter."""
    job = Job(
        id=f"jsonl_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        type=JobType.JSONL
    )
    
    # Check if the script exists and has proper args
    if JSONL_SCRIPT.exists():
        cmd = [sys.executable, str(JSONL_SCRIPT)]
        return run_command(cmd, job, timeout_hours=1)
    else:
        return True, "JSONL converter not found, skipping"

# ==============================================================================
# Pipeline Logic
# ==============================================================================

def execute_pipeline(year: int, state: OrchestratorState) -> bool:
    """
    Execute the full pipeline for a year:
    Scrape → Verify → Fix → Re-verify → Clean → HTML → JSONL
    """
    logger.info(f"===============================================================")
    logger.info(f"Starting pipeline for year {year}")
    logger.info(f"===============================================================")
    
    pipeline_start = datetime.now()
    
    try:
        # Step 1: Scrape (if not already complete)
        if year in get_incomplete_years():
            logger.info(f"[1/7] Scraping year {year}...")
            state.current_job = {"type": "scrape", "year": year, "status": "running"}
            save_state(state)
            
            success, output = run_scrape_year(year)
            if not success:
                logger.error(f"Scraping failed: {output}")
                send_notification("Scraping Failed", f"Year {year} scraping failed", "error")
                return False
            
            logger.info(f"[1/7] [OK] Scraping complete")
        else:
            logger.info(f"[1/7] [OK] Scraping already complete, skipping")
        
        # Step 2: Verify
        logger.info(f"[2/7] Verifying year {year}...")
        state.current_job = {"type": "verify", "year": year, "status": "running"}
        save_state(state)
        
        success, output = run_verification(year)
        if not success:
            logger.error(f"Verification failed: {output}")
            # Continue anyway - we can still try to fix
        else:
            logger.info(f"[2/7] [OK] Verification complete")
        
        # Step 3: Fix missing cases (up to MAX_FIX_ATTEMPTS)
        missing = get_missing_cases()
        fix_key = str(year)
        
        if year in missing and missing[year]:
            total_missing = sum(len(cases) for cases in missing[year].values())
            attempt = state.fix_attempts.get(fix_key, 0)
            
            if attempt < MAX_FIX_ATTEMPTS:
                logger.info(f"[3/7] Fixing {total_missing} missing cases (attempt {attempt + 1}/{MAX_FIX_ATTEMPTS})...")
                state.current_job = {"type": "fix", "year": year, "status": "running"}
                save_state(state)
                
                success, output = run_fix_missing(year)
                state.fix_attempts[fix_key] = attempt + 1
                save_state(state)
                
                if success:
                    logger.info(f"[3/7] [OK] Fix complete")
                else:
                    logger.warning(f"[3/7] Fix had issues: {output[:200]}")
            else:
                logger.warning(f"[3/7] Max fix attempts reached for {year}, skipping")
        else:
            logger.info(f"[3/7] [OK] No missing cases to fix")
        
        # Step 4: Re-verify
        logger.info(f"[4/7] Re-verifying year {year}...")
        state.current_job = {"type": "verify", "year": year, "status": "running"}
        save_state(state)
        
        success, output = run_verification(year)
        if success:
            logger.info(f"[4/7] [OK] Re-verification complete")
        
        # Step 5: Clean data
        logger.info(f"[5/7] Cleaning data...")
        state.current_job = {"type": "clean", "year": year, "status": "running"}
        save_state(state)
        
        success, output = run_cleaner()
        if success:
            logger.info(f"[5/7] [OK] Data cleaning complete")
        else:
            logger.warning(f"[5/7] Cleaning had issues: {output[:200]}")
        
        # Step 6: Generate HTML
        logger.info(f"[6/7] Generating HTML...")
        state.current_job = {"type": "html", "year": year, "status": "running"}
        save_state(state)
        
        success, output = run_html_generator()
        if success:
            logger.info(f"[6/7] [OK] HTML generation complete")
        else:
            logger.warning(f"[6/7] HTML generation had issues: {output[:200]}")
        
        # Step 7: Update JSONL
        logger.info(f"[7/7] Updating JSONL...")
        state.current_job = {"type": "jsonl", "year": year, "status": "running"}
        save_state(state)
        
        success, output = run_jsonl_converter()
        if success:
            logger.info(f"[7/7] [OK] JSONL update complete")
        else:
            logger.warning(f"[7/7] JSONL update had issues: {output[:200]}")
        
        # Pipeline complete!
        duration = datetime.now() - pipeline_start
        logger.info(f"===============================================================")
        logger.info(f"Pipeline for year {year} completed in {duration}")
        logger.info(f"===============================================================")
        
        # Update state
        state.last_successful_run[str(year)] = datetime.now().isoformat()
        state.current_job = None
        save_state(state)
        
        send_notification(
            "Pipeline Complete",
            f"Year {year} processed successfully in {duration}",
            "info"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}\n{traceback.format_exc()}")
        send_notification("Pipeline Error", f"Year {year}: {str(e)[:100]}", "error")
        return False

def run_pending_work(state: OrchestratorState):
    """Check for and execute all pending work."""
    logger.info("Checking for pending work...")
    
    # Check operating hours
    if not is_within_operating_hours():
        pkt = get_pkt_time()
        next_window = get_next_operating_window()
        logger.info(f"Outside PLS operating hours (current: {pkt.strftime('%H:%M')} PKT)")
        logger.info(f"Next window: {next_window.strftime('%Y-%m-%d %H:%M')} PKT")
        return
    
    # Check if already running
    if is_scraper_running():
        logger.info("Scraper already running, skipping this check")
        return
    
    # Priority 1: Fix missing cases from verification
    missing = get_missing_cases()
    if missing:
        for year, reporters in missing.items():
            total_missing = sum(len(cases) for cases in reporters.values())
            fix_key = str(year)
            attempts = state.fix_attempts.get(fix_key, 0)
            
            if attempts < MAX_FIX_ATTEMPTS and total_missing > 0:
                logger.info(f"Found {total_missing} missing cases for year {year}")
                
                # Run fix
                state.current_job = {"type": "fix", "year": year, "status": "running"}
                save_state(state)
                
                success, output = run_fix_missing(year)
                state.fix_attempts[fix_key] = attempts + 1
                
                if success:
                    logger.info(f"Fixed missing cases for {year}")
                    # Re-verify
                    run_verification(year)
                else:
                    logger.warning(f"Fix attempt {attempts + 1} failed for {year}")
                
                save_state(state)
                return  # One job at a time
    
    # Priority 2: Complete incomplete years
    incomplete = get_incomplete_years()
    if incomplete:
        year = incomplete[0]  # Process oldest incomplete first
        logger.info(f"Found incomplete year: {year}")
        
        # Run full pipeline
        execute_pipeline(year, state)
        return
    
    # Priority 3: Verify unverified years
    schedule = load_schedule()
    current_year = schedule.get("current_year")
    if current_year and needs_verification(current_year):
        logger.info(f"Year {current_year} needs verification")
        
        state.current_job = {"type": "verify", "year": current_year, "status": "running"}
        save_state(state)
        
        success, output = run_verification(current_year)
        state.current_job = None
        save_state(state)
        
        if success:
            logger.info(f"Verification complete for {current_year}")
        return
    
    logger.info("No pending work found")

# ==============================================================================
# Daemon Mode
# ==============================================================================

_shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global _shutdown_requested
    logger.info("Shutdown signal received...")
    _shutdown_requested = True

def run_daemon():
    """Run as a daemon, checking for work periodically."""
    global _shutdown_requested
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("===============================================================")
    logger.info("Qanoon Pipeline Orchestrator - Starting Daemon Mode")
    logger.info(f"Check interval: {CHECK_INTERVAL_MINUTES} minutes")
    logger.info(f"Operating hours: {PLS_OPERATING_HOURS[0]}:00 - {PLS_OPERATING_HOURS[1]}:00 PKT")
    logger.info("===============================================================")
    
    state = load_state()
    state.running = True
    state.started_at = datetime.now().isoformat()
    state.pid = os.getpid()
    save_state(state)
    
    send_notification("Orchestrator Started", "Pipeline daemon is now running", "info")
    
    try:
        while not _shutdown_requested:
            try:
                state = load_state()
                state.last_check = datetime.now().isoformat()
                save_state(state)
                
                run_pending_work(state)
                
            except Exception as e:
                logger.error(f"Error in daemon loop: {e}\n{traceback.format_exc()}")
                send_notification("Orchestrator Error", str(e)[:100], "error")
            
            # Sleep with interrupt checking
            for _ in range(CHECK_INTERVAL_MINUTES * 60):
                if _shutdown_requested:
                    break
                time.sleep(1)
    
    finally:
        state = load_state()
        state.running = False
        state.current_job = None
        state.pid = None
        save_state(state)
        
        logger.info("Daemon stopped")
        send_notification("Orchestrator Stopped", "Pipeline daemon has stopped", "info")

# ==============================================================================
# Status Display
# ==============================================================================

def show_status():
    """Display current orchestrator status."""
    state = load_state()
    schedule = load_schedule()
    missing = get_missing_cases()
    incomplete = get_incomplete_years()
    verification = get_latest_verification()
    
    print("\n" + "=" * 60)
    print("  QANOON PIPELINE ORCHESTRATOR STATUS")
    print("=" * 60 + "\n")
    
    # Daemon status
    if state.running and state.pid:
        try:
            import psutil
            if psutil.pid_exists(state.pid):
                print(f"[OK] Daemon: RUNNING (PID {state.pid})")
            else:
                print(f"[X] Daemon: STALE (PID {state.pid} not found)")
        except ImportError:
            print(f"[~] Daemon: POSSIBLY RUNNING (PID {state.pid})")
    else:
        print(f"[X] Daemon: NOT RUNNING")
    
    # Operating hours
    pkt = get_pkt_time()
    if is_within_operating_hours():
        print(f"[OK] Operating Hours: ACTIVE ({pkt.strftime('%H:%M')} PKT)")
    else:
        next_window = get_next_operating_window()
        print(f"[~] Operating Hours: INACTIVE ({pkt.strftime('%H:%M')} PKT)")
        print(f"   Next window: {next_window.strftime('%Y-%m-%d %H:%M')} PKT")
    
    print()
    
    # Current job
    if state.current_job:
        print(f"[JOB] Current Job: {state.current_job.get('type', 'unknown')} "
              f"(Year {state.current_job.get('year', '?')})")
    else:
        print(f"[JOB] Current Job: None")
    
    # Last check
    if state.last_check:
        print(f"[TIME] Last Check: {state.last_check}")
    
    print()
    
    # Pending work
    print("PENDING WORK:")
    print("-" * 40)
    
    if missing:
        for year, reporters in missing.items():
            total = sum(len(cases) for cases in reporters.values())
            print(f"  [X] Year {year}: {total} missing cases")
            for reporter, cases in reporters.items():
                if cases:
                    print(f"     - {reporter}: {len(cases)} cases")
    else:
        print("  [OK] No missing cases")
    
    if incomplete:
        print(f"  [~] Incomplete years: {incomplete}")
    else:
        print("  [OK] No incomplete years")
    
    print()
    
    # Verification summary
    if verification:
        print("LATEST VERIFICATION:")
        print("-" * 40)
        print(f"  Generated: {verification.get('generated_at', 'unknown')}")
        print(f"  Total PLS: {verification.get('total_pls_cases', 0)}")
        print(f"  Total Local: {verification.get('total_local_cases', 0)}")
        print(f"  Missing: {verification.get('total_missing', 0)}")
    
    print()
    
    # Recent jobs
    if state.job_history:
        print("RECENT JOBS:")
        print("-" * 40)
        for job in state.job_history[-5:]:
            status_icon = "[OK]" if job.get("status") == "completed" else "[X]"
            print(f"  {status_icon} {job.get('type', '?')} "
                  f"(Year {job.get('year', '?')}) - {job.get('completed_at', 'unknown')}")
    
    print("\n" + "=" * 60 + "\n")

# ==============================================================================
# Main Entry Point
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Qanoon Pipeline Orchestrator - Autonomous pipeline supervisor"
    )
    parser.add_argument("--daemon", action="store_true",
                        help="Run as daemon, checking every 30 minutes")
    parser.add_argument("--run-pending", action="store_true",
                        help="One-shot: run all pending work")
    parser.add_argument("--status", action="store_true",
                        help="Show current status")
    parser.add_argument("--force-fix", action="store_true",
                        help="Force fix missing cases now")
    parser.add_argument("--year", type=int,
                        help="Specific year to process")
    parser.add_argument("--pipeline", action="store_true",
                        help="Run full pipeline for --year")
    
    args = parser.parse_args()
    
    if args.status:
        show_status()
        return
    
    if args.daemon:
        run_daemon()
        return
    
    state = load_state()
    
    if args.force_fix:
        # Reset fix attempts and run
        missing = get_missing_cases()
        if missing:
            for year in missing.keys():
                state.fix_attempts[str(year)] = 0
            save_state(state)
        run_pending_work(state)
        return
    
    if args.pipeline and args.year:
        execute_pipeline(args.year, state)
        return
    
    if args.run_pending:
        run_pending_work(state)
        return
    
    # Default: show status
    show_status()

if __name__ == "__main__":
    main()
