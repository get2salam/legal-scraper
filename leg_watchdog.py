"""
Legislation Scraper Watchdog
Checks if scraper is alive + making progress. Revives if dead or stuck.
"""
import os, sys, json, time, subprocess
sys.stdout.reconfigure(encoding='utf-8', errors='replace') if hasattr(sys.stdout, 'reconfigure') else None
from datetime import datetime, timezone
from pathlib import Path

SCRAPER_DIR = Path(__file__).parent
LOG_FILE = SCRAPER_DIR / "legislation_scraper.log"
STDOUT_FILE = SCRAPER_DIR / "legislation_stdout.log"
PROGRESS_FILE = SCRAPER_DIR / "data_v2" / "legislation" / "progress.json"
PYTHON = r"C:\Python314\python.exe"
SCRIPT = str(SCRAPER_DIR / "legislation_scraper.py")
STALE_MINUTES = 20  # If log hasn't updated in 20 min, consider stuck

def get_legislation_pid():
    """Find running legislation_scraper.py PID."""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'legislation_scraper' } | Select-Object ProcessId | ConvertTo-Json"],
            capture_output=True, text=True, timeout=15
        )
        out = result.stdout.strip()
        if not out or out == "null":
            return None
        data = json.loads(out)
        if isinstance(data, list):
            return data[0]["ProcessId"] if data else None
        return data.get("ProcessId")
    except Exception as e:
        print(f"PID check error: {e}")
        return None

def log_age_minutes():
    """Return how many minutes ago the log was last updated."""
    if not LOG_FILE.exists():
        return 9999
    mtime = LOG_FILE.stat().st_mtime
    age = (time.time() - mtime) / 60
    return age

def get_last_log_lines(n=5):
    """Get last N lines of the log."""
    if not LOG_FILE.exists():
        return "No log file found"
    lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])

def count_statutes():
    """Count total legislation JSONs."""
    leg_dir = SCRAPER_DIR / "data_v2" / "legislation"
    total = 0
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        d = leg_dir / letter
        if d.is_dir():
            total += sum(1 for f in d.iterdir() if f.suffix == ".json")
    return total

def current_letter():
    """Get current letter from progress file."""
    try:
        p = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        cur = p.get("current_alphabet") or "?"
        done = p.get("completed_alphabets", [])
        return cur, done
    except:
        return "?", []

def kill_pid(pid):
    """Kill a process by PID."""
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {pid} -Force"],
            timeout=10
        )
        time.sleep(2)
    except:
        pass

def start_scraper():
    """Start legislation scraper in background."""
    subprocess.Popen(
        [PYTHON, SCRIPT, "resume"],
        stdout=open(STDOUT_FILE, "a"),
        stderr=open(LOG_FILE, "a"),
        cwd=str(SCRAPER_DIR),
        creationflags=0x00000008  # DETACHED_PROCESS on Windows
    )
    time.sleep(5)
    return get_legislation_pid()

def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Legislation Watchdog running...")
    
    total = count_statutes()
    letter, done = current_letter()
    age = log_age_minutes()
    pid = get_legislation_pid()
    last_lines = get_last_log_lines(3)
    
    print(f"  Statutes: {total}/10,915 | Letter: {letter} | Done: {done}")
    print(f"  Log age: {age:.1f} min | PID: {pid or 'NONE'}")
    print(f"  Last log:\n    {last_lines.replace(chr(10), chr(10)+'    ')}")
    
    action = "OK"
    new_pid = None
    
    # Case 1: Not running at all → start it
    if pid is None:
        print("\n⚠️  SCRAPER NOT RUNNING — Starting...")
        new_pid = start_scraper()
        if new_pid:
            action = f"REVIVED (PID {new_pid})"
            print(f"  ✅ Started with PID {new_pid}")
        else:
            action = "FAILED TO START"
            print("  ❌ Failed to start scraper!")
    
    # Case 2: Running but log is stale → stuck, kill + restart
    elif age > STALE_MINUTES:
        print(f"\n⚠️  SCRAPER STUCK (log {age:.0f} min old) — Killing PID {pid} and restarting...")
        kill_pid(pid)
        time.sleep(3)
        new_pid = start_scraper()
        if new_pid:
            action = f"UNSTUCK+REVIVED (PID {new_pid})"
            print(f"  ✅ Restarted with PID {new_pid}")
        else:
            action = "FAILED TO RESTART"
            print("  ❌ Failed to restart scraper!")
    
    else:
        print(f"\n✅ Scraper healthy (PID {pid}, log {age:.1f} min ago)")
    
    # Final status line for monitor to parse
    print(f"\nSTATUS: {action}")
    print(f"TOTAL: {total} | TARGET: 10,915 | REMAINING: {10915 - total}")
    
    # Return non-zero exit if action needed
    return 0 if action == "OK" else 0  # Always exit 0, let monitor interpret

if __name__ == "__main__":
    sys.exit(main())
