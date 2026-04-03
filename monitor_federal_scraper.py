#!/usr/bin/env python3
"""
Federal Laws Scraper Monitor
==============================
Watches PID 32400 (federal_laws_scraper.py) and auto-triggers the
verification script once the scraper finishes.

Usage:
    python monitor_federal_scraper.py

What it does:
1. Checks if PID 32400 is still running using psutil
2. If running: prints status, sleeps 60 seconds, repeats
3. Once PID is gone (scraper finished):
   - Runs verify_federal_laws.py automatically
   - Prints the verification results
   - Saves a combined status to data_v2/federal_laws/final_status.json
"""

import io
import os
import sys
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone

# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import psutil

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

TARGET_PID = 32400
POLL_INTERVAL = 60  # seconds between checks
PROJECT_DIR = Path(__file__).parent
VERIFIER_SCRIPT = PROJECT_DIR / "verify_federal_laws.py"
DATA_DIR = PROJECT_DIR / "data_v2" / "federal_laws"
PROGRESS_FILE = DATA_DIR / "progress.json"
FINAL_STATUS_FILE = DATA_DIR / "final_status.json"


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def is_pid_running(pid: int) -> bool:
    """Check if a PID is still running."""
    try:
        proc = psutil.Process(pid)
        # Check if the process is actually alive (not zombie)
        status = proc.status()
        if status == psutil.STATUS_ZOMBIE:
            return False
        return proc.is_running()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return False


def get_process_info(pid: int) -> dict:
    """Get info about the running process."""
    try:
        proc = psutil.Process(pid)
        info = {
            "pid": pid,
            "name": proc.name(),
            "status": proc.status(),
            "cpu_percent": proc.cpu_percent(interval=0.5),
            "memory_mb": round(proc.memory_info().rss / 1024 / 1024, 1),
            "create_time": datetime.fromtimestamp(proc.create_time()).isoformat(),
            "cmdline": " ".join(proc.cmdline()[:5]),  # First 5 args
        }
        # Runtime
        runtime_sec = time.time() - proc.create_time()
        hours = int(runtime_sec // 3600)
        minutes = int((runtime_sec % 3600) // 60)
        info["runtime"] = f"{hours}h {minutes}m"
        return info
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        return {"pid": pid, "error": str(e)}


def get_progress_summary() -> str:
    """Quick summary from progress.json."""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                p = json.load(f)
            parsed = p.get("laws_parsed", 0)
            downloaded = p.get("pdfs_downloaded", 0)
            pct = (downloaded / parsed * 100) if parsed > 0 else 0
            return f"Parsed: {parsed}, Downloaded: {downloaded}/{parsed} ({pct:.1f}%)"
        return "progress.json not found"
    except Exception as e:
        return f"Error reading progress: {e}"


def run_verifier() -> dict:
    """Run the verifier script and capture its output."""
    print("\n" + "=" * 70)
    print("  RUNNING INDEPENDENT VERIFICATION...")
    print("=" * 70 + "\n")

    try:
        result = subprocess.run(
            [sys.executable, str(VERIFIER_SCRIPT)],
            cwd=str(PROJECT_DIR),
            capture_output=False,  # Let stdout/stderr flow through
            text=True,
            timeout=600,  # 10 min max
        )

        # Read the report file
        report_file = DATA_DIR / "verification_report.json"
        if report_file.exists():
            with open(report_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"error": "Report file not generated", "exit_code": result.returncode}

    except subprocess.TimeoutExpired:
        return {"error": "Verifier timed out after 600 seconds"}
    except Exception as e:
        return {"error": str(e)}


def save_final_status(verification_report: dict) -> None:
    """Save combined final status."""
    final = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scraper_pid": TARGET_PID,
        "scraper_finished": True,
        "verification": verification_report.get("summary", {}),
        "verification_counts": verification_report.get("counts", {}),
        "verification_elapsed": verification_report.get("elapsed_seconds"),
    }

    # Add progress.json data
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
            final["scraper_progress"] = {
                "laws_parsed": progress.get("laws_parsed", 0),
                "pdfs_downloaded": progress.get("pdfs_downloaded", 0),
                "texts_extracted": progress.get("texts_extracted", 0),
                "htmls_generated": progress.get("htmls_generated", 0),
                "phase1_complete": progress.get("phase1_complete"),
                "phase2_complete": progress.get("phase2_complete"),
                "phase3_complete": progress.get("phase3_complete"),
                "failures_count": len(progress.get("failures", [])),
            }
    except Exception:
        pass

    FINAL_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FINAL_STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"\nFinal status saved to: {FINAL_STATUS_FILE}")


# ══════════════════════════════════════════════════════════════════════════════
# Main Loop
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print(f"  Federal Laws Scraper Monitor — Watching PID {TARGET_PID}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Poll interval: {POLL_INTERVAL}s")
    print("=" * 70)
    print()

    check_count = 0
    start_time = time.time()

    while True:
        check_count += 1
        now = datetime.now().strftime("%H:%M:%S")

        if is_pid_running(TARGET_PID):
            info = get_process_info(TARGET_PID)
            progress = get_progress_summary()

            print(f"[{now}] Check #{check_count}: PID {TARGET_PID} is RUNNING")
            print(f"  Process: {info.get('name', '?')} | "
                  f"CPU: {info.get('cpu_percent', '?')}% | "
                  f"RAM: {info.get('memory_mb', '?')} MB | "
                  f"Runtime: {info.get('runtime', '?')}")
            print(f"  Progress: {progress}")
            print(f"  Next check in {POLL_INTERVAL}s...")
            print()

            time.sleep(POLL_INTERVAL)
        else:
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)

            print(f"[{now}] PID {TARGET_PID} is NO LONGER RUNNING!")
            print(f"  Monitored for {minutes} minutes ({check_count} checks)")
            print()
            print("Scraper finished! Running verifier...")
            print()

            # Run the verifier
            report = run_verifier()

            # Save final status
            save_final_status(report)

            # Print final summary
            summary = report.get("summary", {})
            if summary:
                print()
                print("=" * 70)
                print("  FINAL VERDICT")
                print("=" * 70)
                print(f"  Website total:    {summary.get('website_total_laws', '?')}")
                print(f"  On disk:          {summary.get('actually_on_disk', '?')}")
                print(f"  Download %:       {summary.get('download_percentage', '?')}%")
                print(f"  Missing:          {summary.get('missing_count', '?')}")
                print("=" * 70)

            break

    print("\nMonitor finished.")


if __name__ == "__main__":
    main()
