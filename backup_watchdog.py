"""
backup_watchdog.py — Robocopy backup monitor and manager

Monitors a robocopy backup process: auto-restarts on completion,
tracks speed/ETA, and provides full audit capability.

Usage:
    python backup_watchdog.py --daemon      # Run as continuous watchdog
    python backup_watchdog.py --start       # Start robocopy
    python backup_watchdog.py --stop        # Kill robocopy
    python backup_watchdog.py --status      # Show progress
    python backup_watchdog.py --audit       # Full file-by-file audit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time

# Fix Windows console encoding for rich/unicode output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ── Third-party (rich for coloured output) ──────────────────────────────────
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
    from rich.table import Table
    from rich import print as rprint
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# ── Paths ────────────────────────────────────────────────────────────────────
SOURCE_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
DEST_DIR   = Path(r"G:\My Drive\Qanoon\data_v2")
SCRIPT_DIR = Path(__file__).parent
STATE_FILE  = SCRIPT_DIR / "backup_state.json"
AUDIT_FILE  = SCRIPT_DIR / "audit_report.json"
LOG_FILE    = SCRIPT_DIR / "backup_watchdog.log"

# ── Robocopy settings ────────────────────────────────────────────────────────
ROBOCOPY_EXE   = "robocopy"
ROBOCOPY_FLAGS = [
    "/E",         # Copy subdirectories including empty ones
    "/MT:128",    # 128 threads
    "/R:3",       # 3 retries on failure
    "/W:5",       # 5 seconds between retries
    "/NP",        # No progress percentage (cleaner logs)
    "/NDL",       # No directory listing
    "/LOG+:" + str(SCRIPT_DIR / "robocopy.log"),  # Append to log
    "/XF", ".env",          # Exclude secrets
    "/XD", "__pycache__", ".git",  # Exclude dev dirs
    "/XF", "*.log",         # Exclude log files
]

# Daemon check interval in seconds
CHECK_INTERVAL = 300  # 5 minutes

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)

console = Console() if HAS_RICH else None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print(msg: str, style: str = "") -> None:
    if HAS_RICH and console:
        console.print(msg, style=style)
    else:
        print(msg)


def _banner() -> None:
    banner = """
╔══════════════════════════════════════════════════════╗
║          BACKUP WATCHDOG  —  robocopy monitor        ║
║  Source : data_v2 (local)                            ║
║  Dest   : G:\\My Drive (Google Drive)                 ║
╚══════════════════════════════════════════════════════╝"""
    if HAS_RICH and console:
        console.print(Panel(banner.strip(), style="bold cyan"))
    else:
        print(banner)


# ─────────────────────────────────────────────────────────────────────────────
# State management
# ─────────────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not load state file: %s", exc)
    return {
        "runs": [],
        "last_start": None,
        "last_complete": None,
        "speed_samples": [],  # list of {"ts": ..., "files_per_min": ..., "mb_per_min": ...}
    }


def _save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
    except OSError as exc:
        logger.error("Could not save state file: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Robocopy process management
# ─────────────────────────────────────────────────────────────────────────────

def _find_robocopy_pid() -> Optional[int]:
    """Return PID of a running robocopy process, or None."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq robocopy.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, encoding="utf-8",
        )
        for line in result.stdout.splitlines():
            if "robocopy.exe" in line.lower():
                parts = line.strip('"').split('","')
                if len(parts) >= 2:
                    try:
                        return int(parts[1])
                    except ValueError:
                        pass
    except OSError as exc:
        logger.error("tasklist failed: %s", exc)
    return None


def is_robocopy_running() -> bool:
    return _find_robocopy_pid() is not None


def start_robocopy(background: bool = True) -> Optional[subprocess.Popen]:
    """Launch robocopy. Returns the Popen object (or None on error)."""
    if is_robocopy_running():
        _print("[yellow]robocopy is already running.[/yellow]")
        return None

    cmd = [ROBOCOPY_EXE, str(SOURCE_DIR), str(DEST_DIR)] + ROBOCOPY_FLAGS
    _print(f"[green]Starting robocopy with /MT:128 ...[/green]")
    logger.info("Starting robocopy: %s", " ".join(cmd))

    try:
        kwargs: dict = {"encoding": "utf-8"}
        if background:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
        else:
            proc = subprocess.Popen(cmd, **kwargs)
        logger.info("robocopy started, PID=%s", proc.pid)
        _print(f"[green]robocopy started (PID {proc.pid})[/green]")
        return proc
    except OSError as exc:
        logger.error("Failed to start robocopy: %s", exc)
        _print(f"[red]ERROR: Could not start robocopy: {exc}[/red]")
        return None


def stop_robocopy() -> bool:
    """Kill robocopy gracefully. Returns True if killed."""
    pid = _find_robocopy_pid()
    if pid is None:
        _print("[yellow]robocopy is not running.[/yellow]")
        return False
    try:
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=True, capture_output=True)
        logger.info("Killed robocopy PID=%s", pid)
        _print(f"[green]Killed robocopy (PID {pid})[/green]")
        return True
    except subprocess.CalledProcessError as exc:
        logger.error("taskkill failed: %s", exc)
        _print(f"[red]ERROR: Could not kill robocopy: {exc}[/red]")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Progress counting
# ─────────────────────────────────────────────────────────────────────────────

def _count_files(path: Path) -> tuple[int, int]:
    """Return (file_count, total_bytes) for a directory tree.
    Uses os.scandir recursion for speed (much faster than rglob on 500K+ files).
    """
    total_files = 0
    total_bytes = 0

    def _walk(p: str) -> None:
        nonlocal total_files, total_bytes
        try:
            with os.scandir(p) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            total_files += 1
                            total_bytes += entry.stat(follow_symlinks=False).st_size
                        elif entry.is_dir(follow_symlinks=False):
                            _walk(entry.path)
                    except OSError:
                        pass
        except OSError:
            pass

    _walk(str(path))
    return total_files, total_bytes


def _format_size(bytes_: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unit}"
        bytes_ //= 1024
    return f"{bytes_:.1f} PB"


def get_status() -> dict:
    """Collect current backup status."""
    state = _load_state()

    src_files, src_bytes = _count_files(SOURCE_DIR)
    dst_files, dst_bytes = _count_files(DEST_DIR)

    missing = src_files - dst_files
    pct = (dst_files / src_files * 100) if src_files else 0

    # Speed from recent state samples
    samples = state.get("speed_samples", [])
    avg_files_min = 0.0
    avg_mb_min = 0.0
    if len(samples) >= 2:
        recent = samples[-10:]
        avg_files_min = sum(s["files_per_min"] for s in recent) / len(recent)
        avg_mb_min = sum(s["mb_per_min"] for s in recent) / len(recent)

    eta_str = "unknown"
    if avg_files_min > 0 and missing > 0:
        eta_mins = missing / avg_files_min
        eta_str = str(timedelta(minutes=int(eta_mins)))

    return {
        "running": is_robocopy_running(),
        "source_files": src_files,
        "source_bytes": src_bytes,
        "dest_files": dst_files,
        "dest_bytes": dst_bytes,
        "missing_files": missing,
        "pct_complete": round(pct, 2),
        "complete": missing == 0 and src_files > 0,
        "avg_files_per_min": round(avg_files_min, 1),
        "avg_mb_per_min": round(avg_mb_min, 1),
        "eta": eta_str,
        "timestamp": datetime.now().isoformat(),
    }


def show_status() -> None:
    _banner()
    _print("[cyan]Gathering status (this may take a moment)...[/cyan]")
    s = get_status()

    if HAS_RICH and console:
        t = Table(show_header=False, box=None, padding=(0, 2))
        t.add_column(style="bold")
        t.add_column()

        status_icon = "[green]✓ RUNNING[/green]" if s["running"] else "[yellow]✗ STOPPED[/yellow]"
        complete_icon = "[bold green]✓ COMPLETE[/bold green]" if s["complete"] else f"[yellow]{s['pct_complete']}%[/yellow]"

        t.add_row("Robocopy", status_icon)
        t.add_row("Progress", complete_icon)
        t.add_row("Source files", f"{s['source_files']:,}  ({_format_size(s['source_bytes'])})")
        t.add_row("Dest files",   f"{s['dest_files']:,}  ({_format_size(s['dest_bytes'])})")
        t.add_row("Missing",      f"{s['missing_files']:,} files")
        t.add_row("Avg speed",    f"{s['avg_files_per_min']} files/min  |  {s['avg_mb_per_min']} MB/min")
        t.add_row("ETA",          s["eta"])
        t.add_row("Checked at",   s["timestamp"])

        console.print(Panel(t, title="Backup Status", border_style="cyan"))
    else:
        print(f"\nRobocopy running : {s['running']}")
        print(f"Progress         : {s['pct_complete']}%  ({'COMPLETE' if s['complete'] else 'in progress'})")
        print(f"Source           : {s['source_files']:,} files  {_format_size(s['source_bytes'])}")
        print(f"Destination      : {s['dest_files']:,} files  {_format_size(s['dest_bytes'])}")
        print(f"Missing          : {s['missing_files']:,} files")
        print(f"Speed            : {s['avg_files_per_min']} files/min  |  {s['avg_mb_per_min']} MB/min")
        print(f"ETA              : {s['eta']}")


# ─────────────────────────────────────────────────────────────────────────────
# Audit
# ─────────────────────────────────────────────────────────────────────────────

def run_audit() -> dict:
    """Full file-by-file audit comparing source to destination."""
    _banner()
    _print("[cyan]Starting full audit — comparing source vs destination...[/cyan]")
    logger.info("Audit started")

    missing: list[str] = []
    size_mismatch: list[dict] = []
    corrupt_src: list[str] = []       # 0-byte source files
    corrupt_dst: list[str] = []       # 0-byte destination files
    matched = 0
    total_scanned = 0

    start_ts = time.time()

    # Build destination lookup: relative_path → size
    _print("[cyan]Indexing destination...[/cyan]")
    dst_index: dict[str, int] = {}
    try:
        for entry in DEST_DIR.rglob("*"):
            if entry.is_file():
                rel = entry.relative_to(DEST_DIR)
                try:
                    dst_index[str(rel)] = entry.stat().st_size
                except OSError:
                    dst_index[str(rel)] = -1
    except OSError as exc:
        logger.error("Error indexing destination: %s", exc)
        _print(f"[red]ERROR indexing destination: {exc}[/red]")

    _print(f"[cyan]Destination index built: {len(dst_index):,} files[/cyan]")
    _print("[cyan]Scanning source...[/cyan]")

    try:
        if HAS_RICH and console:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed:,} files"),
                TimeElapsedColumn(),
                console=console,
            )
            task = progress.add_task("Auditing...", total=None)
            with progress:
                for entry in SOURCE_DIR.rglob("*"):
                    if not entry.is_file():
                        continue
                    total_scanned += 1
                    progress.update(task, completed=total_scanned, description=f"Auditing ({total_scanned:,} scanned)")

                    try:
                        src_size = entry.stat().st_size
                    except OSError:
                        continue

                    rel = str(entry.relative_to(SOURCE_DIR))
                    if src_size == 0:
                        corrupt_src.append(rel)
                    elif rel not in dst_index:
                        missing.append(rel)
                    elif dst_index[rel] == 0:
                        corrupt_dst.append(rel)
                    elif dst_index[rel] != src_size:
                        size_mismatch.append({
                            "file": rel,
                            "source_bytes": src_size,
                            "dest_bytes": dst_index[rel],
                        })
                    else:
                        matched += 1
        else:
            for entry in SOURCE_DIR.rglob("*"):
                if not entry.is_file():
                    continue
                total_scanned += 1
                if total_scanned % 10_000 == 0:
                    print(f"  Scanned {total_scanned:,} files...")

                try:
                    src_size = entry.stat().st_size
                except OSError:
                    continue

                rel = str(entry.relative_to(SOURCE_DIR))
                if src_size == 0:
                    corrupt_src.append(rel)
                elif rel not in dst_index:
                    missing.append(rel)
                elif dst_index[rel] == 0:
                    corrupt_dst.append(rel)
                elif dst_index[rel] != src_size:
                    size_mismatch.append({
                        "file": rel,
                        "source_bytes": src_size,
                        "dest_bytes": dst_index[rel],
                    })
                else:
                    matched += 1

    except OSError as exc:
        logger.error("Error scanning source: %s", exc)
        _print(f"[red]ERROR scanning source: {exc}[/red]")

    elapsed = time.time() - start_ts
    report = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "total_source_files": total_scanned,
        "total_dest_files": len(dst_index),
        "matched": matched,
        "missing_count": len(missing),
        "size_mismatch_count": len(size_mismatch),
        "corrupt_source_count": len(corrupt_src),
        "corrupt_dest_count": len(corrupt_dst),
        "missing_files": missing[:1000],          # Cap at 1000 for readability
        "size_mismatches": size_mismatch[:500],
        "corrupt_source_files": corrupt_src[:500],
        "corrupt_dest_files": corrupt_dst[:500],
    }

    try:
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info("Audit report written to %s", AUDIT_FILE)
    except OSError as exc:
        logger.error("Could not write audit report: %s", exc)

    # ── Summary output ────────────────────────────────────────────────────
    if HAS_RICH and console:
        t = Table(show_header=False, box=None, padding=(0, 2))
        t.add_column(style="bold")
        t.add_column()
        t.add_row("Source files scanned", f"{total_scanned:,}")
        t.add_row("Destination files",    f"{len(dst_index):,}")
        t.add_row("[green]Matched[/green]",        f"[green]{matched:,}[/green]")
        t.add_row("[red]Missing[/red]",            f"[red]{len(missing):,}[/red]")
        t.add_row("[yellow]Size mismatch[/yellow]",f"[yellow]{len(size_mismatch):,}[/yellow]")
        t.add_row("[red]Corrupt (src)[/red]",      f"[red]{len(corrupt_src):,}[/red]")
        t.add_row("[red]Corrupt (dst)[/red]",      f"[red]{len(corrupt_dst):,}[/red]")
        t.add_row("Elapsed",               f"{elapsed:.0f}s")
        t.add_row("Report saved",          str(AUDIT_FILE))
        console.print(Panel(t, title="Audit Summary", border_style="cyan"))
    else:
        print(f"\n── Audit Summary ──────────────────────────────")
        print(f"Source files : {total_scanned:,}")
        print(f"Dest files   : {len(dst_index):,}")
        print(f"Matched      : {matched:,}")
        print(f"Missing      : {len(missing):,}")
        print(f"Size mismatch: {len(size_mismatch):,}")
        print(f"Corrupt src  : {len(corrupt_src):,}")
        print(f"Corrupt dst  : {len(corrupt_dst):,}")
        print(f"Elapsed      : {elapsed:.0f}s")
        print(f"Report       : {AUDIT_FILE}")

    logger.info(
        "Audit complete: scanned=%d matched=%d missing=%d mismatch=%d corrupt_src=%d corrupt_dst=%d",
        total_scanned, matched, len(missing), len(size_mismatch), len(corrupt_src), len(corrupt_dst),
    )
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Daemon watchdog
# ─────────────────────────────────────────────────────────────────────────────

def _check_gdrive_busy() -> bool:
    """
    Heuristic: if the destination drive has low free space,
    GDrive may be queuing heavily. Return True if we should wait.
    """
    try:
        import shutil
        usage = shutil.disk_usage(DEST_DIR.drive + "\\")
        free_gb = usage.free / (1024 ** 3)
        if free_gb < 2:
            logger.warning("Destination drive has only %.1f GB free — pausing restart.", free_gb)
            return True
    except OSError:
        pass
    return False


def run_daemon() -> None:
    """Continuous watchdog loop."""
    _banner()
    _print("[cyan]Starting daemon watchdog (Ctrl+C to stop)...[/cyan]")
    logger.info("Daemon started")

    state = _load_state()
    last_dst_files = 0
    last_check_ts  = time.time()

    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            running = is_robocopy_running()

            # Quick count (non-recursive shell call for speed)
            src_files, src_bytes = _count_files(SOURCE_DIR)
            dst_files, dst_bytes = _count_files(DEST_DIR)
            missing = src_files - dst_files

            # Speed calculation
            elapsed_min = (time.time() - last_check_ts) / 60
            if elapsed_min > 0 and last_dst_files > 0:
                files_delta = dst_files - last_dst_files
                bytes_delta = dst_bytes  # approximation
                files_per_min = files_delta / elapsed_min
                mb_per_min    = (bytes_delta / (1024 ** 2)) / elapsed_min if elapsed_min > 0 else 0

                sample = {
                    "ts": now,
                    "files_per_min": round(files_per_min, 1),
                    "mb_per_min": round(mb_per_min, 1),
                }
                state.setdefault("speed_samples", []).append(sample)
                state["speed_samples"] = state["speed_samples"][-50:]  # keep last 50
                _save_state(state)

            last_dst_files  = dst_files
            last_check_ts   = time.time()

            pct = (dst_files / src_files * 100) if src_files else 0

            status_line = (
                f"[{now}] running={running}  "
                f"src={src_files:,}  dst={dst_files:,}  "
                f"missing={missing:,}  {pct:.1f}%"
            )
            _print(f"[dim]{status_line}[/dim]")
            logger.info(status_line)

            # ── Backup complete? ──────────────────────────────────────────
            if missing == 0 and src_files > 0:
                _print("[bold green]✓ Backup is 100% complete![/bold green]")
                logger.info("Backup is 100%% complete.")
                if running:
                    _print("[yellow]robocopy still running despite 0 missing — that is normal, letting it finish.[/yellow]")
                else:
                    state["last_complete"] = now
                    _save_state(state)
                    _print("[bold green]All done. Daemon exiting.[/bold green]")
                    logger.info("Daemon exiting — backup complete.")
                    break

            # ── Auto-restart if not running and files remain ──────────────
            elif not running and missing > 0:
                if _check_gdrive_busy():
                    _print("[yellow]GDrive appears busy (low free space). Waiting before restart...[/yellow]")
                else:
                    _print(f"[yellow]robocopy not running, {missing:,} files missing — restarting...[/yellow]")
                    logger.info("Auto-restarting robocopy (%d missing files)", missing)
                    state.setdefault("runs", []).append({"ts": now, "missing_at_start": missing})
                    state["last_start"] = now
                    _save_state(state)
                    start_robocopy(background=True)

        except KeyboardInterrupt:
            _print("\n[yellow]Watchdog stopped by user.[/yellow]")
            logger.info("Daemon stopped by user (KeyboardInterrupt)")
            break
        except Exception as exc:
            logger.exception("Unexpected error in daemon loop: %s", exc)
            _print(f"[red]Error in daemon loop: {exc}[/red]")

        time.sleep(CHECK_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backup watchdog — robocopy monitor and manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daemon",  action="store_true", help="Run as continuous watchdog")
    group.add_argument("--start",   action="store_true", help="Start robocopy")
    group.add_argument("--stop",    action="store_true", help="Kill robocopy")
    group.add_argument("--status",  action="store_true", help="Show current progress")
    group.add_argument("--audit",   action="store_true", help="Full file-by-file audit")

    args = parser.parse_args()

    if args.daemon:
        run_daemon()
    elif args.start:
        _banner()
        start_robocopy(background=True)
    elif args.stop:
        _banner()
        stop_robocopy()
    elif args.status:
        show_status()
    elif args.audit:
        run_audit()


if __name__ == "__main__":
    main()
