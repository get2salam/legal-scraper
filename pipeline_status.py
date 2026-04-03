#!/usr/bin/env python3
"""
Pipeline Status Reporter
=========================
Shared module for scripts to report their status to the orchestrator.
"""

import os
import json
import atexit
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data_v2"
STATUS_FILE = DATA_DIR / "pipeline_status.json"
ORCHESTRATOR_STATE = DATA_DIR / "orchestrator_state.json"

# Ensure data dir exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# Status Classes
# ══════════════════════════════════════════════════════════════════════════════

class ScriptStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

class ScriptType(str, Enum):
    SCRAPER = "scraper"
    VERIFIER = "verifier"
    CLEANER = "cleaner"
    HTML_GEN = "html_generator"
    JSONL_GEN = "jsonl_generator"

# ══════════════════════════════════════════════════════════════════════════════
# Status Management
# ══════════════════════════════════════════════════════════════════════════════

class PipelineStatusReporter:
    """Reports script status to a shared file for orchestrator monitoring."""
    
    def __init__(self, script_type: ScriptType, script_name: str = None):
        self.script_type = script_type
        self.script_name = script_name or script_type.value
        self.pid = os.getpid()
        self.started_at = datetime.now().isoformat()
        self.status = ScriptStatus.IDLE
        self.current_task = None
        self.progress = {}
        
        # Register cleanup on exit
        atexit.register(self._cleanup)
    
    def _load_status(self) -> Dict:
        """Load current status file."""
        if STATUS_FILE.exists():
            try:
                return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
            except:
                pass
        return {"scripts": {}, "last_updated": None}
    
    def _save_status(self, data: Dict):
        """Save status file."""
        data["last_updated"] = datetime.now().isoformat()
        try:
            STATUS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Warning: Failed to save status: {e}")
    
    def _cleanup(self):
        """Clean up status on exit."""
        try:
            data = self._load_status()
            if self.script_name in data.get("scripts", {}):
                script_data = data["scripts"][self.script_name]
                if script_data.get("pid") == self.pid:
                    script_data["status"] = ScriptStatus.COMPLETED.value
                    script_data["ended_at"] = datetime.now().isoformat()
                    self._save_status(data)
        except:
            pass
    
    def start(self, task: str = None, **metadata):
        """Report that script has started."""
        self.status = ScriptStatus.RUNNING
        self.current_task = task
        
        data = self._load_status()
        data["scripts"][self.script_name] = {
            "type": self.script_type.value,
            "status": self.status.value,
            "pid": self.pid,
            "started_at": self.started_at,
            "current_task": task,
            "progress": {},
            "metadata": metadata
        }
        self._save_status(data)
    
    def update(self, task: str = None, progress: Dict = None, **metadata):
        """Update current status."""
        data = self._load_status()
        
        if self.script_name not in data.get("scripts", {}):
            data["scripts"][self.script_name] = {}
        
        script_data = data["scripts"][self.script_name]
        script_data["status"] = self.status.value
        script_data["pid"] = self.pid
        
        if task:
            script_data["current_task"] = task
            self.current_task = task
        
        if progress:
            script_data["progress"] = progress
            self.progress = progress
        
        if metadata:
            script_data.setdefault("metadata", {}).update(metadata)
        
        self._save_status(data)
    
    def progress_update(self, current: int, total: int, message: str = None):
        """Report progress (e.g., 50/100 cases scraped)."""
        self.update(progress={
            "current": current,
            "total": total,
            "percent": round(current / total * 100, 1) if total > 0 else 0,
            "message": message
        })
    
    def complete(self, success: bool = True, message: str = None):
        """Report completion."""
        self.status = ScriptStatus.COMPLETED if success else ScriptStatus.FAILED
        
        data = self._load_status()
        if self.script_name in data.get("scripts", {}):
            script_data = data["scripts"][self.script_name]
            script_data["status"] = self.status.value
            script_data["ended_at"] = datetime.now().isoformat()
            script_data["success"] = success
            if message:
                script_data["message"] = message
        self._save_status(data)
    
    def fail(self, error: str):
        """Report failure."""
        self.status = ScriptStatus.FAILED
        
        data = self._load_status()
        if self.script_name in data.get("scripts", {}):
            script_data = data["scripts"][self.script_name]
            script_data["status"] = self.status.value
            script_data["ended_at"] = datetime.now().isoformat()
            script_data["error"] = error
            script_data["success"] = False
        self._save_status(data)
    
    def pause(self, reason: str = None):
        """Report that script is paused."""
        self.status = ScriptStatus.PAUSED
        self.update(metadata={"pause_reason": reason} if reason else {})

# ══════════════════════════════════════════════════════════════════════════════
# Utility Functions
# ══════════════════════════════════════════════════════════════════════════════

def get_running_scripts() -> Dict[str, Dict]:
    """Get all currently running scripts."""
    if not STATUS_FILE.exists():
        return {}
    
    try:
        data = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        return {
            name: info
            for name, info in data.get("scripts", {}).items()
            if info.get("status") == "running"
        }
    except:
        return {}

def is_script_running(script_type: ScriptType = None, script_name: str = None) -> bool:
    """Check if a specific script (or any script) is running."""
    running = get_running_scripts()
    
    if not running:
        return False
    
    if script_name:
        return script_name in running
    
    if script_type:
        return any(
            info.get("type") == script_type.value
            for info in running.values()
        )
    
    return bool(running)

def get_script_progress(script_name: str) -> Optional[Dict]:
    """Get progress info for a specific script."""
    if not STATUS_FILE.exists():
        return None
    
    try:
        data = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        script_data = data.get("scripts", {}).get(script_name)
        if script_data:
            return script_data.get("progress")
    except:
        pass
    return None

def clear_stale_statuses(max_age_hours: int = 24):
    """Clear status entries older than max_age_hours."""
    if not STATUS_FILE.exists():
        return
    
    try:
        data = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        now = datetime.now()
        
        cleaned = {}
        for name, info in data.get("scripts", {}).items():
            started_at = info.get("started_at")
            if started_at:
                try:
                    start_time = datetime.fromisoformat(started_at)
                    age_hours = (now - start_time).total_seconds() / 3600
                    if age_hours < max_age_hours:
                        cleaned[name] = info
                except:
                    pass
        
        data["scripts"] = cleaned
        STATUS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# Context Manager for Easy Use
# ══════════════════════════════════════════════════════════════════════════════

class StatusContext:
    """Context manager for script status reporting."""
    
    def __init__(self, script_type: ScriptType, task: str = None, **metadata):
        self.reporter = PipelineStatusReporter(script_type)
        self.task = task
        self.metadata = metadata
    
    def __enter__(self):
        self.reporter.start(self.task, **self.metadata)
        return self.reporter
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.reporter.fail(str(exc_val))
        else:
            self.reporter.complete(success=True)
        return False

# ══════════════════════════════════════════════════════════════════════════════
# Example Usage
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Example: Using as context manager
    with StatusContext(ScriptType.SCRAPER, task="Scraping 2024") as status:
        for i in range(10):
            status.progress_update(i + 1, 10, f"Processing item {i + 1}")
            import time
            time.sleep(0.5)
    
    # Example: Using reporter directly
    reporter = PipelineStatusReporter(ScriptType.VERIFIER)
    reporter.start(task="Verifying 2024", year=2024)
    reporter.progress_update(50, 100, "Half done")
    reporter.complete(success=True, message="Verification complete")
    
    # Check running scripts
    print(f"Running scripts: {get_running_scripts()}")
