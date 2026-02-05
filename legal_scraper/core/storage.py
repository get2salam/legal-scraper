"""
Storage utilities for scraped legal data.

Supports both JSON (human-readable) and JSONL (batch processing) formats.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Storage:
    """
    Manages storage of scraped case data.
    
    Features:
    - Dual format: JSON + JSONL
    - Organized directory structure
    - Progress tracking
    - Resume support
    """
    
    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or os.environ.get("DATA_DIR", "data"))
        self.cases_dir = self.data_dir / "cases"
        self.jsonl_dir = self.data_dir / "jsonl"
        self.progress_file = self.data_dir / "progress.json"
        
        # Create directories
        self.cases_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_dir.mkdir(parents=True, exist_ok=True)
        
        # Load or initialize progress
        self.progress = self._load_progress()
    
    def _load_progress(self) -> dict:
        """Load progress from file or create new."""
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                return json.load(f)
        return {
            "created": datetime.now().isoformat(),
            "fetched_ids": [],
            "total_count": 0,
        }
    
    def _save_progress(self):
        """Persist progress to file."""
        self.progress["updated"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def is_fetched(self, case_id: str) -> bool:
        """Check if a case has already been fetched."""
        return case_id in self.progress.get("fetched_ids", [])
    
    def save_case(self, case: dict, format: str = "both") -> bool:
        """
        Save a case to storage.
        
        Args:
            case: Case dict with at least 'id' key
            format: 'json', 'jsonl', or 'both'
        
        Returns:
            True if saved successfully
        """
        case_id = case.get("id")
        if not case_id:
            logger.error("Case missing 'id' field")
            return False
        
        # Add metadata
        case["_scraped_at"] = datetime.now().isoformat()
        
        try:
            # Save as individual JSON
            if format in ("json", "both"):
                json_path = self.cases_dir / f"{case_id}.json"
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(case, f, indent=2, ensure_ascii=False)
            
            # Append to JSONL
            if format in ("jsonl", "both"):
                # Group by year if available
                year = case.get("year", "unknown")
                jsonl_path = self.jsonl_dir / f"cases_{year}.jsonl"
                with open(jsonl_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(case, ensure_ascii=False) + '\n')
            
            # Update progress
            if case_id not in self.progress["fetched_ids"]:
                self.progress["fetched_ids"].append(case_id)
                self.progress["total_count"] = len(self.progress["fetched_ids"])
                self._save_progress()
            
            logger.info(f"Saved case: {case_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save case {case_id}: {e}")
            return False
    
    def load_case(self, case_id: str) -> Optional[dict]:
        """Load a case from JSON storage."""
        json_path = self.cases_dir / f"{case_id}.json"
        if json_path.exists():
            with open(json_path, encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def get_all_ids(self) -> list[str]:
        """Get list of all fetched case IDs."""
        return self.progress.get("fetched_ids", [])
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        json_files = list(self.cases_dir.glob("*.json"))
        jsonl_files = list(self.jsonl_dir.glob("*.jsonl"))
        
        return {
            "total_cases": len(json_files),
            "jsonl_files": len(jsonl_files),
            "data_dir": str(self.data_dir),
            "progress_tracked": self.progress.get("total_count", 0),
        }
