#!/usr/bin/env python3
"""Sync data_v2 from local to Google Drive cloud backup.
Copies any JSON files that exist locally but not on cloud.
Designed to run as a scheduled task / cron job."""

import os
import shutil
import json
from pathlib import Path
from datetime import datetime

LOCAL_DIR = Path(r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2")
CLOUD_DIR = Path(r"G:\My Drive\Qanoon\data_v2")
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
LOG_FILE = Path(r"C:\Users\gempo\.openclaw\workspace\memory\cloud-sync-log.json")

def sync():
    if not CLOUD_DIR.exists():
        print("ERROR: Google Drive not mounted at G:\\")
        return
    
    results = {}
    total_copied = 0
    
    for reporter in REPORTERS:
        local_path = LOCAL_DIR / reporter
        cloud_path = CLOUD_DIR / reporter
        
        if not local_path.exists():
            continue
        
        copied = 0
        for root, dirs, files in os.walk(local_path):
            for f in files:
                if not f.endswith('.json'):
                    continue
                local_file = Path(root) / f
                # Mirror the relative path structure
                rel = local_file.relative_to(local_path)
                cloud_file = cloud_path / rel
                
                if not cloud_file.exists():
                    cloud_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(local_file), str(cloud_file))
                    copied += 1
        
        local_count = sum(1 for _ in local_path.rglob("*.json"))
        cloud_count = sum(1 for _ in cloud_path.rglob("*.json"))
        
        results[reporter] = {
            "local": local_count,
            "cloud": cloud_count,
            "copied": copied,
            "match": local_count == cloud_count
        }
        total_copied += copied
    
    # Also sync legislation, courts, federal_laws if they exist
    for extra in ["legislation", "court_cases", "federal_laws"]:
        local_path = LOCAL_DIR / extra
        cloud_path = CLOUD_DIR / extra
        if local_path.exists():
            copied = 0
            for root, dirs, files in os.walk(local_path):
                for f in files:
                    if not f.endswith('.json'):
                        continue
                    local_file = Path(root) / f
                    rel = local_file.relative_to(local_path)
                    cloud_file = cloud_path / rel
                    if not cloud_file.exists():
                        cloud_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(str(local_file), str(cloud_file))
                        copied += 1
            total_copied += copied
            if copied > 0:
                results[extra] = {"copied": copied}
    
    # Log results
    log_entry = {
        "timestamp": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
        "total_copied": total_copied,
        "reporters": results
    }
    
    # Append to log
    log_data = []
    if LOG_FILE.exists():
        try:
            log_data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except:
            log_data = []
    log_data.append(log_entry)
    # Keep last 30 entries
    log_data = log_data[-30:]
    LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
    
    if total_copied > 0:
        print(f"SYNCED: {total_copied} new files copied to cloud")
        for k, v in results.items():
            if isinstance(v, dict) and v.get("copied", 0) > 0:
                print(f"  {k}: +{v['copied']}")
    else:
        print("IN SYNC: All files already backed up")

if __name__ == "__main__":
    sync()
