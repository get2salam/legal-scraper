"""
Scraper & Dashboard Watchdog — Zero-token process monitor
Runs via Windows Task Scheduler every 15 minutes.
- Checks if scraper processes are alive, logs status, alerts if dead.
- Checks if dashboard frontend/backend are alive, auto-restarts if dead.
Does NOT auto-restart scrapers (Abdul controls scraper lifecycle).
"""

import subprocess
import json
import os
import urllib.request
from datetime import datetime

LOG_FILE = os.path.join(os.path.dirname(__file__), "data_v2", "watchdog_log.json")
DASHBOARD_FRONTEND_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\qanoon-dashboard\frontend"
DASHBOARD_BACKEND_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\qanoon-dashboard\backend"

def get_scraper_processes():
    """Find running scraper Python processes."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { "
             "$_.CommandLine -match 'historical_scraper|pls_scraper|scraper_chain|fill_2006|fill_gaps|fill_all' "
             "-and $_.Name -match 'python' } | Select-Object ProcessId, CommandLine | ConvertTo-Json"],
            capture_output=True, text=True, timeout=15
        )
        if result.stdout.strip():
            data = json.loads(result.stdout)
            if isinstance(data, dict):
                data = [data]
            return data
        return []
    except:
        return []

def check_url(url, timeout=5):
    """Check if a URL is reachable."""
    try:
        req = urllib.request.urlopen(url, timeout=timeout)
        return req.status == 200
    except:
        return False

def restart_dashboard_backend():
    """Restart FastAPI backend."""
    try:
        subprocess.Popen(
            ["C:\\Python314\\python.exe", "run.py"],
            cwd=DASHBOARD_BACKEND_DIR,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        return True
    except:
        return False

def restart_dashboard_frontend():
    """Restart Next.js frontend."""
    try:
        subprocess.Popen(
            ["cmd.exe", "/c", "npm", "run", "dev"],
            cwd=DASHBOARD_FRONTEND_DIR,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        return True
    except:
        return False

def load_log():
    try:
        with open(LOG_FILE) as f:
            return json.load(f)
    except:
        return {"entries": [], "last_alive": None, "consecutive_dead": 0}

def save_log(log):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2)

def send_telegram_alert(message):
    """Write alert file for Marathon Monitor to pick up."""
    alert_file = os.path.join(os.path.dirname(__file__), "data_v2", "watchdog_alert.txt")
    with open(alert_file, 'a') as f:
        f.write(f"{datetime.now().isoformat()}\n{message}\n---\n")

def main():
    now = datetime.now()
    processes = get_scraper_processes()
    log = load_log()
    
    # === SCRAPER CHECK ===
    entry = {
        "timestamp": now.isoformat(),
        "alive": len(processes) > 0,
        "process_count": len(processes),
        "pids": [p.get("ProcessId") for p in processes]
    }
    
    if processes:
        log["last_alive"] = now.isoformat()
        log["consecutive_dead"] = 0
        status = f"[{now.strftime('%H:%M')}] ✅ {len(processes)} scraper(s) alive: PIDs {entry['pids']}"
    else:
        log["consecutive_dead"] = log.get("consecutive_dead", 0) + 1
        status = f"[{now.strftime('%H:%M')}] ⚠️ No scrapers running (dead count: {log['consecutive_dead']})"
        
        if log["consecutive_dead"] == 2:
            send_telegram_alert(f"🚨 Scraper down for 30+ minutes! Last alive: {log.get('last_alive', 'unknown')}")
        
        if log["consecutive_dead"] > 2 and log["consecutive_dead"] % 8 == 0:
            hours = log["consecutive_dead"] * 15 / 60
            send_telegram_alert(f"🚨 Scraper still down ({hours:.0f}h)! Last alive: {log.get('last_alive', 'unknown')}")
    
    print(status)
    
    # === DASHBOARD CHECK (auto-restart) ===
    backend_up = check_url("http://localhost:8000/health")
    frontend_up = check_url("http://localhost:3000")
    
    if not backend_up:
        print(f"[{now.strftime('%H:%M')}] ❌ Dashboard backend DOWN — restarting...")
        if restart_dashboard_backend():
            print(f"[{now.strftime('%H:%M')}] 🔄 Backend restart triggered")
            entry["backend_restarted"] = True
        else:
            print(f"[{now.strftime('%H:%M')}] ❌ Backend restart FAILED")
    else:
        print(f"[{now.strftime('%H:%M')}] ✅ Dashboard backend UP")
    
    if not frontend_up:
        print(f"[{now.strftime('%H:%M')}] ❌ Dashboard frontend DOWN — restarting...")
        if restart_dashboard_frontend():
            print(f"[{now.strftime('%H:%M')}] 🔄 Frontend restart triggered")
            entry["frontend_restarted"] = True
        else:
            print(f"[{now.strftime('%H:%M')}] ❌ Frontend restart FAILED")
    else:
        print(f"[{now.strftime('%H:%M')}] ✅ Dashboard frontend UP")
    
    entry["backend_up"] = backend_up
    entry["frontend_up"] = frontend_up
    
    # Keep last 100 entries
    log["entries"] = (log.get("entries", []) + [entry])[-100:]
    save_log(log)

if __name__ == "__main__":
    main()
