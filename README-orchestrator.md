# Qanoon Pipeline Orchestrator

Autonomous supervisor for the Pakistan legislation scraping pipeline. Monitors work, executes jobs, handles errors—no human intervention needed.

## Quick Start

```bash
# Check current status
python orchestrator.py --status

# Run pending work once (respects operating hours)
python orchestrator.py --run-pending

# Run as daemon (checks every 30 minutes)
python orchestrator.py --daemon

# Force fix missing cases (ignores retry limits)
python orchestrator.py --force-fix

# Run full pipeline for a specific year
python orchestrator.py --year 2024 --pipeline
```

Or use the batch file:
```bash
start_orchestrator.bat status
start_orchestrator.bat run
start_orchestrator.bat daemon
start_orchestrator.bat fix
start_orchestrator.bat stop
```

## Pipeline Order

When processing a year, the orchestrator runs this pipeline:

```
1. Scrape Year     → pls_scraper_v2.py scrape --year YYYY
2. Verify          → verify_scraper.py --year YYYY
3. Fix Missing     → verify_scraper.py --year YYYY --fix
4. Re-verify       → verify_scraper.py --year YYYY
5. Clean           → data_cleaner.py clean
6. Generate HTML   → generate_html.py
7. Update JSONL    → convert_to_jsonl.py
```

## Features

### Operating Hours Awareness
- PLS website is only available 7AM-9PM PKT (Pakistan Time)
- Orchestrator automatically pauses outside these hours
- Shows next available window when paused

### Smart Work Detection
1. **Missing Cases**: Reads verification reports from `data_v2/audit/`
2. **Incomplete Years**: Checks `daily_schedule.json` and `progress.json`
3. **Running Processes**: Prevents duplicate runs

### Error Handling
- Max 3 fix attempts per year (prevents infinite loops)
- Graceful handling of network errors
- Process timeout protection (8 hours default)
- Crash recovery via state file

### Windows Notifications
- Sends toast notifications on completion/errors
- Supports winotify, win10toast, or PowerShell fallback

## Windows Task Scheduler Setup

Run `setup_orchestrator.bat` as Administrator to register the orchestrator:

```bash
setup_orchestrator.bat
```

This creates a task that:
- Runs at system startup (2 minute delay)
- Runs at user logon (1 minute delay)
- Runs every 30 minutes
- Restarts on failure (up to 3 times)

### Manual Task Management
```bash
# Run task manually
schtasks /run /tn "QanoonPipelineOrchestrator"

# Disable task
schtasks /change /tn "QanoonPipelineOrchestrator" /disable

# Enable task
schtasks /change /tn "QanoonPipelineOrchestrator" /enable

# Delete task
schtasks /delete /tn "QanoonPipelineOrchestrator" /f
```

## State Files

### `data_v2/orchestrator_state.json`
Tracks:
- Running status
- Current job
- Job history (last 100)
- Last successful run per year
- Fix attempt counts

### `data_v2/pipeline_status.json`
Shared status file that scripts report to:
- Which scripts are running
- Current progress
- Completion status

## Monitoring

### Check Status
```bash
python orchestrator.py --status
```

Shows:
- Daemon status (running/stopped)
- Operating hours status
- Current job
- Pending work (missing cases, incomplete years)
- Latest verification summary
- Recent job history

### View Logs
```bash
# Full log
type data_v2\logs\orchestrator.log

# Last 50 lines
powershell "Get-Content data_v2\logs\orchestrator.log -Tail 50"
```

## Integration with Existing Scripts

The orchestrator integrates with:
- `pls_scraper_v2.py` - Main scraper
- `verify_scraper.py` - Verification and fix
- `data_cleaner.py` - Data cleaning
- `generate_html.py` - HTML generation
- `convert_to_jsonl.py` - JSONL export

Scripts now report their status via `pipeline_status.py`:
```python
from pipeline_status import PipelineStatusReporter, ScriptType

reporter = PipelineStatusReporter(ScriptType.SCRAPER)
reporter.start(task="Scraping 2024")
reporter.progress_update(50, 100, "Half done")
reporter.complete(success=True)
```

## Troubleshooting

### "Outside operating hours"
PLS is only available 7AM-9PM PKT. Wait or use `--force-fix` during testing.

### "Scraper already running"
Another scraper process is active. Check `data_v2/pipeline_status.json` or use Task Manager.

### "Max fix attempts reached"
Use `--force-fix` to reset attempt counters and try again.

### Daemon won't start
Check for existing Python processes:
```bash
tasklist /fi "imagename eq python.exe" /v
```

Kill stale processes:
```bash
taskkill /f /im python.exe
```

## Architecture

```
orchestrator.py
├── Work Detection
│   ├── get_missing_cases() - Reads audit reports
│   ├── get_incomplete_years() - Checks schedule
│   └── is_scraper_running() - Prevents duplicates
│
├── Job Execution
│   ├── run_scrape_year()
│   ├── run_verification()
│   ├── run_fix_missing()
│   ├── run_cleaner()
│   ├── run_html_generator()
│   └── run_jsonl_converter()
│
├── Pipeline Logic
│   ├── execute_pipeline() - Full year pipeline
│   └── run_pending_work() - Priority-based work
│
└── Daemon Mode
    ├── run_daemon() - Main loop
    └── signal_handler() - Graceful shutdown
```

## Dependencies

- Python 3.10+
- curl_cffi (for scraper)
- beautifulsoup4
- python-dotenv

Optional for notifications:
- winotify (preferred)
- win10toast (fallback)
- psutil (for process detection)

Install:
```bash
pip install winotify psutil
```
