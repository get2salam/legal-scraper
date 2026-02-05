@echo off
REM Stealth Scraper — Runs in 4 segments throughout Pakistani business hours
REM Triggered at 2:00 AM GMT (7:00 AM PKT) by Windows Task Scheduler
REM Script manages its own schedule: 7-8AM, 12-1PM, 4-5PM, 8-9PM PKT

cd /d "C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper"
C:\Python314\python.exe stealth_scrape.py >> data\pakistanlawsite\task_scheduler.log 2>&1
