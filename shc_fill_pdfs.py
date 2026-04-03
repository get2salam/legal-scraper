#!/usr/bin/env python3
"""
shc_fill_pdfs.py - Download missing PDFs for SHC court cases.

Structure:
  data_v2/court_cases/SHC/{bench}/{year}/*.json   -> metadata
  data_v2/court_cases/SHC/{bench}/{year}/original/*.pdf  -> PDFs

This script finds JSONs without matching PDFs in the original/ subdir
and downloads them from the download_url in the JSON metadata.
"""

import os
import sys
import json
import time
import requests
import logging
from pathlib import Path

# Config
BASE_DIR = Path(__file__).parent / "data_v2" / "court_cases" / "SHC"
MAX_DOWNLOADS = 5000
DELAY_SECONDS = 1
TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("shc_fill_pdfs.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/pdf,application/octet-stream,*/*",
    "Referer": "https://caselaw.shc.gov.pk/",
})


def find_missing_pdfs(base_dir: Path):
    """Walk all bench/year dirs and find JSONs without matching PDFs."""
    missing = []
    for bench_dir in sorted(base_dir.iterdir()):
        if not bench_dir.is_dir():
            continue
        bench = bench_dir.name
        for year_dir in sorted(bench_dir.iterdir()):
            if not year_dir.is_dir() or year_dir.name == "original":
                continue
            orig_dir = year_dir / "original"
            json_files = list(year_dir.glob("*.json"))
            for jf in json_files:
                pdf_name = jf.stem + ".pdf"
                pdf_path = orig_dir / pdf_name
                if not pdf_path.exists():
                    missing.append((bench, year_dir.name, jf, pdf_path))
    return missing


def download_pdf(download_url: str, pdf_path: Path) -> bool:
    """Download a PDF from download_url to pdf_path. Returns True on success."""
    try:
        resp = session.get(download_url, timeout=TIMEOUT, stream=True)
        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            content = resp.content
            # Check it's actually a PDF (starts with %PDF)
            if content[:4] == b"%PDF" or "pdf" in content_type.lower():
                pdf_path.parent.mkdir(parents=True, exist_ok=True)
                pdf_path.write_bytes(content)
                return True
            else:
                log.warning(f"Not a PDF response for {download_url}: ContentType={content_type}, size={len(content)}")
                return False
        else:
            log.warning(f"HTTP {resp.status_code} for {download_url}")
            return False
    except requests.RequestException as e:
        log.error(f"Request error for {download_url}: {e}")
        return False


def main():
    log.info(f"SHC Fill PDFs - scanning {BASE_DIR}")
    missing = find_missing_pdfs(BASE_DIR)
    log.info(f"Found {len(missing)} missing PDFs")

    downloaded = 0
    failed = 0
    skipped = 0

    for i, (bench, year, json_path, pdf_path) in enumerate(missing):
        if downloaded >= MAX_DOWNLOADS:
            log.info(f"Reached max downloads ({MAX_DOWNLOADS}), stopping.")
            break

        # Read JSON to get download_url
        try:
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log.error(f"Error reading {json_path}: {e}")
            skipped += 1
            continue

        download_url = data.get("download_url") or data.get("source_url")
        if not download_url:
            log.warning(f"No download_url in {json_path}")
            skipped += 1
            continue

        # Convert source_url to download_url if needed
        if "view-file/" in download_url and "download-file" not in download_url:
            doc_id = download_url.rstrip("/").split("/")[-1]
            download_url = f"https://caselaw.shc.gov.pk/caselaw/download-file.php?doc={doc_id}"

        log.info(f"[{i+1}/{len(missing)}] {bench}/{year}/{json_path.stem} -> {pdf_path.name}")

        success = download_pdf(download_url, pdf_path)
        if success:
            downloaded += 1
            log.info(f"  OK ({downloaded} downloaded so far)")
        else:
            failed += 1
            log.warning(f"  FAILED")

        time.sleep(DELAY_SECONDS)

    log.info(f"\n=== DONE ===")
    log.info(f"Downloaded: {downloaded}")
    log.info(f"Failed:     {failed}")
    log.info(f"Skipped:    {skipped}")
    log.info(f"Total missing was: {len(missing)}")


if __name__ == "__main__":
    main()
