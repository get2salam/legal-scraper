"""Fix 3 format issues in PLS case law data."""
import sys
import os
import json

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = r"\\?\C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = {"SCMR", "PLD", "MLD", "CLC", "PCrLJ", "YLR", "PTD", "PLC", "CLD", "GBLR"}

READABLE_CSS = """body { font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 20px; line-height: 1.8; color: #333; background: #fafafa; }
h1,h2,h3 { color: #1a1a2e; } .citation { font-weight: bold; color: #16213e; font-size: 1.2em; } .metadata { background: #f0f0f0; padding: 15px; border-radius: 8px; margin: 20px 0; } .judgment { text-align: justify; }"""

STYLE_BLOCK = f"<style>\n{READABLE_CSS}\n</style>"


def scandir_recursive(root):
    """Recursively yield DirEntry objects using os.scandir (safe for \\?\ paths)."""
    try:
        with os.scandir(root) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    yield from scandir_recursive(entry.path)
                else:
                    yield entry
    except (PermissionError, OSError):
        pass


# ── Task 1: Schema backfill (reporter / year fields) ─────────────────────────

def task1_schema_backfill():
    fixed = 0
    scanned = 0
    errors = 0

    for reporter in REPORTERS:
        reporter_dir = os.path.join(BASE, reporter)
        if not os.path.isdir(reporter_dir):
            continue
        try:
            with os.scandir(reporter_dir) as year_it:
                year_dirs = [e for e in year_it if e.is_dir(follow_symlinks=False)]
        except OSError:
            continue

        for year_entry in year_dirs:
            year_str = year_entry.name
            if not year_str.isdigit():
                continue
            year_int = int(year_str)

            try:
                with os.scandir(year_entry.path) as file_it:
                    json_files = [
                        e for e in file_it
                        if e.is_file() and e.name.endswith(".json")
                        and not e.name.endswith("_readable.json")
                    ]
            except OSError:
                continue

            for entry in json_files:
                scanned += 1
                try:
                    with open(entry.path, "r", encoding="utf-8") as fh:
                        data = json.load(fh)

                    changed = False
                    if "reporter" not in data:
                        data["reporter"] = reporter
                        changed = True
                    if "year" not in data:
                        data["year"] = year_int
                        changed = True

                    if changed:
                        with open(entry.path, "w", encoding="utf-8") as fh:
                            json.dump(data, fh, ensure_ascii=False, indent=2)
                        fixed += 1

                except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
                    print(f"  [T1 ERR] {entry.path}: {exc}")
                    errors += 1

    print(f"Task 1 — Schema backfill: scanned={scanned}, fixed={fixed}, errors={errors}")
    return fixed


# ── Task 2: Readable HTML missing CSS ────────────────────────────────────────

def task2_readable_html_css():
    fixed = 0
    scanned = 0
    errors = 0

    for entry in scandir_recursive(BASE):
        if not entry.name.endswith("_readable.html"):
            continue
        scanned += 1
        try:
            with open(entry.path, "r", encoding="utf-8") as fh:
                content = fh.read()

            if "<style>" in content or "<STYLE>" in content:
                continue  # already has CSS

            # Insert <style> block into <head>; if no <head>, prepend one
            if "<head>" in content:
                new_content = content.replace("<head>", f"<head>\n{STYLE_BLOCK}", 1)
            elif "<HEAD>" in content:
                new_content = content.replace("<HEAD>", f"<HEAD>\n{STYLE_BLOCK}", 1)
            elif "<html>" in content or "<HTML>" in content or "<!DOCTYPE" in content:
                # Has html root but no head — insert after opening html/doctype
                insert_after = None
                for tag in ("<html>", "<HTML>", "<!DOCTYPE html>", "<!DOCTYPE HTML>"):
                    if tag in content:
                        insert_after = tag
                        break
                if insert_after:
                    new_content = content.replace(
                        insert_after,
                        f"{insert_after}\n<head>\n{STYLE_BLOCK}\n</head>",
                        1,
                    )
                else:
                    new_content = f"<head>\n{STYLE_BLOCK}\n</head>\n" + content
            else:
                # Plain content — prepend head
                new_content = f"<head>\n{STYLE_BLOCK}\n</head>\n" + content

            with open(entry.path, "w", encoding="utf-8") as fh:
                fh.write(new_content)
            fixed += 1

        except (OSError, UnicodeDecodeError) as exc:
            print(f"  [T2 ERR] {entry.path}: {exc}")
            errors += 1

    print(f"Task 2 — Readable HTML CSS: scanned={scanned}, fixed={fixed}, errors={errors}")
    return fixed


# ── Task 3: 2026 SCMR plain-text HTML wrapping ───────────────────────────────

def task3_scmr_2026_html():
    fixed = 0
    scanned = 0
    errors = 0

    scmr_2026 = os.path.join(BASE, "SCMR", "2026")
    if not os.path.isdir(scmr_2026):
        print(f"Task 3 — SCMR/2026 not found at {scmr_2026}")
        return 0

    try:
        with os.scandir(scmr_2026) as it:
            html_files = [
                e for e in it
                if e.is_file()
                and e.name.endswith(".html")
                and not e.name.endswith("_readable.html")
            ]
    except OSError as exc:
        print(f"  [T3 ERR] scandir failed: {exc}")
        return 0

    for entry in html_files:
        scanned += 1
        try:
            with open(entry.path, "r", encoding="utf-8") as fh:
                content = fh.read()

            stripped = content.lstrip()
            is_plain = not (
                stripped.startswith("<html")
                or stripped.startswith("<HTML")
                or stripped.startswith("<!DOCTYPE")
                or stripped.startswith("<!doctype")
            )

            if not is_plain:
                continue

            wrapped = (
                "<html><head><meta charset='utf-8'></head>"
                f"<body>{content}</body></html>"
            )
            with open(entry.path, "w", encoding="utf-8") as fh:
                fh.write(wrapped)
            fixed += 1

        except (OSError, UnicodeDecodeError) as exc:
            print(f"  [T3 ERR] {entry.path}: {exc}")
            errors += 1

    print(f"Task 3 — SCMR/2026 HTML wrap: scanned={scanned}, fixed={fixed}, errors={errors}")
    return fixed


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PLS Data Format Fix — 3 Tasks")
    print("=" * 60)

    t1 = task1_schema_backfill()
    t2 = task2_readable_html_css()
    t3 = task3_scmr_2026_html()

    print("=" * 60)
    print(f"SUMMARY: T1={t1} JSON fixed | T2={t2} HTML styled | T3={t3} HTML wrapped")
    print("=" * 60)
