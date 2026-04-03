# Court Website Scrapers — Architecture Document

## Overview
Build independent scrapers for all Pakistani court websites to supplement PLS data.
These are FREE, PUBLIC sources. Many cases here aren't in PLS (or appear months later).

## Priority Order

### TIER 1 — Build Immediately

#### 1. Sindh High Court (SHC) — `caselaw.shc.gov.pk`
**Status:** PUBLIC access, no login required for `/caselaw/public/home`
**Citation system:** `2026 SHC KHI 304`, `2025 SHC HYD 44`, etc.
**Benches:** Karachi, Hyderabad, Sukkur, Larkana, Mirpurkhas
**Data available:**
- Case type, parties, matter category
- Advocates with SBC registration numbers
- Judge names + author judge
- Tag lines / headnotes
- Full judgment text (downloadable)
- Citation number

**Download URL pattern:**
```
https://caselaw.shc.gov.pk/caselaw/download-file.php?doc=BASE64_ENCODED&citation=YEAR SHC BENCH PAGE
```

**Scraping approach:**
1. Paginate through `/caselaw/public/home` (appears to list all recent judgments)
2. Extract case metadata from HTML
3. Download full judgment via download-file.php endpoint
4. Parse base64 doc parameter for case ID
5. Store in `data_v2/SHC_KHI/YEAR/`, `data_v2/SHC_HYD/YEAR/`, etc.

**Estimated yield:** 3,000-5,000+ cases/year (5 benches × ~600-1,000 each)

---

#### 2. Supreme Court of Pakistan (SCP) — `supremecourt.gov.pk`
**Status:** PUBLIC, but JS-heavy search form
**Endpoint:** `/judgement-search/` (WordPress + AJAX)
**Search fields:**
- Honorable Judges (sitting/former)
- Parties Name, Tagline, Keywords
- Case Type, Case Number, Case Year
- Date of Announcement
- Citation, SC Citation
- Reported checkbox

**Results table columns:**
Sr. No, Case Subject, Case No, Case Title, Author Judge, Upload Date, Judgment Date, Citation(s), SCCitation(s), Download

**Scraping approach:**
1. Use browser automation (Playwright) to interact with JS search form
2. OR: reverse-engineer the AJAX endpoint (likely WordPress REST or admin-ajax.php)
3. Search by year, iterate through case types
4. Download PDFs of judgments
5. Extract text from PDFs (PyMuPDF/pdfplumber)
6. Store in `data_v2/SC/YEAR/`

**Estimated yield:** 500-1,500 cases/year (Supreme Court cases are fewer but high value)

---

### TIER 2 — Build Next

#### 3. Lahore High Court (LHC) — `sys.lhc.gov.pk/appjudgments/`
**Status:** Main domain blocked by FortiGuard, but judgments subdomain may work
**Note:** Oxford LibGuide confirms LHC has searchable database since 2002+
**Approach:** Try `sys.lhc.gov.pk` endpoints, may need browser or VPN

#### 4. Peshawar High Court (PHC) — `peshawarhighcourt.gov.pk`
**Endpoint:** `/PHCCMS/reportedJudgments.php` (JS-heavy, needs browser)
**Benches:** Main (Peshawar), Abbottabad (`peshawarhcatd.gov.pk`), Mingora (`peshawarhcmb.gov.pk`)
**Approach:** Browser automation for reported judgments; case search via bench-specific URLs

#### 5. Islamabad High Court (IHC) — `ihc.gov.pk`
**Status:** Heavy JavaScript frontend
**Approach:** Browser automation or API reverse-engineering

#### 6. Balochistan High Court (BHC) — `bhc.gov.pk`
**Status:** Needs investigation (fetch failed earlier)

---

## Data Storage Schema

```
data_v2/
├── court_cases/               ← NEW: Court website cases
│   ├── SHC/                   ← Sindh High Court
│   │   ├── KHI/              ← Karachi bench
│   │   │   ├── 2026/
│   │   │   │   ├── SHC_KHI_304.json
│   │   │   │   ├── original/SHC_KHI_304.html
│   │   │   │   └── ...
│   │   │   ├── 2025/
│   │   │   └── ...
│   │   ├── HYD/              ← Hyderabad bench
│   │   ├── SUK/              ← Sukkur bench
│   │   ├── LAR/              ← Larkana bench
│   │   └── MIR/              ← Mirpurkhas bench
│   ├── SC/                    ← Supreme Court
│   │   ├── 2026/
│   │   ├── 2025/
│   │   └── ...
│   ├── LHC/                   ← Lahore High Court
│   ├── PHC/                   ← Peshawar High Court
│   ├── IHC/                   ← Islamabad High Court
│   └── BHC/                   ← Balochistan High Court
├── SCMR/                      ← Existing PLS reporters
├── PLD/
└── ...
```

## JSON Schema for Court Cases

```json
{
    "source": "SHC",
    "bench": "KHI",
    "citation": "2026 SHC KHI 304",
    "case_number": "Const. P. 1779/2024",
    "case_type": "D.B.",
    "parties": {
        "petitioner": "City School (Pvt) Ltd",
        "respondent": "Province of Sindh & Others"
    },
    "matter": "COOP. HOUSING SOCIETIES",
    "judges": [
        {"name": "Mr. Justice Yousuf Ali Sayeed", "author": false},
        {"name": "Mr. Justice Abdul Mobeen Lakho", "author": true}
    ],
    "advocates": [
        {"name": "Ravi Pinjani", "registration": "ADVO-11673-SBC-KHI"},
        {"name": "Rizwana Ismail", "registration": "ADVO-4484-SBC-KHI"}
    ],
    "tagline": "",
    "order_date": "2026-02-16",
    "judgment_raw": "<html>...</html>",
    "judgment_text": "...",
    "approved_for_reporting": true,
    "fetched_at": "2026-02-16T19:00:00Z",
    "source_url": "https://caselaw.shc.gov.pk/caselaw/download-file.php?doc=..."
}
```

## Cross-Reference Strategy
After scraping court websites, cross-reference with PLS data:
1. Match by case number (e.g., "Const. P. 1779/2024")
2. Match by parties + date
3. Match by PLS citation → court citation
4. Flag cases ONLY on court website (not in PLS) — these are our competitive edge

## Implementation Notes
- Use `curl_cffi` for SHC (simple HTTP, no JS)
- Use `playwright` for SC, PHC, IHC (JS-heavy forms)
- Same 4-format output: JSON, Original HTML, Readable HTML, JSONL
- Human-like delays (these are government sites, be respectful)
- Progress tracking per court/bench/year
- Resume capability (skip already-downloaded cases)
