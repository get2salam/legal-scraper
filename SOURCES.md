# Data Sources — Qanun.pk Scraping Registry

Last updated: 2026-03-30

---

## ✅ ACTIVE / COMPLETE

### 1. Pakistan Law Site (PLS) — `pakistanlawsite.com`
- **Type:** Case law (reporters) + Legislation
- **Auth:** Username/password (paid subscription)
- **Login:** POST `/Login/Login` with `Login.UserName`, `Login.Password`
- **Search:** POST `/Login/CitationSearch`
- **Fetch:** POST `/Login/GetCaseFile` with `caseName` (internal ID e.g. `2024SC101`)
- **⚠️ One session at a time** — use `ClearLoginHistory` before login
- **Session:** Expires silently, must check with `check_session()`
- **Scrapers:** `pls_scraper_v2.py`, `historical_scraper.py`, `legislation_scraper.py`, `scrape_new_reporters.py`
- **Status:** ✅ COMPLETE — 178,989 cases across 15 reporters (1947–2026), 10,733 statutes
- **Known limits:** Pre-digitisation cases return sentinel "1" (PCrLJ 1978, PLC 1985, PTD pre-1961)

---

### 2. Peshawar High Court (PHC) — `peshawarhighcourt.gov.pk`
- **Type:** Court judgments (PDFs)
- **Auth:** None
- **Endpoint:** POST to internal search API
- **Scraper:** `phc_scraper.py`
- **Status:** ✅ COMPLETE — 5,585 PDFs + JSON (2010–2026)
- **Format:** PDF + JSON metadata

---

### 3. Sindh High Court (SHC) — `caselaw.shc.gov.pk`
- **Type:** Court judgments (PDFs)
- **Auth:** None
- **URL pattern:** `https://caselaw.shc.gov.pk/caselaw/view-file/{id}`
- **Scraper:** `shc_scraper.py`, `shc_fill_pdfs.py`
- **Status:** ✅ COMPLETE — 3,587 JSON + 3,587 PDFs across 5 benches (KHI, HYD, LAR, MIR, SUK)
- **Benches:** Karachi (KHI), Hyderabad (HYD), Larkana (LAR), Mirpur Khas (MIR), Sukkur (SUK)
- **Note:** Site goes down occasionally (521 errors). Retry scraper handles this.

---

## ❌ BLOCKED / INACCESSIBLE FROM UK

### 4. Lahore High Court (LHC) — `lhc.gov.pk` / `data.lhc.gov.pk`
- **Type:** Court judgments (PDFs)
- **Auth:** None (public)
- **Block reason:** FortiGuard URLfilter on this machine's network blocks `lhc.gov.pk`
- **Also tried:** `data.lhc.gov.pk` — times out
- **Status:** ❌ BLOCKED — needs mobile hotspot, VPN, or Pakistan cloud VM
- **We have:** 50 JSON stubs (metadata only), 0 PDFs
- **To fix:** Run scraper from Pakistan IP or use VPN exit node in Pakistan

---

### 5. Federal Shariat Court (FSC) — `fsc.gov.pk`
- **Type:** Court judgments
- **Block reason:** DNS resolution fails — domain not resolving from UK
- **Status:** ❌ BLOCKED — domain unreachable from London
- **We have:** 11,483 JSON stubs (mostly empty), 712 PDFs with real content
- **Note:** Most FSC entries on PLS are empty stubs; real usable data ≈ 712 cases

---

### 6. Balochistan High Court (BHC) — `bhc.gov.pk`
- **Type:** Court judgments
- **Block reason:** Incapsula anti-bot protection
- **Status:** ❌ BLOCKED — returns JS challenge, no data accessible
- **We have:** 0 data
- **To fix:** Use curl-impersonate or real browser automation from Pakistan IP

---

### 7. AJK High Court — `ajkhc.gov.pk`
- **Type:** Court judgments
- **Block reason:** DNS resolution fails
- **Status:** ❌ BLOCKED — domain not resolving
- **We have:** 0 data

---

### 8. Islamabad High Court (IHC) — `ihc.gov.pk`
- **Type:** Court judgments
- **Status:** ❌ TIMES OUT from UK
- **We have:** 1,374 JSON + 1,344 PDFs (scraped previously via different method)

---

## 🔄 PLANNED / IN PROGRESS

### 9. Internet Archive (Wayback Machine) — `web.archive.org`
- **Type:** Archived Pakistani legislation
- **Purpose:** Fill body text for 4,732 metadata-only legislation stubs
- **API:** CDX Search API + `web.archive.org/web/{timestamp}/{url}`
- **Source URLs archived:** `pakistancode.gov.pk`, `molaw.gov.pk`, `nalaw.gov.pk`
- **Scraper:** TBD (being built)
- **Status:** 🔄 IN PROGRESS — agent probing now

---

### 10. Pakistan Code — `pakistancode.gov.pk`
- **Type:** Federal legislation (full text)
- **Auth:** None (public)
- **Status:** ❌ TIMES OUT from UK — archived version being tried via Wayback Machine
- **Potential:** Thousands of legislation full texts

---

### 11. National Assembly — `na.gov.pk/en/legislation.php`
- **Type:** Bills + Acts
- **Status:** ❌ TIMES OUT from UK
- **Potential:** Recent legislation (2020–2026)

---

### 12. Senate — `senate.gov.pk/en/bills.php`
- **Type:** Senate bills + Acts
- **Status:** ❌ TIMES OUT from UK
- **Potential:** Recent legislation

---

### 13. Ministry of Law — `molaw.gov.pk`
- **Type:** Federal legislation
- **Status:** ❌ TIMES OUT from UK
- **Potential:** Official legislation texts

---

## 📋 FUTURE TARGETS (not yet attempted)

| Source | URL | Type | Notes |
|--------|-----|------|-------|
| Supreme Court | `supremecourt.gov.pk` | Judgments | Already scraped via PLS (3,091 cases) |
| Federal Tax Ombudsman | `fto.gov.pk` | Tax decisions | Potential PTD supplement |
| Competition Commission | `cc.gov.pk` | CCP orders | New reporter potential |
| SECP | `secp.gov.pk` | Corporate decisions | New reporter potential |
| NEPRA | `nepra.org.pk` | Energy decisions | Regulatory |
| OGRA | `ogra.org.pk` | Oil/Gas decisions | Regulatory |
| Punjab Laws | `punjablaws.gov.pk` | Provincial legislation | Provincial acts |
| Sindh Laws | `sindhlaws.gov.pk` | Provincial legislation | Provincial acts |
| KP Laws | `kpk.gov.pk/laws` | Provincial legislation | Provincial acts |
| Balochistan Laws | `balochistan.gov.pk` | Provincial legislation | Provincial acts |

---

## 🔑 Access Notes

- **Pakistan IP needed for:** LHC, BHC, FSC, AJK HC, PakistanCode, NA, Senate, MoLaw
- **No Pakistan IP needed:** PHC ✅, SHC ✅, PLS ✅ (subscription-based, works globally), Wayback Machine ✅
- **Subscription required:** PLS (paid — Abdul's account)
- **One session limit:** PLS — never run two PLS scrapers simultaneously

---

## 📊 Coverage Summary

| Source | Cases/Files | Status |
|--------|-------------|--------|
| PLS (15 reporters) | 178,989 | ✅ Complete |
| PLS (legislation) | 10,733 | ✅ Complete (4,732 stubs) |
| PHC | 5,585 | ✅ Complete |
| SHC | 3,587 | ✅ Complete |
| IHC | 1,374 | ✅ Complete |
| SC (via PLS) | 3,091 | ✅ Complete |
| FSC | 712 real / 11,483 stubs | ⚠️ Partial |
| LHC | 50 stubs, 0 PDFs | ❌ Blocked |
| BHC | 0 | ❌ Blocked |
| AJK HC | 0 | ❌ Blocked |
| **TOTAL** | **~216,000+** | |
