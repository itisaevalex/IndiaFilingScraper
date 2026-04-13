<div align="center">

```
 ██╗███╗   ██╗██████╗ ██╗ █████╗
 ██║████╗  ██║██╔══██╗██║██╔══██╗
 ██║██╔██╗ ██║██║  ██║██║███████║
 ██║██║╚██╗██║██║  ██║██║██╔══██║
 ██║██║ ╚████║██████╔╝██║██║  ██║
 ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═╝╚═╝  ╚═╝
  ███████╗██╗██╗     ██╗███╗   ██╗ ██████╗ ███████╗
  ██╔════╝██║██║     ██║████╗  ██║██╔════╝ ██╔════╝
  █████╗  ██║██║     ██║██╔██╗ ██║██║  ███╗███████╗
  ██╔══╝  ██║██║     ██║██║╚██╗██║██║   ██║╚════██║
  ██║     ██║███████╗██║██║ ╚████║╚██████╔╝███████║
  ╚═╝     ╚═╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝╚══════╝
   ███████╗ ██████╗██████╗  █████╗ ██████╗ ███████╗██████╗
   ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
   ███████╗██║     ██████╔╝███████║██████╔╝█████╗  ██████╔╝
   ╚════██║██║     ██╔══██╗██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗
   ███████║╚██████╗██║  ██║██║  ██║██║     ███████╗██║  ██║
   ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝
```

**India's securities filing systems — reverse-engineered from scratch.**

*BSE + NSE + SEBI. 7,000+ filings in 6 seconds. Three exchanges, one scraper. Pure HTTP.*

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</div>

---

Multi-source scraper for Indian financial filings. Extracts corporate announcements, financial statements, IPO prospectuses, and regulatory filings from BSE, NSE, and SEBI into structured JSON + downloaded documents.

Part of a multi-country financial filings scraper project (siblings: Canada SEDAR+, Mexico CNBV).

## Quick Start

```bash
pip install -r requirements.txt

# Crawl BSE announcements (default, easiest)
python scraper.py crawl --source bse --max-pages 10 --download

# BSE with date filter
python scraper.py crawl --source bse --max-pages 5 --from-date 2026-04-01 --to-date 2026-04-13

# Crawl NSE announcements (richer data, 150+ endpoints)
python scraper.py crawl --source nse --max-pages 5 --download

# NSE with multiple data types
python scraper.py crawl --source nse --nse-type announcements board_meetings financial_results
python scraper.py crawl --source nse --nse-type all  # all endpoint types

# Crawl SEBI filings (IPOs, takeovers, buybacks)
python scraper.py crawl --source sebi --max-pages 5 --sebi-category public_issues

# SEBI with multiple categories
python scraper.py crawl --source sebi --sebi-category public_issues takeovers buybacks
python scraper.py crawl --source sebi --sebi-category all  # all 11 categories

# Crawl all sources
python scraper.py crawl --source all --max-pages 5 --download

# Monitor for new filings
python scraper.py monitor --source bse --interval 300 --download

# Export to JSON
python scraper.py export --output filings.json

# Show statistics
python scraper.py stats --source all
```

## Architecture

```
Plain requests (no browser, no curl_cffi)
  → BSE: REST JSON API (stateless pagination, 50/page)
  → NSE: REST JSON API (date-range pagination)
  → SEBI: Struts AJAX POST (page-based, 25/page)
    → SQLite cache (dedup + download tracking)
      → Parallel document downloads (ThreadPoolExecutor)
```

This is the **simplest** of the three country scrapers:
- No TLS fingerprinting bypass needed (unlike Canada/SEDAR+)
- No ViewState/state machine (unlike Mexico/CNBV)
- No headless browser fallback needed
- All three portals work with plain `requests` + correct headers

## Data Sources

### BSE (Bombay Stock Exchange) — Primary

| Property | Value |
|----------|-------|
| API | `api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w` |
| Format | JSON (REST) |
| Auth | `Referer` + `Origin` + `User-Agent` headers (no cookies) |
| Pagination | Stateless page-based (50/page, any page directly) |
| Bot Protection | LOW — Akamai CDN, no TLS fingerprinting |
| Coverage | All BSE-listed companies — announcements, results, corporate actions |
| Documents | Direct PDF URLs based on `PDFFLAG` (0=AttachLive, 1=AttachHis, 2=CorpAttachment) |

### NSE (National Stock Exchange) — Secondary

| Property | Value |
|----------|-------|
| API | `nseindia.com/api/corporate-announcements?index=equities` |
| Format | JSON array |
| Auth | Browser `User-Agent` header only |
| Pagination | Date-range based (`from_date`/`to_date`, DD-MM-YYYY) |
| Bot Protection | MEDIUM — Akamai WAF present but not enforcing on `/api/` paths |
| Coverage | All NSE-listed companies — 150+ API endpoints |
| Documents | Direct downloads from `nsearchives.nseindia.com` |

**150+ API endpoints** available including:
- `/api/corporate-announcements` — announcements
- `/api/annual-reports` — annual reports
- `/api/corporate-board-meetings` — board meetings
- `/api/corporate-share-holdings-master` — shareholding patterns
- `/api/corporates-financial-results` — financial results
- `/api/corporates-corporateActions` — corporate actions

### SEBI (Securities and Exchange Board of India) — Tertiary

| Property | Value |
|----------|-------|
| API | `POST sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp` |
| Format | HTML split by `#@#` delimiter |
| Auth | `User-Agent` + `Referer` + `Origin` headers |
| Pagination | Page-based (25/page, 0-indexed) |
| Bot Protection | LOW-MEDIUM — returns 530 BLOCKED without proper headers |
| Coverage | IPOs, takeovers, buybacks, mutual funds, InvIT/REIT |
| Documents | Links to SEBI filing pages (HTML, with embedded PDF links) |

**Filing categories** (via `--sebi-category`):
`public_issues`, `rights_issues`, `debt_offers`, `takeovers`, `buybacks`, `mutual_funds`, `invit_public`, `invit_private`, `invit_rights`, `reit`, `sm_reit`

### Not Targeted

- **CFDS (corpfiling.co.in):** Unreachable — likely geo-restricted to Indian IPs
- **SEBI SI Portal (siportal.sebi.gov.in):** Login-only portal for registered intermediaries
- **BSE Listing Centre (listing.bseindia.com):** Unreachable — decommissioned or geo-restricted

## Reverse-Engineering Journey

### Phase 1: Parallel Reconnaissance

Five parallel agents probed all four portals simultaneously:

1. **BSE recon** — Discovered AngularJS 1.x SPA + ASP.NET Web API (REST, NOT WebForms). No `__VIEWSTATE`, no state machine. Clean JSON API behind Akamai CDN. `Referer` + `Origin` headers mandatory (API returns 301 redirect without them). User-Agent also required (403 without).

2. **NSE recon** — Major surprise: despite Akamai WAF with `_abck` bot manager cookies, the `/api/` endpoints are **freely accessible** with just a browser User-Agent. No cookies, no session init, no TLS fingerprinting bypass needed. Homepage returns 403, but API endpoints return clean JSON.

3. **CFDS/SEBI recon** — CFDS completely unreachable (DNS resolves but all ports filtered). SEBI SI Portal is auth-only. But `sebi.gov.in/filings.html` is fully accessible — Apache Struts backend with JSP AJAX endpoints. Returns 530 BLOCKED without User-Agent + Referer headers.

4. **GitHub research** — Found `BennyThadikaran/BseIndiaApi` (64 stars, PyPI `bse`) and `BennyThadikaran/NseIndiaApi` (128 stars, PyPI `nse`). Confirmed BSE=low protection, NSE=medium. Zero prior art for CFDS.

5. **Sibling project review** — Studied Canada (SEDAR+/task2) and Mexico (CNBV/task1) scraper architecture for reusable patterns.

### Phase 2: Key Discoveries

**BSE API reverse-engineering:**
- AngularJS controller at `/D90/Controller/AppnewController.js` revealed all API base URLs
- 4 API bases: primary (`BseIndiaAPI`), legacy (`bseindia`), real-time, search
- Document URL routing via `PDFFLAG` field (0, 1, 2 → different base paths)
- Pagination is stateless — any page directly accessible (unlike SEDAR+ sequential pagination)
- No robots.txt (404) — no explicit crawling restrictions

**NSE's surprising accessibility:**
- Akamai Bot Manager is deployed (`_abck` cookie) but not enforcing on API paths
- Homepage blocked (403) → API endpoints open (200)
- 150+ API endpoints discovered from `corporate-filings.js`
- WebSocket for real-time data at `wss://streamer.nseindia.com/`
- RSS feed at `nsearchives.nseindia.com/content/RSS/Online_announcements.xml`

**SEBI's quirky API:**
- Struts framework with JSP AJAX endpoint
- Response format: HTML table + breadcrumb, split by `#@#` delimiter
- 14 filing categories controlled by `ssid` parameter
- 5,605 public issue records alone (225 pages)
- Dates in "Apr 10, 2026" format (not ISO)

### Phase 3: Header Requirements (Learned by Trial)

| Portal | Required Headers | Without → |
|--------|-----------------|-----------|
| BSE API | `Referer` + `Origin` + `User-Agent` | 301 redirect (no Referer) or 403 (no UA) |
| BSE Docs | `User-Agent` | 403 Forbidden |
| NSE API | `User-Agent` (browser-like) | H2 stream error |
| SEBI | `User-Agent` + `Referer` + `Origin` | 530 BLOCKED |

### Phase 4: What Made This Easy

Compared to siblings:
- **No `curl_cffi`** — plain `requests` works for all three portals (SEDAR+ needed TLS impersonation)
- **No ViewState** — no server-side state machine to manage (CNBV had ASP.NET WebForms)
- **No session init** — no cookie harvesting or Playwright captures needed
- **Stateless pagination** — BSE allows jumping to any page (SEDAR+ was sequential only)
- **Direct document URLs** — no encrypted tokens or session-dependent resource paths

### Phase 5: Production Hardening (Code Review)

Automated code review found and fixed 12 issues:
- **Path traversal defense** — `os.path.basename()` on `content-disposition` filenames
- **Retry with backoff** — proper `urllib3.Retry` with `status_forcelist=[429,500,502,503,504]`
- **try/finally on SQLite** — prevents WAL journal corruption on unhandled exceptions
- **Per-page error handling** — transient 503s skip the page instead of killing the entire crawl
- **Download error visibility** — failures logged at WARNING (were silently swallowed at DEBUG)
- **None-safe field extraction** — BSE API returns JSON nulls for optional fields like `SUBCATNAME`
- **SEBI pagination** — now parses `totalpage` hidden input instead of counting rows heuristically

## Output

```
scraper.py              # Main scraper (all 3 sources)
requirements.txt        # Python dependencies (requests, beautifulsoup4, lxml)
.gitignore              # Excludes generated files
filings_cache.db        # SQLite cache (auto-generated)
documents/              # Downloaded PDFs (auto-generated)
filings.json            # Exported filings (via export command)
_investigation/         # Reverse-engineering artifacts
README.md               # This file
```

### SQLite Schema

```sql
CREATE TABLE filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,        -- 'bse', 'nse', 'sebi'
    filing_id TEXT,              -- unique per source
    company_name TEXT,
    symbol TEXT,                 -- BSE scrip code or NSE ticker
    isin TEXT,
    category TEXT,
    subcategory TEXT,
    subject TEXT,
    description TEXT,
    filing_date TEXT,
    document_url TEXT,
    file_size TEXT,
    has_xbrl INTEGER DEFAULT 0,
    downloaded INTEGER DEFAULT 0,
    local_path TEXT,
    first_seen TEXT,
    page_number INTEGER,
    raw_json TEXT,
    UNIQUE(source, filing_id)
);
```

## Performance

Tested 2026-04-13:

| Source | Pages | Filings | Time | Downloads |
|--------|-------|---------|------|-----------|
| BSE | 2 | 100 | 2.6s | — |
| NSE | 2 (14 days) | 7,037 | 5.7s | — |
| SEBI | 2 | 50 | 3.1s | — |
| BSE + download | 1 | 50 | 7.9s | 49 PDFs |

## Dependencies

- `requests` — HTTP client (no curl_cffi needed)
- `beautifulsoup4` + `lxml` — HTML parsing (SEBI only)

## Future Work

- Expand NSE to use additional endpoints (annual reports, board meetings, financial results)
- Add SEBI multi-category crawling (currently single category per run)
- Add date-range filtering for BSE
- Consider `curl_cffi` fallback if Akamai tightens API protection
- CFDS integration if accessible from Indian IPs
