# CLAUDE.md — India Securities Filing Scraper

## Mission

Reverse-engineer and scrape India's securities filing systems. Extract filings (annual reports, prospectuses, financial statements) from Indian regulatory portals into structured JSON + downloaded documents.

Part of a multi-country financial filings scraper project. Sibling scrapers: Canada (SEDAR+, task2/), Mexico (CNBV STIV-2, task1/).

## Current State (2026-04-13)

**Status: PRODUCTION-READY** — All three sources (BSE, NSE, SEBI) are crawling, caching, and downloading. Code review completed and all HIGH/MEDIUM issues resolved.

**Repo:** https://github.com/itisaevalex/IndiaFilingScraper

### What Works

```bash
# BSE
python scraper.py crawl --source bse --max-pages 10 --download
python scraper.py crawl --source bse --from-date 2026-04-01 --to-date 2026-04-13

# NSE (announcements, board meetings, financial results, annual reports)
python scraper.py crawl --source nse --max-pages 5 --download
python scraper.py crawl --source nse --nse-type board_meetings financial_results
python scraper.py crawl --source nse --nse-type all

# SEBI (11 categories: public issues, takeovers, buybacks, mutual funds, etc.)
python scraper.py crawl --source sebi --sebi-category all
python scraper.py crawl --source sebi --sebi-category public_issues takeovers buybacks

# All sources
python scraper.py crawl --source all --max-pages 5 --download

# Monitor / Export / Stats
python scraper.py monitor --source bse --interval 300 --download
python scraper.py export --output filings.json
python scraper.py stats --source all
```

### Verified Performance (2026-04-13)

| Source | Pages | Filings | Time | Downloads |
|--------|-------|---------|------|-----------|
| BSE | 2 | 100 | 2.6s | — |
| BSE (date filtered) | 2 | 100 | 8.0s | — |
| NSE announcements | 1 (7 days) | 4,116 | 3.9s | — |
| NSE board meetings | 1 | 86 | 0.8s | — |
| NSE financial results | 1 (3 months) | 15 | 0.4s | — |
| SEBI (all 11 categories) | 1/each | 252 | 5.7s | — |
| BSE + download | 1 | 50 | 7.9s | 49 PDFs |

---

## Reasoning Trace — How We Got Here

### Decision 1: Parallel Reconnaissance (not sequential)

**Choice:** Launch 5 agents simultaneously — BSE recon, NSE recon, CFDS/SEBI recon, GitHub research, sibling project review.

**Why:** The CLAUDE.md methodology says "DO THIS FIRST" for recon. Rather than sequentially probing each portal, we parallelized all reconnaissance. This completed in ~5 minutes instead of ~25 minutes sequential.

**Result:** All 5 agents returned within 5 minutes. We had the complete picture before writing a single line of scraper code.

### Decision 2: NSE is Easier Than Expected — Don't Deprioritize It

**Initial assumption:** CLAUDE.md warned "NSE is known to have aggressive bot protection — it may require curl_cffi or browser cookies. BSE tends to be more accessible. Start with BSE."

**What recon found:** NSE has Akamai WAF with `_abck` bot manager cookies, BUT the `/api/` endpoints are freely accessible with just a browser User-Agent. No cookies needed. No TLS fingerprinting. 150+ API endpoints returning clean JSON.

**Decision:** Include NSE as a co-equal source, not deprioritize it. NSE actually has the richest data (7,000+ filings per 2-week window vs BSE's 50 per page).

### Decision 3: Plain `requests` for Everything

**Choice:** Use `requests` for all three portals. No `curl_cffi`, no Playwright, no headless browser.

**Why:** Recon showed:
- BSE: REST JSON API, needs Referer + Origin + User-Agent (no TLS fingerprinting)
- NSE: REST JSON API, needs browser User-Agent only (Akamai not enforcing on /api/)
- SEBI: Struts AJAX POST, needs User-Agent + Referer + Origin

None of these require TLS impersonation (unlike SEDAR+ which needed `curl_cffi` to bypass Radware JA3/JA4 fingerprinting).

**Trade-off:** If Akamai tightens API protection later, we may need to add `curl_cffi` fallback. But for now, `requests` keeps the dependency footprint minimal (3 deps vs 5+ for SEDAR+).

### Decision 4: Multi-Source Architecture (BSE + NSE + SEBI in One File)

**Choice:** Single `scraper.py` with `--source` flag, not separate scripts per portal.

**Why:**
- Unified SQLite schema with `source` column enables cross-source dedup and export
- Single CLI entry point matches sibling project pattern
- Shared download manager, cache, and CLI code (DRY)
- File is ~950 lines (slightly over 800 guideline but justified by 3 sources vs 1)

**Alternative rejected:** Separate `bse_scraper.py`, `nse_scraper.py`, `sebi_scraper.py` — would duplicate 40% of code (cache, downloads, CLI).

### Decision 5: BSE Pagination is Stateless — No Sequential Requirement

**Key finding:** BSE API returns `Table1[0].ROWCNT` with total count and allows jumping to any page directly via `pageno` param. This is fundamentally different from SEDAR+ (Oracle Catalyst) where pagination was sequential-only and state-dependent.

**Impact:** No need for the download-before-paginate pattern from SEDAR+. BSE URLs are permanent and don't invalidate when you navigate. Cross-page parallelism would work here (though not implemented yet — not needed at current scale).

### Decision 6: NSE Date-Range Pagination

**Problem:** NSE API has no `page` parameter. It returns all results for a given date range in one response.

**Solution:** Implemented sliding window pagination — walk backwards in 7-day chunks. This naturally handles the fact that some days have hundreds of filings (trading days) and some have zero (weekends).

**Trade-off:** 7-day window means each "page" can be 500-5000 filings. This is fine for crawling but means memory usage spikes. If this becomes an issue, shrink to 1-day windows.

### Decision 7: SEBI HTML-to-PDF Resolution

**Problem:** SEBI filing links point to `.html` pages, not direct PDFs. The actual PDF is embedded in an `<iframe>` with a `file=` query parameter pointing to `sebi_data/attachdocs/`.

**Solution:** Added `resolve_sebi_pdf()` function that follows the HTML page, finds the iframe, and extracts the actual PDF URL. This adds one extra HTTP request per SEBI download but gives us the actual document.

**Also discovered:** SEBI listing pages contain companion document links (abridged prospectuses, etc.) as direct PDF URLs alongside the main filing HTML link. The parser now captures both.

### Decision 8: User-Agent Required Everywhere

**Learned by trial and error:**
- First attempt: BSE API returned 403 (missing User-Agent, only had Referer+Origin)
- Fix: Added User-Agent to BSE headers → API returned 200 with JSON
- Second issue: Document downloads returned 403 (download function only sent UA for NSE)
- Fix: All downloads now send User-Agent regardless of source
- Third issue: SEBI returned 530 BLOCKED (recon agent said "NONE" protection but tested differently)
- Fix: Added User-Agent + Referer + Origin to SEBI headers

**Lesson for future portals:** Always send a browser User-Agent. It costs nothing and prevents the most common 403/530 blocks. The recon agents testing via WebFetch may have different header behavior than the Python `requests` library.

### Decision 9: Code Review Hardening (Production-Ready)

Code review found 5 HIGH, 7 MEDIUM, 6 LOW issues. All HIGH and MEDIUM fixed:

**Fixed (HIGH):**
- `try/finally` on all `FilingCache.close()` calls — prevents SQLite WAL corruption on exceptions
- Per-page error handling in all `_crawl_*` functions — transient 503s skip the page, don't kill the run
- Download errors logged at WARNING (not DEBUG) with network vs I/O distinction
- SEBI `has_more` now parses `totalpage` hidden input from response (not heuristic count)
- Thread-safety invariant documented in download manager

**Fixed (MEDIUM):**
- Path traversal via `content-disposition` — added `os.path.basename()` defense
- Proper `urllib3.Retry` with backoff and `status_forcelist=[429,500,502,503,504]`
- BSE `_build_bse_doc_url` returns empty string on unparseable dates (not wrong URL)
- SEBI category names precomputed at module load (not linear scan per call)
- None-safe field extraction for BSE API (fields like `SUBCATNAME` can be JSON null)
- `.gitignore` now excludes `venv/`, `.venv/`, `*.env`
- `resolve_sebi_pdf` logs warning on failure (not silent swallow)

**E2E verified after all fixes:** BSE 50 filings, NSE 4,103 filings, SEBI 35 filings, export 4,188 filings, downloads 3/3 PDFs.

---

## Portal Details (Verified)

### BSE (Bombay Stock Exchange)

- **Tech Stack:** AngularJS 1.x SPA + ASP.NET Web API (REST, NOT WebForms)
- **API Base:** `https://api.bseindia.com/BseIndiaAPI/api/`
- **Key Endpoint:** `GET /AnnSubCategoryGetData/w` — paginated announcements
- **Required Headers:** `Referer: https://www.bseindia.com/`, `Origin: https://www.bseindia.com`, `User-Agent` (browser-like)
- **Pagination:** Stateless, 50/page, `pageno` param (1-indexed), `Table1[0].ROWCNT` for total
- **Bot Protection:** Akamai CDN, no TLS fingerprinting, no cookies needed
- **Documents:** Direct URL from `ATTACHMENTNAME` field, routed by `PDFFLAG`:
  - 0 → `bseindia.com/xml-data/corpfiling/AttachLive/{name}`
  - 1 → `bseindia.com/xml-data/corpfiling/AttachHis/{name}`
  - 2 → `bseindia.com/xml-data/corpfiling/CorpAttachment/{year}/{month}/{name}`
- **robots.txt:** 404 (none exists)
- **listing.bseindia.com:** Unreachable (geo-restricted or decommissioned, not needed)

### NSE (National Stock Exchange)

- **Tech Stack:** Node.js/Express + jQuery frontend
- **API Base:** `https://www.nseindia.com/api/`
- **Key Endpoint:** `GET /corporate-announcements?index=equities`
- **Required Headers:** `User-Agent` (browser-like) — no cookies, no Referer needed
- **Pagination:** Date-range via `from_date`/`to_date` (DD-MM-YYYY format)
- **Bot Protection:** Akamai WAF with `_abck`/`bm_sz` cookies, but NOT enforcing on `/api/` paths
- **Documents:** Direct URLs in `attchmntFile` field (can be "-" for no attachment), hosted at `nsearchives.nseindia.com`
- **File Types:** Mostly PDF, some ZIP files (~1% of filings)
- **150+ endpoints** including annual-reports, board-meetings, financial-results, share-holdings, corporate-actions, voting-results, investor-complaints
- **Response format:** Flat JSON array (no envelope)
- **robots.txt:** Permissive (only blocks `/market-data-test`)

### SEBI (Securities and Exchange Board of India)

- **Tech Stack:** Apache Struts + JSP AJAX
- **Endpoint:** `POST https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp`
- **Required Headers:** `User-Agent`, `Referer: https://www.sebi.gov.in/filings.html`, `Origin: https://www.sebi.gov.in`
- **Without headers:** Returns `530 BLOCKED`
- **Pagination:** Page-based (25/page, 0-indexed via `doDirect` param)
- **Response format:** HTML split by `#@#` delimiter (content | breadcrumb)
- **Documents:** Main filings are `.html` pages with embedded PDF via `<iframe src="...?file=URL.pdf">`. Companion docs (abridged prospectuses) are direct PDF links in the listing.
- **Categories (ssid values):** Public Issues (15), Rights Issues (16), Debt Offers (17), Takeovers (20), Buybacks (22), Mutual Funds (39), InvIT Public (55), InvIT Private (73), InvIT Rights (89), REIT (74), SM REIT (98)
- **Coverage:** IPOs, takeovers, buybacks, mutual funds, InvIT/REIT — NOT annual reports or quarterly financials (those are on BSE/NSE)
- **Data volume:** 5,605 public issue records alone (225 pages)
- **robots.txt:** Permissive (only blocks /js and /css)

### Not Viable

- **CFDS (corpfiling.co.in):** DNS resolves to 43.228.176.33 but all ports filtered. Likely geo-restricted to Indian IPs. Zero prior art on GitHub.
- **SEBI SI Portal (siportal.sebi.gov.in):** Auth-only portal for SEBI-registered intermediaries. IBM Tivoli backend. Returns 505 BLOCKED.
- **BSE Listing Centre (listing.bseindia.com):** DNS resolves to 43.228.176.106 but 100% packet loss. Geo-restricted or decommissioned.

---

## Prior Art Found (GitHub Research)

| Repository | Stars | What It Does |
|-----------|-------|-------------|
| `BennyThadikaran/BseIndiaApi` | 64 | Complete BSE Python wrapper (PyPI: `bse`). 8 RPS rate limit. |
| `BennyThadikaran/NseIndiaApi` | 128 | Complete NSE Python wrapper (PyPI: `nse`). Cookie-based session management. 3 RPS rate limit. |
| `SurbhiSinghania13/nse-annual-reports-scraper` | — | Selenium-based annual report downloader. Says JS rendering is "non-negotiable" for annual reports specifically. |
| `aeron7/nsepython` | 347 | Popular NSE wrapper but focused on market/options data, no corporate filings. |

**Decision:** Built our own scraper rather than wrapping `bse`/`nse` PyPI packages because:
1. We need unified multi-source architecture
2. We need specific filing-focused endpoints, not market data
3. We need SQLite caching and download tracking (not in those libraries)
4. The packages add cookie/session management complexity that we discovered isn't needed for API access

---

## Methodology — Lessons from Previous Scrapers

The following methodology was developed across two successful scraper projects (Canada SEDAR+ and Mexico CNBV). Follow this playbook — it saves days of wasted effort.

### Phase 1: Reconnaissance (DO THIS FIRST)

1. **Identify the tech stack** — what backend (ASP.NET? Java? Oracle? React SPA?) and what WAF/bot protection (Cloudflare, Radware, Akamai, Azure WAF, etc.)
2. **Check response headers** — `Server`, `X-Powered-By`, cookie names reveal the stack
3. **Inspect the page source** — look for framework-specific patterns:
   - ASP.NET: `__VIEWSTATE`, `__EVENTVALIDATION`, `ScriptManager`
   - Oracle Catalyst: `viewInstanceKey`, `_CBNAME_`, `_VIKEY_`
   - DevExpress: `dxgvDataRow`, `ASPxClientCallbackPanel`, `WebForm_DoCallback`
   - React/Vue SPA: API calls in Network tab, no server-rendered HTML
4. **Test plain curl** — does it work? Does it redirect to a captcha/challenge page?
5. **Check robots.txt** — what's disallowed?

**India finding:** NSE was expected to be hardest (Akamai WAF) but API endpoints are freely accessible. Always test the actual API paths, not just the homepage.

### Phase 2: GitHub Research (BEFORE writing any code)

1. Run `gh search repos` and `gh search code` for the target site
2. Look for existing scrapers, partial reverse-engineering, API documentation
3. Check if anyone has documented the protocol or found bypasses
4. **This saved days on all three projects** — prior art existed for BSE, NSE, SEDAR+, and CNBV

### Phase 3: HTTP Library Selection

| Library | When it works | When it fails |
|---------|--------------|---------------|
| `requests` | No TLS fingerprinting (Mexico CNBV, India BSE/NSE/SEBI) | Radware, some Cloudflare |
| `curl_cffi` | TLS fingerprint-sensitive WAFs (Canada SEDAR+) | Some sites reject impersonation |
| `httpx` | HTTP/2 required sites | Same TLS issues as requests |

**India:** Plain `requests` works for all three portals. This is the simplest of the three country scrapers.

### Phase 4: Use Playwright as a DEBUGGING TOOL, not the scraper

Not needed for India. All three portals have clean APIs accessible via raw HTTP.

### Phase 5: Understand the State Machine

**India has no state machines.** This was the biggest simplification:
- BSE: Stateless REST API (any page directly accessible)
- NSE: Stateless REST API (date-range queries, no server state)
- SEBI: Minimal state (page index in POST body, no ViewState/tokens)

Compare: SEDAR+ had Oracle Catalyst sequential pagination, CNBV had ASP.NET ViewState.

### Phase 6: Download Pattern

**India doesn't need download-before-paginate.** BSE document URLs are permanent (UUID-based). NSE document URLs are permanent (archive paths). No URL invalidation on navigation.

**SEBI needs HTML-to-PDF resolution.** Main filing links are `.html` pages with embedded PDFs in iframes. The `resolve_sebi_pdf()` function follows the HTML page to extract the actual PDF URL.

### Phase 7: Rate Limiting & Bot Protection

| Portal | Protection | Required Headers |
|--------|-----------|-----------------|
| BSE API | Akamai (not enforcing TLS) | `Referer` + `Origin` + `User-Agent` |
| BSE Docs | Akamai | `User-Agent` |
| NSE API | Akamai (not enforcing on /api/) | `User-Agent` |
| SEBI | Unknown WAF | `User-Agent` + `Referer` + `Origin` |

**Key lesson from India:** Always send a browser User-Agent as a baseline. It costs nothing and prevents the most common blocks.

### Phase 8: Production Architecture

```
Plain requests (no browser, no curl_cffi)
  → BSE: REST JSON API (stateless pagination, 50/page)
  → NSE: REST JSON API (date-range pagination)
  → SEBI: Struts AJAX POST (page-based, 25/page)
    → SQLite cache (dedup via UNIQUE(source, filing_id))
      → Parallel document downloads (ThreadPoolExecutor)
```

**No browser in the loop.** No session init. No cookie management. This is as simple as a scraper gets.

## Output Format

```
scraper.py              # Main scraper (crawl, monitor, export, stats)
requirements.txt        # Python dependencies (requests, beautifulsoup4, lxml)
.gitignore              # Excludes generated files
filings_cache.db        # SQLite cache (auto-generated)
documents/              # Downloaded files (auto-generated)
filings.json            # Exported filings (via export command)
_investigation/         # Reverse-engineering artifacts
README.md               # Documentation with full RE journey
CLAUDE.md               # This file — methodology + reasoning trace
```

## Future Work

- ~~Expand NSE to use additional endpoints~~ **DONE** — board_meetings, financial_results, annual_reports (annual_reports requires `symbol` param)
- ~~Add SEBI multi-category crawling~~ **DONE** — `--sebi-category all` crawls all 11 categories
- ~~Add date-range filtering for BSE~~ **DONE** — `--from-date`/`--to-date` in YYYY-MM-DD format
- NSE annual reports requires a symbol list — add `--symbol` param or auto-fetch from NSE index endpoint
- Consider `curl_cffi` fallback if Akamai tightens API protection
- CFDS integration if accessible from Indian IPs (needs VPN testing)
- XBRL filing support via BSE's `/XbrlAnnouncementCategory/w` endpoint

## Investigation Artifacts

All reverse-engineering work is saved in `_investigation/`:
- `recon_bse.md` — Full BSE reconnaissance report
- `recon_cfds_sebi.md` — CFDS and SEBI reconnaissance report
- `github_research.md` — GitHub prior art survey
- `nse_api_sample_response.json` — Live NSE API response sample
- `nse_api_headers.txt`, `nse_filings_headers.txt`, `nse_homepage_headers.txt` — Raw header captures
- `nse_homepage_block_page.html` — Akamai 403 block page HTML
- `corporate-filings.js` — NSE's frontend JS with all 150+ API endpoints
