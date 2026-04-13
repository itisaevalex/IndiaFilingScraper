# Reconnaissance: BSE India (Bombay Stock Exchange)
Date: 2026-04-13

## 1. Target URLs Investigated

### A. Corporate Announcements Page
**URL:** https://www.bseindia.com/corporates/ann.html
**Status:** FULLY ACCESSIBLE via plain curl/requests

### B. Listing Centre
**URL:** https://listing.bseindia.com
**Status:** UNREACHABLE - Connection timeout
- DNS resolves to `listing.gslb.bseindia.com` -> `43.228.176.106`
- Ping: 100% packet loss
- curl: Connection timeout on both HTTP/HTTPS
- Assessment: May be geo-restricted (India-only) or decommissioned. The corporate announcements are served from the main site anyway.

### C. robots.txt
**URL:** https://www.bseindia.com/robots.txt
**Status:** 404 Not Found
- BSE does not have a robots.txt file
- No explicit crawling restrictions

---

## 2. Response Headers Analysis

### www.bseindia.com (Corporate Announcements Page)

```
HTTP/2 200
content-type: text/html
etag: "6ee98d7bd84fdc1:0"
strict-transport-security: max-age=31536000; includeSubDomains; preload
x-frame-options: SAMEORIGIN
x-xss-protection: 1; mode=block
x-content-type-options: nosniff
access-control-allow-origin: https://www.bseindia.com,https://api.bseindia.com
access-control-allow-methods: GET,POST
akamai-grn: 0.1a3e1202.1776083783.15e5271a
```

**Key observations:**
- **CDN:** Akamai (confirmed by `akamai-grn` header)
- **Backend:** IIS (etag format `"hex:0"` is IIS-specific) on ASP.NET/Windows
- **No Server header exposed** (stripped by Akamai)
- **No X-Powered-By header** (stripped or not set)
- **CORS:** Only allows `https://www.bseindia.com` and `https://api.bseindia.com`
- **CSP:** Very detailed Content-Security-Policy header, allows `*.bseindia.com`
- **No cookies set** on the HTML page itself
- **No WAF challenge pages** - plain curl gets the full HTML

### api.bseindia.com (API Endpoint)

```
HTTP/2 200
content-type: application/json; charset=utf-8
strict-transport-security: max-age=31536000; includeSubDomains; preload
x-frame-options: SAMEORIGIN
access-control-allow-methods: GET,POST
access-control-allow-origin: https://www.bseindia.com
cache-control: max-age=60
akamai-grn: 0.103e1202.1776083857.16b17d35
```

**Key observations:**
- **JSON API** - returns `application/json` directly
- **Akamai CDN** (same as main site)
- **CORS enforcement:** `access-control-allow-origin: https://www.bseindia.com` ONLY
- **Cache:** 60 seconds (max-age=60)
- **CRITICAL: Referer/Origin required** - Without proper `Referer: https://www.bseindia.com` header, API returns `301 redirect` to `https://www.bseindia.com/members/showinterest.aspx`

---

## 3. Tech Stack Identification

### Frontend
- **Framework:** AngularJS 1.x (confirmed by `ng-app="corpann"`, `ng-controller="corpannController"`)
- **UI:** Bootstrap (CSS), jQuery + jQuery UI
- **Angular modules:** `ui.bootstrap`, `ngSanitize`, custom `smartsearch`
- **Streaming:** Socket.io connection to `bnotification.bseindia.com`

### Backend
- **Web Server:** IIS (behind Akamai CDN)
- **API:** ASP.NET Web API (confirmed by URL patterns `/api/...` and IIS etag format)
- **Pattern:** RESTful JSON API - no `__VIEWSTATE`, no `ScriptManager`, no server-side state machine
- **This is NOT ASP.NET WebForms** -- it's a clean SPA + REST API architecture

### CDN/Protection
- **CDN:** Akamai
- **WAF:** Minimal - no JavaScript challenges, no CAPTCHA, no TLS fingerprinting
- **Bot protection:** Only Referer/Origin header validation on the API
- **No cookie requirements** for API access (just headers)

---

## 4. API Architecture (FULLY REVERSE-ENGINEERED)

### API Base URLs (from AppnewController.js)

```javascript
mainapi = {
    'domain': 'https://www.bseindia.com/',
    'api_domain': 'https://api.bseindia.com/bseindia/api/',
    'newapi_domain': 'https://api.bseindia.com/BseIndiaAPI/api/',
    'api_domainRealTime': 'https://api.bseindia.com/RealTimeBseIndiaAPI/api/',
    'api_domainSearch': 'https://api.bseindia.com/'
}
```

### Primary API: Corporate Announcements

**Endpoint:** `GET https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w`

**Parameters:**
| Param | Type | Description | Example |
|-------|------|-------------|---------|
| strScrip | string | BSE scrip code (empty for all) | `500325` |
| strCat | string | Category filter | `Result`, `Board Meeting`, `-1` (all) |
| strPrevDate | string | From date (YYYYMMDD) | `20260101` |
| strToDate | string | To date (YYYYMMDD) | `20260413` |
| strSearch | string | Search type: P=period, A=all | `P` |
| strType | string | Segment: C=Equity, D=Debt, M=MF | `C` |
| pageno | int | Page number (1-indexed) | `1` |
| subcategory | string | Sub-category filter | `Financial Results`, `-1` (all) |

**Required headers:**
```
Referer: https://www.bseindia.com/corporates/ann.html
Origin: https://www.bseindia.com
```

**Response structure:**
```json
{
  "Table": [
    {
      "NEWSID": "uuid",
      "SCRIP_CD": 500325,
      "NEWSSUB": "Subject line",
      "HEADLINE": "Brief text",
      "MORE": "Extended text (HTML)",
      "NEWS_DT": "2026-01-16T19:07:13.5",
      "DissemDT": "2026-01-16T19:07:13.5",
      "News_submission_dt": "2026-01-16T19:07:13",
      "CATEGORYNAME": "Result",
      "SUBCATNAME": "Financial Results",
      "SLONGNAME": "Reliance Industries Ltd",
      "ATTACHMENTNAME": "uuid.pdf",
      "PDFFLAG": 0|1|2,
      "Fld_Attachsize": 4775345,
      "NSURL": "https://www.bseindia.com/stock-share-price/...",
      "TotalPageCnt": 2,
      "FILESTATUS": "N    " | "X",
      "XML_NAME": "...",
      "AUDIO_VIDEO_FILE": null,
      "RN": 1,
      "OLD": 1
    }
  ],
  "Table1": [{ "ROWCNT": 66 }]
}
```

**Pagination:**
- 50 records per page
- `Table1[0].ROWCNT` = total records
- `TotalPageCnt` = total pages (math.ceil(ROWCNT / 50))
- Pages are 1-indexed
- **Stateless** - any page can be fetched directly (no sequential requirement!)

### Document Download URLs

Three patterns based on `PDFFLAG`:
1. **PDFFLAG=0 (Live):** `https://www.bseindia.com/xml-data/corpfiling/AttachLive/{ATTACHMENTNAME}`
2. **PDFFLAG=1 (Historical):** `https://www.bseindia.com/xml-data/corpfiling/AttachHis/{ATTACHMENTNAME}`
3. **PDFFLAG=2 (Date-based):** `https://www.bseindia.com/xml-data/corpfiling/CorpAttachment/{YEAR}/{MONTH}/{ATTACHMENTNAME}`
   - Year/Month extracted from NEWS_DT

**Download headers:**
- No Referer required for document downloads
- Documents served directly with proper Content-Type
- No cookie or session needed

### XBRL Announcements

**Endpoint:** `GET https://api.bseindia.com/BseIndiaAPI/api/XbrlAnnouncementCategory/w`
Same parameters as AnnSubCategoryGetData/w.

XBRL filings have `FILESTATUS="X"` and `XML_NAME` points to an HTML viewer:
- `https://www.bseindia.com/XBRLFILES/{type}DuplicateUploadDocument/{file}.html`

### Sub-Category Lookup

**Endpoint:** `GET https://api.bseindia.com/BseIndiaAPI/api/DDLSubCategoryData/w`
**Param:** `categoryname={category}`

### Company Search

**Endpoint:** `GET https://api.bseindia.com/BseIndiaAPI/api/PeerSmartSearch/w`
**Params:** `text={query}&Type=SS`
Returns HTML `<li>` elements with scrip codes and ISIN numbers.

---

## 5. Categories and Sub-Categories (Complete)

### AGM/EGM
- AGM, Book Closure / AGM, Court Convened Meeting, EGM, Postal Ballot

### Board Meeting
- Board Meeting, Committee Meeting, Outcome of Board Meeting

### Company Update
- 140+ subcategories including: Acquisition, Agreement, Allotment of Equity Shares, Annual Disclosure, Buy back, Change in Directors, Credit Rating, Delisting, Financial Results (under Result), Insider Trading, Merger, Press Release, Reg. 30 - Awarding of orders/contracts, Restructuring, Scheme of Arrangement, Winding-up, and many more

### Corp. Action
- Amalgamation/Merger/Demerger, Bonds/Right issue, Bonus, Book Closure, Capital Reduction, Consolidation of Shares, Dividend, Record Date, Sub-division/Stock Split

### Insider Trading / SAST
- 20+ subcategories covering various SEBI regulation disclosures

### New Listing
- New Listing

### Result
- Auditors Report, Change in Accounting Year, Financial Results, Limited Review Report

### Integrated Filing
- (subcategories not retrieved - likely new category)

### Others
- (general catch-all)

---

## 6. Rate Limiting & Bot Protection Assessment

### Rate Limiting
- **No explicit rate limit headers** (no `X-RateLimit-*`, no `Retry-After`)
- **60-second cache** on API responses (may serve stale data if hitting same URL rapidly)
- **Occasional 301 redirects** when rapid-firing (Akamai may have implicit rate limiting)
- **Recommended:** 5-8 requests per second with brief pauses between pages
- **Existing library (BseIndiaApi)** uses 8 RPS with mthrottle library

### Bot Protection
- **Level: LOW**
- No TLS fingerprinting (curl works fine, no curl_cffi needed)
- No JavaScript challenges
- No CAPTCHA
- No cookie requirements for API access
- **Only requirement:** Referer/Origin headers must be set

### Header Requirements
Minimum viable headers for API access:
```python
headers = {
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com"
}
```
User-Agent is NOT required (works with curl default UA).

---

## 7. Verified Working Examples

### Get today's equity announcements (page 1)
```bash
curl -H "Referer: https://www.bseindia.com/" \
     -H "Origin: https://www.bseindia.com" \
     "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strCat=-1&strPrevDate=20260413&strToDate=20260413&strSearch=P&strType=C&pageno=1&subcategory=-1&strScrip="
```

### Get Reliance Industries financial results (all time)
```bash
curl -H "Referer: https://www.bseindia.com/" \
     -H "Origin: https://www.bseindia.com" \
     "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strScrip=500325&strCat=Result&strPrevDate=&strToDate=&strSearch=A&strType=C&pageno=1&subcategory=Financial%20Results"
```

### Download a filing PDF
```bash
curl -o filing.pdf "https://www.bseindia.com/xml-data/corpfiling/AttachHis/38a2f910-438f-4fc0-8abc-c6cd5933b5ac.pdf"
```

### Search for companies
```bash
curl -H "Referer: https://www.bseindia.com/" \
     -H "Origin: https://www.bseindia.com" \
     "https://api.bseindia.com/BseIndiaAPI/api/PeerSmartSearch/w?text=Reliance&Type=SS"
```

---

## 8. Key Findings for Scraper Implementation

### Architecture: IDEAL for scraping
1. **Stateless JSON API** - No server-side state machine (unlike SEDAR+ Oracle Catalyst)
2. **No ViewState** - Not ASP.NET WebForms (unlike CNBV)
3. **Random page access** - Can jump to any page directly (no sequential pagination)
4. **Direct document URLs** - No encrypted tokens, no session-dependent downloads
5. **No browser needed** - Plain `requests` library sufficient

### Compared to SEDAR+ and CNBV
| Feature | BSE India | SEDAR+ (Canada) | CNBV (Mexico) |
|---------|-----------|-----------------|---------------|
| API type | REST JSON | Oracle Catalyst state machine | ASP.NET WebForms |
| HTTP library | requests | curl_cffi (TLS fingerprint) | requests |
| State management | Stateless | Sequential state | ViewState |
| Page access | Random | Sequential only | Random |
| Bot protection | Referer header only | TLS + cookies + WAF | None |
| Document URLs | Static/deterministic | Session-dependent | Encrypted tokens |
| Difficulty | EASY | HARD | MEDIUM |

### Scraper architecture recommendation
```
requests session with Referer header
  -> Paginated API calls (50 per page)
    -> Parse JSON response
      -> Download PDFs (parallel, no session needed)
        -> Cache to SQLite
```

No browser, no curl_cffi, no cookie management, no state tracking needed.
This is the easiest of the three country scrapers to implement.
