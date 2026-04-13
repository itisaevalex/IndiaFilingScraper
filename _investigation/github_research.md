# GitHub Research - India Securities Filing Scraper
## Date: 2026-04-13

---

## 1. Key Repositories Found

### Tier 1 - Directly Usable (BSE)

#### BennyThadikaran/BseIndiaApi
- **URL:** https://github.com/BennyThadikaran/BseIndiaApi
- **Stars:** 64 | **Forks:** 14 | **Last updated:** Feb 2026 (v3.2.0)
- **License:** GPL-3.0
- **What it does:** Complete Python wrapper for BSE India's internal JSON APIs
- **Dependencies:** `requests`, Python 3.8+
- **CRITICAL - Has corporate announcements with pagination**
- **Has:** announcements(), actions(), resultCalendar(), listSecurities(), lookup()
- **Rate limiting:** 8 RPS default, 15 RPS for search. Uses `mthrottle` library.
- **PyPI package:** `pip install bse`

#### theofficialvedantjoshi/bsescraper
- **URL:** https://github.com/theofficialvedantjoshi/bsescraper
- **What it does:** Python library for BSE corporate announcements
- **Categories supported:** Board Meeting, Company Update, Corp. Action, AGM/EGM, New Listing, Results, Others
- **PyPI:** `pip install bsescraper`

### Tier 1 - Directly Usable (NSE)

#### BennyThadikaran/NseIndiaApi
- **URL:** https://github.com/BennyThadikaran/NseIndiaApi
- **Stars:** 128 | **Forks:** 32 | **Last updated:** Mar 2026 (v2.1.3)
- **License:** GPL-3.0
- **CRITICAL - Has corporate announcements, annual reports, board meetings, shareholding**
- **Bot protection handling:** Cookie-based session management, Firefox UA spoofing
- **Rate limiting:** 3 RPS hard limit (NSE enforces this)
- **Dependencies:** `requests` (local) or `httpx[http2]` (server)
- **PyPI:** `pip install nse`

#### SurbhiSinghania13/nse-annual-reports-scraper
- **URL:** https://github.com/SurbhiSinghania13/nse-annual-reports-scraper
- **Created:** Aug 2025
- **What it does:** Downloads annual report PDFs from NSE for all listed companies
- **Tech:** Selenium + BeautifulSoup (not pure HTTP)
- **Performance:** 2137 companies in 8-12 hours, single-threaded
- **Gotcha:** NSE requires JavaScript rendering - Selenium dependency is "non-negotiable" per author

### Tier 2 - Reference/Supplementary

#### aeron7/nsepython (347 stars)
- **URL:** https://github.com/aeron7/nsepython
- **Most popular NSE wrapper overall**
- **Focus:** Market data, options, indices
- **No corporate filings functionality documented**

#### sdabhi23/bsedata (115 stars)
- **URL:** https://github.com/sdabhi23/bsedata
- **Scrapes m.bseindia.com (mobile site)**
- **No corporate filings, just live quotes**
- **Author warns:** "Do not use for production"

#### hi-imcodeman/stock-nse-india (252 stars, Node.js)
- **URL:** https://github.com/hi-imcodeman/stock-nse-india
- **Node.js/TypeScript, provides REST/GraphQL server**
- **30+ MCP tools for NSE data**
- **No direct corporate filings endpoints documented**

#### 0xramm/Indian-Stock-Market-API
- **URL:** https://github.com/0xramm/Indian-Stock-Market-API
- **REST API wrapping Yahoo Finance for NSE/BSE data**
- **Python Flask, no API key required**

---

## 2. API Endpoints Discovered

### BSE India API (api.bseindia.com)

**Base URL:** `https://api.bseindia.com/BseIndiaAPI/api`

| Endpoint | Purpose |
|----------|---------|
| `/AnnSubCategoryGetData/w` | Corporate announcements (paginated) |
| `/DefaultData/w` | Corporate actions (dividends, splits, bonus) |
| `/Corpforthresults/w` | Results calendar |
| `/MktRGainerLoserData/w` | Gainers/losers |
| `/MktHighLowData/w` | 52-week highs/lows |
| `/getScripHeaderData/w` | Stock quotes |
| `/HighLow/w` | Weekly high/low |
| `/ListofScripData/w` | List all securities |
| `/PeerSmartSearch/w` | Symbol lookup/search |
| `/BindDDLEQ/w` | Get scrip groups |
| `/StockTrading/w` | Trading statistics |
| `/ComHeadernew/w` | Equity metadata |
| `/StockReachGraph/w` | Price/volume 12-month data |
| `/TabResults_PAR/w` | Financial results snapshot |
| `/IndexArchDailyAll/w` | Historical index data |
| `/ProduceCSVForDate/w` | Historical index CSV |
| `/FillddlIndex/w` | Index names |
| `/Indexarchive_filedownload/w` | Index report metadata |
| `/getQouteSearch.aspx?Type=EQ&text={}&flag=site` | Quote search |

**Document download pattern:**
- Historical: `https://www.bseindia.com/xml-data/corpfiling/AttachHis/{UUID}.pdf`
- Live: `https://www.bseindia.com/xml-data/corpfiling/AttachLive/{UUID}.pdf`

**Required headers:**
```
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)...
Accept: application/json, text/plain, */*
Accept-Language: en-US,en;q=0.5
Origin: https://www.bseindia.com/
Referer: https://www.bseindia.com/
Connection: keep-alive
```

### NSE India API (nseindia.com/api)

**Base URL:** `https://www.nseindia.com/api`
**Archive URL:** `https://nsearchives.nseindia.com`

**Corporate Filings Endpoints:**
| Endpoint | Purpose |
|----------|---------|
| `/corporate-announcements` | Company announcements |
| `/corporates-corporateActions` | Corporate actions |
| `/corporate-board-meetings` | Board meetings |
| `/annual-reports` | Annual report links |
| `/corporate-share-holdings-master` | Shareholding patterns |
| `/corporates-financial-results` | Financial results |
| `/results-comparision?symbol=X` | Results comparison |
| `/circulars` | Exchange circulars |
| `/latest-circular` | Latest circular |
| `/event-calendar` | Corporate events |

**Market Data Endpoints:**
| Endpoint | Purpose |
|----------|---------|
| `/marketStatus` | Market status |
| `/search/autocomplete` | Symbol lookup |
| `/equity-meta-info` | Stock metadata |
| `/quote-equity` | Equity quotes |
| `/block-deal` | Block deals |
| `/ipo-current-issue` | Current IPOs |
| `/all-upcoming-issues?category=ipo` | Upcoming IPOs |
| `/public-past-issues` | Past IPOs |

**Annual report download pattern:**
```
https://nsearchives.nseindia.com/annual_reports/AR_{ID}_{COMPANY}_{FROM}_{TO}_{TIMESTAMP}.pdf
```

**Required headers:**
```
User-Agent: Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/118.0
Accept: */*
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate
Referer: https://www.nseindia.com/get-quotes/equity?symbol=HDFCBANK
```

**Cookie management:** Cookies fetched from https://www.nseindia.com/option-chain, stored as JSON, auto-refreshed on expiry.

---

## 3. API Response Structures

### BSE Announcements Response
```json
{
  "Table": [
    {
      "NEWSID": "unique_id",
      "SCRIP_CD": "500209",
      "NEWSSUB": "Full subject text",
      "HEADLINE": "Brief headline",
      "NEWS_DT": "2024-01-15T10:30:00",
      "ATTACHMENTNAME": "uuid-string.pdf",
      "SLONGNAME": "Company Full Name",
      "SUBCATNAME": "Regulation category",
      "TotalPageCnt": 5,
      "Fld_Attachsize": 524288
    }
  ],
  "Table1": [
    {"ROWCNT": 1292}
  ]
}
```
Note: BSE can return 2000+ announcements/day requiring 50+ paginated requests.

### NSE Announcements Response
```json
{
  "symbol": "IDEA",
  "desc": "Updates",
  "dt": "DDMMYYYYHHmmss",
  "attchmntFile": "https://nsearchives.nseindia.com/...",
  "sm_name": "Vodafone Idea Limited",
  "sm_isin": "INE669E01016",
  "an_dt": "Human-readable date",
  "sort_date": "YYYY-MM-DD HH:mm:ss",
  "seq_id": "unique_sequence_id",
  "smIndustry": "Telecommunication - Services",
  "attchmntText": "Brief summary"
}
```

### NSE Annual Reports Response
```json
{
  "data": [
    {
      "companyName": "HDFC Bank Limited",
      "fromYr": "2023",
      "toYr": "2024",
      "broadcast_dttm": "18-JUL-2024 18:34:53",
      "fileName": "https://nsearchives.nseindia.com/annual_reports/AR_24576_HDFCBANK_2023_2024_18072024183453.pdf"
    }
  ]
}
```

---

## 4. Bot Protection Analysis

### BSE India
- **Protection level:** LOW - No significant bot protection
- **HTTP library needed:** `requests` works fine
- **Session management:** Standard session with browser headers
- **Rate limits:** Internal throttling at 8 RPS
- **Key insight:** BSE APIs return JSON directly, no JavaScript challenge

### NSE India
- **Protection level:** MEDIUM - Cookie-based session validation
- **HTTP library needed:** `requests` (local) or `httpx[http2]` (server)
- **Session management:** Must first visit a page to get cookies, store them, refresh on expiry
- **Rate limits:** Hard 3 RPS limit, recommend extra 0.5-1s sleep between requests
- **Key insight:** No TLS fingerprinting detected (unlike Canada's SEDAR+), but cookies are mandatory
- **Gotcha:** For annual reports specifically, JavaScript rendering may be needed (per nse-annual-reports-scraper author)
- **Recommendation:** Download large reports after market hours (evening)

### corpfiling.co.in (CFDS)
- **No scrapers or APIs found**
- **No community tools exist**
- **Will need manual reconnaissance**

---

## 5. PyPI Packages Available

| Package | Version | What it does |
|---------|---------|-------------|
| `bse` | 3.2.0 | BseIndiaApi wrapper (best for BSE) |
| `nse` | 2.1.3 | NseIndiaApi wrapper (best for NSE) |
| `bsescraper` | 1.0.6 | BSE corporate announcements |
| `bsedata` | 0.x | BSE mobile site scraper (quotes only) |
| `nsetools` | - | NSE market data |
| `nsepython` | - | NSE options/indices data |
| `india-stocks-api` | - | Broker API wrapper |

---

## 6. Key Takeaways for Implementation

### Recommended Strategy: BSE First

1. **BSE is easier** - No significant bot protection, clean JSON APIs, `requests` works out of the box
2. **BSE has the best announcements API** - `/AnnSubCategoryGetData/w` with pagination, date filtering, category filtering
3. **BSE document URLs are deterministic** - `bseindia.com/xml-data/corpfiling/AttachHis/{UUID}.pdf`
4. **Existing `bse` PyPI package** handles session management, rate limiting, and pagination

### NSE as Secondary

1. **NSE needs cookie management** but not TLS impersonation
2. **NSE has a dedicated annual_reports endpoint** - Returns direct PDF download URLs
3. **NSE announcements include attachment URLs** - Direct nsearchives.nseindia.com links
4. **Consider using `nse` PyPI package** or adapting its session/cookie management

### What We Don't Need

1. **No curl_cffi needed** - Unlike SEDAR+, neither BSE nor NSE uses TLS fingerprinting
2. **No Playwright needed for BSE** - Pure JSON APIs work with requests
3. **Playwright may be needed for NSE annual reports specifically** - But API endpoints exist for announcements

### Architecture Decision

Given the prior art:
- Use `requests` library (not curl_cffi) for both BSE and NSE
- Implement cookie management for NSE (harvest from option-chain page)
- Use BSE's `api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w` as primary announcements source
- Use NSE's `nseindia.com/api/annual-reports` for annual report PDFs
- Documents download from `bseindia.com/xml-data/corpfiling/` and `nsearchives.nseindia.com/`
- SQLite for caching (consistent with SEDAR+ and CNBV scrapers)
- Rate limit: 8 RPS for BSE, 3 RPS for NSE

### corpfiling.co.in - Unknown Territory

No prior art exists. Will need Phase 1 reconnaissance (check tech stack, test curl, inspect headers). May aggregate what BSE+NSE already provide, making it redundant.
