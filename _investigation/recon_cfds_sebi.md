# Reconnaissance: CFDS & SEBI Portals
Date: 2026-04-13

## 1. CFDS (Corporate Filing Dissemination System)

### URL: https://www.corpfiling.co.in

**Status: COMPLETELY UNREACHABLE**

- DNS resolves: www.corpfiling.co.in -> corpfiling.co.in -> 43.228.176.33
- All ports (80, 443, 8080, 8443) return CLOSED/FILTERED (code 11 = EAGAIN)
- Both HTTP and HTTPS fail with connection timeout
- Both www and non-www variants fail
- robots.txt: Could not be fetched (site unreachable)

**Assessment:** The site appears to be down or behind a geo-restricted firewall.
May only be accessible from Indian IP addresses. This is a blocker --
would need to test from an Indian IP/VPN to confirm.

---

## 2. SEBI SI Portal (Intermediary Portal)

### URL: https://siportal.sebi.gov.in

**Status: PARTIALLY ACCESSIBLE (WAF BLOCKED on sub-pages)**

### Response Headers (root /)
```
HTTP/1.1 200 OK
content-length: 740
content-type: text/html
p3p: CP="NON CUR OTPi OUR NOR UNI"
access-control-allow-origin: SAMEORIGIN
permissions-policy: camera=(); microphone=(); geolocation=(); payment=()
x-frame-options: SAMEORIGIN
x-content-type-options: nosniff
x-xss-protection: 1; mode=block
strict-transport-security: max-age=63072000; includeSubdomains
```

### Tech Stack
- **Backend:** Java (JSP) - confirmed by `.jsp` file extensions, `JSESSIONID` cookies
- **Frontend:** Bootstrap 5.3.3, jQuery 3.7.1, jQuery UI, jQuery Validate, DataTables
- **TLS:** TLSv1.3 / TLS_AES_256_GCM_SHA384
- **Certificate:** *.sebi.gov.in (Sectigo DV)
- **Authentication:** IBM Tivoli/Dascom (per HTML comments), IDAM 2FA via silogin.sebi.gov.in

### Root Page Behavior
The root page is a JavaScript redirect:
- Detects URL and redirects to `https://siportal.sebi.gov.in/intermediary/index.html`
- Also handles password reset redirect to `pwreset.sebi.gov.in`

### Intermediary Index Page
- Bootstrap-based login portal for registered intermediaries
- Login form POSTs to `https://silogin.sebi.gov.in/global-protect/login.esp`
- Has CSRF token protection (`CSRFToken` hidden fields)
- Self-registration available at `createUser.html`
- Self-registration query at `intermediarySelfRegQuery.html`

### WAF/Bot Protection
- Sub-pages return `HTTP/1.1 505 BLOCKED` when accessed via curl
- Login portal, self-reg query - all blocked with 505
- The intermediary login redirects to `silogin.sebi.gov.in`
- No Server header exposed on blocked responses

### Assessment
This is a **registration/authentication portal** for SEBI-registered intermediaries.
NOT a public filing search portal. It requires login credentials.
**Not useful for scraping public filings.**

---

## 3. SEBI Main Website - Filings Section

### URL: https://www.sebi.gov.in/filings.html

**Status: FULLY ACCESSIBLE via plain curl/requests**

### Response Headers
```
HTTP/1.1 200 OK
Server: Apache
Content-Type: text/html; charset=UTF-8
X-Frame-Options: SAMEORIGIN
X-XSS-Protection: 1; mode=block
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer-when-downgrade
Strict-Transport-Security: max-age=631138519; includeSubDomains
Cache-Control: no-cache, no-store, must-revalidate
Vary: User-Agent
```

### Tech Stack
- **Server:** Apache
- **Backend:** Java (Struts framework) - confirmed by:
  - `HomeAction.do` URL pattern (Struts ActionServlet)
  - `JSESSIONID` cookie
  - `org.apache.struts.taglib.html.TOKEN` hidden field
  - JSP-based AJAX endpoints (`/sebiweb/ajax/home/getnewslistinfo.jsp`)
- **Frontend:** jQuery 1.7.1 + jQuery 2.2.4, Kendo UI, custom JS
- **TLS:** TLSv1.3 / TLS_AES_256_GCM_SHA384
- **No WAF/bot protection** on main www.sebi.gov.in content pages

### robots.txt
```
User-agent: *
Disallow: 
Disallow: /js
Disallow: /hindi/js
Disallow: /css
Disallow: /hindi/css
```
Very permissive - only blocks JS and CSS directories.
Note: robots.txt returned 530 BLOCKED on HEAD request but 200 on verbose GET.

### Filing Categories Available
The filings.html page is a static index linking to subcategories:

| Category | URL Pattern |
|----------|-------------|
| Processing Status | /filings/processing-status.html |
| Public Issues | /filings/public-issues.html |
| Rights Issues | /filings/rights-issues.html |
| Debt Offer Document | /filings/debt-offer-document.html |
| Takeovers | /filings/takeovers.html |
| Mutual Funds | /filings/mutual-funds.html |
| Buybacks | /filings/buybacks.html |
| InvIT Public Issues | /filings/invit-public-issues.html |
| InvIT Private Issues | /filings/invit-private-issues.html |
| InvIT Rights Issues | /filings/invit-rights-issues.html |
| REIT Issues | /filings/reit-issues.html |
| SM REIT Issues | /filings/sm-reit-issues.html |
| Municipal Debt Securities (Private) | /filings/municipal-debt-securities-privately-issues.html |
| Municipal Debt Securities (Public) | /filings/municipal-debt-securities-public-issues.html |

### Listing Endpoint (THE KEY API)
**URL:** `/sebiweb/home/HomeAction.do?doListing=yes&sid=3&ssid={ssid}&smid={smid}`

Parameters:
- `sid` = 3 (Filings section)
- `ssid` = sub-section ID (15=Public Issues, 16=Rights, 17=Debt, 20=Takeovers, etc.)
- `smid` = sub-sub-section ID (10=Draft Offer Docs, 11=Red Herring, 12=Final Offer, etc.)

Returns a session cookie: `JSESSIONID=...; Path=/sebiweb; HttpOnly;Secure;SameSite=Strict`

### AJAX Pagination Endpoint (CRITICAL)
**URL:** `POST /sebiweb/ajax/home/getnewslistinfo.jsp`
**Content-Type:** `application/x-www-form-urlencoded`

POST parameters:
```
nextValue={page_number}   # Current page (1-indexed)
next={s|n}                # 's' for search, 'n' for pagination
search={keyword}          # Search by title, keywords, entity name
fromDate={DD-MM-YYYY}     # Date range filter start
toDate={DD-MM-YYYY}       # Date range filter end
fromYear={YYYY}           # Year range filter start
toYear={YYYY}             # Year range filter end
deptId=                    # Department ID (usually empty)
sid=3                      # Section ID
ssid={ssid}               # Sub-section ID
smid={smid}               # Sub-sub-section ID
ssidhidden={ssid}          # Hidden copy of ssid
intmid=-1                  # Intermediary ID
sText=Filings             # Section text
ssText={subsection_name}   # Sub-section text
smText={subsubsection_name} # Sub-sub-section text
doDirect={page_number}     # Direct page jump
```

Response format: `div1_html#@#div2_breadcrumb_html`
- Uses `#@#` as separator between content div and breadcrumb div

### Pagination
- 25 records per page
- Total records shown (e.g., "1 to 25 of 2117 records")
- Page navigation: numbered pages + Next/Last
- Last page computed as total_pages - 1 (e.g., page 84 for 2117 records)
- Pagination calls `searchFormNewsList('n', '{page_index}')` where page_index is 0-based

### Filing Detail Pages
**URL Pattern:** `/filings/public-issues/{month-year}/{slug}_{entry_id}.html`
Example: `https://www.sebi.gov.in/filings/public-issues/apr-2026/expression-360-services-india-limited-drhp_100861.html`

Hidden form fields on detail pages:
- `entryId` = numeric filing ID (e.g., 100861)
- `mno` = month number
- `ssid` = sub-section ID
- `keywordIds` = keyword IDs

### Document Download URLs
Two types of document URLs found:

1. **Main filing PDF (via iframe viewer):**
   `https://www.sebi.gov.in/sebi_data/attachdocs/{month-year}/{numeric_id}.pdf`
   Example: `https://www.sebi.gov.in/sebi_data/attachdocs/apr-2026/1775819035932.pdf`
   Viewer wrapper: `/web/?file={pdf_url}`

2. **Companion documents (abridged prospectus, etc.):**
   `https://www.sebi.gov.in/sebi_data/commondocs/{month-year}/{Company_Name}-{Doc_Type}_p.pdf`
   Example: `https://www.sebi.gov.in/sebi_data/commondocs/apr-2026/Expression%20360%20Services%20India%20Limited-Abridged%20Prospectus_p.pdf`

### Other AJAX Endpoints Discovered
- `/sebiweb/ajax/home/marquee.jsp` - Marquee/ticker content
- `/sebiweb/ajax/home/checklogin.jsp` - Login status check
- `/sebiweb/ajax/home/login.jsp` - Login submission
- `/sebiweb/ajax/home/forgot.jsp` - Password recovery

### ssid/smid Mapping (from dropdown)

**Sub-sections (ssid):**
| ssid | Name |
|------|------|
| 14 | Processing Status |
| 15 | Public Issues |
| 16 | Rights Issues |
| 17 | Debt Offer Document |
| 20 | Takeovers |
| 22 | Buybacks |
| 39 | Mutual Funds |
| 55 | InvIT Public Issues |
| 73 | InvIT Private Issues |
| 74 | REIT Issues |
| 87 | Municipal Debt Securities Public Issues |
| 88 | Municipal Debt Securities Privately Issues |
| 89 | InvIT Rights Issues |
| 98 | SM REIT Issues |

**Sub-sub-sections (smid) for Public Issues (ssid=15):**
| smid | Name |
|------|------|
| 10 | Draft Offer Documents filed with SEBI |
| 11 | Red Herring Documents filed with ROC |
| 12 | Final Offer Documents filed with ROC |
| 78 | Other Documents |

### XBRL Support
**No XBRL indicators found** on SEBI filings pages. Documents are PDFs only.
XBRL filings are handled by BSE/NSE/CFDS, not SEBI directly.

---

## Summary & Recommendations

### Portal Accessibility Matrix

| Portal | Accessible? | Bot Protection | Filing Data? |
|--------|------------|----------------|--------------|
| CFDS (corpfiling.co.in) | NO - connection timeout | Unknown (unreachable) | Yes (if accessible) |
| SEBI SI Portal | Partial (WAF blocks sub-pages) | 505 BLOCKED on curl | No (auth portal only) |
| SEBI Filings (www.sebi.gov.in) | YES - fully works | None on content pages | Yes - IPOs, takeovers, etc. |

### Scraping Viability for SEBI Filings

**HIGH VIABILITY** - Plain `requests` library should work:
1. No WAF/bot protection on content pages
2. Clean AJAX API with POST parameters
3. Predictable pagination (25 per page, numeric page indexes)
4. Direct PDF download URLs
5. Session management via JSESSIONID cookie
6. No CAPTCHA on public filing pages

### Recommended Approach
1. Initialize session with GET to listing page (obtain JSESSIONID)
2. Use POST to `/sebiweb/ajax/home/getnewslistinfo.jsp` for paginated listing
3. Parse HTML table rows from response (split on `#@#`)
4. Extract filing detail URLs and PDF download URLs
5. Download PDFs directly (no token/encryption needed)

### What SEBI Filings Cover (and Don't Cover)
**Covers:** IPO prospectuses, draft offer documents, red herring docs, takeover offers, buyback offers, mutual fund docs, InvIT/REIT issues, debt offers
**Does NOT cover:** Annual reports, quarterly financial statements, board meeting outcomes, general corporate announcements

For annual reports and financial statements, BSE/NSE are still needed.
