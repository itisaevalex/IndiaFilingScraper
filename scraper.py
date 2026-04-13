"""
India Securities Filing Scraper
================================

Multi-source scraper for Indian financial filings.
Targets: BSE (Bombay Stock Exchange), NSE (National Stock Exchange), SEBI.

Architecture:
  1. Plain `requests` — no TLS fingerprinting, no browser automation
  2. BSE: REST JSON API with stateless pagination (50/page)
  3. NSE: REST JSON API with date-range pagination
  4. SEBI: Struts AJAX with page-based pagination (25/page)
  5. SQLite cache for dedup and download tracking
  6. Parallel document downloads within each page

Usage:
  python scraper.py crawl --source bse --max-pages 10 --download
  python scraper.py crawl --source nse --max-pages 10 --download
  python scraper.py crawl --source sebi --max-pages 10 --download
  python scraper.py monitor --interval 300 --download
  python scraper.py export --output filings.json
  python scraper.py stats
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_FILE = "filings_cache.db"
DELAY_BETWEEN_PAGES = 1.5
DELAY_BETWEEN_DOWNLOADS = 0.3

# BSE Configuration
BSE_API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_ANNOUNCEMENTS_URL = f"{BSE_API_BASE}/AnnSubCategoryGetData/w"
BSE_DOC_BASES = {
    "0": "https://www.bseindia.com/xml-data/corpfiling/AttachLive/",
    "1": "https://www.bseindia.com/xml-data/corpfiling/AttachHis/",
}
BSE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}
BSE_PAGE_SIZE = 50

# NSE Configuration
NSE_API_BASE = "https://www.nseindia.com/api"
NSE_ENDPOINTS = {
    "announcements": f"{NSE_API_BASE}/corporate-announcements",
    "annual_reports": f"{NSE_API_BASE}/annual-reports",
    "board_meetings": f"{NSE_API_BASE}/corporate-board-meetings",
    "financial_results": f"{NSE_API_BASE}/corporates-financial-results",
}
NSE_ANNOUNCEMENTS_URL = NSE_ENDPOINTS["announcements"]
NSE_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

# SEBI Configuration
SEBI_FILINGS_URL = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
SEBI_DOC_BASE = "https://www.sebi.gov.in"
SEBI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.sebi.gov.in/filings.html",
    "Origin": "https://www.sebi.gov.in",
}
SEBI_CATEGORIES = {
    "public_issues": 15,
    "rights_issues": 16,
    "debt_offers": 17,
    "takeovers": 20,
    "buybacks": 22,
    "mutual_funds": 39,
    "invit_public": 55,
    "invit_private": 73,
    "invit_rights": 89,
    "reit": 74,
    "sm_reit": 98,
}
SEBI_PAGE_SIZE = 25
SEBI_CATEGORY_NAMES: dict[int, str] = {
    v: k.replace("_", " ").title() for k, v in SEBI_CATEGORIES.items()
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("india-scraper")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------


def create_session() -> requests.Session:
    """Create a requests session with retry and backoff on 5xx errors."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = requests.adapters.HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ---------------------------------------------------------------------------
# BSE Source
# ---------------------------------------------------------------------------


def fetch_bse_page(
    session: requests.Session,
    page_num: int = 1,
    from_date: str = "",
    to_date: str = "",
    category: str = "",
    subcategory: str = "",
    scrip: str = "",
    search_type: str = "P",
    filing_type: str = "C",
) -> tuple[list[dict], int]:
    """Fetch one page of BSE announcements. Returns (filings, total_count)."""
    params = {
        "pageno": str(page_num),
        "strCat": category,
        "strPrevDate": from_date,
        "strToDate": to_date,
        "strSearch": search_type,
        "strType": filing_type,
        "subcategory": subcategory,
        "strScrip": scrip,
    }

    resp = session.get(
        BSE_ANNOUNCEMENTS_URL,
        params=params,
        headers=BSE_HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    table = data.get("Table", [])
    total = 0
    table1 = data.get("Table1", [])
    if table1:
        total = int(table1[0].get("ROWCNT", 0))

    filings = []
    for row in table:
        attachment = (row.get("ATTACHMENTNAME") or "").strip()
        doc_url = _build_bse_doc_url(row) if attachment else ""

        filings.append({
            "source": "bse",
            "filing_id": str(row.get("NEWSID") or ""),
            "company_name": (row.get("SLONGNAME") or "").strip(),
            "symbol": str(row.get("SCRIP_CD") or "").strip(),
            "isin": "",
            "category": (row.get("CATEGORYNAME") or "").strip(),
            "subcategory": (row.get("SUBCATNAME") or "").strip(),
            "subject": (row.get("NEWSSUB") or "").strip(),
            "description": (row.get("HEADLINE") or "").strip(),
            "filing_date": (row.get("NEWS_DT") or "").strip(),
            "document_url": doc_url,
            "file_size": str(row.get("Fld_Attachsize") or ""),
            "has_xbrl": False,
            "raw_json": json.dumps(row, ensure_ascii=False),
        })

    return filings, total


def _build_bse_doc_url(row: dict) -> str:
    """Build BSE document download URL based on PDFFLAG."""
    attachment = row.get("ATTACHMENTNAME", "").strip()
    if not attachment:
        return ""

    flag = str(row.get("PDFFLAG", "0")).strip()

    if flag == "2":
        news_dt = row.get("NEWS_DT", "")
        try:
            dt = datetime.strptime(news_dt.split(" ")[0], "%d/%m/%Y")
            return (
                f"https://www.bseindia.com/xml-data/corpfiling/CorpAttachment/"
                f"{dt.year}/{dt.month}/{attachment}"
            )
        except (ValueError, IndexError):
            log.warning("BSE: unparseable date for PDFFLAG=2: %r", news_dt)
            return ""

    base = BSE_DOC_BASES.get(flag, BSE_DOC_BASES["0"])
    return base + attachment


# ---------------------------------------------------------------------------
# NSE Source
# ---------------------------------------------------------------------------



def fetch_nse_endpoint(
    session: requests.Session,
    endpoint_type: str = "announcements",
    index_type: str = "equities",
    from_date: str = "",
    to_date: str = "",
    symbol: str = "",
) -> list[dict]:
    """Fetch any NSE endpoint and normalize to common filing format."""
    url = NSE_ENDPOINTS.get(endpoint_type, NSE_ENDPOINTS["announcements"])
    params: dict[str, str] = {"index": index_type}
    if from_date:
        params["from_date"] = from_date
    if to_date:
        params["to_date"] = to_date
    if symbol:
        params["symbol"] = symbol
    if endpoint_type == "financial_results":
        params["period"] = "Quarterly"

    resp = session.get(url, params=params, headers=NSE_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Some endpoints return a list, others return an object with a nested list
    if isinstance(data, dict):
        # Try common nested keys
        for key in ("data", "results", "records"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            log.warning("NSE %s: unexpected response structure", endpoint_type)
            return []

    if not isinstance(data, list):
        log.warning("NSE %s: non-list response: %s", endpoint_type, type(data).__name__)
        return []

    filings = []
    for row in data:
        filing = _normalize_nse_record(row, endpoint_type)
        if filing:
            filings.append(filing)

    return filings


def _normalize_nse_record(row: dict, endpoint_type: str) -> dict | None:
    """Normalize any NSE API record to the common filing schema."""
    # Common fields across most NSE endpoints
    symbol = (row.get("symbol") or "").strip()
    company = (row.get("sm_name") or row.get("companyName") or "").strip()
    isin = (row.get("sm_isin") or row.get("isin") or "").strip()

    if endpoint_type == "announcements":
        att_file = (row.get("attchmntFile") or "").strip()
        return {
            "source": "nse",
            "filing_id": str(row.get("seq_id") or ""),
            "company_name": company,
            "symbol": symbol,
            "isin": isin,
            "category": (row.get("desc") or "").strip(),
            "subcategory": row.get("smIndustry") or "",
            "subject": (row.get("attchmntText") or "").strip(),
            "description": (row.get("attchmntText") or "").strip(),
            "filing_date": (row.get("sort_date") or "").strip(),
            "document_url": att_file if att_file and att_file != "-" else "",
            "file_size": str(row.get("fileSize") or ""),
            "has_xbrl": bool(row.get("hasXbrl")),
            "raw_json": json.dumps(row, ensure_ascii=False),
        }

    elif endpoint_type == "annual_reports":
        doc_url = (row.get("fileName") or "").strip()
        from_yr = row.get("fromYr") or ""
        to_yr = row.get("toYr") or ""
        return {
            "source": "nse",
            "filing_id": f"ar_{symbol}_{from_yr}_{to_yr}",
            "company_name": company,
            "symbol": symbol,
            "isin": isin,
            "category": "Annual Report",
            "subcategory": f"{from_yr}-{to_yr}",
            "subject": f"Annual Report {from_yr}-{to_yr} - {company}",
            "description": "",
            "filing_date": (row.get("broadcast_dttm") or "").strip(),
            "document_url": doc_url if doc_url and doc_url != "-" else "",
            "file_size": str(row.get("attFileSize") or ""),
            "has_xbrl": False,
            "raw_json": json.dumps(row, ensure_ascii=False),
        }

    elif endpoint_type == "board_meetings":
        # Note: NSE has field typo "sm_indusrty" (not "sm_industry")
        att_file = (row.get("attachment") or "").strip()
        bm_symbol = (row.get("bm_symbol") or symbol).strip()
        return {
            "source": "nse",
            "filing_id": f"bm_{bm_symbol}_{row.get('bm_timestamp', '')}",
            "company_name": (row.get("sm_name") or company).strip(),
            "symbol": bm_symbol,
            "isin": (row.get("sm_isin") or isin).strip(),
            "category": "Board Meeting",
            "subcategory": (row.get("bm_purpose") or "").strip(),
            "subject": (row.get("bm_purpose") or "").strip(),
            "description": (row.get("bm_desc") or "").strip(),
            "filing_date": (row.get("bm_timestamp") or row.get("bm_date") or "").strip(),
            "document_url": att_file if att_file and att_file != "-" else "",
            "file_size": str(row.get("attFileSize") or ""),
            "has_xbrl": bool(att_file),
            "raw_json": json.dumps(row, ensure_ascii=False),
        }

    elif endpoint_type == "financial_results":
        xbrl_url = (row.get("xbrl") or "").strip()
        doc_url = xbrl_url if xbrl_url and not xbrl_url.endswith("/-") else ""
        return {
            "source": "nse",
            "filing_id": f"fr_{row.get('seqNumber', '')}",
            "company_name": company,
            "symbol": symbol,
            "isin": (row.get("isin") or isin).strip(),
            "category": "Financial Results",
            "subcategory": (row.get("relatingTo") or "").strip(),
            "subject": f"{row.get('relatingTo', '')} Results - {company} ({row.get('audited', '')})",
            "description": (row.get("resultDescription") or "").strip(),
            "filing_date": (row.get("filingDate") or row.get("broadCastDate") or "").strip(),
            "document_url": doc_url,
            "file_size": "",
            "has_xbrl": bool(doc_url),
            "raw_json": json.dumps(row, ensure_ascii=False),
        }

    return None


def fetch_nse_paginated(
    session: requests.Session,
    max_pages: int = 10,
    endpoint_type: str = "announcements",
    index_type: str = "equities",
    symbol: str = "",
    days_per_page: int = 7,
    from_date_override: str = "",
    to_date_override: str = "",
) -> list[dict]:
    """Paginate NSE by date ranges (no built-in page param).

    NSE returns all results for a date range in one response.
    We paginate by sliding the date window backwards.
    If from_date_override/to_date_override are set, fetches that single range only.
    """
    all_filings: list[dict] = []

    # Single date range mode (explicit --from-date/--to-date)
    if from_date_override or to_date_override:
        log.info(
            "NSE %s: %s to %s",
            endpoint_type, from_date_override or "start", to_date_override or "now",
        )
        try:
            filings = fetch_nse_endpoint(
                session, endpoint_type=endpoint_type, index_type=index_type,
                from_date=from_date_override, to_date=to_date_override, symbol=symbol,
            )
            all_filings.extend(filings)
            log.info("  %d filings", len(filings))
        except (requests.RequestException, ValueError) as e:
            log.warning("NSE %s fetch failed: %s", endpoint_type, e)
        return all_filings

    # Sliding window pagination
    end_date = datetime.now()

    for page in range(max_pages):
        start_date = end_date - timedelta(days=days_per_page)
        from_str = start_date.strftime("%d-%m-%Y")
        to_str = end_date.strftime("%d-%m-%Y")

        log.info("NSE %s page %d: %s to %s", endpoint_type, page + 1, from_str, to_str)

        try:
            filings = fetch_nse_endpoint(
                session, endpoint_type=endpoint_type, index_type=index_type,
                from_date=from_str, to_date=to_str, symbol=symbol,
            )
        except (requests.RequestException, ValueError) as e:
            log.warning("NSE %s fetch failed for %s to %s: %s — skipping", endpoint_type, from_str, to_str, e)
            end_date = start_date - timedelta(days=1)
            continue

        if not filings:
            log.info("NSE %s: no filings for %s to %s. Stopping.", endpoint_type, from_str, to_str)
            break

        all_filings.extend(filings)
        log.info("  %d filings", len(filings))

        end_date = start_date - timedelta(days=1)
        if page < max_pages - 1:
            time.sleep(DELAY_BETWEEN_PAGES)

    return all_filings


# ---------------------------------------------------------------------------
# SEBI Source
# ---------------------------------------------------------------------------


def fetch_sebi_page(
    session: requests.Session,
    page_num: int = 0,
    category_id: int = 15,
    from_date: str = "",
    to_date: str = "",
    search: str = "",
) -> tuple[list[dict], bool]:
    """Fetch one page of SEBI filings. Returns (filings, has_more)."""
    params = {
        "next": "n" if page_num > 0 else "s",
        "nextValue": "",
        "search": search,
        "fromDate": from_date,
        "toDate": to_date,
        "sid": "3",
        "ssid": str(category_id),
        "ssidhidden": str(category_id),
        "smid": "",
        "doDirect": str(page_num),
    }

    resp = session.post(
        SEBI_FILINGS_URL,
        data=params,
        headers=SEBI_HEADERS,
        timeout=30,
    )
    resp.raise_for_status()

    parts = resp.text.split("#@#")
    html = parts[0] if parts else ""

    # Parse pagination info from the hidden inputs in the response
    has_more = _sebi_has_next_page(html, page_num)

    filings = _parse_sebi_filings(html, category_id)
    return filings, has_more


def _sebi_has_next_page(html: str, current_page: int) -> bool:
    """Determine if SEBI has more pages from the pagination metadata."""
    # SEBI includes <input type='hidden' name='totalpage' value=N />
    total_match = re.search(r"name='totalpage'\s+value=(\d+)", html)
    if total_match:
        total_pages = int(total_match.group(1))
        return current_page < total_pages - 1  # 0-indexed
    # Fallback: check for "Next" link
    return "javascript: searchFormNewsList('n'" in html


def _parse_sebi_filings(html: str, category_id: int) -> list[dict]:
    """Parse SEBI HTML table into filing records.

    Each row may have:
    - A main filing link (.html page with embedded PDF)
    - Companion PDF links (abridged prospectuses, etc.)
    Both are captured as separate filing records.
    """
    soup = BeautifulSoup(html, "lxml")
    filings = []
    category_name = SEBI_CATEGORY_NAMES.get(category_id, f"Category {category_id}")

    rows = soup.select("tr[role='row']")
    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        date_text = cells[0].get_text(strip=True)

        all_links = cells[1].find_all("a", href=True)
        if not all_links:
            continue

        # First link is the main filing page
        main_link = all_links[0]
        main_href = main_link.get("href", "")
        if not main_href:
            continue

        main_url = main_href if main_href.startswith("http") else f"{SEBI_DOC_BASE}{main_href}"
        title = main_link.get_text(strip=True)
        title = re.sub(r"<[^>]+>", "", title).strip()

        fid_match = re.search(r"_(\d+)\.html", main_url) or re.search(r"/(\d+)\.\w+$", main_url)
        filing_id = fid_match.group(1) if fid_match else main_url.split("?")[0].rstrip("/")

        filings.append({
            "source": "sebi",
            "filing_id": filing_id,
            "company_name": title,
            "symbol": "",
            "isin": "",
            "category": category_name,
            "subcategory": "",
            "subject": title,
            "description": "",
            "filing_date": date_text,
            "document_url": main_url,
            "file_size": "",
            "has_xbrl": False,
            "raw_json": "",
        })

        # Additional links are companion documents (direct PDFs)
        for extra_link in all_links[1:]:
            extra_href = extra_link.get("href", "")
            if not extra_href:
                continue
            extra_url = extra_href if extra_href.startswith("http") else f"{SEBI_DOC_BASE}{extra_href}"
            extra_title = extra_link.get_text(strip=True)

            extra_fid = re.search(r"/([^/]+)\.\w+$", extra_url)
            extra_id = f"{filing_id}_companion_{extra_fid.group(1)}" if extra_fid else extra_url

            filings.append({
                "source": "sebi",
                "filing_id": extra_id,
                "company_name": title,
                "symbol": "",
                "isin": "",
                "category": category_name,
                "subcategory": "companion",
                "subject": extra_title or f"{title} - Companion",
                "description": "",
                "filing_date": date_text,
                "document_url": extra_url,
                "file_size": "",
                "has_xbrl": False,
                "raw_json": "",
            })

    return filings


def resolve_sebi_pdf(session: requests.Session, html_url: str) -> str:
    """Follow a SEBI filing HTML page to extract the embedded PDF URL."""
    try:
        resp = session.get(html_url, headers=SEBI_HEADERS, timeout=30)
        if resp.status_code != 200:
            return html_url

        soup = BeautifulSoup(resp.text, "lxml")

        # Check iframe/embed for PDF viewer
        for tag in soup.find_all(["iframe", "embed"]):
            src = tag.get("src", tag.get("data", ""))
            pdf_match = re.search(r"file=(https?://[^\s&\"']+\.pdf)", src)
            if pdf_match:
                return pdf_match.group(1)

        # Check direct PDF links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "sebi_data" in href and href.endswith(".pdf"):
                return href if href.startswith("http") else f"{SEBI_DOC_BASE}{href}"

        return html_url
    except Exception as e:
        log.warning("SEBI PDF resolution failed for %s: %s", html_url, e)
        return html_url


# ---------------------------------------------------------------------------
# SQLite Cache
# ---------------------------------------------------------------------------


class FilingCache:
    def __init__(self, db_path: str = DB_FILE):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                filing_id TEXT,
                company_name TEXT,
                symbol TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_source ON filings(source);
            CREATE INDEX IF NOT EXISTS idx_doc_url ON filings(document_url);
            CREATE INDEX IF NOT EXISTS idx_dl ON filings(downloaded);
            CREATE INDEX IF NOT EXISTS idx_date ON filings(filing_date);
        """)
        self.conn.commit()

    def insert_batch(self, filings: list[dict], page_num: int = 0) -> int:
        """Insert filings, skipping duplicates. Returns count of new records."""
        before = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        for f in filings:
            fid = f.get("filing_id", "")
            source = f.get("source", "")
            if not fid or not source:
                continue
            self.conn.execute(
                """INSERT OR IGNORE INTO filings
                   (source, filing_id, company_name, symbol, isin, category,
                    subcategory, subject, description, filing_date, document_url,
                    file_size, has_xbrl, first_seen, page_number, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    source, fid, f.get("company_name", ""), f.get("symbol", ""),
                    f.get("isin", ""), f.get("category", ""), f.get("subcategory", ""),
                    f.get("subject", ""), f.get("description", ""),
                    f.get("filing_date", ""), f.get("document_url", ""),
                    f.get("file_size", ""), int(f.get("has_xbrl", False)),
                    datetime.now().isoformat(), page_num, f.get("raw_json", ""),
                ),
            )
        self.conn.commit()
        return self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0] - before

    def mark_downloaded(self, source: str, filing_id: str, path: str):
        self.conn.execute(
            "UPDATE filings SET downloaded=1, local_path=? WHERE source=? AND filing_id=?",
            (path, source, filing_id),
        )
        self.conn.commit()

    def get_known_keys(self, source: str = "") -> set[str]:
        """Return set of 'source|filing_id' keys for dedup."""
        if source:
            rows = self.conn.execute(
                "SELECT source, filing_id FROM filings WHERE source=?", (source,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT source, filing_id FROM filings").fetchall()
        return {f"{r[0]}|{r[1]}" for r in rows}

    def stats(self, source: str = "") -> dict:
        where = "WHERE source=?" if source else ""
        params = (source,) if source else ()
        r = self.conn.execute(
            f"""SELECT COUNT(*) as total,
                SUM(CASE WHEN downloaded=1 THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN downloaded=0 THEN 1 ELSE 0 END) as pending,
                MIN(filing_date) as oldest, MAX(filing_date) as newest
            FROM filings {where}""",
            params,
        ).fetchone()
        return dict(r)

    def export_json(self, path: str, source: str = ""):
        where = "WHERE source=?" if source else ""
        params = (source,) if source else ()
        rows = self.conn.execute(
            f"SELECT * FROM filings {where} ORDER BY filing_date DESC", params
        ).fetchall()

        out = {
            "metadata": {
                "sources": ["bse", "nse", "sebi"],
                "exported_at": datetime.now().isoformat(),
                "total": len(rows),
                "stats": self.stats(source),
            },
            "filings": [dict(r) for r in rows],
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        log.info("Exported %d filings to %s", len(rows), path)

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Download Manager
# ---------------------------------------------------------------------------


def download_filings(
    session: requests.Session,
    filings: list[dict],
    doc_dir: str,
    cache: FilingCache,
    parallel: int = 5,
) -> int:
    """Download documents for filings in parallel. Returns count downloaded.

    Thread safety: each worker uses the shared session for GET requests (safe
    with urllib3's connection pooling). All SQLite writes happen on the main
    thread after workers complete — do not move cache.mark_downloaded into workers.
    """
    to_download = [f for f in filings if f.get("document_url")]
    if not to_download:
        return 0

    os.makedirs(doc_dir, exist_ok=True)
    results: list[tuple[str, str, str]] = []

    def _download_one(filing: dict) -> tuple[str, str, str] | None:
        url = filing["document_url"]
        source = filing["source"]
        filing_id = filing["filing_id"]

        # SEBI main filings are .html pages with embedded PDFs — resolve first
        if source == "sebi" and url.endswith(".html"):
            url = resolve_sebi_pdf(session, url)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        }

        try:
            resp = session.get(url, headers=headers, timeout=120)
            if resp.status_code != 200:
                log.warning("Download HTTP %d for %s", resp.status_code, url)
                return None
        except requests.RequestException as e:
            log.warning("Download failed for %s: %s", url, e)
            return None

        # Determine filename — use os.path.basename to prevent path traversal
        cd = resp.headers.get("content-disposition", "")
        fname_m = re.search(r'filename="?([^";\n]+)', cd)
        if fname_m:
            fname = os.path.basename(fname_m.group(1).strip())
        else:
            fname = os.path.basename(url.split("?")[0])

        # Ensure file has an extension
        if "." not in fname:
            ct = resp.headers.get("content-type", "")
            ext = ".pdf" if "pdf" in ct else ".zip" if "zip" in ct else ".bin"
            fname = fname + ext

        safe_name = re.sub(r'[<>:"/\\|?*]', "_", fname)[:120]
        prefix = f"{source}_{filing_id}_" if filing_id else f"{source}_"
        filepath = os.path.join(doc_dir, f"{prefix}{safe_name}")

        try:
            with open(filepath, "wb") as fh:
                fh.write(resp.content)
        except OSError as e:
            log.error("I/O error saving %s: %s", filepath, e)
            return None

        time.sleep(DELAY_BETWEEN_DOWNLOADS)
        return (source, filing_id, filepath)

    if parallel > 1 and len(to_download) > 1:
        with ThreadPoolExecutor(max_workers=min(parallel, len(to_download))) as pool:
            futs = {pool.submit(_download_one, f): f for f in to_download}
            for fut in as_completed(futs):
                result = fut.result()
                if result:
                    results.append(result)
    else:
        for f in to_download:
            result = _download_one(f)
            if result:
                results.append(result)

    for source, filing_id, path in results:
        cache.mark_downloaded(source, filing_id, path)

    return len(results)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_crawl(args):
    session = create_session()
    cache = FilingCache(args.db)
    try:
        doc_dir = args.doc_dir
        t_start = time.time()
        total_filings = 0
        total_new = 0
        total_downloaded = 0

        sources = (
            ["bse", "nse", "sebi"] if args.source == "all"
            else [args.source]
        )

        for source in sources:
            log.info("=== Crawling %s ===", source.upper())

            if source == "bse":
                total_f, new_f, dl_f = _crawl_bse(
                    session, cache, doc_dir, args.max_pages, args.download, args.parallel,
                    from_date=args.from_date, to_date=args.to_date,
                )
            elif source == "nse":
                total_f, new_f, dl_f = _crawl_nse(
                    session, cache, doc_dir, args.max_pages, args.download, args.parallel,
                    nse_types=args.nse_type,
                    from_date=args.from_date, to_date=args.to_date,
                )
            elif source == "sebi":
                total_f, new_f, dl_f = _crawl_sebi(
                    session, cache, doc_dir, args.max_pages, args.download, args.parallel,
                    categories=args.sebi_category,
                )
            else:
                log.error("Unknown source: %s", source)
                continue

            total_filings += total_f
            total_new += new_f
            total_downloaded += dl_f

        elapsed = time.time() - t_start
        log.info(
            "Done: %d filings (%d new), %d downloaded in %.1fs.",
            total_filings, total_new, total_downloaded, elapsed,
        )
    finally:
        cache.close()


def _crawl_bse(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    from_date: str = "",
    to_date: str = "",
) -> tuple[int, int, int]:
    """Crawl BSE announcements. Returns (total, new, downloaded)."""
    total_f = total_new = total_dl = 0
    search_type = "A" if (from_date or to_date) else "P"

    if from_date or to_date:
        log.info("BSE date filter: %s to %s", from_date or "start", to_date or "now")

    for page_num in range(1, max_pages + 1):
        if page_num > 1:
            time.sleep(DELAY_BETWEEN_PAGES)

        try:
            filings, total_count = fetch_bse_page(
                session, page_num=page_num,
                from_date=from_date, to_date=to_date, search_type=search_type,
            )
        except (requests.RequestException, ValueError) as e:
            log.warning("BSE page %d fetch failed: %s — skipping", page_num, e)
            continue

        if not filings:
            log.info("BSE page %d: no filings. Stopping.", page_num)
            break

        new = cache.insert_batch(filings, page_num)
        total_f += len(filings)
        total_new += new

        max_page = (total_count + BSE_PAGE_SIZE - 1) // BSE_PAGE_SIZE if total_count else "?"
        log.info(
            "BSE page %d/%s: %d filings (%d new) [total: %s]",
            page_num, max_page, len(filings), new, total_count,
        )

        if download and filings:
            dl = download_filings(session, filings, doc_dir, cache, parallel)
            total_dl += dl

        if new == 0 and page_num > 2:
            log.info("BSE: no new filings — caught up.")
            break

    return total_f, total_new, total_dl


def _crawl_nse(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    nse_types: list[str] | None = None,
    from_date: str = "",
    to_date: str = "",
) -> tuple[int, int, int]:
    """Crawl NSE filings via date-range pagination, optionally multi-type."""
    if not nse_types:
        nse_types = ["announcements"]
    if "all" in nse_types:
        nse_types = list(NSE_ENDPOINTS.keys())

    total_f = total_new = total_dl = 0

    for nse_type in nse_types:
        log.info("--- NSE: %s ---", nse_type)
        filings = fetch_nse_paginated(
            session, max_pages=max_pages, endpoint_type=nse_type,
            from_date_override=from_date, to_date_override=to_date,
        )
        new = cache.insert_batch(filings)
        total_f += len(filings)
        total_new += new

        if download and filings:
            dl = download_filings(session, filings, doc_dir, cache, parallel)
            total_dl += dl

        log.info("NSE %s: %d filings (%d new), %d downloaded", nse_type, len(filings), new, total_dl)

    return total_f, total_new, total_dl


def _crawl_sebi(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    categories: list[str] | None = None,
) -> tuple[int, int, int]:
    """Crawl SEBI filings for one or more categories."""
    if not categories:
        categories = ["public_issues"]
    if "all" in categories:
        categories = list(SEBI_CATEGORIES.keys())

    total_f = total_new = total_dl = 0

    for category in categories:
        category_id = SEBI_CATEGORIES.get(category, 15)
        category_name = SEBI_CATEGORY_NAMES.get(category_id, f"Category {category_id}")
        log.info("--- SEBI: %s ---", category_name)

        cat_f, cat_new, cat_dl = _crawl_sebi_category(
            session, cache, doc_dir, max_pages, download, parallel,
            category_id, category_name,
        )
        total_f += cat_f
        total_new += cat_new
        total_dl += cat_dl

    return total_f, total_new, total_dl


def _crawl_sebi_category(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    category_id: int,
    category_name: str,
) -> tuple[int, int, int]:
    """Crawl SEBI filings for a single category."""
    total_f = total_new = total_dl = 0

    for page_num in range(max_pages):
        if page_num > 0:
            time.sleep(DELAY_BETWEEN_PAGES)

        try:
            filings, has_more = fetch_sebi_page(
                session, page_num=page_num, category_id=category_id,
            )
        except (requests.RequestException, ValueError) as e:
            log.warning("SEBI page %d fetch failed: %s — skipping", page_num + 1, e)
            continue

        if not filings:
            log.info("SEBI page %d: no filings. Stopping.", page_num + 1)
            break

        new = cache.insert_batch(filings, page_num + 1)
        total_f += len(filings)
        total_new += new

        log.info(
            "SEBI page %d: %d filings (%d new) [%s]",
            page_num + 1, len(filings), new, category_name,
        )

        if download and filings:
            dl = download_filings(session, filings, doc_dir, cache, parallel)
            total_dl += dl

        if not has_more:
            break

    return total_f, total_new, total_dl


def cmd_monitor(args):
    cache = FilingCache(args.db)
    known_keys = cache.get_known_keys(args.source if args.source != "all" else "")
    session = create_session()

    log.info(
        "Monitoring %s for new filings every %ds. Known: %d. Ctrl+C to stop.",
        args.source.upper(), args.interval, len(known_keys),
    )

    polls = 0
    try:
        while True:
            polls += 1
            new_count = 0

            if args.source in ("bse", "all"):
                filings, _ = fetch_bse_page(session, page_num=1)
                new_filings = [
                    f for f in filings
                    if f"{f['source']}|{f['filing_id']}" not in known_keys
                ]
                if new_filings:
                    n = cache.insert_batch(new_filings)
                    new_count += n
                    for f in new_filings:
                        known_keys.add(f"{f['source']}|{f['filing_id']}")
                    if args.download:
                        download_filings(
                            session, new_filings, args.doc_dir, cache, args.parallel,
                        )
                    log.info("[Poll %d] BSE: %d new filings", polls, len(new_filings))

            if args.source in ("nse", "all"):
                today = datetime.now()
                yesterday = today - timedelta(days=1)
                filings = fetch_nse_endpoint(
                    session,
                    from_date=yesterday.strftime("%d-%m-%Y"),
                    to_date=today.strftime("%d-%m-%Y"),
                )
                new_filings = [
                    f for f in filings
                    if f"{f['source']}|{f['filing_id']}" not in known_keys
                ]
                if new_filings:
                    n = cache.insert_batch(new_filings)
                    new_count += n
                    for f in new_filings:
                        known_keys.add(f"{f['source']}|{f['filing_id']}")
                    if args.download:
                        download_filings(
                            session, new_filings, args.doc_dir, cache, args.parallel,
                        )
                    log.info("[Poll %d] NSE: %d new filings", polls, len(new_filings))

            if new_count == 0:
                log.info("[Poll %d] No new filings. Known: %d", polls, len(known_keys))

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Monitor stopped. %d polls, %d filings known.", polls, len(known_keys))
    finally:
        cache.close()


def cmd_export(args):
    cache = FilingCache(args.db)
    try:
        cache.export_json(args.output, source=args.source if args.source != "all" else "")
    finally:
        cache.close()


def cmd_stats(args):
    cache = FilingCache(args.db)
    try:
        sources = ["bse", "nse", "sebi"] if args.source == "all" else [args.source]

        for source in sources:
            s = cache.stats(source)
            print(f"\n--- {source.upper()} ---")
            print(f"  Total:      {s['total'] or 0}")
            print(f"  Downloaded: {s['downloaded'] or 0}")
            print(f"  Pending:    {s['pending'] or 0}")
            print(f"  Oldest:     {s['oldest'] or 'N/A'}")
            print(f"  Newest:     {s['newest'] or 'N/A'}")

        if args.source == "all":
            s = cache.stats()
            print(f"\n--- ALL SOURCES ---")
            print(f"  Total:      {s['total'] or 0}")
            print(f"  Downloaded: {s['downloaded'] or 0}")
            print(f"  Pending:    {s['pending'] or 0}")
    finally:
        cache.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(
        description="India Securities Filing Scraper (BSE, NSE, SEBI)",
    )
    sub = p.add_subparsers(dest="command")

    # crawl
    c = sub.add_parser("crawl", help="Crawl filings from Indian exchanges")
    c.add_argument(
        "--source", choices=["bse", "nse", "sebi", "all"], default="bse",
        help="Filing source (default: bse)",
    )
    c.add_argument("--max-pages", type=int, default=10)
    c.add_argument("--download", action="store_true", help="Download documents")
    c.add_argument("--parallel", type=int, default=5, help="Download workers (default: 5)")
    c.add_argument("--doc-dir", default="documents")
    c.add_argument("--db", default=DB_FILE)
    c.add_argument(
        "--from-date", default="",
        help="Start date (BSE: YYYY-MM-DD, NSE: DD-MM-YYYY)",
    )
    c.add_argument(
        "--to-date", default="",
        help="End date (BSE: YYYY-MM-DD, NSE: DD-MM-YYYY)",
    )
    c.add_argument(
        "--sebi-category", nargs="+", default=["public_issues"],
        choices=list(SEBI_CATEGORIES.keys()) + ["all"],
        help="SEBI filing categories (default: public_issues, use 'all' for everything)",
    )
    c.add_argument(
        "--nse-type", nargs="+", default=["announcements"],
        choices=["announcements", "annual_reports", "board_meetings", "financial_results", "all"],
        help="NSE data types to crawl (default: announcements)",
    )

    # monitor
    m = sub.add_parser("monitor", help="Watch for new filings")
    m.add_argument(
        "--source", choices=["bse", "nse", "all"], default="bse",
        help="Source to monitor (default: bse)",
    )
    m.add_argument("--interval", type=int, default=300, help="Poll interval secs (default: 300)")
    m.add_argument("--download", action="store_true", help="Auto-download new filings")
    m.add_argument("--parallel", type=int, default=5)
    m.add_argument("--doc-dir", default="documents")
    m.add_argument("--db", default=DB_FILE)

    # export
    e = sub.add_parser("export", help="Export filings to JSON")
    e.add_argument("--output", default="filings.json")
    e.add_argument("--source", choices=["bse", "nse", "sebi", "all"], default="all")
    e.add_argument("--db", default=DB_FILE)

    # stats
    st = sub.add_parser("stats", help="Show cache statistics")
    st.add_argument("--source", choices=["bse", "nse", "sebi", "all"], default="all")
    st.add_argument("--db", default=DB_FILE)

    args = p.parse_args()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    cmds = {
        "crawl": cmd_crawl,
        "monitor": cmd_monitor,
        "export": cmd_export,
        "stats": cmd_stats,
    }

    if args.command in cmds:
        cmds[args.command](args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
