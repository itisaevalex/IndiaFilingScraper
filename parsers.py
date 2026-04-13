"""
parsers.py — Parsing for all 3 India scraper sources: BSE JSON, NSE JSON, SEBI HTML/#@#.

Also contains classify_filing_type() which works across all sources.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

log = logging.getLogger("india-scraper")

# ---------------------------------------------------------------------------
# BSE configuration (doc URL routing)
# ---------------------------------------------------------------------------

BSE_DOC_BASES: dict[str, str] = {
    "0": "https://www.bseindia.com/xml-data/corpfiling/AttachLive/",
    "1": "https://www.bseindia.com/xml-data/corpfiling/AttachHis/",
}

# ---------------------------------------------------------------------------
# SEBI configuration
# ---------------------------------------------------------------------------

SEBI_DOC_BASE = "https://www.sebi.gov.in"
SEBI_CATEGORIES: dict[str, int] = {
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

SEBI_CATEGORY_NAMES: dict[int, str] = {
    v: k.replace("_", " ").title() for k, v in SEBI_CATEGORIES.items()
}

# NSE endpoint type keys (exported for CLI choices validation)
NSE_ENDPOINTS_MAP: list[str] = [
    "announcements",
    "annual_reports",
    "board_meetings",
    "financial_results",
]

# ---------------------------------------------------------------------------
# Filing type classification
# ---------------------------------------------------------------------------

_FILING_TYPE_RULES: list[tuple[str, list[str]]] = [
    ("Annual Report", ["annual report", "annual-report", "annualreport"]),
    ("Financial Results", [
        "financial result", "quarterly result", "half-year", "half year",
        "q1 result", "q2 result", "q3 result", "q4 result",
        "unaudited", "audited result", "results for",
    ]),
    # "Outcome of Meeting" must come BEFORE "Board Meeting" — "outcome of board meeting"
    # contains "board meeting" and would match the wrong rule if order was reversed.
    ("Outcome of Meeting", ["outcome of board", "outcome of meeting"]),
    ("Board Meeting", ["board meeting", "board of directors meeting", "board meet"]),
    ("AGM/EGM", ["agm", "egm", "annual general meeting", "extraordinary general meeting"]),
    ("Dividend", ["dividend", "interim dividend", "final dividend"]),
    ("Buyback", ["buyback", "buy-back", "buy back", "repurchase"]),
    ("Takeover / Merger", [
        "takeover", "merger", "amalgamation", "acquisition", "scheme of arrangement",
        "demerger", "composite scheme",
    ]),
    ("IPO / Rights Issue", [
        "ipo", "initial public offer", "rights issue", "rights offer",
        "follow-on public offer", "fpo", "prospectus", "offer for sale",
        "public issue", "debt offer",
    ]),
    ("Insider Trading", ["insider trading", "insider dealing", "upsi"]),
    ("Regulatory Filing", [
        "compliance", "regulation", "sebi", "listing obligation", "lodr",
        "corporate governance", "shareholding pattern",
    ]),
    ("XBRL Filing", ["xbrl"]),
    ("Credit Rating", ["credit rating", "rating downgrade", "rating upgrade"]),
    ("Change in Management", [
        "appointment", "resignation", "cessation", "change in director",
        "key managerial", "kmp", "whole-time director",
    ]),
    ("Newspaper Publication", ["newspaper", "publication in newspaper"]),
]


def classify_filing_type(headline: str) -> str:
    """Classify a filing into a canonical type based on its headline / subject.

    Works across all 3 sources (BSE, NSE, SEBI).  Uses case-insensitive
    substring matching against a priority-ordered rule list.

    Args:
        headline: The filing subject, description, category, or any combined text.

    Returns:
        A canonical type string such as 'Annual Report', 'Financial Results', etc.
        Returns 'Other' if no rule matches.
    """
    if not headline:
        return "Other"

    lower = headline.lower()
    for filing_type, keywords in _FILING_TYPE_RULES:
        for kw in keywords:
            if kw in lower:
                return filing_type
    return "Other"


# ---------------------------------------------------------------------------
# BSE parser
# ---------------------------------------------------------------------------


def build_bse_doc_url(row: dict) -> str:
    """Build BSE document download URL based on PDFFLAG routing.

    PDFFLAG routing:
        0 -> AttachLive (current/live attachments)
        1 -> AttachHis  (historical attachments)
        2 -> CorpAttachment/<year>/<month>/<filename> (date-routed)

    Args:
        row: A single BSE API result row.

    Returns:
        Full document URL, or empty string if attachment name is missing/unparseable.
    """
    attachment = (row.get("ATTACHMENTNAME") or "").strip()
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


def parse_bse_response(data: dict) -> tuple[list[dict], int]:
    """Parse a BSE API JSON response into normalized filings.

    The BSE API returns:
        { "Table": [...rows...], "Table1": [{"ROWCNT": N}] }

    Args:
        data: Parsed JSON response dict from the BSE API.

    Returns:
        Tuple of (list_of_filing_dicts, total_row_count).
    """
    table = data.get("Table", [])
    total = 0
    table1 = data.get("Table1", [])
    if table1:
        try:
            total = int(table1[0].get("ROWCNT", 0))
        except (ValueError, TypeError):
            total = 0

    filings: list[dict] = []
    for row in table:
        attachment = (row.get("ATTACHMENTNAME") or "").strip()
        doc_url = build_bse_doc_url(row) if attachment else ""

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


# ---------------------------------------------------------------------------
# NSE parser
# ---------------------------------------------------------------------------


def _normalize_nse_record(row: dict, endpoint_type: str) -> Optional[dict]:
    """Normalize a single NSE API row to the common filing schema.

    Args:
        row: A single NSE API result row.
        endpoint_type: One of 'announcements', 'annual_reports',
                       'board_meetings', 'financial_results'.

    Returns:
        Normalized filing dict, or None if endpoint_type is unrecognised.
    """
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

    if endpoint_type == "annual_reports":
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

    if endpoint_type == "board_meetings":
        # NSE has documented typo "sm_indusrty" (not "sm_industry") in some responses
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

    if endpoint_type == "financial_results":
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
            "subject": (
                f"{row.get('relatingTo', '')} Results - {company} ({row.get('audited', '')})"
            ),
            "description": (row.get("resultDescription") or "").strip(),
            "filing_date": (row.get("filingDate") or row.get("broadCastDate") or "").strip(),
            "document_url": doc_url,
            "file_size": "",
            "has_xbrl": bool(doc_url),
            "raw_json": json.dumps(row, ensure_ascii=False),
        }

    return None


def parse_nse_response(data: object, endpoint_type: str) -> list[dict]:
    """Parse an NSE API response (list or dict) into normalized filings.

    Some NSE endpoints return a bare list; others return a dict with a nested
    'data', 'results', or 'records' key.

    Args:
        data: Parsed JSON object from the NSE API.
        endpoint_type: NSE endpoint type string.

    Returns:
        List of normalized filing dicts.
    """
    if isinstance(data, dict):
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

    filings: list[dict] = []
    for row in data:
        filing = _normalize_nse_record(row, endpoint_type)
        if filing:
            filings.append(filing)
    return filings


# ---------------------------------------------------------------------------
# SEBI parser
# ---------------------------------------------------------------------------


def sebi_has_next_page(html: str, current_page: int) -> bool:
    """Determine if SEBI has more pages from pagination metadata embedded in HTML.

    SEBI embeds: <input type='hidden' name='totalpage' value=N />

    Args:
        html: The HTML portion of the SEBI #@# split response.
        current_page: The 0-based current page index.

    Returns:
        True if there are more pages.
    """
    total_match = re.search(r"name='totalpage'\s+value=(\d+)", html)
    if total_match:
        total_pages = int(total_match.group(1))
        return current_page < total_pages - 1
    # Fallback: check for "Next" link
    return "javascript: searchFormNewsList('n'" in html


def parse_sebi_response(raw_text: str, category_id: int) -> tuple[list[dict], bool, int]:
    """Parse a SEBI Struts AJAX response into normalized filings.

    The response format is:
        <html_table>#@#<extra_data>#@#...

    The HTML part contains a table of filings with links. Each row may have
    multiple links — the first is the main filing (usually an HTML page with
    an embedded PDF), and subsequent ones are companion documents (direct PDFs).

    Args:
        raw_text: Raw response text from the SEBI getnewslistinfo.jsp endpoint.
        category_id: The SEBI category ID used to look up a human-readable name.

    Returns:
        Tuple of (list_of_filing_dicts, has_more_pages, current_page_inferred).
        current_page_inferred is 0 for the first page, used by the caller to
        decide whether to stop.
    """
    parts = raw_text.split("#@#")
    html = parts[0] if parts else ""

    # Extract current page from totalpage metadata (0-indexed)
    # We can't infer current page from the HTML reliably — pass it externally.
    has_more = sebi_has_next_page(html, 0)

    filings = _parse_sebi_html(html, category_id)
    return filings, has_more, 0


def parse_sebi_page(raw_text: str, category_id: int, current_page: int) -> tuple[list[dict], bool]:
    """Parse a SEBI response for a known page number.

    Args:
        raw_text: Raw response text.
        category_id: SEBI category integer.
        current_page: 0-based page index (needed for has_more calculation).

    Returns:
        Tuple of (list_of_filing_dicts, has_more_pages).
    """
    parts = raw_text.split("#@#")
    html = parts[0] if parts else ""
    has_more = sebi_has_next_page(html, current_page)
    filings = _parse_sebi_html(html, category_id)
    return filings, has_more


def _parse_sebi_html(html: str, category_id: int) -> list[dict]:
    """Internal: parse the HTML table portion of a SEBI response.

    Args:
        html: HTML string from the first #@# segment.
        category_id: Numeric SEBI category used for the 'category' field.

    Returns:
        List of normalized filing dicts.
    """
    soup = BeautifulSoup(html, "lxml")
    filings: list[dict] = []
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

        main_link = all_links[0]
        main_href = main_link.get("href", "")
        if not main_href:
            continue

        main_url = (
            main_href if main_href.startswith("http") else f"{SEBI_DOC_BASE}{main_href}"
        )
        title = re.sub(r"<[^>]+>", "", main_link.get_text(strip=True)).strip()

        fid_match = re.search(r"_(\d+)\.html", main_url) or re.search(
            r"/(\d+)\.\w+$", main_url
        )
        filing_id = (
            fid_match.group(1) if fid_match else main_url.split("?")[0].rstrip("/")
        )

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
            extra_url = (
                extra_href
                if extra_href.startswith("http")
                else f"{SEBI_DOC_BASE}{extra_href}"
            )
            extra_title = extra_link.get_text(strip=True)

            extra_fid_match = re.search(r"/([^/]+)\.\w+$", extra_url)
            extra_id = (
                f"{filing_id}_companion_{extra_fid_match.group(1)}"
                if extra_fid_match
                else extra_url
            )

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
