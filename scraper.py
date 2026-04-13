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
import sys
import time
from datetime import datetime, timedelta

import requests

from db import DB_FILE, FilingCache
from downloader import download_filings
from http_utils import BSE_HEADERS, NSE_HEADERS, SEBI_HEADERS, create_session
from parsers import (
    SEBI_CATEGORIES,
    SEBI_CATEGORY_NAMES,
    parse_bse_response,
    parse_nse_response,
    parse_sebi_page,
)

# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------

EXIT_OK = 0       # Success
EXIT_ERROR = 1    # General error (fetch failed, parse error, etc.)
EXIT_PARTIAL = 2  # Partial success (some sources failed)
EXIT_FATAL = 3    # Fatal error (DB inaccessible, bad config, etc.)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BSE_API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_ANNOUNCEMENTS_URL = f"{BSE_API_BASE}/AnnSubCategoryGetData/w"
BSE_PAGE_SIZE = 50

NSE_API_BASE = "https://www.nseindia.com/api"
NSE_ENDPOINTS: dict[str, str] = {
    "announcements": f"{NSE_API_BASE}/corporate-announcements",
    "annual_reports": f"{NSE_API_BASE}/annual-reports",
    "board_meetings": f"{NSE_API_BASE}/corporate-board-meetings",
    "financial_results": f"{NSE_API_BASE}/corporates-financial-results",
}

SEBI_FILINGS_URL = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
SEBI_PAGE_SIZE = 25

DELAY_BETWEEN_PAGES = 1.5

log = logging.getLogger("india-scraper")


def _configure_logging(log_file: str | None = None) -> None:
    """Configure root logger with optional file output.

    Args:
        log_file: Optional file path to write log output. If None, logs only
                  to stdout.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
        force=True,
    )


# ---------------------------------------------------------------------------
# BSE fetcher
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
    """Fetch one page of BSE announcements.

    Args:
        session: Requests session.
        page_num: 1-based page number.
        from_date: Start date filter (YYYY-MM-DD format accepted by BSE).
        to_date: End date filter (YYYY-MM-DD format).
        category: BSE category filter string.
        subcategory: BSE subcategory filter string.
        scrip: BSE scrip code filter.
        search_type: 'P' for paginated, 'A' for date-range.
        filing_type: 'C' for company filings (default).

    Returns:
        Tuple of (list_of_filing_dicts, total_row_count).
    """
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
    return parse_bse_response(data)


# ---------------------------------------------------------------------------
# NSE fetcher
# ---------------------------------------------------------------------------


def fetch_nse_endpoint(
    session: requests.Session,
    endpoint_type: str = "announcements",
    index_type: str = "equities",
    from_date: str = "",
    to_date: str = "",
    symbol: str = "",
) -> list[dict]:
    """Fetch any NSE endpoint and normalize to the common filing format.

    Args:
        session: Requests session.
        endpoint_type: One of the NSE_ENDPOINTS keys.
        index_type: NSE index (e.g. 'equities', 'sme').
        from_date: Start date (DD-MM-YYYY format required by NSE).
        to_date: End date (DD-MM-YYYY format).
        symbol: Optional symbol filter.

    Returns:
        List of normalized filing dicts.
    """
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
    return parse_nse_response(data, endpoint_type)


def fetch_nse_paginated(
    session: requests.Session,
    max_pages: int = 10,
    endpoint_type: str = "announcements",
    index_type: str = "equities",
    symbol: str = "",
    days_per_page: int = 7,
    from_date_override: str = "",
    to_date_override: str = "",
    resume_date: str = "",
) -> list[dict]:
    """Paginate NSE by sliding date windows backwards.

    NSE has no built-in page number parameter; we simulate pagination by
    sliding the date window earlier by `days_per_page` days on each iteration.

    If from_date_override/to_date_override are both set, fetches that single
    range and returns immediately.

    Args:
        session: Requests session.
        max_pages: Maximum date-window iterations.
        endpoint_type: NSE endpoint type.
        index_type: NSE index.
        symbol: Optional symbol filter.
        days_per_page: Width of each date window in days.
        from_date_override: If set, use as exact start date.
        to_date_override: If set, use as exact end date.
        resume_date: If set (DD-MM-YYYY), resume crawl from this date (--resume).

    Returns:
        List of all collected filing dicts.
    """
    all_filings: list[dict] = []

    if from_date_override or to_date_override:
        log.info(
            "NSE %s: %s to %s",
            endpoint_type,
            from_date_override or "start",
            to_date_override or "now",
        )
        try:
            filings = fetch_nse_endpoint(
                session,
                endpoint_type=endpoint_type,
                index_type=index_type,
                from_date=from_date_override,
                to_date=to_date_override,
                symbol=symbol,
            )
            all_filings.extend(filings)
            log.info("  %d filings", len(filings))
        except (requests.RequestException, ValueError) as exc:
            log.warning("NSE %s fetch failed: %s", endpoint_type, exc)
        return all_filings

    # Determine sliding window start
    if resume_date:
        try:
            end_date = datetime.strptime(resume_date, "%d-%m-%Y")
        except ValueError:
            log.warning("NSE: invalid resume_date %r, ignoring", resume_date)
            end_date = datetime.now()
    else:
        end_date = datetime.now()

    for page in range(max_pages):
        start_date = end_date - timedelta(days=days_per_page)
        from_str = start_date.strftime("%d-%m-%Y")
        to_str = end_date.strftime("%d-%m-%Y")

        log.info("NSE %s page %d: %s to %s", endpoint_type, page + 1, from_str, to_str)

        try:
            filings = fetch_nse_endpoint(
                session,
                endpoint_type=endpoint_type,
                index_type=index_type,
                from_date=from_str,
                to_date=to_str,
                symbol=symbol,
            )
        except (requests.RequestException, ValueError) as exc:
            log.warning(
                "NSE %s fetch failed for %s to %s: %s — skipping",
                endpoint_type,
                from_str,
                to_str,
                exc,
            )
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
# SEBI fetcher
# ---------------------------------------------------------------------------


def fetch_sebi_page(
    session: requests.Session,
    page_num: int = 0,
    category_id: int = 15,
    from_date: str = "",
    to_date: str = "",
    search: str = "",
) -> tuple[list[dict], bool]:
    """Fetch one page of SEBI filings.

    SEBI uses a Struts AJAX POST endpoint that returns HTML split by '#@#'.
    The required User-Agent, Referer, and Origin headers prevent HTTP 530 BLOCKED.

    Args:
        session: Requests session.
        page_num: 0-based page index.
        category_id: Numeric SEBI category ID.
        from_date: Start date filter.
        to_date: End date filter.
        search: Optional keyword search.

    Returns:
        Tuple of (list_of_filing_dicts, has_more_pages).
    """
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
    return parse_sebi_page(resp.text, category_id, page_num)


# ---------------------------------------------------------------------------
# Crawl helpers
# ---------------------------------------------------------------------------


def _crawl_bse(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    from_date: str = "",
    to_date: str = "",
    incremental: bool = False,
    resume: bool = False,
) -> tuple[int, int, int]:
    """Crawl BSE announcements.

    Args:
        session: Requests session.
        cache: Filing cache.
        doc_dir: Document download directory.
        max_pages: Maximum pages to fetch.
        download: Whether to download documents.
        parallel: Download worker count.
        from_date: Date filter start.
        to_date: Date filter end.
        incremental: Stop on first page with no new filings.
        resume: Resume from last saved page in cache.

    Returns:
        Tuple of (total_filings, new_filings, downloaded_count).
    """
    total_f = total_new = total_dl = 0
    pages_crawled = 0
    errors: list[str] = []
    search_type = "A" if (from_date or to_date) else "P"

    start_page = 1
    if resume:
        saved = cache.get_crawl_state("bse", "last_page")
        if saved:
            try:
                start_page = int(saved) + 1
                log.info("BSE: resuming from page %d", start_page)
            except ValueError:
                pass

    if from_date or to_date:
        log.info("BSE date filter: %s to %s", from_date or "start", to_date or "now")

    log_id = cache.log_crawl_start("bse", source="bse")

    try:
        for page_num in range(start_page, start_page + max_pages):
            if page_num > start_page:
                time.sleep(DELAY_BETWEEN_PAGES)

            try:
                filings, total_count = fetch_bse_page(
                    session,
                    page_num=page_num,
                    from_date=from_date,
                    to_date=to_date,
                    search_type=search_type,
                )
            except (requests.RequestException, ValueError) as exc:
                log.warning("BSE page %d fetch failed: %s — skipping", page_num, exc)
                errors.append(str(exc))
                continue

            pages_crawled += 1

            if not filings:
                log.info("BSE page %d: no filings. Stopping.", page_num)
                break

            new = cache.insert_batch(filings, page_num)
            total_f += len(filings)
            total_new += new

            max_page = (
                (total_count + BSE_PAGE_SIZE - 1) // BSE_PAGE_SIZE if total_count else "?"
            )
            log.info(
                "BSE page %d/%s: %d filings (%d new) [total: %s]",
                page_num,
                max_page,
                len(filings),
                new,
                total_count,
            )

            cache.save_crawl_state("bse", "last_page", str(page_num))

            if download and filings:
                dl = download_filings(session, filings, doc_dir, cache, parallel)
                total_dl += dl

            if incremental and new == 0 and page_num > start_page:
                log.info("BSE: no new filings — caught up (--incremental).")
                break
            elif not incremental and new == 0 and page_num > start_page + 1:
                log.info("BSE: no new filings — caught up.")
                break
    finally:
        cache.log_crawl_complete(
            log_id,
            filings_found=total_f,
            filings_new=total_new,
            pages_crawled=pages_crawled,
            errors="; ".join(errors) if errors else "",
        )

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
    incremental: bool = False,
    resume: bool = False,
) -> tuple[int, int, int]:
    """Crawl NSE filings via date-range pagination, optionally multi-type.

    Args:
        session: Requests session.
        cache: Filing cache.
        doc_dir: Document download directory.
        max_pages: Date-window iterations per type.
        download: Whether to download documents.
        parallel: Download worker count.
        nse_types: List of NSE endpoint types (or ['all']).
        from_date: Date filter start (DD-MM-YYYY).
        to_date: Date filter end (DD-MM-YYYY).
        incremental: Not used for NSE (date-window pagination is inherently incremental).
        resume: Resume from last saved date per type.

    Returns:
        Tuple of (total_filings, new_filings, downloaded_count).
    """
    if not nse_types:
        nse_types = ["announcements"]
    if "all" in nse_types:
        nse_types = list(NSE_ENDPOINTS.keys())

    total_f = total_new = total_dl = 0

    log_id = cache.log_crawl_start("nse", source="nse")
    total_errors: list[str] = []

    try:
        for nse_type in nse_types:
            log.info("--- NSE: %s ---", nse_type)

            resume_date = ""
            if resume:
                resume_date = cache.get_crawl_state("nse", f"last_date_{nse_type}") or ""
                if resume_date:
                    log.info("NSE %s: resuming from %s", nse_type, resume_date)

            filings = fetch_nse_paginated(
                session,
                max_pages=max_pages,
                endpoint_type=nse_type,
                from_date_override=from_date,
                to_date_override=to_date,
                resume_date=resume_date,
            )
            new = cache.insert_batch(filings)
            total_f += len(filings)
            total_new += new

            # Persist last fetched date for resume
            if filings:
                cache.save_crawl_state(
                    "nse",
                    f"last_date_{nse_type}",
                    datetime.now().strftime("%d-%m-%Y"),
                )

            if download and filings:
                dl = download_filings(session, filings, doc_dir, cache, parallel)
                total_dl += dl

            log.info(
                "NSE %s: %d filings (%d new), %d downloaded",
                nse_type,
                len(filings),
                new,
                total_dl,
            )
    finally:
        cache.log_crawl_complete(
            log_id,
            filings_found=total_f,
            filings_new=total_new,
            pages_crawled=len(nse_types),
            errors="; ".join(total_errors) if total_errors else "",
        )

    return total_f, total_new, total_dl


def _crawl_sebi(
    session: requests.Session,
    cache: FilingCache,
    doc_dir: str,
    max_pages: int,
    download: bool,
    parallel: int,
    categories: list[str] | None = None,
    incremental: bool = False,
    resume: bool = False,
) -> tuple[int, int, int]:
    """Crawl SEBI filings for one or more categories.

    Args:
        session: Requests session.
        cache: Filing cache.
        doc_dir: Document download directory.
        max_pages: Maximum pages per category.
        download: Whether to download documents.
        parallel: Download worker count.
        categories: List of SEBI category keys (or ['all']).
        incremental: Stop on first page with no new filings.
        resume: Resume from last saved page per category.

    Returns:
        Tuple of (total_filings, new_filings, downloaded_count).
    """
    if not categories:
        categories = ["public_issues"]
    if "all" in categories:
        categories = list(SEBI_CATEGORIES.keys())

    total_f = total_new = total_dl = 0

    log_id = cache.log_crawl_start("sebi", source="sebi")

    try:
        for category in categories:
            category_id = SEBI_CATEGORIES.get(category, 15)
            category_name = SEBI_CATEGORY_NAMES.get(category_id, f"Category {category_id}")
            log.info("--- SEBI: %s ---", category_name)

            start_page = 0
            if resume:
                saved = cache.get_crawl_state("sebi", f"last_page_{category}")
                if saved:
                    try:
                        start_page = int(saved) + 1
                        log.info("SEBI %s: resuming from page %d", category, start_page)
                    except ValueError:
                        pass

            cat_f, cat_new, cat_dl = _crawl_sebi_category(
                session,
                cache,
                doc_dir,
                max_pages,
                download,
                parallel,
                category_id,
                category_name,
                category_key=category,
                start_page=start_page,
                incremental=incremental,
            )
            total_f += cat_f
            total_new += cat_new
            total_dl += cat_dl
    finally:
        cache.log_crawl_complete(
            log_id,
            filings_found=total_f,
            filings_new=total_new,
            pages_crawled=len(categories),
        )

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
    category_key: str = "",
    start_page: int = 0,
    incremental: bool = False,
) -> tuple[int, int, int]:
    """Crawl SEBI filings for a single category.

    Args:
        session: Requests session.
        cache: Filing cache.
        doc_dir: Document download directory.
        max_pages: Maximum pages to fetch.
        download: Whether to download documents.
        parallel: Download worker count.
        category_id: Numeric SEBI category.
        category_name: Human-readable category name.
        category_key: Category key string for state persistence.
        start_page: 0-based page to start from (for resume).
        incremental: Stop on first page with no new filings.

    Returns:
        Tuple of (total_filings, new_filings, downloaded_count).
    """
    total_f = total_new = total_dl = 0

    for page_num in range(start_page, start_page + max_pages):
        if page_num > start_page:
            time.sleep(DELAY_BETWEEN_PAGES)

        try:
            filings, has_more = fetch_sebi_page(
                session,
                page_num=page_num,
                category_id=category_id,
            )
        except (requests.RequestException, ValueError) as exc:
            log.warning("SEBI page %d fetch failed: %s — skipping", page_num + 1, exc)
            continue

        if not filings:
            log.info("SEBI page %d: no filings. Stopping.", page_num + 1)
            break

        new = cache.insert_batch(filings, page_num + 1)
        total_f += len(filings)
        total_new += new

        log.info(
            "SEBI page %d: %d filings (%d new) [%s]",
            page_num + 1,
            len(filings),
            new,
            category_name,
        )

        if category_key:
            cache.save_crawl_state("sebi", f"last_page_{category_key}", str(page_num))

        if download and filings:
            dl = download_filings(session, filings, doc_dir, cache, parallel)
            total_dl += dl

        if incremental and new == 0 and page_num > start_page:
            log.info("SEBI %s: no new filings — caught up (--incremental).", category_name)
            break

        if not has_more:
            break

    return total_f, total_new, total_dl


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_crawl(args: argparse.Namespace) -> int:
    """Execute the crawl command.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Exit code (EXIT_OK / EXIT_ERROR / EXIT_PARTIAL / EXIT_FATAL).
    """
    try:
        session = create_session()
    except Exception as exc:  # pragma: no cover
        log.error("Failed to create HTTP session: %s", exc)
        return EXIT_FATAL

    try:
        cache = FilingCache(args.db)
    except Exception as exc:
        log.error("Failed to open DB %r: %s", args.db, exc)
        return EXIT_FATAL

    source_errors: list[str] = []
    try:
        doc_dir = args.doc_dir
        t_start = time.time()
        total_filings = 0
        total_new = 0
        total_downloaded = 0

        sources = (
            ["bse", "nse", "sebi"] if args.source == "all" else [args.source]
        )

        for source in sources:
            log.info("=== Crawling %s ===", source.upper())

            try:
                if source == "bse":
                    total_f, new_f, dl_f = _crawl_bse(
                        session,
                        cache,
                        doc_dir,
                        args.max_pages,
                        args.download,
                        args.parallel,
                        from_date=args.from_date,
                        to_date=args.to_date,
                        incremental=args.incremental,
                        resume=args.resume,
                    )
                elif source == "nse":
                    total_f, new_f, dl_f = _crawl_nse(
                        session,
                        cache,
                        doc_dir,
                        args.max_pages,
                        args.download,
                        args.parallel,
                        nse_types=args.nse_type,
                        from_date=args.from_date,
                        to_date=args.to_date,
                        incremental=args.incremental,
                        resume=args.resume,
                    )
                elif source == "sebi":
                    total_f, new_f, dl_f = _crawl_sebi(
                        session,
                        cache,
                        doc_dir,
                        args.max_pages,
                        args.download,
                        args.parallel,
                        categories=args.sebi_category,
                        incremental=args.incremental,
                        resume=args.resume,
                    )
                else:
                    log.error("Unknown source: %s", source)
                    source_errors.append(source)
                    continue
            except Exception as exc:
                log.error("Source %s failed: %s", source, exc)
                source_errors.append(source)
                continue

            total_filings += total_f
            total_new += new_f
            total_downloaded += dl_f

        elapsed = time.time() - t_start
        log.info(
            "Done: %d filings (%d new), %d downloaded in %.1fs.",
            total_filings,
            total_new,
            total_downloaded,
            elapsed,
        )
    finally:
        cache.close()

    if source_errors and len(source_errors) == len(
        ["bse", "nse", "sebi"] if args.source == "all" else [args.source]
    ):
        return EXIT_ERROR  # All sources failed
    if source_errors:
        return EXIT_PARTIAL  # Some sources failed
    return EXIT_OK


def cmd_monitor(args: argparse.Namespace) -> int:
    """Execute the monitor command.

    Args:
        args: Parsed CLI arguments.

    Returns:
        EXIT_OK when stopped, EXIT_FATAL if DB cannot be opened.
    """
    try:
        cache = FilingCache(args.db)
    except Exception as exc:
        log.error("Failed to open DB %r: %s", args.db, exc)
        return EXIT_FATAL
    known_keys = cache.get_known_keys(args.source if args.source != "all" else "")
    session = create_session()

    log.info(
        "Monitoring %s for new filings every %ds. Known: %d. Ctrl+C to stop.",
        args.source.upper(),
        args.interval,
        len(known_keys),
    )

    polls = 0
    try:
        while True:
            polls += 1
            new_count = 0

            if args.source in ("bse", "all"):
                try:
                    filings, _ = fetch_bse_page(session, page_num=1)
                    new_filings = [
                        f
                        for f in filings
                        if f"{f['source']}|{f['filing_id']}" not in known_keys
                    ]
                    if new_filings:
                        n = cache.insert_batch(new_filings)
                        new_count += n
                        for f in new_filings:
                            known_keys.add(f"{f['source']}|{f['filing_id']}")
                        if args.download:
                            download_filings(
                                session, new_filings, args.doc_dir, cache, args.parallel
                            )
                        log.info("[Poll %d] BSE: %d new filings", polls, len(new_filings))
                except (requests.RequestException, ValueError) as exc:
                    log.warning("[Poll %d] BSE fetch error: %s", polls, exc)

            if args.source in ("nse", "all"):
                try:
                    today = datetime.now()
                    yesterday = today - timedelta(days=1)
                    filings = fetch_nse_endpoint(
                        session,
                        from_date=yesterday.strftime("%d-%m-%Y"),
                        to_date=today.strftime("%d-%m-%Y"),
                    )
                    new_filings = [
                        f
                        for f in filings
                        if f"{f['source']}|{f['filing_id']}" not in known_keys
                    ]
                    if new_filings:
                        n = cache.insert_batch(new_filings)
                        new_count += n
                        for f in new_filings:
                            known_keys.add(f"{f['source']}|{f['filing_id']}")
                        if args.download:
                            download_filings(
                                session, new_filings, args.doc_dir, cache, args.parallel
                            )
                        log.info("[Poll %d] NSE: %d new filings", polls, len(new_filings))
                except (requests.RequestException, ValueError) as exc:
                    log.warning("[Poll %d] NSE fetch error: %s", polls, exc)

            if new_count == 0:
                log.info("[Poll %d] No new filings. Known: %d", polls, len(known_keys))

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Monitor stopped. %d polls, %d filings known.", polls, len(known_keys))
    finally:
        cache.close()
    return EXIT_OK


def cmd_export(args: argparse.Namespace) -> int:
    """Execute the export command.

    Args:
        args: Parsed CLI arguments.

    Returns:
        EXIT_OK on success, EXIT_FATAL / EXIT_ERROR on failure.
    """
    try:
        cache = FilingCache(args.db)
    except Exception as exc:
        log.error("Failed to open DB %r: %s", args.db, exc)
        return EXIT_FATAL

    try:
        cache.export_json(
            args.output, source=args.source if args.source != "all" else ""
        )
        log.info("Exported to %s", args.output)
        return EXIT_OK
    except OSError as exc:
        log.error("Failed to write export file %r: %s", args.output, exc)
        return EXIT_ERROR
    finally:
        cache.close()


def _compute_health(
    total: int,
    last_completed_at: str | None,
) -> str:
    """Compute a health status string based on filing counts and crawl recency.

    Uses the most recent crawl completion timestamp from crawl_log to determine
    freshness.  The 48-hour threshold is the primary signal; the filing-date
    fallback (newest) is no longer used.

    Health levels:
      - "empty":    no filings at all
      - "ok":       a completed crawl exists within the last 48 hours
      - "stale":    last completed crawl was between 48 hours and 30 days ago
      - "degraded": last completed crawl was more than 30 days ago
      - "error":    cannot determine (no completed crawl, or unparseable timestamp)

    Args:
        total: Total number of filings in the DB.
        last_completed_at: ISO datetime string of the most recent completed crawl
            from crawl_log.completed_at, or None when no completed crawl exists.

    Returns:
        Health status string.
    """
    if total == 0:
        return "empty"
    if not last_completed_at:
        return "error"

    try:
        completed_dt = datetime.fromisoformat(last_completed_at)
        age_hours = (datetime.now() - completed_dt).total_seconds() / 3600
        if age_hours <= 48:
            return "ok"
        age_days = age_hours / 24
        if age_days <= 30:
            return "stale"
        return "degraded"
    except ValueError:
        return "error"


def _documents_dir_size(doc_dir: str) -> int:
    """Return total bytes in the documents directory (0 if not present)."""
    if not os.path.isdir(doc_dir):
        return 0
    total = 0
    for dirpath, _dirnames, filenames in os.walk(doc_dir):
        for fname in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, fname))
            except OSError:
                pass
    return total


def cmd_stats(args: argparse.Namespace) -> int:
    """Execute the stats command.

    Args:
        args: Parsed CLI arguments.

    Returns:
        EXIT_OK on success, EXIT_FATAL if DB cannot be opened.
    """
    try:
        cache = FilingCache(args.db)
    except Exception as exc:
        log.error("Failed to open DB %r: %s", args.db, exc)
        if getattr(args, "json", False):
            print(json.dumps({"error": str(exc), "health": "error"}, indent=2))
        return EXIT_FATAL

    try:
        if getattr(args, "json", False):
            # --- JSON output ---
            s = cache.stats()
            total = int(s["total"] or 0)
            downloaded = int(s["downloaded"] or 0)
            pending = int(s["pending"] or 0)
            newest = s.get("newest") or None
            oldest = s.get("oldest") or None

            db_size = 0
            try:
                db_size = os.path.getsize(args.db)
            except OSError:
                pass

            doc_dir = getattr(args, "doc_dir", "documents")
            docs_size = _documents_dir_size(doc_dir)

            unique_companies = cache.unique_companies()
            crawl_runs = cache.total_crawl_runs()
            last_completed_at = cache.last_crawl_completed_at()
            health = _compute_health(total, last_completed_at)

            output = {
                "scraper": "india-scraper",
                "country": "IN",
                "sources": ["bse", "nse", "sebi"],
                "total_filings": total,
                "downloaded": downloaded,
                "pending_download": pending,
                "unique_companies": unique_companies,
                "total_crawl_runs": crawl_runs,
                "earliest_record": oldest,
                "latest_record": newest,
                "db_size_bytes": db_size,
                "documents_size_bytes": docs_size,
                "health": health,
            }
            print(json.dumps(output, indent=2))
            return EXIT_OK

        # --- Human-readable output ---
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

        return EXIT_OK
    finally:
        cache.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point: parse CLI arguments and dispatch to command handlers."""
    p = argparse.ArgumentParser(
        description="India Securities Filing Scraper (BSE, NSE, SEBI)",
    )
    sub = p.add_subparsers(dest="command")

    # --- crawl ---
    c = sub.add_parser("crawl", help="Crawl filings from Indian exchanges")
    c.add_argument(
        "--source",
        choices=["bse", "nse", "sebi", "all"],
        default="bse",
        help="Filing source (default: bse)",
    )
    c.add_argument("--max-pages", type=int, default=10)
    c.add_argument("--download", action="store_true", help="Download documents")
    c.add_argument(
        "--parallel", type=int, default=5, help="Download workers (default: 5)"
    )
    c.add_argument("--doc-dir", default="documents")
    c.add_argument("--db", default=DB_FILE)
    c.add_argument(
        "--from-date",
        default="",
        help="Start date (BSE: YYYY-MM-DD, NSE: DD-MM-YYYY)",
    )
    c.add_argument(
        "--to-date",
        default="",
        help="End date (BSE: YYYY-MM-DD, NSE: DD-MM-YYYY)",
    )
    c.add_argument(
        "--sebi-category",
        nargs="+",
        default=["public_issues"],
        choices=list(SEBI_CATEGORIES.keys()) + ["all"],
        help="SEBI filing categories (default: public_issues, use 'all' for everything)",
    )
    c.add_argument(
        "--nse-type",
        nargs="+",
        default=["announcements"],
        choices=[
            "announcements",
            "annual_reports",
            "board_meetings",
            "financial_results",
            "all",
        ],
        help="NSE data types to crawl (default: announcements)",
    )
    c.add_argument(
        "--incremental",
        action="store_true",
        help="Stop on the first page with no new filings (faster catch-up)",
    )
    c.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last saved crawl position in the cache",
    )
    c.add_argument("--log-file", default="", help="Optional log file path")

    # --- monitor ---
    m = sub.add_parser("monitor", help="Watch for new filings")
    m.add_argument(
        "--source",
        choices=["bse", "nse", "all"],
        default="bse",
        help="Source to monitor (default: bse)",
    )
    m.add_argument(
        "--interval", type=int, default=300, help="Poll interval secs (default: 300)"
    )
    m.add_argument("--download", action="store_true", help="Auto-download new filings")
    m.add_argument("--parallel", type=int, default=5)
    m.add_argument("--doc-dir", default="documents")
    m.add_argument("--db", default=DB_FILE)
    m.add_argument("--log-file", default="", help="Optional log file path")

    # --- export ---
    e = sub.add_parser("export", help="Export filings to JSON")
    e.add_argument("--output", default="filings.json")
    e.add_argument("--source", choices=["bse", "nse", "sebi", "all"], default="all")
    e.add_argument("--db", default=DB_FILE)
    e.add_argument("--log-file", default="", help="Optional log file path")

    # --- stats ---
    st = sub.add_parser("stats", help="Show cache statistics")
    st.add_argument("--source", choices=["bse", "nse", "sebi", "all"], default="all")
    st.add_argument("--db", default=DB_FILE)
    st.add_argument(
        "--json",
        action="store_true",
        help="Output statistics as a JSON object (machine-readable)",
    )
    st.add_argument(
        "--doc-dir",
        default="documents",
        help="Documents directory (used for disk size in --json mode)",
    )
    st.add_argument("--log-file", default="", help="Optional log file path")

    args = p.parse_args()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    log_file = getattr(args, "log_file", "") or ""
    _configure_logging(log_file or None)

    cmds = {
        "crawl": cmd_crawl,
        "monitor": cmd_monitor,
        "export": cmd_export,
        "stats": cmd_stats,
    }

    if args.command in cmds:
        exit_code = cmds[args.command](args)
        sys.exit(exit_code if isinstance(exit_code, int) else EXIT_OK)
    else:
        p.print_help()
        sys.exit(EXIT_OK)


if __name__ == "__main__":
    main()
