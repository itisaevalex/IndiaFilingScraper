"""
downloader.py — Document download logic for BSE, NSE, and SEBI filings.

BSE:  direct PDF URLs, no special routing needed beyond the URL built by parsers.py
NSE:  direct PDF/XBRL URLs, downloadable as-is
SEBI: main filings are .html pages that embed a PDF viewer; we resolve to the
      actual PDF URL before downloading.
"""

from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from db import FilingCache
from http_utils import DOWNLOAD_HEADERS, SEBI_HEADERS

log = logging.getLogger("india-scraper")

DELAY_BETWEEN_DOWNLOADS = 0.3


# ---------------------------------------------------------------------------
# SEBI PDF resolution
# ---------------------------------------------------------------------------


def resolve_sebi_pdf(session: requests.Session, html_url: str) -> str:
    """Follow a SEBI filing HTML page to extract the embedded PDF URL.

    SEBI main filing pages use a PDF viewer iframe/embed.  The actual PDF
    URL is in a `file=` query param of the viewer src attribute.

    Args:
        session: Requests session (with SEBI headers pre-set or passed separately).
        html_url: URL of the SEBI filing HTML page.

    Returns:
        Direct PDF URL if found, otherwise the original html_url as fallback.
    """
    from bs4 import BeautifulSoup

    try:
        resp = session.get(html_url, headers=SEBI_HEADERS, timeout=30)
        if resp.status_code != 200:
            return html_url

        soup = BeautifulSoup(resp.text, "lxml")

        # Check iframe/embed for PDF viewer with file= param
        for tag in soup.find_all(["iframe", "embed"]):
            src = tag.get("src", tag.get("data", ""))
            pdf_match = re.search(r"file=(https?://[^\s&\"']+\.pdf)", src)
            if pdf_match:
                return pdf_match.group(1)

        # Check direct PDF links within SEBI's data directory
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "sebi_data" in href and href.endswith(".pdf"):
                return href if href.startswith("http") else f"https://www.sebi.gov.in{href}"

        return html_url
    except Exception as exc:
        log.warning("SEBI PDF resolution failed for %s: %s", html_url, exc)
        return html_url


# ---------------------------------------------------------------------------
# Core download function
# ---------------------------------------------------------------------------


def download_filings(
    session: requests.Session,
    filings: list[dict],
    doc_dir: str,
    cache: FilingCache,
    parallel: int = 5,
) -> int:
    """Download documents for a batch of filings in parallel.

    Thread safety: each worker reads the shared session (safe — urllib3's
    connection pool handles concurrent use).  All SQLite writes happen on the
    calling thread after workers complete; do NOT call cache.mark_downloaded
    inside worker threads.

    Args:
        session: Shared requests session.
        filings: List of filing dicts.  Only those with a non-empty
                 'document_url' are processed.
        doc_dir: Directory to save downloaded files.
        cache: FilingCache instance for marking downloads complete.
        parallel: Max worker threads (default 5).

    Returns:
        Count of successfully downloaded files.
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

        try:
            resp = session.get(url, headers=DOWNLOAD_HEADERS, timeout=120)
            if resp.status_code != 200:
                log.warning("Download HTTP %d for %s", resp.status_code, url)
                return None
        except requests.RequestException as exc:
            log.warning("Download failed for %s: %s", url, exc)
            return None

        # Determine filename; use os.path.basename to prevent path traversal
        cd = resp.headers.get("content-disposition", "")
        fname_match = re.search(r'filename="?([^";\n]+)', cd)
        if fname_match:
            fname = os.path.basename(fname_match.group(1).strip())
        else:
            fname = os.path.basename(url.split("?")[0])

        # Ensure the filename has an extension
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
        except OSError as exc:
            log.error("I/O error saving %s: %s", filepath, exc)
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
