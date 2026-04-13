"""
http_utils.py — Session factory with per-source header configs and retry logic.

Each source (BSE, NSE, SEBI) requires specific headers to avoid 403/530 blocks:
  - BSE: Referer + Origin pointing to bseindia.com (API rejects requests without them)
  - NSE: Browser User-Agent (returns 403 without it)
  - SEBI: User-Agent + Referer + Origin (returns HTTP 530 BLOCKED without them)
"""

from __future__ import annotations

import requests
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Shared browser User-Agent
# ---------------------------------------------------------------------------

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Per-source header sets
# ---------------------------------------------------------------------------

BSE_HEADERS: dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    # CRITICAL: BSE API at api.bseindia.com rejects requests without these two
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
    "User-Agent": _UA,
}

NSE_HEADERS: dict[str, str] = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9",
    # CRITICAL: NSE API returns 403 without a realistic browser User-Agent
    "User-Agent": _UA,
}

SEBI_HEADERS: dict[str, str] = {
    # CRITICAL: SEBI returns HTTP 530 BLOCKED without all three of these headers
    "User-Agent": _UA,
    "Referer": "https://www.sebi.gov.in/filings.html",
    "Origin": "https://www.sebi.gov.in",
}

DOWNLOAD_HEADERS: dict[str, str] = {
    "User-Agent": _UA,
}

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------


def create_session() -> requests.Session:
    """Create a requests.Session with retry/backoff on transient errors.

    Retries up to 3 times with exponential backoff on 429, 500, 502, 503, 504.
    Connection pool is sized for moderate parallel use.

    Returns:
        Configured requests.Session instance.
    """
    session = requests.Session()
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
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
