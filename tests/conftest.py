"""
conftest.py — Shared pytest fixtures for all test modules.
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# ---------------------------------------------------------------------------
# File fixture loaders
# ---------------------------------------------------------------------------


@pytest.fixture
def bse_response_data() -> dict:
    """Load the BSE JSON response fixture."""
    with open(os.path.join(FIXTURES_DIR, "bse_response.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def nse_announcements_data() -> list:
    """Load the NSE announcements JSON fixture."""
    with open(os.path.join(FIXTURES_DIR, "nse_announcements.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def nse_annual_reports_data() -> list:
    """Load the NSE annual reports JSON fixture."""
    with open(os.path.join(FIXTURES_DIR, "nse_annual_reports.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def nse_board_meetings_data() -> list:
    """Load the NSE board meetings JSON fixture."""
    with open(os.path.join(FIXTURES_DIR, "nse_board_meetings.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def nse_financial_results_data() -> list:
    """Load the NSE financial results JSON fixture."""
    with open(
        os.path.join(FIXTURES_DIR, "nse_financial_results.json"), encoding="utf-8"
    ) as f:
        return json.load(f)


@pytest.fixture
def sebi_response_text() -> str:
    """Load the SEBI #@# response fixture."""
    with open(os.path.join(FIXTURES_DIR, "sebi_response.txt"), encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Database fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db():
    """Provide a temporary FilingCache backed by a real SQLite file.

    Yields the FilingCache instance and cleans up after the test.
    """
    import sys

    # Ensure the project root is on sys.path
    project_root = os.path.dirname(os.path.dirname(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from db import FilingCache

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    cache = FilingCache(db_path)
    yield cache
    cache.close()
    os.unlink(db_path)


# ---------------------------------------------------------------------------
# Sample filing dicts
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_bse_filing() -> dict:
    """A minimal valid BSE filing dict (L3 spec — ISO date)."""
    return {
        "source": "bse",
        "filing_id": "TEST_BSE_001",
        "company_name": "Test Corp",
        "ticker": "TESTCORP",
        "symbol": "TESTCORP",
        "isin": "",
        "category": "Board Meeting",
        "subcategory": "",
        "headline": "Board Meeting Notice",
        "subject": "Board Meeting Notice",
        "description": "Notice of Board Meeting",
        "filing_date": "2024-01-01",          # ISO date (already normalized)
        "filing_time": "10:00:00",
        "document_url": "https://www.bseindia.com/xml-data/corpfiling/AttachLive/test.pdf",
        "direct_download_url": "https://www.bseindia.com/xml-data/corpfiling/AttachLive/test.pdf",
        "file_size": "12345",
        "has_xbrl": False,
        "raw_json": "{}",
        "raw_metadata": "{}",
        "country": "IN",
    }


@pytest.fixture
def sample_nse_filing() -> dict:
    """A minimal valid NSE filing dict (L3 spec — ISO date)."""
    return {
        "source": "nse",
        "filing_id": "TEST_NSE_001",
        "company_name": "NSE Test Corp",
        "ticker": "NSETEST",
        "symbol": "NSETEST",
        "isin": "INE123456789",
        "category": "Financial Results",
        "subcategory": "IT",
        "headline": "Q3 Financial Results",
        "subject": "Q3 Financial Results",
        "description": "Q3 Financial Results",
        "filing_date": "2024-01-01",          # ISO date (already normalized)
        "filing_time": "10:00:00",
        "document_url": "https://nsearchives.nseindia.com/corporate/xbrl/test.pdf",
        "direct_download_url": "https://nsearchives.nseindia.com/corporate/xbrl/test.pdf",
        "file_size": "98765",
        "has_xbrl": True,
        "raw_json": "{}",
        "raw_metadata": "{}",
        "country": "IN",
    }


@pytest.fixture
def sample_sebi_filing() -> dict:
    """A minimal valid SEBI filing dict (L3 spec — ISO date)."""
    return {
        "source": "sebi",
        "filing_id": "12345",
        "company_name": "SEBI Test Corp IPO",
        "ticker": "",
        "symbol": "",
        "isin": "",
        "category": "Public Issues",
        "subcategory": "",
        "headline": "Draft Red Herring Prospectus",
        "subject": "Draft Red Herring Prospectus",
        "description": "",
        "filing_date": "2024-01-10",          # ISO date (already normalized)
        "filing_time": "",
        "document_url": "https://www.sebi.gov.in/sebi_data/commondocs/filings/PublicIssues_12345.html",
        "direct_download_url": "https://www.sebi.gov.in/sebi_data/commondocs/filings/PublicIssues_12345.html",
        "file_size": "",
        "has_xbrl": False,
        "raw_json": "",
        "raw_metadata": "",
        "country": "IN",
    }
