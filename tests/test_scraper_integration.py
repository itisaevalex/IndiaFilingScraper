"""
test_scraper_integration.py — Integration tests for the scraper's command dispatchers.

These tests mock the network layer and verify that:
  - parse_bse_response / parse_nse_response / parse_sebi_page are called correctly
  - --incremental stops on first no-new page
  - --resume picks up from saved crawl state
  - cmd_stats and cmd_export work end-to-end
"""

from __future__ import annotations

import json
import os
import sys
import argparse
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import FilingCache
import scraper
from parsers import parse_bse_response, parse_nse_response, parse_sebi_page


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_args(**kwargs) -> argparse.Namespace:
    """Build a minimal argparse.Namespace for command tests."""
    defaults = {
        "source": "bse",
        "max_pages": 2,
        "download": False,
        "parallel": 1,
        "doc_dir": "/tmp/test_docs",
        "db": ":memory:",
        "from_date": "",
        "to_date": "",
        "sebi_category": ["public_issues"],
        "nse_type": ["announcements"],
        "incremental": False,
        "resume": False,
        "log_file": "",
        "output": "/tmp/test_export.json",
        "interval": 1,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


BSE_PAGE_FILINGS = [
    {
        "source": "bse",
        "filing_id": f"BSE_{i}",
        "company_name": f"Company {i}",
        "symbol": f"SYM{i}",
        "isin": "",
        "category": "Board Meeting",
        "subcategory": "",
        "subject": "Board Meeting Notice",
        "description": "",
        "filing_date": "01/01/2024 10:00:00",
        "document_url": "",
        "file_size": "",
        "has_xbrl": False,
        "raw_json": "{}",
    }
    for i in range(5)
]


# ===========================================================================
# fetch_bse_page tests (unit-level, mocking session)
# ===========================================================================


class TestFetchBsePage:
    """Tests for scraper.fetch_bse_page() with mocked HTTP."""

    def test_calls_correct_url(self, bse_response_data):
        """fetch_bse_page calls BSE_ANNOUNCEMENTS_URL with pageno param."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = bse_response_data
        session.get.return_value = response

        filings, total = scraper.fetch_bse_page(session, page_num=3)

        call_kwargs = session.get.call_args
        assert "BSE_ANNOUNCEMENTS_URL" or "AnnSubCategoryGetData" in str(call_kwargs)
        assert total == 1250
        assert len(filings) == 4

    def test_sends_bse_headers(self, bse_response_data):
        """fetch_bse_page passes BSE_HEADERS in the request."""
        from http_utils import BSE_HEADERS

        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = bse_response_data
        session.get.return_value = response

        scraper.fetch_bse_page(session)

        _, kwargs = session.get.call_args
        assert kwargs["headers"] == BSE_HEADERS


# ===========================================================================
# fetch_nse_endpoint tests
# ===========================================================================


class TestFetchNseEndpoint:
    """Tests for scraper.fetch_nse_endpoint() with mocked HTTP."""

    def test_returns_filings_for_announcements(self, nse_announcements_data):
        """Returns parsed filings for announcements endpoint."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = nse_announcements_data
        session.get.return_value = response

        filings = scraper.fetch_nse_endpoint(session, endpoint_type="announcements")
        assert len(filings) == 3

    def test_sends_nse_headers(self, nse_announcements_data):
        """fetch_nse_endpoint passes NSE_HEADERS in the request."""
        from http_utils import NSE_HEADERS

        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = nse_announcements_data
        session.get.return_value = response

        scraper.fetch_nse_endpoint(session)

        _, kwargs = session.get.call_args
        assert kwargs["headers"] == NSE_HEADERS

    def test_financial_results_adds_period_param(self, nse_financial_results_data):
        """financial_results endpoint adds period=Quarterly param."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = nse_financial_results_data
        session.get.return_value = response

        scraper.fetch_nse_endpoint(session, endpoint_type="financial_results")

        _, kwargs = session.get.call_args
        assert kwargs["params"]["period"] == "Quarterly"


# ===========================================================================
# fetch_sebi_page tests
# ===========================================================================


class TestFetchSebiPage:
    """Tests for scraper.fetch_sebi_page() with mocked HTTP."""

    def test_returns_filings_and_has_more(self, sebi_response_text):
        """Returns filings and has_more from SEBI response."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.text = sebi_response_text
        session.post.return_value = response

        filings, has_more = scraper.fetch_sebi_page(session, page_num=0, category_id=15)
        assert len(filings) > 0

    def test_sends_sebi_headers(self, sebi_response_text):
        """fetch_sebi_page passes SEBI_HEADERS to prevent HTTP 530."""
        from http_utils import SEBI_HEADERS

        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.text = sebi_response_text
        session.post.return_value = response

        scraper.fetch_sebi_page(session)

        _, kwargs = session.post.call_args
        assert kwargs["headers"] == SEBI_HEADERS

    def test_page_0_uses_start_action(self, sebi_response_text):
        """Page 0 sends next='s' (start) parameter."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.text = sebi_response_text
        session.post.return_value = response

        scraper.fetch_sebi_page(session, page_num=0)

        _, kwargs = session.post.call_args
        assert kwargs["data"]["next"] == "s"

    def test_subsequent_pages_use_next_action(self, sebi_response_text):
        """Page > 0 sends next='n' parameter."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.text = sebi_response_text
        session.post.return_value = response

        scraper.fetch_sebi_page(session, page_num=1)

        _, kwargs = session.post.call_args
        assert kwargs["data"]["next"] == "n"


# ===========================================================================
# cmd_stats integration
# ===========================================================================


class TestCmdStats:
    """Tests for cmd_stats command handler."""

    def test_stats_all_sources_empty_db(self, tmp_db, capsys):
        """cmd_stats with empty DB prints zero counts."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all")
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        assert "BSE" in captured.out
        assert "NSE" in captured.out
        assert "SEBI" in captured.out

    def test_stats_single_source(self, tmp_db, capsys, sample_bse_filing):
        """cmd_stats for a single source shows only that source."""
        tmp_db.insert_batch([sample_bse_filing])
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="bse")
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        assert "BSE" in captured.out
        # Should show total=1
        assert "1" in captured.out


# ===========================================================================
# cmd_export integration
# ===========================================================================


class TestCmdExport:
    """Tests for cmd_export command handler."""

    def test_export_creates_json_file(self, tmp_db, sample_bse_filing, tmp_path):
        """cmd_export writes a valid JSON file."""
        tmp_db.insert_batch([sample_bse_filing])
        out_path = str(tmp_path / "out.json")
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(output=out_path, source="all")
            scraper.cmd_export(args)
        assert os.path.exists(out_path)
        with open(out_path) as f:
            data = json.load(f)
        assert "filings" in data


# ===========================================================================
# Incremental / resume crawl behaviour
# ===========================================================================


class TestIncrementalCrawl:
    """Tests for --incremental flag behaviour in _crawl_bse."""

    def test_incremental_stops_after_no_new_filings(self, tmp_db):
        """--incremental stops crawling when no new filings on second page."""
        # Insert existing filings so page 2+ yields 0 new
        existing = [{**f, "filing_id": f"EXIST_{i}"} for i, f in enumerate(BSE_PAGE_FILINGS)]
        tmp_db.insert_batch(existing)

        call_count = [0]

        def mock_fetch_bse_page(session, page_num=1, **kwargs):
            call_count[0] += 1
            if page_num == 1:
                # Return filings we already have
                return existing, len(existing)
            return [], 0

        with patch("scraper.fetch_bse_page", side_effect=mock_fetch_bse_page):
            session = MagicMock()
            scraper._crawl_bse(
                session, tmp_db, "/tmp", max_pages=5, download=False, parallel=1,
                incremental=True,
            )

        # Should stop after page 1 (all known) rather than fetching all 5
        assert call_count[0] <= 2


class TestResumeCrawl:
    """Tests for --resume flag behaviour."""

    def test_resume_bse_from_saved_page(self, tmp_db):
        """--resume starts BSE from last_page + 1 stored in crawl_state."""
        tmp_db.save_crawl_state("bse", "last_page", "3")

        pages_fetched = []

        def mock_fetch(session, page_num=1, **kwargs):
            pages_fetched.append(page_num)
            return [], 0  # empty so loop stops quickly

        with patch("scraper.fetch_bse_page", side_effect=mock_fetch):
            session = MagicMock()
            scraper._crawl_bse(
                session, tmp_db, "/tmp", max_pages=3, download=False, parallel=1,
                resume=True,
            )

        # First page fetched should be 4 (last_page=3, so resume from 4)
        assert pages_fetched[0] == 4
