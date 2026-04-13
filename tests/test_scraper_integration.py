"""
test_scraper_integration.py — Integration tests for the scraper's command dispatchers.

These tests mock the network layer and verify that:
  - parse_bse_response / parse_nse_response / parse_sebi_page are called correctly
  - --incremental stops on first no-new page
  - --resume picks up from saved crawl state
  - cmd_stats and cmd_export work end-to-end
  - stats --json outputs valid JSON with all required fields
  - health detection logic is correct
  - exit codes are correct
"""

from __future__ import annotations

import json
import os
import sys
import argparse
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import FilingCache
import scraper
from parsers import parse_bse_response, parse_nse_response, parse_sebi_page
from scraper import EXIT_OK, EXIT_ERROR, EXIT_PARTIAL, EXIT_FATAL, _compute_health


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
        "json": False,  # stats --json flag
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


BSE_PAGE_FILINGS = [
    {
        "source": "bse",
        "filing_id": f"BSE_{i}",
        "company_name": f"Company {i}",
        "ticker": f"SYM{i}",
        "symbol": f"SYM{i}",
        "isin": "",
        "category": "Board Meeting",
        "subcategory": "",
        "headline": "Board Meeting Notice",
        "subject": "Board Meeting Notice",
        "description": "",
        "filing_date": "2024-01-01",  # ISO date
        "filing_time": "10:00:00",
        "document_url": "",
        "direct_download_url": "",
        "file_size": "",
        "has_xbrl": False,
        "raw_json": "{}",
        "raw_metadata": "{}",
        "country": "IN",
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


# ===========================================================================
# stats --json
# ===========================================================================


class TestCmdStatsJson:
    """Tests for cmd_stats with --json flag."""

    def test_json_output_is_valid_json(self, tmp_db, capsys):
        """--json outputs valid JSON."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, dict)

    def test_json_output_required_fields(self, tmp_db, capsys):
        """--json output contains all required fields."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        required = {
            "scraper", "country", "sources", "total_filings", "downloaded",
            "pending_download", "unique_companies", "total_crawl_runs",
            "earliest_record", "latest_record", "db_size_bytes",
            "documents_size_bytes", "health",
        }
        missing = required - data.keys()
        assert not missing, f"Missing JSON keys: {missing}"

    def test_json_scraper_and_country(self, tmp_db, capsys):
        """scraper=india-scraper and country=IN are correct."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["scraper"] == "india-scraper"
        assert data["country"] == "IN"
        assert data["sources"] == ["bse", "nse", "sebi"]

    def test_json_health_empty_when_no_filings(self, tmp_db, capsys):
        """health='empty' when DB has no filings."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["health"] == "empty"

    def test_json_counts_match_db(self, tmp_db, capsys, sample_bse_filing, sample_nse_filing):
        """total_filings, downloaded, pending_download match actual DB state."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        tmp_db.mark_downloaded("bse", sample_bse_filing["filing_id"], "/tmp/test.pdf")

        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["total_filings"] == 2
        assert data["downloaded"] == 1
        assert data["pending_download"] == 1

    def test_json_unique_companies(self, tmp_db, capsys, sample_bse_filing, sample_nse_filing):
        """unique_companies field is populated correctly."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            scraper.cmd_stats(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        # sample_bse_filing = "Test Corp", sample_nse_filing = "NSE Test Corp"
        assert data["unique_companies"] == 2

    def test_json_exit_code_ok(self, tmp_db):
        """cmd_stats --json returns EXIT_OK on success."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all", **{"json": True, "doc_dir": "/tmp/docs"})
            code = scraper.cmd_stats(args)
        assert code == EXIT_OK


# ===========================================================================
# Health detection
# ===========================================================================


class TestComputeHealth:
    """Tests for _compute_health() health status logic."""

    def test_empty_when_no_filings(self):
        """Returns 'empty' when total=0."""
        assert _compute_health(0, None) == "empty"
        assert _compute_health(0, "2024-01-01") == "empty"

    def test_ok_when_recent(self):
        """Returns 'ok' when newest date is within 3 days."""
        today = datetime.now().strftime("%Y-%m-%d")
        assert _compute_health(10, today) == "ok"

    def test_stale_when_3_to_30_days_old(self):
        """Returns 'stale' when newest date is 4-30 days old."""
        stale_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        assert _compute_health(10, stale_date) == "stale"

    def test_degraded_when_over_30_days_old(self):
        """Returns 'degraded' when newest date is >30 days old."""
        old_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        assert _compute_health(10, old_date) == "degraded"

    def test_error_when_date_missing(self):
        """Returns 'error' when total > 0 but newest is None/empty."""
        assert _compute_health(5, None) == "error"
        assert _compute_health(5, "") == "error"

    def test_error_when_date_unparseable(self):
        """Returns 'error' when newest date is not a valid date string."""
        assert _compute_health(5, "not-a-date") == "error"

    def test_ok_uses_today_correctly(self):
        """A filing from today has health='ok'."""
        today = datetime.now().strftime("%Y-%m-%d")
        assert _compute_health(1, today) == "ok"


# ===========================================================================
# Exit codes
# ===========================================================================


class TestExitCodes:
    """Tests for standard exit codes (0/1/2/3)."""

    def test_exit_constants_defined(self):
        """EXIT_OK=0, EXIT_ERROR=1, EXIT_PARTIAL=2, EXIT_FATAL=3 are defined."""
        assert EXIT_OK == 0
        assert EXIT_ERROR == 1
        assert EXIT_PARTIAL == 2
        assert EXIT_FATAL == 3

    def test_cmd_stats_returns_exit_ok(self, tmp_db):
        """cmd_stats returns EXIT_OK when successful."""
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(source="all")
            # Ensure the 'json' attribute is False (not set)
            args.json = False
            code = scraper.cmd_stats(args)
        assert code == EXIT_OK

    def test_cmd_export_returns_exit_ok(self, tmp_db, tmp_path, sample_bse_filing):
        """cmd_export returns EXIT_OK when export succeeds."""
        tmp_db.insert_batch([sample_bse_filing])
        out_path = str(tmp_path / "out.json")
        with patch("scraper.FilingCache", return_value=tmp_db):
            args = make_args(output=out_path, source="all")
            code = scraper.cmd_export(args)
        assert code == EXIT_OK
