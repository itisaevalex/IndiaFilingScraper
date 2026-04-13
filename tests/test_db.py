"""
test_db.py — Unit tests for FilingCache (SQLite CRUD operations).

Tests:
  - Schema creation and idempotency
  - insert_batch dedup behaviour
  - mark_downloaded
  - get_known_keys
  - save_crawl_state / get_crawl_state (--resume support)
  - stats()
  - export_json()
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import FilingCache


# ===========================================================================
# Schema
# ===========================================================================


class TestSchemaCreation:
    """FilingCache creates all tables and indexes on __init__."""

    def test_filings_table_created(self, tmp_db):
        """The 'filings' table exists after FilingCache.__init__."""
        row = tmp_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='filings'"
        ).fetchone()
        assert row is not None

    def test_crawl_state_table_created(self, tmp_db):
        """The 'crawl_state' table exists after FilingCache.__init__."""
        row = tmp_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='crawl_state'"
        ).fetchone()
        assert row is not None

    def test_idempotent_init(self, tmp_db):
        """Opening the same DB file twice does not raise an error."""
        db_path = tmp_db.conn.execute("PRAGMA database_list").fetchone()[2]
        cache2 = FilingCache(db_path)
        cache2.close()


# ===========================================================================
# insert_batch
# ===========================================================================


class TestInsertBatch:
    """Tests for FilingCache.insert_batch()."""

    def test_inserts_new_filings(self, tmp_db, sample_bse_filing):
        """Inserting a new filing returns count=1."""
        n = tmp_db.insert_batch([sample_bse_filing])
        assert n == 1

    def test_duplicate_skipped(self, tmp_db, sample_bse_filing):
        """Inserting the same filing twice returns 0 on the second call."""
        tmp_db.insert_batch([sample_bse_filing])
        n = tmp_db.insert_batch([sample_bse_filing])
        assert n == 0

    def test_multiple_sources_all_inserted(self, tmp_db, sample_bse_filing, sample_nse_filing, sample_sebi_filing):
        """Filings from different sources are all inserted."""
        n = tmp_db.insert_batch([sample_bse_filing, sample_nse_filing, sample_sebi_filing])
        assert n == 3

    def test_skips_filings_without_filing_id(self, tmp_db):
        """Filings missing filing_id are silently skipped."""
        filing = {"source": "bse", "filing_id": ""}
        n = tmp_db.insert_batch([filing])
        assert n == 0

    def test_skips_filings_without_source(self, tmp_db):
        """Filings missing source are silently skipped."""
        filing = {"source": "", "filing_id": "SOME_ID"}
        n = tmp_db.insert_batch([filing])
        assert n == 0

    def test_page_number_stored(self, tmp_db, sample_bse_filing):
        """Page number is stored against the inserted filing."""
        tmp_db.insert_batch([sample_bse_filing], page_num=7)
        row = tmp_db.conn.execute(
            "SELECT page_number FROM filings WHERE filing_id=?",
            (sample_bse_filing["filing_id"],),
        ).fetchone()
        assert row[0] == 7

    def test_filing_type_classified_on_insert(self, tmp_db, sample_bse_filing):
        """filing_type is auto-classified from subject/description on insert."""
        tmp_db.insert_batch([sample_bse_filing])
        row = tmp_db.conn.execute(
            "SELECT filing_type FROM filings WHERE filing_id=?",
            (sample_bse_filing["filing_id"],),
        ).fetchone()
        # Sample subject is "Board Meeting Notice" -> "Board Meeting"
        assert row[0] == "Board Meeting"

    def test_empty_list_returns_zero(self, tmp_db):
        """Inserting an empty list returns 0."""
        assert tmp_db.insert_batch([]) == 0

    def test_first_seen_populated(self, tmp_db, sample_nse_filing):
        """first_seen timestamp is populated on insert."""
        tmp_db.insert_batch([sample_nse_filing])
        row = tmp_db.conn.execute(
            "SELECT first_seen FROM filings WHERE filing_id=?",
            (sample_nse_filing["filing_id"],),
        ).fetchone()
        assert row[0]  # non-empty


# ===========================================================================
# mark_downloaded
# ===========================================================================


class TestMarkDownloaded:
    """Tests for FilingCache.mark_downloaded()."""

    def test_marks_downloaded(self, tmp_db, sample_bse_filing):
        """mark_downloaded sets downloaded=1 and local_path."""
        tmp_db.insert_batch([sample_bse_filing])
        tmp_db.mark_downloaded(
            sample_bse_filing["source"],
            sample_bse_filing["filing_id"],
            "/tmp/test.pdf",
        )
        row = tmp_db.conn.execute(
            "SELECT downloaded, local_path FROM filings WHERE filing_id=?",
            (sample_bse_filing["filing_id"],),
        ).fetchone()
        assert row["downloaded"] == 1
        assert row["local_path"] == "/tmp/test.pdf"

    def test_mark_nonexistent_is_noop(self, tmp_db):
        """mark_downloaded on a non-existent filing does not raise."""
        tmp_db.mark_downloaded("bse", "NONEXISTENT", "/tmp/test.pdf")


# ===========================================================================
# get_known_keys
# ===========================================================================


class TestGetKnownKeys:
    """Tests for FilingCache.get_known_keys()."""

    def test_returns_set_of_source_pipe_id(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """Returns composite 'source|filing_id' keys."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        keys = tmp_db.get_known_keys()
        assert "bse|TEST_BSE_001" in keys
        assert "nse|TEST_NSE_001" in keys

    def test_source_filter(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """Source filter returns only keys for that source."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        bse_keys = tmp_db.get_known_keys("bse")
        assert "bse|TEST_BSE_001" in bse_keys
        assert "nse|TEST_NSE_001" not in bse_keys

    def test_empty_db_returns_empty_set(self, tmp_db):
        """Empty database returns empty set."""
        assert tmp_db.get_known_keys() == set()


# ===========================================================================
# crawl_state (resume support)
# ===========================================================================


class TestCrawlState:
    """Tests for save_crawl_state / get_crawl_state."""

    def test_save_and_retrieve(self, tmp_db):
        """Saved state can be retrieved by the same source+key."""
        tmp_db.save_crawl_state("bse", "last_page", "5")
        value = tmp_db.get_crawl_state("bse", "last_page")
        assert value == "5"

    def test_update_overwrites(self, tmp_db):
        """Saving the same key again updates the value."""
        tmp_db.save_crawl_state("bse", "last_page", "5")
        tmp_db.save_crawl_state("bse", "last_page", "10")
        assert tmp_db.get_crawl_state("bse", "last_page") == "10"

    def test_missing_key_returns_none(self, tmp_db):
        """get_crawl_state returns None for a missing key."""
        assert tmp_db.get_crawl_state("nse", "nonexistent") is None

    def test_different_sources_isolated(self, tmp_db):
        """State is isolated by source."""
        tmp_db.save_crawl_state("bse", "last_page", "3")
        tmp_db.save_crawl_state("nse", "last_page", "7")
        assert tmp_db.get_crawl_state("bse", "last_page") == "3"
        assert tmp_db.get_crawl_state("nse", "last_page") == "7"


# ===========================================================================
# stats
# ===========================================================================


class TestStats:
    """Tests for FilingCache.stats()."""

    def test_stats_empty_db(self, tmp_db):
        """Stats on empty DB returns total=0."""
        s = tmp_db.stats()
        assert s["total"] == 0

    def test_stats_counts_correctly(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """Total and pending counts are correct after inserts."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        s = tmp_db.stats()
        assert s["total"] == 2
        assert s["pending"] == 2
        assert s["downloaded"] == 0

    def test_stats_after_download(self, tmp_db, sample_bse_filing):
        """downloaded count increments after mark_downloaded."""
        tmp_db.insert_batch([sample_bse_filing])
        tmp_db.mark_downloaded("bse", "TEST_BSE_001", "/tmp/test.pdf")
        s = tmp_db.stats("bse")
        assert s["downloaded"] == 1
        assert s["pending"] == 0

    def test_stats_source_filter(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """Stats with source filter only counts that source's filings."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        s_bse = tmp_db.stats("bse")
        s_nse = tmp_db.stats("nse")
        assert s_bse["total"] == 1
        assert s_nse["total"] == 1


# ===========================================================================
# export_json
# ===========================================================================


class TestExportJson:
    """Tests for FilingCache.export_json()."""

    def test_export_creates_valid_json(self, tmp_db, sample_bse_filing, tmp_path):
        """export_json writes valid JSON to the output file."""
        tmp_db.insert_batch([sample_bse_filing])
        out_path = str(tmp_path / "filings.json")
        tmp_db.export_json(out_path)
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
        assert "filings" in data
        assert "metadata" in data

    def test_export_contains_all_filings(
        self, tmp_db, sample_bse_filing, sample_nse_filing, tmp_path
    ):
        """Exported filings list contains all inserted records."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        out_path = str(tmp_path / "filings.json")
        tmp_db.export_json(out_path)
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["metadata"]["total"] == 2
        assert len(data["filings"]) == 2

    def test_export_source_filter(
        self, tmp_db, sample_bse_filing, sample_nse_filing, tmp_path
    ):
        """export_json with source filter only exports that source."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        out_path = str(tmp_path / "bse_only.json")
        tmp_db.export_json(out_path, source="bse")
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
        assert all(f["source"] == "bse" for f in data["filings"])
