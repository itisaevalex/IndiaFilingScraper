"""
test_db.py — Unit tests for FilingCache (SQLite CRUD operations).

Tests:
  - Schema creation and idempotency (L3 spec columns)
  - insert_batch dedup behaviour
  - mark_downloaded
  - get_known_keys
  - save_crawl_state / get_crawl_state (--resume support)
  - stats()
  - unique_companies() / total_crawl_runs()
  - export_json()
  - Schema migration (L2 -> L3)
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import FilingCache, _SCHEMA


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

    def test_l3_column_filing_id_text_primary_key(self, tmp_db):
        """The filings table has filing_id TEXT PRIMARY KEY (L3 spec)."""
        info = tmp_db.conn.execute("PRAGMA table_info(filings)").fetchall()
        col_map = {row[1]: row for row in info}
        assert "filing_id" in col_map
        # pk=1 means it's part of the primary key
        assert col_map["filing_id"][5] == 1, "filing_id should be PRIMARY KEY"

    def test_l3_columns_present(self, tmp_db):
        """All required L3 spec columns are present."""
        info = tmp_db.conn.execute("PRAGMA table_info(filings)").fetchall()
        col_names = {row[1] for row in info}
        required = {
            "filing_id", "source", "country", "ticker", "company_name",
            "filing_date", "filing_time", "headline", "filing_type", "category",
            "document_url", "direct_download_url", "file_size", "num_pages",
            "price_sensitive", "downloaded", "download_path", "raw_metadata",
            "created_at",
        }
        missing = required - col_names
        assert not missing, f"Missing L3 columns: {missing}"

    def test_no_l2_specific_columns(self, tmp_db):
        """L2-specific columns (symbol, isin, raw_json, local_path) are absent."""
        info = tmp_db.conn.execute("PRAGMA table_info(filings)").fetchall()
        col_names = {row[1] for row in info}
        l2_only = {"symbol", "isin", "raw_json", "local_path", "first_seen",
                   "subcategory", "subject", "description", "has_xbrl", "page_number"}
        present = l2_only & col_names
        assert not present, f"L2-only columns still present: {present}"


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

    def test_filing_type_classified_on_insert(self, tmp_db, sample_bse_filing):
        """filing_type is auto-classified from headline/subject on insert."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = f"bse_{sample_bse_filing['filing_id']}"
        row = tmp_db.conn.execute(
            "SELECT filing_type FROM filings WHERE filing_id=?",
            (l3_fid,),
        ).fetchone()
        # Sample headline is "Board Meeting Notice" -> "Board Meeting"
        assert row[0] == "Board Meeting"

    def test_empty_list_returns_zero(self, tmp_db):
        """Inserting an empty list returns 0."""
        assert tmp_db.insert_batch([]) == 0

    def test_created_at_populated(self, tmp_db, sample_nse_filing):
        """created_at timestamp is populated on insert."""
        tmp_db.insert_batch([sample_nse_filing])
        l3_fid = f"nse_{sample_nse_filing['filing_id']}"
        row = tmp_db.conn.execute(
            "SELECT created_at FROM filings WHERE filing_id=?",
            (l3_fid,),
        ).fetchone()
        assert row[0]  # non-empty

    def test_l3_filing_id_is_composite(self, tmp_db, sample_bse_filing):
        """The stored filing_id is 'source_original_id' (L3 composite key)."""
        tmp_db.insert_batch([sample_bse_filing])
        row = tmp_db.conn.execute(
            "SELECT filing_id, source FROM filings WHERE company_name='Test Corp'"
        ).fetchone()
        assert row["filing_id"] == "bse_TEST_BSE_001"
        assert row["source"] == "bse"

    def test_country_default_in(self, tmp_db, sample_bse_filing):
        """country defaults to 'IN' for all filings."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT country FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0] == "IN"

    def test_iso_date_stored(self, tmp_db, sample_bse_filing):
        """filing_date stored is in YYYY-MM-DD format."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT filing_date FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0] == "2024-01-01"

    def test_headline_stored(self, tmp_db, sample_bse_filing):
        """headline field is stored correctly."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT headline FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0] == "Board Meeting Notice"

    def test_ticker_stored(self, tmp_db, sample_bse_filing):
        """ticker (L3 name for symbol) is stored correctly."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT ticker FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0] == "TESTCORP"

    def test_direct_download_url_stored(self, tmp_db, sample_bse_filing):
        """direct_download_url is stored alongside document_url."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT direct_download_url FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0].startswith("https://")

    def test_raw_metadata_stored(self, tmp_db, sample_bse_filing):
        """raw_metadata (replacing raw_json) is stored."""
        tmp_db.insert_batch([sample_bse_filing])
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT raw_metadata FROM filings WHERE filing_id=?", (l3_fid,)
        ).fetchone()
        assert row[0] is not None


# ===========================================================================
# mark_downloaded
# ===========================================================================


class TestMarkDownloaded:
    """Tests for FilingCache.mark_downloaded()."""

    def test_marks_downloaded(self, tmp_db, sample_bse_filing):
        """mark_downloaded sets downloaded=1 and download_path."""
        tmp_db.insert_batch([sample_bse_filing])
        tmp_db.mark_downloaded(
            sample_bse_filing["source"],
            sample_bse_filing["filing_id"],
            "/tmp/test.pdf",
        )
        l3_fid = "bse_TEST_BSE_001"
        row = tmp_db.conn.execute(
            "SELECT downloaded, download_path FROM filings WHERE filing_id=?",
            (l3_fid,),
        ).fetchone()
        assert row["downloaded"] == 1
        assert row["download_path"] == "/tmp/test.pdf"

    def test_mark_nonexistent_is_noop(self, tmp_db):
        """mark_downloaded on a non-existent filing does not raise."""
        tmp_db.mark_downloaded("bse", "NONEXISTENT", "/tmp/test.pdf")


# ===========================================================================
# get_known_keys
# ===========================================================================


class TestGetKnownKeys:
    """Tests for FilingCache.get_known_keys()."""

    def test_returns_set_of_source_pipe_id(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """Returns composite 'source|original_filing_id' keys."""
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

    def test_stats_oldest_newest_are_iso(self, tmp_db, sample_bse_filing, sample_nse_filing):
        """oldest and newest dates are in YYYY-MM-DD format."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing])
        s = tmp_db.stats()
        import re
        if s["oldest"]:
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", s["oldest"]), (
                f"oldest not ISO: {s['oldest']}"
            )
        if s["newest"]:
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", s["newest"]), (
                f"newest not ISO: {s['newest']}"
            )


# ===========================================================================
# unique_companies / total_crawl_runs
# ===========================================================================


class TestAggregates:
    """Tests for unique_companies() and total_crawl_runs()."""

    def test_unique_companies_empty(self, tmp_db):
        """Returns 0 when no filings are present."""
        assert tmp_db.unique_companies() == 0

    def test_unique_companies_counts_distinct_names(
        self, tmp_db, sample_bse_filing, sample_nse_filing, sample_sebi_filing
    ):
        """Returns count of distinct company names."""
        tmp_db.insert_batch([sample_bse_filing, sample_nse_filing, sample_sebi_filing])
        assert tmp_db.unique_companies() == 3

    def test_unique_companies_dedupes(self, tmp_db):
        """Same company_name across two sources is counted once."""
        f1 = {
            "source": "bse", "filing_id": "DUPE_001", "company_name": "SameCo",
            "filing_date": "2024-01-01",
        }
        f2 = {
            "source": "nse", "filing_id": "DUPE_001", "company_name": "SameCo",
            "filing_date": "2024-01-02",
        }
        tmp_db.insert_batch([f1, f2])
        assert tmp_db.unique_companies() == 1

    def test_total_crawl_runs_empty(self, tmp_db):
        """Returns 0 when no crawl state entries exist."""
        assert tmp_db.total_crawl_runs() == 0

    def test_total_crawl_runs_increments(self, tmp_db):
        """total_crawl_runs increases with each save_crawl_state call."""
        tmp_db.save_crawl_state("bse", "last_page", "1")
        tmp_db.save_crawl_state("nse", "last_date_announcements", "01-01-2024")
        assert tmp_db.total_crawl_runs() == 2


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


# ===========================================================================
# Schema migration: L2 -> L3
# ===========================================================================


class TestL2ToL3Migration:
    """Tests for backwards-compatible migration from L2 schema to L3 spec."""

    def _create_l2_db(self, db_path: str) -> None:
        """Create an L2-schema DB at db_path with sample data."""
        conn = sqlite3.connect(db_path)
        conn.executescript("""
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
            filing_type TEXT DEFAULT '',
            UNIQUE(source, filing_id)
        );
        CREATE TABLE IF NOT EXISTS crawl_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source, key)
        );
        INSERT INTO filings (source, filing_id, company_name, symbol, filing_date,
                             document_url, raw_json, filing_type)
        VALUES
            ('bse', 'BSE_L2_001', 'Legacy Corp', 'LEGACY',
             '15/03/2024 10:00:00', 'https://bse.example.com/test.pdf', '{}', 'Board Meeting'),
            ('nse', 'NSE_L2_001', 'NSE Legacy', 'NSELEGACY',
             '15-Mar-2024 09:00:00', 'https://nse.example.com/test.pdf', '{}', 'Financial Results'),
            ('sebi', '99999', 'SEBI Legacy Corp', '',
             'Mar 15, 2024', 'https://sebi.example.com/test.html', '{}', 'IPO / Rights Issue');
        """)
        conn.commit()
        conn.close()

    def test_migration_runs_without_error(self, tmp_path):
        """FilingCache opens an L2 DB without raising."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        cache.close()

    def test_migration_preserves_row_count(self, tmp_path):
        """All rows from the L2 table are migrated to L3."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        s = cache.stats()
        cache.close()
        assert s["total"] == 3

    def test_migration_normalizes_bse_date(self, tmp_path):
        """BSE date '15/03/2024 10:00:00' is migrated to '2024-03-15'."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        row = cache.conn.execute(
            "SELECT filing_date FROM filings WHERE source='bse'"
        ).fetchone()
        cache.close()
        assert row[0] == "2024-03-15"

    def test_migration_normalizes_nse_date(self, tmp_path):
        """NSE date '15-Mar-2024 09:00:00' is migrated to '2024-03-15'."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        row = cache.conn.execute(
            "SELECT filing_date FROM filings WHERE source='nse'"
        ).fetchone()
        cache.close()
        assert row[0] == "2024-03-15"

    def test_migration_normalizes_sebi_date(self, tmp_path):
        """SEBI date 'Mar 15, 2024' is migrated to '2024-03-15'."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        row = cache.conn.execute(
            "SELECT filing_date FROM filings WHERE source='sebi'"
        ).fetchone()
        cache.close()
        assert row[0] == "2024-03-15"

    def test_migration_is_idempotent(self, tmp_path):
        """Opening an already-migrated L3 DB does not re-migrate."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache1 = FilingCache(db_path)
        count1 = cache1.stats()["total"]
        cache1.close()

        # Open again — should not double-migrate
        cache2 = FilingCache(db_path)
        count2 = cache2.stats()["total"]
        cache2.close()

        assert count1 == count2

    def test_l2_backup_table_created(self, tmp_path):
        """Migration creates a filings_l2_backup table."""
        db_path = str(tmp_path / "l2.db")
        self._create_l2_db(db_path)
        cache = FilingCache(db_path)
        row = cache.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='filings_l2_backup'"
        ).fetchone()
        cache.close()
        assert row is not None

    def test_fresh_db_no_migration_needed(self, tmp_path):
        """A fresh (empty) DB bypasses migration and creates L3 schema directly."""
        db_path = str(tmp_path / "fresh.db")
        cache = FilingCache(db_path)
        info = cache.conn.execute("PRAGMA table_info(filings)").fetchall()
        col_names = {row[1] for row in info}
        cache.close()
        # Must have L3 cols
        assert "filing_id" in col_names
        assert "ticker" in col_names
        # Must NOT have L2 cols
        assert "symbol" not in col_names
