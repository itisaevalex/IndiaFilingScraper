"""
db.py — SQLite schema, dataclasses, and CRUD for the India filing scraper.

L3 Schema: spec-compliant column names, filing_id TEXT PRIMARY KEY, ISO dates.
Includes backwards-compatible migration for existing L1/L2 databases.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

log = logging.getLogger("india-scraper")

DB_FILE = "filings_cache.db"


@dataclass(frozen=True)
class Filing:
    """Normalized filing record shared across BSE, NSE, and SEBI sources."""

    source: str
    filing_id: str
    ticker: str = ""
    company_name: str = ""
    filing_date: str = ""        # MUST be YYYY-MM-DD
    filing_time: str = ""
    headline: str = ""
    filing_type: str = "other"
    category: str = ""
    document_url: str = ""
    direct_download_url: str = ""
    file_size: str = ""
    num_pages: int = 0
    price_sensitive: bool = False
    downloaded: bool = False
    download_path: str = ""
    raw_metadata: str = ""
    created_at: str = ""
    country: str = "IN"

    def to_dict(self) -> dict:
        """Return a plain dict suitable for DB insertion."""
        return {
            "source": self.source,
            "filing_id": self.filing_id,
            "country": self.country,
            "ticker": self.ticker,
            "company_name": self.company_name,
            "filing_date": self.filing_date,
            "filing_time": self.filing_time,
            "headline": self.headline,
            "filing_type": self.filing_type,
            "category": self.category,
            "document_url": self.document_url,
            "direct_download_url": self.direct_download_url,
            "file_size": self.file_size,
            "num_pages": self.num_pages,
            "price_sensitive": self.price_sensitive,
            "downloaded": self.downloaded,
            "download_path": self.download_path,
            "raw_metadata": self.raw_metadata,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Filing":
        """Build a Filing from a plain dict (e.g. a DB row or parsed response)."""
        return cls(
            source=d.get("source", ""),
            filing_id=str(d.get("filing_id", "")),
            country=d.get("country", "IN"),
            ticker=d.get("ticker", ""),
            company_name=d.get("company_name", ""),
            filing_date=d.get("filing_date", ""),
            filing_time=d.get("filing_time", ""),
            headline=d.get("headline", ""),
            filing_type=d.get("filing_type", "other"),
            category=d.get("category", ""),
            document_url=d.get("document_url", ""),
            direct_download_url=d.get("direct_download_url", ""),
            file_size=str(d.get("file_size", "")),
            num_pages=int(d.get("num_pages", 0) or 0),
            price_sensitive=bool(d.get("price_sensitive", False)),
            downloaded=bool(d.get("downloaded", False)),
            download_path=d.get("download_path", ""),
            raw_metadata=d.get("raw_metadata", ""),
            created_at=d.get("created_at", ""),
        )


# ---------------------------------------------------------------------------
# Schema (L3 spec)
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
    filing_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    country TEXT DEFAULT 'IN',
    ticker TEXT,
    company_name TEXT,
    filing_date TEXT,
    filing_time TEXT,
    headline TEXT,
    filing_type TEXT DEFAULT 'other',
    category TEXT,
    document_url TEXT,
    direct_download_url TEXT,
    file_size TEXT,
    num_pages INTEGER,
    price_sensitive BOOLEAN DEFAULT FALSE,
    downloaded BOOLEAN DEFAULT FALSE,
    download_path TEXT,
    raw_metadata TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_source ON filings(source);
CREATE INDEX IF NOT EXISTS idx_doc_url ON filings(document_url);
CREATE INDEX IF NOT EXISTS idx_dl ON filings(downloaded);
CREATE INDEX IF NOT EXISTS idx_date ON filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_type ON filings(filing_type);

CREATE TABLE IF NOT EXISTS crawl_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, key)
);

CREATE TABLE IF NOT EXISTS crawl_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_type TEXT NOT NULL,
    source TEXT,
    query_params TEXT,
    filings_found INTEGER DEFAULT 0,
    filings_new INTEGER DEFAULT 0,
    pages_crawled INTEGER DEFAULT 0,
    errors TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    duration_seconds REAL
);
CREATE INDEX IF NOT EXISTS idx_crawl_log_completed_at ON crawl_log(completed_at);
CREATE INDEX IF NOT EXISTS idx_crawl_log_source ON crawl_log(source);
"""

_INSERT_SQL = """
INSERT OR IGNORE INTO filings
   (filing_id, source, country, ticker, company_name, filing_date, filing_time,
    headline, filing_type, category, document_url, direct_download_url,
    file_size, num_pages, price_sensitive, downloaded, download_path,
    raw_metadata, created_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

# ---------------------------------------------------------------------------
# Migration: L1/L2 -> L3
# ---------------------------------------------------------------------------

_L2_COLUMNS = {
    # Maps L2 column names to True if the column existed
    "id",
    "source",
    "filing_id",
    "company_name",
    "symbol",
    "isin",
    "category",
    "subcategory",
    "subject",
    "description",
    "filing_date",
    "document_url",
    "file_size",
    "has_xbrl",
    "downloaded",
    "local_path",
    "first_seen",
    "page_number",
    "raw_json",
    "filing_type",
}


def _get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _get_table_column_names_ordered(conn: sqlite3.Connection, table: str) -> list[str]:
    """Return ordered list of column names for a table (matches SELECT * order)."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # PRAGMA table_info returns rows ordered by cid (column index)
    return [row[1] for row in sorted(rows, key=lambda r: r[0])]


def _migrate_l2_to_l3(conn: sqlite3.Connection) -> None:
    """Migrate an existing L2 (INTEGER PK) filings table to L3 spec.

    Strategy:
    1. Read all L2 rows into memory
    2. Rename old table to filings_l2_backup (cannot be done inside executescript)
    3. Create the new L3 filings table via executescript
    4. Copy rows with column renaming and date normalization
    5. Commit everything

    Note: executescript() issues an implicit COMMIT before running, so we read
    the rows *before* calling executescript to avoid losing them.
    """
    log.info("db: L2->L3 migration detected — migrating schema")

    from parsers import normalize_date_bse, normalize_date_nse, normalize_date_sebi

    # 1. Read all L2 rows and column names into memory first (ordered)
    cols = _get_table_column_names_ordered(conn, "filings")
    raw_rows = conn.execute("SELECT * FROM filings").fetchall()

    # 2. Rename old table — executescript will commit after this
    conn.execute("ALTER TABLE filings RENAME TO filings_l2_backup")
    conn.commit()

    # 3. Create new L3 schema (executescript issues implicit commit)
    conn.executescript(_SCHEMA)

    # 4. Copy rows with mapping
    migrated = 0
    for row in raw_rows:
        d = dict(zip(cols, row))

        # Build the composite filing_id for L3 PRIMARY KEY
        src = d.get("source", "")
        old_fid = str(d.get("filing_id", "") or "")
        if not old_fid or not src:
            continue

        new_fid = f"{src}_{old_fid}"

        # Normalize date to YYYY-MM-DD
        raw_date = str(d.get("filing_date", "") or "")
        if src == "bse":
            iso_date = normalize_date_bse(raw_date)
        elif src == "nse":
            iso_date = normalize_date_nse(raw_date)
        elif src == "sebi":
            iso_date = normalize_date_sebi(raw_date)
        else:
            iso_date = raw_date[:10] if len(raw_date) >= 10 else raw_date

        # Derive headline from subject/description
        headline = str(d.get("subject", "") or d.get("description", "") or "").strip()
        ticker = str(d.get("symbol", "") or "").strip()
        raw_metadata = d.get("raw_json", "") or ""

        conn.execute(
            _INSERT_SQL,
            (
                new_fid,
                src,
                "IN",
                ticker,
                str(d.get("company_name", "") or ""),
                iso_date,
                "",
                headline,
                str(d.get("filing_type", "") or "other"),
                str(d.get("category", "") or ""),
                str(d.get("document_url", "") or ""),
                "",
                str(d.get("file_size", "") or ""),
                0,
                False,
                bool(d.get("downloaded", 0)),
                str(d.get("local_path", "") or ""),
                raw_metadata,
                str(d.get("first_seen", "") or datetime.now().isoformat()),
            ),
        )
        migrated += 1

    conn.commit()
    log.info("db: migration complete — %d rows migrated", migrated)


def _needs_migration(conn: sqlite3.Connection) -> bool:
    """Return True if the filings table exists but uses the old L2 schema."""
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "filings" not in tables:
        return False  # Fresh DB — no migration needed

    cols = _get_table_columns(conn, "filings")
    # L3 has 'filing_id TEXT PRIMARY KEY' and no 'id INTEGER' autoincrement
    # L2 had 'id INTEGER PRIMARY KEY AUTOINCREMENT' and 'symbol' column
    return "symbol" in cols or ("id" in cols and "ticker" not in cols)


class FilingCache:
    """SQLite-backed cache for filing records with dedup and download tracking."""

    def __init__(self, db_path: str = DB_FILE) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        if _needs_migration(self.conn):
            _migrate_l2_to_l3(self.conn)
        else:
            self.conn.executescript(_SCHEMA)
            self.conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert_batch(self, filings: list[dict], page_num: int = 0) -> int:
        """Insert filings, skipping duplicates. Returns count of new records.

        Args:
            filings: List of filing dicts (must have 'source' and 'filing_id').
            page_num: Page number to record against each filing (unused in L3,
                      kept for backwards compat with callers).

        Returns:
            Number of newly inserted records.
        """
        from parsers import classify_filing_type  # deferred to avoid import-time cycle

        before = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        for f in filings:
            fid = f.get("filing_id", "")
            source = f.get("source", "")
            if not fid or not source:
                continue

            # Build a unique filing_id for the L3 PRIMARY KEY
            l3_fid = f"{source}_{fid}"

            filing_type = f.get("filing_type") or classify_filing_type(
                f.get("headline", "")
                or f.get("subject", "")
                or f.get("description", "")
                or f.get("category", "")
            )
            if not filing_type:
                filing_type = "other"

            # Headline: prefer 'headline', fall back to 'subject' / 'description'
            headline = (
                f.get("headline", "")
                or f.get("subject", "")
                or f.get("description", "")
                or ""
            ).strip()

            # raw_metadata: prefer 'raw_metadata', fall back to 'raw_json'
            raw_metadata = f.get("raw_metadata", "") or f.get("raw_json", "") or ""

            self.conn.execute(
                _INSERT_SQL,
                (
                    l3_fid,
                    source,
                    f.get("country", "IN"),
                    f.get("ticker", "") or f.get("symbol", ""),
                    f.get("company_name", ""),
                    f.get("filing_date", ""),
                    f.get("filing_time", ""),
                    headline,
                    filing_type,
                    f.get("category", ""),
                    f.get("document_url", ""),
                    f.get("direct_download_url", ""),
                    f.get("file_size", ""),
                    int(f.get("num_pages", 0) or 0),
                    bool(f.get("price_sensitive", False)),
                    bool(f.get("downloaded", False)),
                    f.get("download_path", "") or f.get("local_path", ""),
                    raw_metadata,
                    datetime.now().isoformat(),
                ),
            )
        self.conn.commit()
        after = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        return after - before

    def mark_downloaded(self, source: str, filing_id: str, path: str) -> None:
        """Record successful document download.

        Args:
            source: Filing source (bse/nse/sebi).
            filing_id: Filing identifier (the original short ID, not the l3_fid).
            path: Local file path where the document was saved.
        """
        l3_fid = f"{source}_{filing_id}"
        self.conn.execute(
            "UPDATE filings SET downloaded=1, download_path=? WHERE filing_id=?",
            (path, l3_fid),
        )
        self.conn.commit()

    def log_crawl_start(
        self,
        crawl_type: str,
        source: str = "",
        query_params: str = "",
    ) -> int:
        """Insert a crawl_log row at the start of a crawl run.

        Args:
            crawl_type: Human-readable label (e.g. 'bse', 'nse_announcements').
            source: Source key ('bse', 'nse', 'sebi').
            query_params: Optional JSON string of query parameters used.

        Returns:
            Row id of the new crawl_log entry (pass to log_crawl_complete).
        """
        cur = self.conn.execute(
            """INSERT INTO crawl_log
               (crawl_type, source, query_params, started_at)
               VALUES (?, ?, ?, ?)""",
            (crawl_type, source, query_params, datetime.now().isoformat()),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def log_crawl_complete(
        self,
        log_id: int,
        filings_found: int = 0,
        filings_new: int = 0,
        pages_crawled: int = 0,
        errors: str = "",
    ) -> None:
        """Update a crawl_log row when the crawl finishes.

        Args:
            log_id: Row id returned by log_crawl_start.
            filings_found: Total filings seen during the crawl.
            filings_new: Count of newly inserted filings.
            pages_crawled: Number of pages / date windows fetched.
            errors: Optional error summary string.
        """
        started_row = self.conn.execute(
            "SELECT started_at FROM crawl_log WHERE id=?", (log_id,)
        ).fetchone()
        duration: Optional[float] = None
        if started_row:
            try:
                started_dt = datetime.fromisoformat(started_row[0])
                duration = (datetime.now() - started_dt).total_seconds()
            except ValueError:
                pass

        self.conn.execute(
            """UPDATE crawl_log
               SET filings_found=?, filings_new=?, pages_crawled=?,
                   errors=?, completed_at=?, duration_seconds=?
               WHERE id=?""",
            (
                filings_found,
                filings_new,
                pages_crawled,
                errors or None,
                datetime.now().isoformat(),
                duration,
                log_id,
            ),
        )
        self.conn.commit()

    def save_crawl_state(self, source: str, key: str, value: str) -> None:
        """Persist a crawl resume state key-value pair.

        Args:
            source: The scraper source (bse/nse/sebi).
            key: State key (e.g. 'last_page', 'last_date').
            value: State value.
        """
        self.conn.execute(
            """INSERT INTO crawl_state (source, key, value, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(source, key) DO UPDATE SET value=excluded.value,
               updated_at=excluded.updated_at""",
            (source, key, value, datetime.now().isoformat()),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_crawl_state(self, source: str, key: str) -> Optional[str]:
        """Retrieve a previously saved crawl state value.

        Args:
            source: The scraper source.
            key: State key.

        Returns:
            Stored value string, or None if not found.
        """
        row = self.conn.execute(
            "SELECT value FROM crawl_state WHERE source=? AND key=?",
            (source, key),
        ).fetchone()
        return row[0] if row else None

    def get_known_keys(self, source: str = "") -> set[str]:
        """Return set of 'source|filing_id' keys for incremental dedup.

        Note: The 'filing_id' returned here is the original short ID (without
        the source prefix), so callers can still build 'source|id' keys.

        Args:
            source: Filter by source. Empty string returns all sources.

        Returns:
            Set of composite keys in format 'source|original_filing_id'.
        """
        if source:
            rows = self.conn.execute(
                "SELECT source, filing_id FROM filings WHERE source=?", (source,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT source, filing_id FROM filings").fetchall()
        # l3_fid is "source_original_id"; we need to strip the "source_" prefix
        result: set[str] = set()
        for r in rows:
            src = r[0]
            l3_fid = r[1]
            prefix = f"{src}_"
            orig_id = l3_fid[len(prefix):] if l3_fid.startswith(prefix) else l3_fid
            result.add(f"{src}|{orig_id}")
        return result

    def stats(self, source: str = "") -> dict:
        """Return aggregate statistics for a source (or all sources).

        Args:
            source: Filter by source. Empty string aggregates all.

        Returns:
            Dict with total, downloaded, pending, oldest, newest.
        """
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

    def unique_companies(self) -> int:
        """Return count of distinct company names."""
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT company_name) FROM filings WHERE company_name != ''"
        ).fetchone()
        return int(row[0] or 0)

    def last_crawl_completed_at(self, source: str = "") -> Optional[str]:
        """Return the ISO datetime of the most recent completed crawl.

        Args:
            source: Filter by source key. Empty string checks all sources.

        Returns:
            ISO datetime string of the latest completed_at, or None if no
            completed crawl exists.
        """
        if source:
            row = self.conn.execute(
                """SELECT MAX(completed_at) FROM crawl_log
                   WHERE source=? AND completed_at IS NOT NULL""",
                (source,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT MAX(completed_at) FROM crawl_log WHERE completed_at IS NOT NULL"
            ).fetchone()
        return row[0] if row and row[0] else None

    def total_crawl_runs(self) -> int:
        """Return count of completed crawl runs recorded in crawl_log.

        Falls back to counting crawl_state rows when crawl_log is empty
        (e.g. a DB that was populated before crawl_log was introduced).
        """
        row = self.conn.execute(
            "SELECT COUNT(*) FROM crawl_log WHERE completed_at IS NOT NULL"
        ).fetchone()
        count = int(row[0] or 0)
        if count == 0:
            # Legacy fallback: crawl_state rows as proxy for crawl runs
            legacy_row = self.conn.execute("SELECT COUNT(*) FROM crawl_state").fetchone()
            return int(legacy_row[0] or 0)
        return count

    def export_json(self, path: str, source: str = "") -> None:
        """Export all filings to a JSON file.

        Args:
            path: Output file path.
            source: Filter by source. Empty string exports all.
        """
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

        with open(path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=2, ensure_ascii=False)

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
