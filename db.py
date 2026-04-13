"""
db.py — SQLite schema, dataclasses, and CRUD for the India filing scraper.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

DB_FILE = "filings_cache.db"


@dataclass
class Filing:
    """Normalized filing record shared across BSE, NSE, and SEBI sources."""

    source: str
    filing_id: str
    company_name: str = ""
    symbol: str = ""
    isin: str = ""
    category: str = ""
    subcategory: str = ""
    subject: str = ""
    description: str = ""
    filing_date: str = ""
    document_url: str = ""
    file_size: str = ""
    has_xbrl: bool = False
    raw_json: str = ""
    # DB-managed fields (not set by callers)
    downloaded: int = 0
    local_path: str = ""
    first_seen: str = ""
    page_number: int = 0
    filing_type: str = ""  # L2: classified type

    def to_dict(self) -> dict:
        """Return a plain dict suitable for DB insertion."""
        return {
            "source": self.source,
            "filing_id": self.filing_id,
            "company_name": self.company_name,
            "symbol": self.symbol,
            "isin": self.isin,
            "category": self.category,
            "subcategory": self.subcategory,
            "subject": self.subject,
            "description": self.description,
            "filing_date": self.filing_date,
            "document_url": self.document_url,
            "file_size": self.file_size,
            "has_xbrl": self.has_xbrl,
            "raw_json": self.raw_json,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Filing":
        """Build a Filing from a plain dict (e.g. a DB row or parsed response)."""
        return cls(
            source=d.get("source", ""),
            filing_id=str(d.get("filing_id", "")),
            company_name=d.get("company_name", ""),
            symbol=d.get("symbol", ""),
            isin=d.get("isin", ""),
            category=d.get("category", ""),
            subcategory=d.get("subcategory", ""),
            subject=d.get("subject", ""),
            description=d.get("description", ""),
            filing_date=d.get("filing_date", ""),
            document_url=d.get("document_url", ""),
            file_size=str(d.get("file_size", "")),
            has_xbrl=bool(d.get("has_xbrl", False)),
            raw_json=d.get("raw_json", ""),
            downloaded=int(d.get("downloaded", 0)),
            local_path=d.get("local_path", ""),
            first_seen=d.get("first_seen", ""),
            page_number=int(d.get("page_number", 0)),
            filing_type=d.get("filing_type", ""),
        )


_SCHEMA = """
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
"""

_INSERT_SQL = """
INSERT OR IGNORE INTO filings
   (source, filing_id, company_name, symbol, isin, category,
    subcategory, subject, description, filing_date, document_url,
    file_size, has_xbrl, first_seen, page_number, raw_json, filing_type)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


class FilingCache:
    """SQLite-backed cache for filing records with dedup and download tracking."""

    def __init__(self, db_path: str = DB_FILE) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert_batch(self, filings: list[dict], page_num: int = 0) -> int:
        """Insert filings, skipping duplicates. Returns count of new records.

        Args:
            filings: List of filing dicts (must have 'source' and 'filing_id').
            page_num: Page number to record against each filing.

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
            filing_type = classify_filing_type(
                f.get("subject", "") or f.get("description", "") or f.get("category", "")
            )
            self.conn.execute(
                _INSERT_SQL,
                (
                    source,
                    fid,
                    f.get("company_name", ""),
                    f.get("symbol", ""),
                    f.get("isin", ""),
                    f.get("category", ""),
                    f.get("subcategory", ""),
                    f.get("subject", ""),
                    f.get("description", ""),
                    f.get("filing_date", ""),
                    f.get("document_url", ""),
                    f.get("file_size", ""),
                    int(f.get("has_xbrl", False)),
                    datetime.now().isoformat(),
                    page_num,
                    f.get("raw_json", ""),
                    filing_type,
                ),
            )
        self.conn.commit()
        after = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        return after - before

    def mark_downloaded(self, source: str, filing_id: str, path: str) -> None:
        """Record successful document download.

        Args:
            source: Filing source (bse/nse/sebi).
            filing_id: Filing identifier.
            path: Local file path where the document was saved.
        """
        self.conn.execute(
            "UPDATE filings SET downloaded=1, local_path=? WHERE source=? AND filing_id=?",
            (path, source, filing_id),
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

        Args:
            source: Filter by source. Empty string returns all sources.

        Returns:
            Set of composite keys.
        """
        if source:
            rows = self.conn.execute(
                "SELECT source, filing_id FROM filings WHERE source=?", (source,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT source, filing_id FROM filings").fetchall()
        return {f"{r[0]}|{r[1]}" for r in rows}

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
