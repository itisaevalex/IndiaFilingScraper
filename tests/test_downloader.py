"""
test_downloader.py — Tests for download logic including SEBI PDF resolution.

Tests:
  - resolve_sebi_pdf: iframe src parsing, direct PDF link fallback, error fallback
  - download_filings: successful download, HTTP error handling, path traversal prevention,
    concurrent downloads, cache mark_downloaded integration
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from downloader import resolve_sebi_pdf, download_filings


# ===========================================================================
# resolve_sebi_pdf tests
# ===========================================================================


class TestResolveSebiPdf:
    """Tests for resolve_sebi_pdf()."""

    def _make_session(self, html_content: str, status_code: int = 200):
        """Helper: build a mock session whose GET returns given HTML."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = status_code
        response.text = html_content
        session.get.return_value = response
        return session

    def test_resolves_pdf_from_iframe_file_param(self):
        """Extracts PDF URL from iframe src with file= query param."""
        html = (
            '<iframe src="https://viewer.example.com/view?'
            'file=https://www.sebi.gov.in/sebi_data/docs/report.pdf"></iframe>'
        )
        session = self._make_session(html)
        result = resolve_sebi_pdf(session, "https://www.sebi.gov.in/filing.html")
        assert result == "https://www.sebi.gov.in/sebi_data/docs/report.pdf"

    def test_resolves_pdf_from_embed_tag(self):
        """Extracts PDF URL from embed data= attribute."""
        html = (
            '<embed data="https://viewer.example.com/view?'
            'file=https://www.sebi.gov.in/sebi_data/docs/embedded.pdf" />'
        )
        session = self._make_session(html)
        result = resolve_sebi_pdf(session, "https://www.sebi.gov.in/filing.html")
        assert result == "https://www.sebi.gov.in/sebi_data/docs/embedded.pdf"

    def test_resolves_pdf_from_direct_link(self):
        """Falls back to direct sebi_data link in <a> tags."""
        html = (
            '<a href="/sebi_data/docs/report.pdf">Download</a>'
        )
        session = self._make_session(html)
        result = resolve_sebi_pdf(session, "https://www.sebi.gov.in/filing.html")
        assert result == "https://www.sebi.gov.in/sebi_data/docs/report.pdf"

    def test_falls_back_to_original_url_when_no_pdf_found(self):
        """Returns original HTML URL when no PDF link is found."""
        html = "<html><body>No PDF here</body></html>"
        session = self._make_session(html)
        original = "https://www.sebi.gov.in/filing.html"
        result = resolve_sebi_pdf(session, original)
        assert result == original

    def test_falls_back_on_non_200_response(self):
        """Returns original URL when server returns non-200 status."""
        session = self._make_session("<html></html>", status_code=403)
        original = "https://www.sebi.gov.in/filing.html"
        result = resolve_sebi_pdf(session, original)
        assert result == original

    def test_falls_back_on_request_exception(self):
        """Returns original URL when a request exception occurs."""
        import requests

        session = MagicMock()
        session.get.side_effect = requests.RequestException("Connection failed")
        original = "https://www.sebi.gov.in/filing.html"
        result = resolve_sebi_pdf(session, original)
        assert result == original


# ===========================================================================
# download_filings tests
# ===========================================================================


class TestDownloadFilings:
    """Tests for download_filings()."""

    def _make_session(self, content: bytes = b"PDF_CONTENT", status_code: int = 200,
                      content_type: str = "application/pdf"):
        """Helper: build a mock session for download tests."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = status_code
        response.content = content
        response.headers = {
            "content-type": content_type,
            "content-disposition": "",
        }
        session.get.return_value = response
        return session

    def test_downloads_to_doc_dir(self, tmp_db, sample_bse_filing, tmp_path):
        """Successfully downloaded file is written to doc_dir."""
        session = self._make_session()
        doc_dir = str(tmp_path / "docs")

        tmp_db.insert_batch([sample_bse_filing])
        count = download_filings(session, [sample_bse_filing], doc_dir, tmp_db, parallel=1)

        assert count == 1
        files = os.listdir(doc_dir)
        assert len(files) == 1

    def test_skips_filings_without_url(self, tmp_db, tmp_path):
        """Filings with empty document_url are skipped."""
        filing = {
            "source": "bse",
            "filing_id": "NO_URL",
            "document_url": "",
        }
        session = self._make_session()
        count = download_filings(session, [filing], str(tmp_path / "docs"), tmp_db, parallel=1)
        assert count == 0

    def test_http_error_skips_filing(self, tmp_db, sample_bse_filing, tmp_path):
        """HTTP non-200 response causes the filing to be skipped."""
        session = self._make_session(status_code=404)
        tmp_db.insert_batch([sample_bse_filing])
        count = download_filings(
            session, [sample_bse_filing], str(tmp_path / "docs"), tmp_db, parallel=1
        )
        assert count == 0

    def test_marks_downloaded_in_cache(self, tmp_db, sample_bse_filing, tmp_path):
        """Successful download marks the filing as downloaded in the cache."""
        session = self._make_session()
        tmp_db.insert_batch([sample_bse_filing])
        download_filings(session, [sample_bse_filing], str(tmp_path / "docs"), tmp_db, parallel=1)

        s = tmp_db.stats("bse")
        assert s["downloaded"] == 1

    def test_extension_inferred_from_content_type(self, tmp_db, tmp_path):
        """Extension is inferred from content-type when URL has no extension."""
        filing = {
            "source": "bse",
            "filing_id": "NO_EXT_001",
            "document_url": "https://api.bseindia.com/download/attachment",
        }
        session = self._make_session(content_type="application/pdf")
        tmp_db.insert_batch([filing])
        download_filings(session, [filing], str(tmp_path / "docs"), tmp_db, parallel=1)
        files = os.listdir(str(tmp_path / "docs"))
        assert any(f.endswith(".pdf") for f in files)

    def test_filename_from_content_disposition(self, tmp_db, tmp_path):
        """Filename is taken from Content-Disposition when present."""
        filing = {
            "source": "nse",
            "filing_id": "CD_001",
            "document_url": "https://nsearchives.nseindia.com/download",
        }
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.content = b"DATA"
        response.headers = {
            "content-type": "application/pdf",
            "content-disposition": 'attachment; filename="report_2024.pdf"',
        }
        session.get.return_value = response
        tmp_db.insert_batch([filing])

        download_filings(session, [filing], str(tmp_path / "docs"), tmp_db, parallel=1)
        files = os.listdir(str(tmp_path / "docs"))
        assert any("report_2024.pdf" in f for f in files)

    def test_path_traversal_prevented(self, tmp_db, tmp_path):
        """Filenames with path traversal characters are sanitised."""
        filing = {
            "source": "sebi",
            "filing_id": "TRAVERSE_001",
            "document_url": "https://www.sebi.gov.in/sebi_data/../../etc/passwd.pdf",
        }
        session = self._make_session()
        tmp_db.insert_batch([filing])
        download_filings(session, [filing], str(tmp_path / "docs"), tmp_db, parallel=1)
        for fname in os.listdir(str(tmp_path / "docs")):
            assert ".." not in fname

    def test_sebi_html_url_triggers_pdf_resolution(self, tmp_db, tmp_path):
        """SEBI .html document_url triggers resolve_sebi_pdf before download."""
        filing = {
            "source": "sebi",
            "filing_id": "SEBI_HTML_001",
            "document_url": "https://www.sebi.gov.in/sebi_data/filing.html",
        }
        tmp_db.insert_batch([filing])

        with patch("downloader.resolve_sebi_pdf") as mock_resolve:
            mock_resolve.return_value = (
                "https://www.sebi.gov.in/sebi_data/filing_actual.pdf"
            )
            session = self._make_session()
            download_filings(session, [filing], str(tmp_path / "docs"), tmp_db, parallel=1)
            mock_resolve.assert_called_once()

    def test_empty_filings_list_returns_zero(self, tmp_db, tmp_path):
        """Empty filings list returns 0 without making any requests."""
        session = MagicMock()
        count = download_filings(session, [], str(tmp_path / "docs"), tmp_db)
        assert count == 0
        session.get.assert_not_called()

    def test_parallel_downloads(self, tmp_db, tmp_path):
        """Multiple filings are downloaded in parallel (parallel>1 path)."""
        filings = [
            {
                "source": "bse",
                "filing_id": f"PAR_{i}",
                "document_url": f"https://www.bseindia.com/AttachLive/file{i}.pdf",
            }
            for i in range(4)
        ]
        session = self._make_session()
        tmp_db.insert_batch(filings)
        count = download_filings(session, filings, str(tmp_path / "docs"), tmp_db, parallel=4)
        assert count == 4

    def test_request_exception_skips_filing(self, tmp_db, sample_nse_filing, tmp_path):
        """requests.RequestException causes filing to be skipped gracefully."""
        import requests

        session = MagicMock()
        session.get.side_effect = requests.RequestException("Timeout")
        tmp_db.insert_batch([sample_nse_filing])
        count = download_filings(
            session, [sample_nse_filing], str(tmp_path / "docs"), tmp_db, parallel=1
        )
        assert count == 0
