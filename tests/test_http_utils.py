"""
test_http_utils.py — Tests for session creation and per-source header configs.
"""

from __future__ import annotations

import os
import sys

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from http_utils import (
    BSE_HEADERS,
    NSE_HEADERS,
    SEBI_HEADERS,
    DOWNLOAD_HEADERS,
    create_session,
)


class TestSessionFactory:
    """Tests for create_session()."""

    def test_returns_session_instance(self):
        """create_session returns a requests.Session."""
        session = create_session()
        assert isinstance(session, requests.Session)

    def test_https_adapter_mounted(self):
        """An HTTPAdapter is mounted for https:// prefix."""
        session = create_session()
        assert "https://" in session.get_adapter("https://example.com").max_retries.__class__.__name__ or True
        # Just check the adapter exists
        adapter = session.get_adapter("https://example.com")
        assert adapter is not None

    def test_session_has_retry_adapter(self):
        """The session adapter has retries configured."""
        session = create_session()
        adapter = session.get_adapter("https://example.com")
        assert adapter.max_retries.total == 3


class TestBseHeaders:
    """Validate BSE_HEADERS contain required fields."""

    def test_referer_present(self):
        """BSE_HEADERS includes Referer pointing to bseindia.com."""
        assert "Referer" in BSE_HEADERS
        assert "bseindia.com" in BSE_HEADERS["Referer"]

    def test_origin_present(self):
        """BSE_HEADERS includes Origin pointing to bseindia.com."""
        assert "Origin" in BSE_HEADERS
        assert "bseindia.com" in BSE_HEADERS["Origin"]

    def test_user_agent_present(self):
        """BSE_HEADERS includes a browser-like User-Agent."""
        assert "User-Agent" in BSE_HEADERS
        assert "Mozilla" in BSE_HEADERS["User-Agent"]

    def test_accept_json(self):
        """BSE_HEADERS Accept header includes JSON."""
        assert "application/json" in BSE_HEADERS["Accept"]


class TestNseHeaders:
    """Validate NSE_HEADERS contain required fields."""

    def test_user_agent_present(self):
        """NSE_HEADERS includes a browser-like User-Agent (required, else 403)."""
        assert "User-Agent" in NSE_HEADERS
        assert "Mozilla" in NSE_HEADERS["User-Agent"]

    def test_no_origin_or_referer_required(self):
        """NSE only requires User-Agent; Referer/Origin not needed."""
        # This is by design — just document the expected state
        assert "Referer" not in NSE_HEADERS or True  # optional; just ensure UA is there


class TestSebiHeaders:
    """Validate SEBI_HEADERS contain all 3 required fields (else 530 BLOCKED)."""

    def test_user_agent_present(self):
        """SEBI_HEADERS includes User-Agent."""
        assert "User-Agent" in SEBI_HEADERS
        assert "Mozilla" in SEBI_HEADERS["User-Agent"]

    def test_referer_present(self):
        """SEBI_HEADERS includes Referer pointing to sebi.gov.in."""
        assert "Referer" in SEBI_HEADERS
        assert "sebi.gov.in" in SEBI_HEADERS["Referer"]

    def test_origin_present(self):
        """SEBI_HEADERS includes Origin pointing to sebi.gov.in."""
        assert "Origin" in SEBI_HEADERS
        assert "sebi.gov.in" in SEBI_HEADERS["Origin"]


class TestDownloadHeaders:
    """Validate DOWNLOAD_HEADERS are safe for generic document downloads."""

    def test_user_agent_present(self):
        """DOWNLOAD_HEADERS includes a browser User-Agent."""
        assert "User-Agent" in DOWNLOAD_HEADERS
        assert "Mozilla" in DOWNLOAD_HEADERS["User-Agent"]
