"""
test_parsers.py — Unit tests for all 3 source parsers and type classification.

Tests:
  - Date normalization for BSE, NSE, SEBI (L3 requirement)
  - BSE JSON response parsing + PDFFLAG URL routing
  - NSE JSON response parsing (all 4 endpoint types)
  - SEBI HTML/#@# response parsing
  - classify_filing_type() across all sources
"""

from __future__ import annotations

import json
import sys
import os

import pytest

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from parsers import (
    build_bse_doc_url,
    classify_filing_type,
    normalize_date_bse,
    normalize_date_nse,
    normalize_date_sebi,
    parse_bse_response,
    parse_nse_response,
    parse_sebi_page,
    sebi_has_next_page,
    SEBI_CATEGORY_NAMES,
    SEBI_DOC_BASE,
)

# Fixture scrip codes present in tests/fixtures/bse_response.json
_BSE_ISIN_MAP: dict[str, str] = {
    "500325": "INE002A01018",   # Reliance
    "532540": "INE467B01029",   # TCS
    "500209": "INE009A01021",   # Infosys
    # 500180 (HDFC) intentionally omitted to test missing-code fallback
}


# ===========================================================================
# Date normalization tests (L3 requirement)
# ===========================================================================


class TestNormalizeDateBse:
    """Tests for normalize_date_bse() — converts BSE DD/MM/YYYY to YYYY-MM-DD."""

    @pytest.mark.parametrize("raw, expected", [
        ("01/01/2024 10:00:00", "2024-01-01"),
        ("15/03/2024 09:30:00", "2024-03-15"),
        ("31/12/2023 23:59:59", "2023-12-31"),
        ("01/01/2024",          "2024-01-01"),   # no time component
        ("2024-01-15",          "2024-01-15"),   # already ISO — pass through
        ("",                    ""),             # empty input
    ])
    def test_bse_date_normalization(self, raw, expected):
        """BSE date strings are correctly normalized to YYYY-MM-DD."""
        assert normalize_date_bse(raw) == expected

    def test_bse_date_in_parsed_filings(self, bse_response_data):
        """All filing_date values in BSE response are YYYY-MM-DD."""
        import re
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            date = f["filing_date"]
            if date:
                assert re.match(r"^\d{4}-\d{2}-\d{2}$", date), (
                    f"BSE filing_date not ISO: {date!r}"
                )


class TestNormalizeDateNse:
    """Tests for normalize_date_nse() — handles multiple NSE date formats."""

    @pytest.mark.parametrize("raw, expected", [
        ("01-Jan-2024 10:00:00", "2024-01-01"),
        ("15-Mar-2024 09:30:00", "2024-03-15"),
        ("31-Dec-2023",          "2023-12-31"),
        ("01-jan-2024",          "2024-01-01"),   # lowercase month
        ("2024-01-15T10:00:00",  "2024-01-15"),   # already ISO datetime
        ("2024-01-15",           "2024-01-15"),   # already ISO date
        ("01-01-2024",           "2024-01-01"),   # DD-MM-YYYY numeric
        ("",                     ""),             # empty input
    ])
    def test_nse_date_normalization(self, raw, expected):
        """NSE date strings are correctly normalized to YYYY-MM-DD."""
        assert normalize_date_nse(raw) == expected

    def test_nse_announcements_dates_are_iso(self, nse_announcements_data):
        """All filing_date values in NSE announcements response are YYYY-MM-DD."""
        import re
        filings = parse_nse_response(nse_announcements_data, "announcements")
        for f in filings:
            date = f["filing_date"]
            if date:
                assert re.match(r"^\d{4}-\d{2}-\d{2}$", date), (
                    f"NSE announcement filing_date not ISO: {date!r}"
                )

    def test_nse_board_meetings_dates_are_iso(self, nse_board_meetings_data):
        """All filing_date values in NSE board_meetings response are YYYY-MM-DD."""
        import re
        filings = parse_nse_response(nse_board_meetings_data, "board_meetings")
        for f in filings:
            date = f["filing_date"]
            if date:
                assert re.match(r"^\d{4}-\d{2}-\d{2}$", date), (
                    f"NSE board meeting filing_date not ISO: {date!r}"
                )


class TestNormalizeDateSebi:
    """Tests for normalize_date_sebi() — handles DD-Mon-YYYY and Mon DD, YYYY."""

    @pytest.mark.parametrize("raw, expected", [
        ("10-Jan-2024",   "2024-01-10"),
        ("01-jan-2024",   "2024-01-01"),   # lowercase
        ("15-Mar-2024",   "2024-03-15"),
        ("31-Dec-2023",   "2023-12-31"),
        ("Jan 10, 2024",  "2024-01-10"),   # Mon DD, YYYY
        ("Mar 15, 2024",  "2024-03-15"),
        ("January 10, 2024", "2024-01-10"),  # full month name
        ("2024-01-10",    "2024-01-10"),   # already ISO
        ("",              ""),             # empty input
    ])
    def test_sebi_date_normalization(self, raw, expected):
        """SEBI date strings are correctly normalized to YYYY-MM-DD."""
        assert normalize_date_sebi(raw) == expected

    def test_sebi_page_dates_are_iso(self, sebi_response_text):
        """All filing_date values in SEBI response are YYYY-MM-DD."""
        import re
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            date = f["filing_date"]
            if date:
                assert re.match(r"^\d{4}-\d{2}-\d{2}$", date), (
                    f"SEBI filing_date not ISO: {date!r}"
                )


# ===========================================================================
# BSE parser tests
# ===========================================================================


class TestBseBuildDocUrl:
    """Tests for build_bse_doc_url() PDFFLAG routing."""

    def test_pdfflag_0_uses_attachlive(self):
        """PDFFLAG=0 routes to AttachLive directory."""
        row = {"ATTACHMENTNAME": "test.pdf", "PDFFLAG": "0"}
        url = build_bse_doc_url(row)
        assert "AttachLive" in url
        assert url.endswith("test.pdf")

    def test_pdfflag_1_uses_attachhis(self):
        """PDFFLAG=1 routes to AttachHis (historical) directory."""
        row = {"ATTACHMENTNAME": "old_report.pdf", "PDFFLAG": "1"}
        url = build_bse_doc_url(row)
        assert "AttachHis" in url
        assert url.endswith("old_report.pdf")

    def test_pdfflag_2_uses_corpattachment_with_date(self):
        """PDFFLAG=2 routes to CorpAttachment/<year>/<month>/<filename>."""
        row = {
            "ATTACHMENTNAME": "corp_filing.pdf",
            "PDFFLAG": "2",
            "NEWS_DT": "15/03/2024 10:00:00",
        }
        url = build_bse_doc_url(row)
        assert "CorpAttachment" in url
        assert "2024" in url
        assert "3" in url
        assert url.endswith("corp_filing.pdf")

    def test_pdfflag_2_bad_date_returns_empty(self):
        """PDFFLAG=2 with unparseable date returns empty string."""
        row = {
            "ATTACHMENTNAME": "test.pdf",
            "PDFFLAG": "2",
            "NEWS_DT": "NOT_A_DATE",
        }
        url = build_bse_doc_url(row)
        assert url == ""

    def test_empty_attachment_returns_empty(self):
        """Missing ATTACHMENTNAME returns empty string regardless of PDFFLAG."""
        row = {"ATTACHMENTNAME": "", "PDFFLAG": "0"}
        assert build_bse_doc_url(row) == ""

    def test_missing_attachment_key_returns_empty(self):
        """Missing ATTACHMENTNAME key returns empty string."""
        row = {"PDFFLAG": "0"}
        assert build_bse_doc_url(row) == ""

    def test_unknown_pdfflag_falls_back_to_attachlive(self):
        """Unknown PDFFLAG value falls back to AttachLive."""
        row = {"ATTACHMENTNAME": "file.pdf", "PDFFLAG": "99"}
        url = build_bse_doc_url(row)
        assert "AttachLive" in url

    def test_pdfflag_missing_defaults_to_0(self):
        """Missing PDFFLAG key defaults to AttachLive (flag=0 behavior)."""
        row = {"ATTACHMENTNAME": "file.pdf"}
        url = build_bse_doc_url(row)
        assert "AttachLive" in url


class TestParseBseResponse:
    """Tests for parse_bse_response() full response parsing."""

    def test_parses_filings_from_table(self, bse_response_data):
        """Parses all rows in the Table array into filing dicts."""
        filings, total = parse_bse_response(bse_response_data)
        assert len(filings) == 4
        assert total == 1250

    def test_filing_schema_completeness(self, bse_response_data):
        """Every parsed filing has all required schema fields (L3 spec + isin/lei/language)."""
        filings, _ = parse_bse_response(bse_response_data)
        required = {
            "source", "filing_id", "company_name", "ticker", "symbol",
            "category", "subject", "headline", "filing_date", "filing_time",
            "document_url", "direct_download_url", "file_size", "raw_metadata",
            "country", "isin", "lei", "language",
        }
        for f in filings:
            assert required.issubset(f.keys()), f"Missing keys: {required - f.keys()}"

    def test_source_is_bse(self, bse_response_data):
        """All parsed filings have source='bse'."""
        filings, _ = parse_bse_response(bse_response_data)
        assert all(f["source"] == "bse" for f in filings)

    def test_pdfflag0_url_in_attachlive(self, bse_response_data):
        """Row with PDFFLAG=0 gets an AttachLive document URL."""
        filings, _ = parse_bse_response(bse_response_data)
        ril = next(f for f in filings if f["symbol"] == "500325")
        assert "AttachLive" in ril["document_url"]

    def test_pdfflag1_url_in_attachhis(self, bse_response_data):
        """Row with PDFFLAG=1 gets an AttachHis document URL."""
        filings, _ = parse_bse_response(bse_response_data)
        tcs = next(f for f in filings if f["symbol"] == "532540")
        assert "AttachHis" in tcs["document_url"]

    def test_pdfflag2_url_in_corpattachment(self, bse_response_data):
        """Row with PDFFLAG=2 gets a CorpAttachment URL with year/month."""
        filings, _ = parse_bse_response(bse_response_data)
        infy = next(f for f in filings if f["symbol"] == "500209")
        assert "CorpAttachment" in infy["document_url"]

    def test_empty_attachment_gives_no_url(self, bse_response_data):
        """Row with empty ATTACHMENTNAME gets empty document_url."""
        filings, _ = parse_bse_response(bse_response_data)
        hdfc = next(f for f in filings if f["symbol"] == "500180")
        assert hdfc["document_url"] == ""

    def test_total_count_from_table1(self, bse_response_data):
        """Total row count is extracted from Table1[0].ROWCNT."""
        _, total = parse_bse_response(bse_response_data)
        assert total == 1250

    def test_empty_table1_gives_zero_total(self):
        """Missing Table1 gives total=0."""
        _, total = parse_bse_response({"Table": [], "Table1": []})
        assert total == 0

    def test_empty_response_gives_empty_filings(self):
        """Empty Table array gives empty filings list."""
        filings, total = parse_bse_response({"Table": [], "Table1": []})
        assert filings == []
        assert total == 0

    def test_raw_json_is_valid_json(self, bse_response_data):
        """The raw_json field in each filing is valid JSON."""
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            parsed = json.loads(f["raw_json"])
            assert isinstance(parsed, dict)


# ===========================================================================
# NSE parser tests
# ===========================================================================


class TestParseNseResponse:
    """Tests for parse_nse_response() across all 4 endpoint types."""

    def test_announcements_list_input(self, nse_announcements_data):
        """Parses a bare list response for announcements endpoint."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        assert len(filings) == 3

    def test_announcements_source_is_nse(self, nse_announcements_data):
        """All announcement filings have source='nse'."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        assert all(f["source"] == "nse" for f in filings)

    def test_announcements_dash_url_becomes_empty(self, nse_announcements_data):
        """attchmntFile='-' results in empty document_url."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        wipro = next(f for f in filings if f["symbol"] == "WIPRO")
        assert wipro["document_url"] == ""

    def test_announcements_valid_url_preserved(self, nse_announcements_data):
        """Valid attchmntFile URL is preserved as document_url."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        reliance = next(f for f in filings if f["symbol"] == "RELIANCE")
        assert reliance["document_url"].startswith("https://")

    def test_announcements_dict_input_with_data_key(self, nse_announcements_data):
        """Parses a dict response with a 'data' key wrapping the list."""
        wrapped = {"data": nse_announcements_data, "extra": "ignored"}
        filings = parse_nse_response(wrapped, "announcements")
        assert len(filings) == 3

    def test_announcements_dict_input_with_results_key(self, nse_announcements_data):
        """Parses a dict response with a 'results' key."""
        wrapped = {"results": nse_announcements_data}
        filings = parse_nse_response(wrapped, "announcements")
        assert len(filings) == 3

    def test_announcements_unexpected_dict_returns_empty(self):
        """Dict without known list key returns empty list."""
        filings = parse_nse_response({"foo": "bar"}, "announcements")
        assert filings == []

    def test_announcements_non_list_returns_empty(self):
        """Non-list, non-dict input returns empty list."""
        filings = parse_nse_response("invalid", "announcements")
        assert filings == []

    def test_annual_reports_parsing(self, nse_annual_reports_data):
        """Parses annual_reports endpoint into correct schema."""
        filings = parse_nse_response(nse_annual_reports_data, "annual_reports")
        assert len(filings) == 1
        f = filings[0]
        assert f["category"] == "Annual Report"
        assert "2022" in f["subject"]
        assert "2023" in f["subject"]
        assert f["filing_id"] == "ar_INFY_2022_2023"

    def test_annual_reports_dash_url_becomes_empty(self):
        """fileName='-' results in empty document_url for annual_reports."""
        data = [{"symbol": "TEST", "sm_name": "Test", "fromYr": "2023",
                 "toYr": "2024", "fileName": "-"}]
        filings = parse_nse_response(data, "annual_reports")
        assert filings[0]["document_url"] == ""

    def test_board_meetings_parsing(self, nse_board_meetings_data):
        """Parses board_meetings endpoint into correct schema."""
        filings = parse_nse_response(nse_board_meetings_data, "board_meetings")
        assert len(filings) == 1
        f = filings[0]
        assert f["category"] == "Board Meeting"
        assert f["symbol"] == "HDFCBANK"
        assert "HDFCBANK" in f["filing_id"]

    def test_board_meetings_has_xbrl_when_attachment_present(self, nse_board_meetings_data):
        """has_xbrl is True when attachment field is non-empty."""
        filings = parse_nse_response(nse_board_meetings_data, "board_meetings")
        assert filings[0]["has_xbrl"] is True

    def test_financial_results_parsing(self, nse_financial_results_data):
        """Parses financial_results endpoint into correct schema."""
        filings = parse_nse_response(nse_financial_results_data, "financial_results")
        assert len(filings) == 2

    def test_financial_results_xbrl_url_valid(self, nse_financial_results_data):
        """Valid XBRL URL is preserved as document_url."""
        filings = parse_nse_response(nse_financial_results_data, "financial_results")
        axisbank = next(f for f in filings if f["symbol"] == "AXISBANK")
        assert axisbank["document_url"].endswith(".xml")
        assert axisbank["has_xbrl"] is True

    def test_financial_results_xbrl_slash_dash_becomes_empty(self, nse_financial_results_data):
        """XBRL URL ending in /- results in empty document_url."""
        filings = parse_nse_response(nse_financial_results_data, "financial_results")
        kotak = next(f for f in filings if f["symbol"] == "KOTAKBANK")
        assert kotak["document_url"] == ""
        assert kotak["has_xbrl"] is False

    def test_unknown_endpoint_returns_empty(self, nse_announcements_data):
        """Unknown endpoint_type returns empty list (no match in _normalize_nse_record)."""
        filings = parse_nse_response(nse_announcements_data, "unknown_type")
        assert filings == []


# ===========================================================================
# SEBI parser tests
# ===========================================================================


class TestSebiHasNextPage:
    """Tests for sebi_has_next_page() pagination detection."""

    def test_totalpage_hidden_input_parsed(self):
        """Parses totalpage hidden input to detect more pages."""
        html = "<input type='hidden' name='totalpage' value=5 />"
        assert sebi_has_next_page(html, current_page=0) is True
        assert sebi_has_next_page(html, current_page=3) is True
        assert sebi_has_next_page(html, current_page=4) is False

    def test_no_totalpage_uses_next_link_fallback(self):
        """Falls back to checking for Next JS link when totalpage absent."""
        html = "javascript: searchFormNewsList('n'"
        assert sebi_has_next_page(html, current_page=0) is True

    def test_no_pagination_markers_returns_false(self):
        """Returns False when no pagination markers are present."""
        assert sebi_has_next_page("<html></html>", current_page=0) is False

    def test_last_page_is_false(self):
        """Returns False when current_page is the last page."""
        html = "<input type='hidden' name='totalpage' value=3 />"
        assert sebi_has_next_page(html, current_page=2) is False


class TestParseSebiPage:
    """Tests for parse_sebi_page() full SEBI response parsing."""

    def test_parses_main_filings(self, sebi_response_text):
        """Parses main filing links from all tr[role='row'] rows."""
        filings, has_more = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        main_filings = [f for f in filings if f["subcategory"] != "companion"]
        assert len(main_filings) == 3

    def test_parses_companion_documents(self, sebi_response_text):
        """Companion PDF links create additional filing records."""
        filings, has_more = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        companions = [f for f in filings if f["subcategory"] == "companion"]
        # Row 1 has 1 companion, Row 3 has 2 companions = 3 companions total
        assert len(companions) == 3

    def test_source_is_sebi(self, sebi_response_text):
        """All parsed filings have source='sebi'."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        assert all(f["source"] == "sebi" for f in filings)

    def test_category_name_populated(self, sebi_response_text):
        """Category field is set to the human-readable category name."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        assert all(f["category"] == "Public Issues" for f in filings)

    def test_relative_urls_prefixed_with_sebi_base(self, sebi_response_text):
        """Relative hrefs are prefixed with SEBI_DOC_BASE."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            assert f["document_url"].startswith("http"), f"Non-absolute URL: {f['document_url']}"

    def test_absolute_urls_not_double_prefixed(self, sebi_response_text):
        """Absolute hrefs are not double-prefixed."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            assert not f["document_url"].startswith(f"{SEBI_DOC_BASE}{SEBI_DOC_BASE}")

    def test_has_more_is_true_when_more_pages(self, sebi_response_text):
        """has_more is True when current_page < total_pages - 1."""
        _, has_more = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        assert has_more is True

    def test_has_more_false_on_last_page(self, sebi_response_text):
        """has_more is False when current_page equals total_pages - 1."""
        _, has_more = parse_sebi_page(sebi_response_text, category_id=15, current_page=2)
        assert has_more is False

    def test_date_text_normalized_to_iso(self, sebi_response_text):
        """Filing dates from SEBI are normalized to YYYY-MM-DD."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        main_filings = [f for f in filings if f["subcategory"] != "companion"]
        dates = {f["filing_date"] for f in main_filings}
        assert "2024-01-10" in dates
        assert "2024-01-08" in dates

    def test_filing_id_extracted_from_url(self, sebi_response_text):
        """Filing ID is extracted from the numeric part of the URL."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        ids = {f["filing_id"] for f in filings if f["subcategory"] != "companion"}
        assert "12345" in ids
        assert "12344" in ids

    def test_empty_html_returns_no_filings(self):
        """Empty HTML content returns empty filings list."""
        empty_response = "   #@#extra"
        filings, _ = parse_sebi_page(empty_response, category_id=15, current_page=0)
        assert filings == []

    def test_hash_at_hash_delimiter_split(self):
        """Response is split at #@# and only the first part is parsed as HTML."""
        html_part = (
            "<table><tr role='row'><td>01-Jan-2024</td>"
            "<td><a href='/test_99999.html'>Test Filing</a></td></tr></table>"
        )
        response = f"{html_part}#@#this_should_be_ignored#@#also_ignored"
        filings, _ = parse_sebi_page(response, category_id=15, current_page=0)
        assert len(filings) == 1


# ===========================================================================
# classify_filing_type tests
# ===========================================================================


class TestClassifyFilingType:
    """Tests for classify_filing_type() across all sources."""

    @pytest.mark.parametrize(
        "headline, expected",
        [
            ("Annual Report 2023-2024", "Annual Report"),
            ("Q3 FY2024 Financial Results", "Financial Results"),
            ("Quarterly Results for Dec 2023", "Financial Results"),
            ("Board Meeting Intimation", "Board Meeting"),
            ("Annual General Meeting Notice", "AGM/EGM"),
            ("EGM Notice", "AGM/EGM"),
            ("Declaration of Interim Dividend", "Dividend"),
            ("Final Dividend for FY2024", "Dividend"),
            ("Open Offer - Takeover of XYZ Corp", "Takeover / Merger"),
            ("Scheme of Arrangement between ABC and PQR", "Takeover / Merger"),
            ("Amalgamation Proposal", "Takeover / Merger"),
            ("Draft Red Herring Prospectus - IPO", "IPO / Rights Issue"),
            ("Rights Issue Open", "IPO / Rights Issue"),
            ("Initial Public Offer Document", "IPO / Rights Issue"),
            ("Buyback of Equity Shares", "Buyback"),
            ("Buy-back offer document", "Buyback"),
            ("XBRL Financial Data Submission", "XBRL Filing"),
            ("Credit Rating Upgrade to AA+", "Credit Rating"),
            ("Appointment of New Director", "Change in Management"),
            ("Resignation of CFO", "Change in Management"),
            ("Outcome of Board Meeting Held Today", "Outcome of Meeting"),
            ("Newspaper Publication of Results", "Newspaper Publication"),
            ("Shareholding Pattern Q3 FY24", "Regulatory Filing"),
            ("LODR Compliance Certificate", "Regulatory Filing"),
            ("Insider Trading Policy Update", "Insider Trading"),
            ("Random unknown text about company XYZ", "Other"),
            ("", "Other"),
        ],
    )
    def test_classification(self, headline, expected):
        """Verify filing type classification for various headlines."""
        result = classify_filing_type(headline)
        assert result == expected, f"headline={headline!r}: got {result!r}, want {expected!r}"

    def test_case_insensitive(self):
        """Classification is case-insensitive."""
        assert classify_filing_type("ANNUAL REPORT") == "Annual Report"
        assert classify_filing_type("annual report") == "Annual Report"
        assert classify_filing_type("Annual Report") == "Annual Report"

    def test_none_like_empty_returns_other(self):
        """Empty string returns 'Other'."""
        assert classify_filing_type("") == "Other"

    def test_bse_category_mapped(self):
        """BSE 'Quarterly Results' category maps to Financial Results."""
        assert classify_filing_type("Quarterly Results for Q3") == "Financial Results"

    def test_nse_annual_report_subject(self):
        """NSE annual report subject maps to Annual Report."""
        assert classify_filing_type("Annual Report 2022-2023 - Infosys Ltd") == "Annual Report"

    def test_sebi_public_issue_subject(self):
        """SEBI public issue title maps to IPO / Rights Issue."""
        assert classify_filing_type("Draft Red Herring Prospectus - XYZ Technologies Ltd") == "IPO / Rights Issue"


# ===========================================================================
# isin / lei / language field tests (spec v1.x)
# ===========================================================================


class TestIsinLeiLanguageInParsers:
    """Tests that isin, lei, and language fields are correctly set by all parsers."""

    # ---- BSE ----

    def test_bse_isin_is_none(self, bse_response_data):
        """BSE parser sets isin=None (BSE API does not include ISIN natively)."""
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            assert f["isin"] is None, (
                f"BSE filing {f['filing_id']} should have isin=None, got {f['isin']!r}"
            )

    def test_bse_lei_is_none(self, bse_response_data):
        """BSE parser sets lei=None."""
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            assert f["lei"] is None

    def test_bse_language_is_en(self, bse_response_data):
        """BSE parser sets language='en' for all filings."""
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            assert f["language"] == "en", (
                f"BSE filing {f['filing_id']} should have language='en', got {f['language']!r}"
            )

    # ---- NSE announcements ----

    def test_nse_announcements_isin_populated(self, nse_announcements_data):
        """NSE announcements parser extracts ISIN from sm_isin field."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        reliance = next(f for f in filings if f["symbol"] == "RELIANCE")
        assert reliance["isin"] == "INE002A01018"

    def test_nse_announcements_isin_format_indian(self, nse_announcements_data):
        """NSE ISIN values follow Indian ISIN format: INE + 9 chars."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        for f in filings:
            if f["isin"]:
                assert f["isin"].startswith("INE"), (
                    f"Expected Indian ISIN (INE...) for NSE filing, got {f['isin']!r}"
                )
                assert len(f["isin"]) == 12, (
                    f"ISIN should be 12 chars (ISO 6166), got {len(f['isin'])} for {f['isin']!r}"
                )

    def test_nse_announcements_language_is_en(self, nse_announcements_data):
        """NSE announcements parser sets language='en'."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        for f in filings:
            assert f["language"] == "en"

    def test_nse_announcements_lei_is_none(self, nse_announcements_data):
        """NSE announcements parser sets lei=None."""
        filings = parse_nse_response(nse_announcements_data, "announcements")
        for f in filings:
            assert f["lei"] is None

    # ---- NSE board meetings ----

    def test_nse_board_meetings_isin_from_sm_isin(self, nse_board_meetings_data):
        """NSE board_meetings parser extracts ISIN from sm_isin field."""
        filings = parse_nse_response(nse_board_meetings_data, "board_meetings")
        assert filings[0]["isin"] == "INE040A01034"

    def test_nse_board_meetings_language_is_en(self, nse_board_meetings_data):
        """NSE board_meetings parser sets language='en'."""
        filings = parse_nse_response(nse_board_meetings_data, "board_meetings")
        assert filings[0]["language"] == "en"

    # ---- NSE financial results ----

    def test_nse_financial_results_isin_populated(self, nse_financial_results_data):
        """NSE financial_results parser extracts ISIN from isin/sm_isin fields."""
        filings = parse_nse_response(nse_financial_results_data, "financial_results")
        axisbank = next(f for f in filings if f["symbol"] == "AXISBANK")
        assert axisbank["isin"] == "INE238A01034"

    def test_nse_financial_results_language_is_en(self, nse_financial_results_data):
        """NSE financial_results parser sets language='en'."""
        filings = parse_nse_response(nse_financial_results_data, "financial_results")
        for f in filings:
            assert f["language"] == "en"

    # ---- SEBI ----

    def test_sebi_isin_is_none(self, sebi_response_text):
        """SEBI parser sets isin=None (regulatory filings lack per-security ISIN)."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            assert f["isin"] is None, (
                f"SEBI filing {f['filing_id']} should have isin=None, got {f['isin']!r}"
            )

    def test_sebi_lei_is_none(self, sebi_response_text):
        """SEBI parser sets lei=None."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            assert f["lei"] is None

    def test_sebi_language_is_en(self, sebi_response_text):
        """SEBI parser sets language='en' for all filings including companions."""
        filings, _ = parse_sebi_page(sebi_response_text, category_id=15, current_page=0)
        for f in filings:
            assert f["language"] == "en", (
                f"SEBI filing {f['filing_id']} should have language='en', got {f['language']!r}"
            )

    # ---- BSE ISIN map lookup ----

    def test_bse_isin_populated_from_map(self, bse_response_data):
        """parse_bse_response populates isin when bse_isin_map covers the scrip code."""
        filings, _ = parse_bse_response(bse_response_data, bse_isin_map=_BSE_ISIN_MAP)
        ril = next(f for f in filings if f["symbol"] == "500325")
        assert ril["isin"] == "INE002A01018"

    def test_bse_isin_all_mapped_codes_resolved(self, bse_response_data):
        """All scrip codes present in bse_isin_map are resolved correctly."""
        filings, _ = parse_bse_response(bse_response_data, bse_isin_map=_BSE_ISIN_MAP)
        code_to_isin = {f["symbol"]: f["isin"] for f in filings}
        assert code_to_isin["500325"] == "INE002A01018"
        assert code_to_isin["532540"] == "INE467B01029"
        assert code_to_isin["500209"] == "INE009A01021"

    def test_bse_isin_none_for_missing_scrip_in_map(self, bse_response_data):
        """parse_bse_response sets isin=None when scrip code is absent from the map."""
        filings, _ = parse_bse_response(bse_response_data, bse_isin_map=_BSE_ISIN_MAP)
        # 500180 (HDFC) was intentionally omitted from _BSE_ISIN_MAP
        hdfc = next(f for f in filings if f["symbol"] == "500180")
        assert hdfc["isin"] is None

    def test_bse_isin_none_when_no_map_passed(self, bse_response_data):
        """parse_bse_response keeps isin=None when bse_isin_map is not provided."""
        filings, _ = parse_bse_response(bse_response_data)
        for f in filings:
            assert f["isin"] is None

    def test_bse_isin_none_when_empty_map_passed(self, bse_response_data):
        """parse_bse_response gives isin=None for all filings when map is empty."""
        filings, _ = parse_bse_response(bse_response_data, bse_isin_map={})
        for f in filings:
            assert f["isin"] is None


# ===========================================================================
# fetch_bse_isin_map tests (scraper-level helper)
# ===========================================================================


class TestFetchBseIsinMap:
    """Unit tests for scraper.fetch_bse_isin_map()."""

    def _make_mock_session(self, payload: object, status_code: int = 200) -> "requests.Session":
        """Return a mock session whose .get() returns the given payload."""
        import requests
        from unittest.mock import MagicMock

        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        if status_code != 200:
            mock_resp.raise_for_status.side_effect = requests.HTTPError(
                response=mock_resp
            )
        else:
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = payload

        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp
        return mock_session

    def test_returns_dict_from_bare_list_response(self):
        """Builds the map correctly when the API returns a bare JSON array."""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        payload = [
            {"SCRIP_CD": "500325", "ISIN_NUMBER": "INE002A01018"},
            {"SCRIP_CD": "532540", "ISIN_NUMBER": "INE467B01029"},
        ]
        session = self._make_mock_session(payload)
        result = fetch_bse_isin_map(session)
        assert result == {"500325": "INE002A01018", "532540": "INE467B01029"}

    def test_returns_dict_from_table_wrapped_response(self):
        """Builds the map when the API wraps the list under a 'Table' key."""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        payload = {
            "Table": [{"SCRIP_CD": "500209", "ISIN_NUMBER": "INE009A01021"}],
            "Table1": [],
        }
        session = self._make_mock_session(payload)
        result = fetch_bse_isin_map(session)
        assert result == {"500209": "INE009A01021"}

    def test_returns_empty_dict_on_http_error(self):
        """Returns {} without raising when the HTTP request fails."""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        session = self._make_mock_session({}, status_code=503)
        result = fetch_bse_isin_map(session)
        assert result == {}

    def test_returns_empty_dict_on_network_error(self):
        """Returns {} without raising on connection error."""
        import requests
        import sys
        from unittest.mock import MagicMock
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.side_effect = requests.ConnectionError("network down")
        result = fetch_bse_isin_map(mock_session)
        assert result == {}

    def test_returns_empty_dict_on_unexpected_response_shape(self):
        """Returns {} when the response is not a list or dict with known key."""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        session = self._make_mock_session("unexpected string")
        result = fetch_bse_isin_map(session)
        assert result == {}

    def test_skips_rows_without_scrip_or_isin(self):
        """Rows missing SCRIP_CD or ISIN_NUMBER are silently skipped."""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraper import fetch_bse_isin_map

        payload = [
            {"SCRIP_CD": "500325", "ISIN_NUMBER": "INE002A01018"},
            {"SCRIP_CD": "",       "ISIN_NUMBER": "INE000000000"},  # blank scrip
            {"SCRIP_CD": "999999", "ISIN_NUMBER": ""},              # blank isin
            {},                                                       # no keys at all
        ]
        session = self._make_mock_session(payload)
        result = fetch_bse_isin_map(session)
        assert result == {"500325": "INE002A01018"}
