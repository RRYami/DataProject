"""
Unit tests for companies router.

Tests all company-related endpoints including company details,
price history, and available tickers.
"""

import pytest
from fastapi.testclient import TestClient


class TestGetCompany:
    """Tests for GET /company/{ticker} endpoint."""

    def test_get_company_success(self, client: TestClient):
        """Test successful company retrieval."""
        response = client.get("/company/AAPL")

        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert "results" in data
        assert len(data["results"]) == 1
        assert data["results"][0]["name"] == "Apple Inc."
        assert data["results"][0]["market_cap"] == 3000000000000

    def test_get_company_case_insensitive(self, client: TestClient):
        """Test company lookup is case-insensitive."""
        response = client.get("/company/aapl")

        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"

    def test_get_company_not_found(self, client: TestClient):
        """Test company not found returns 404."""
        response = client.get("/company/INVALID")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_company_with_classification(self, client: TestClient):
        """Test company includes SIC/NAICS classification."""
        response = client.get("/company/AAPL")

        assert response.status_code == 200
        data = response.json()
        result = data["results"][0]
        assert result["sic_code"] == 3571


class TestGetPriceHistory:
    """Tests for GET /company/{ticker}/priceHistory endpoint."""

    def test_get_price_history_all(self, client: TestClient):
        """Test retrieving all price history for a ticker."""
        response = client.get("/company/AAPL/priceHistory")

        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert len(data["results"]) == 2  # Two price records for AAPL

    def test_get_price_history_with_start_date(self, client: TestClient):
        """Test price history with start_date filter."""
        response = client.get(
            "/company/AAPL/priceHistory", params={"start_date": "2024-01-03"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-03"

    def test_get_price_history_with_end_date(self, client: TestClient):
        """Test price history with end_date filter."""
        response = client.get(
            "/company/AAPL/priceHistory", params={"end_date": "2024-01-02"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-02"

    def test_get_price_history_with_date_range(self, client: TestClient):
        """Test price history with both start and end dates."""
        response = client.get(
            "/company/AAPL/priceHistory",
            params={"start_date": "2024-01-02", "end_date": "2024-01-03"},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2

    def test_get_price_history_not_found(self, client: TestClient):
        """Test price history for non-existent ticker returns 404."""
        response = client.get("/company/INVALID/priceHistory")

        assert response.status_code == 404

    def test_get_price_history_data_structure(self, client: TestClient):
        """Test price history data has correct structure."""
        response = client.get("/company/AAPL/priceHistory")

        assert response.status_code == 200
        data = response.json()
        price_record = data["results"][0]

        # Check all expected fields are present
        assert "date" in price_record
        assert "open" in price_record
        assert "high" in price_record
        assert "low" in price_record
        assert "close" in price_record
        assert "volume" in price_record


class TestListAvailableTickers:
    """Tests for GET /company/list/available_tickers endpoint."""

    def test_list_available_tickers_success(self, client: TestClient):
        """Test listing all available tickers."""
        response = client.get("/company/list/available_tickers")

        assert response.status_code == 200
        data = response.json()
        assert "available_tickers" in data
        assert isinstance(data["available_tickers"], list)
        assert "AAPL" in data["available_tickers"]
        assert "MSFT" in data["available_tickers"]

    def test_list_available_tickers_distinct(self, client: TestClient):
        """Test that ticker list contains distinct values."""
        response = client.get("/company/list/available_tickers")

        assert response.status_code == 200
        data = response.json()
        tickers = data["available_tickers"]

        # Check no duplicates
        assert len(tickers) == len(set(tickers))
