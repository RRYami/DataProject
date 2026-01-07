"""
Unit tests for treasury router.

Tests all treasury yield curve-related endpoints with various
query parameter combinations.
"""

import pytest
from fastapi.testclient import TestClient


class TestGetUSTreasuryYieldCurve:
    """Tests for GET /curves/US_treasury_yield endpoint."""

    def test_get_yield_curve_all_data(self, client: TestClient):
        """Test retrieving all yield curve data."""
        response = client.get("/curves/US_treasury_yield")

        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "count" in data
        assert "total_count" in data
        assert "offset" in data
        assert len(data["results"]) == 2

    def test_get_yield_curve_specific_date(self, client: TestClient):
        """Test retrieving yield curve for specific date."""
        response = client.get(
            "/curves/US_treasury_yield", params={"date": "2024-01-15"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-15"

    def test_get_yield_curve_latest_only(self, client: TestClient):
        """Test retrieving only the latest yield curve."""
        response = client.get(
            "/curves/US_treasury_yield", params={"latest_only": True}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-16"  # Latest date

    def test_get_yield_curve_with_start_date(self, client: TestClient):
        """Test yield curve with start_date filter."""
        response = client.get(
            "/curves/US_treasury_yield", params={"start_date": "2024-01-16"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-16"

    def test_get_yield_curve_with_end_date(self, client: TestClient):
        """Test yield curve with end_date filter."""
        response = client.get(
            "/curves/US_treasury_yield", params={"end_date": "2024-01-15"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-15"

    def test_get_yield_curve_with_date_range(self, client: TestClient):
        """Test yield curve with both start_date and end_date."""
        response = client.get(
            "/curves/US_treasury_yield",
            params={"start_date": "2024-01-15", "end_date": "2024-01-16"},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2

    def test_get_yield_curve_with_limit(self, client: TestClient):
        """Test yield curve with limit parameter."""
        response = client.get("/curves/US_treasury_yield", params={"limit": 1})

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert len(data["results"]) == 1
        assert data["total_count"] == 2  # Total in database

    def test_get_yield_curve_with_offset(self, client: TestClient):
        """Test yield curve with offset parameter."""
        response = client.get(
            "/curves/US_treasury_yield", params={"limit": 1, "offset": 1}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["offset"] == 1
        assert len(data["results"]) == 1

    def test_get_yield_curve_pagination(self, client: TestClient):
        """Test pagination through yield curve data."""
        # Get first page
        response1 = client.get(
            "/curves/US_treasury_yield", params={"limit": 1, "offset": 0}
        )
        data1 = response1.json()

        # Get second page
        response2 = client.get(
            "/curves/US_treasury_yield", params={"limit": 1, "offset": 1}
        )
        data2 = response2.json()

        # Verify different results
        assert data1["results"][0]["date"] != data2["results"][0]["date"]

    def test_get_yield_curve_date_takes_precedence(self, client: TestClient):
        """Test that date parameter takes precedence over range."""
        response = client.get(
            "/curves/US_treasury_yield",
            params={
                "date": "2024-01-15",
                "start_date": "2024-01-16",  # Should be ignored
                "end_date": "2024-01-17",  # Should be ignored
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["date"] == "2024-01-15"

    def test_get_yield_curve_data_structure(self, client: TestClient):
        """Test yield curve data has all maturity columns."""
        response = client.get("/curves/US_treasury_yield")

        assert response.status_code == 200
        data = response.json()
        curve = data["results"][0]

        # Check all maturities are present
        assert "date" in curve
        assert "DGS1MO" in curve
        assert "DGS3MO" in curve
        assert "DGS6MO" in curve
        assert "DGS1" in curve
        assert "DGS2" in curve
        assert "DGS5" in curve
        assert "DGS10" in curve
        assert "DGS30" in curve

    def test_get_yield_curve_not_found(self, client: TestClient):
        """Test yield curve returns 404 when no data found."""
        response = client.get(
            "/curves/US_treasury_yield",
            params={"date": "2000-01-01"},  # Date with no data
        )

        assert response.status_code == 404
        detail = response.json()["detail"].lower()
        assert "no" in detail and "found" in detail

    def test_get_yield_curve_ordered_by_date(self, client: TestClient):
        """Test yield curve results are ordered by date descending."""
        response = client.get("/curves/US_treasury_yield")

        assert response.status_code == 200
        data = response.json()
        dates = [result["date"] for result in data["results"]]

        # Should be descending order
        assert dates == sorted(dates, reverse=True)

    def test_get_yield_curve_count_matches_results(self, client: TestClient):
        """Test that count matches actual number of results."""
        response = client.get("/curves/US_treasury_yield")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == len(data["results"])
