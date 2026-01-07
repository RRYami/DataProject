"""
Unit tests for tickers router.

Tests all ticker/indices-related endpoints including listing
available indices and adding new indices.
"""

import pytest
from fastapi.testclient import TestClient


class TestListAvailableIndices:
    """Tests for GET /tickers/indices/available endpoint."""

    def test_list_available_indices_success(self, client: TestClient):
        """Test listing all available indices."""
        response = client.get("/tickers/indices/available")

        assert response.status_code == 200
        data = response.json()
        assert "available_indices" in data
        assert isinstance(data["available_indices"], list)
        assert len(data["available_indices"]) == 2

    def test_list_available_indices_structure(self, client: TestClient):
        """Test indices data has correct structure."""
        response = client.get("/tickers/indices/available")

        assert response.status_code == 200
        data = response.json()
        indices = data["available_indices"]

        # Check first indice structure
        indice = indices[0]
        assert "ticker" in indice
        assert "name" in indice
        assert "market" in indice
        assert "locale" in indice
        assert "active" in indice
        assert "source_feed" in indice

    def test_list_available_indices_data(self, client: TestClient):
        """Test indices contain expected data."""
        response = client.get("/tickers/indices/available")

        assert response.status_code == 200
        data = response.json()
        tickers = [idx["ticker"] for idx in data["available_indices"]]

        assert "SPX" in tickers
        assert "NDX" in tickers


class TestPostIndice:
    """Tests for POST /tickers/indices endpoint."""

    def test_post_indice_success(
        self, client: TestClient, sample_indice_data: dict
    ):
        """Test successfully adding a new indice."""
        response = client.post("/tickers/indices", json=sample_indice_data)

        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert sample_indice_data["indice"] in data["message"]

    def test_post_indice_validation_missing_field(self, client: TestClient):
        """Test indice creation fails with missing required field."""
        incomplete_data = {
            "indice": "DJI",
            "name": "Dow Jones Industrial Average",
            # Missing required fields
        }

        response = client.post("/tickers/indices", json=incomplete_data)

        assert response.status_code == 422  # Validation error

    def test_post_indice_validation_empty_indice(self, client: TestClient):
        """Test indice creation fails with empty indice field."""
        invalid_data = {
            "indice": "",  # Empty string should fail min_length validation
            "name": "Test Index",
            "market": "indices",
            "locale": "us",
            "active": True,
            "source_feed": "polygon",
        }

        response = client.post("/tickers/indices", json=invalid_data)

        assert response.status_code == 422

    def test_post_indice_validation_invalid_type(self, client: TestClient):
        """Test indice creation fails with invalid data type."""
        invalid_data = {
            "indice": "DJI",
            "name": "Dow Jones Industrial Average",
            "market": "indices",
            "locale": "us",
            "active": "not_a_boolean",  # Should be boolean
            "source_feed": "polygon",
        }

        response = client.post("/tickers/indices", json=invalid_data)

        assert response.status_code == 422

    def test_post_indice_verify_insertion(
        self, client: TestClient, sample_indice_data: dict
    ):
        """Test that posted indice can be retrieved."""
        # Post new indice
        post_response = client.post("/tickers/indices", json=sample_indice_data)
        assert post_response.status_code == 200

        # Verify it appears in the list
        get_response = client.get("/tickers/indices/available")
        assert get_response.status_code == 200

        data = get_response.json()
        tickers = [idx["ticker"] for idx in data["available_indices"]]
        assert sample_indice_data["indice"] in tickers

    def test_post_indice_all_fields_present(
        self, client: TestClient, sample_indice_data: dict
    ):
        """Test posted indice has all fields correctly stored."""
        # Post new indice
        client.post("/tickers/indices", json=sample_indice_data)

        # Retrieve and verify
        get_response = client.get("/tickers/indices/available")
        data = get_response.json()

        # Find the posted indice
        posted_indice = next(
            (
                idx
                for idx in data["available_indices"]
                if idx["ticker"] == sample_indice_data["indice"]
            ),
            None,
        )

        assert posted_indice is not None
        assert posted_indice["name"] == sample_indice_data["name"]
        assert posted_indice["market"] == sample_indice_data["market"]
        assert posted_indice["locale"] == sample_indice_data["locale"]
        assert posted_indice["active"] == sample_indice_data["active"]
        assert posted_indice["source_feed"] == sample_indice_data["source_feed"]
