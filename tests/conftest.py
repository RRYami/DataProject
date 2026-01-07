"""
Pytest configuration and shared fixtures for tests.

This module provides reusable fixtures for testing the DataProject API.
"""

import os
from typing import Generator

import duckdb as ddb
import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture(scope="function")
def test_db_path(tmp_path) -> str:
    """
    Create a temporary database path for testing.
    Each test gets its own unique database file.

    Returns:
        str: Path to temporary test database
    """
    db_path = tmp_path / "test_polygon.duckdb"
    return str(db_path)


@pytest.fixture(scope="function")
def test_db(test_db_path: str) -> Generator[ddb.DuckDBPyConnection, None, None]:
    """
    Create a test database with sample data.

    Args:
        test_db_path: Path to test database

    Yields:
        DuckDB connection with test data
    """
    # Set test database path in environment
    original_db_path = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = test_db_path

    conn = ddb.connect(test_db_path)

    # Drop tables if they exist (clean slate for each test)
    conn.execute("DROP TABLE IF EXISTS company_details CASCADE")
    conn.execute("DROP TABLE IF EXISTS price_data CASCADE")
    conn.execute("DROP TABLE IF EXISTS treasury_curves CASCADE")
    conn.execute("DROP TABLE IF EXISTS tickers CASCADE")
    conn.execute("DROP TABLE IF EXISTS sic_to_naics CASCADE")

    # Create test tables
    conn.execute("""
        CREATE TABLE company_details (
            ticker VARCHAR PRIMARY KEY,
            name VARCHAR,
            market_cap BIGINT,
            active BOOLEAN,
            sic_code INT
        )
    """)

    conn.execute("""
        CREATE TABLE price_data (
            ticker VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        )
    """)

    conn.execute("""
        CREATE TABLE treasury_curves (
            date DATE PRIMARY KEY,
            DGS1MO FLOAT,
            DGS3MO FLOAT,
            DGS6MO FLOAT,
            DGS1 FLOAT,
            DGS2 FLOAT,
            DGS5 FLOAT,
            DGS10 FLOAT,
            DGS30 FLOAT
        )
    """)

    conn.execute("""
        CREATE TABLE tickers (
            ticker VARCHAR PRIMARY KEY,
            name VARCHAR,
            market VARCHAR,
            locale VARCHAR,
            active BOOLEAN,
            source_feed VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE sic_to_naics (
            sic_code INT,
            sic_description VARCHAR,
            naics_code INT,
            naics_description VARCHAR
        )
    """)

    # Insert test data
    conn.execute("""
        INSERT INTO company_details VALUES
        ('AAPL', 'Apple Inc.', 3000000000000, true, 3571),
        ('MSFT', 'Microsoft Corp.', 2800000000000, true, 7372)
    """)

    conn.execute("""
        INSERT INTO price_data VALUES
        ('AAPL', '2024-01-02', 184.35, 186.95, 183.89, 185.64, 58414460),
        ('AAPL', '2024-01-03', 185.50, 187.00, 184.00, 186.50, 60000000),
        ('MSFT', '2024-01-02', 375.00, 380.00, 374.00, 378.50, 25000000)
    """)

    conn.execute("""
        INSERT INTO treasury_curves VALUES
        ('2024-01-15', 5.45, 5.38, 5.25, 4.95, 4.42, 4.12, 4.05, 4.25),
        ('2024-01-16', 5.46, 5.39, 5.26, 4.96, 4.43, 4.13, 4.06, 4.26)
    """)

    conn.execute("""
        INSERT INTO tickers VALUES
        ('SPX', 'S&P 500 Index', 'indices', 'us', true, 'polygon'),
        ('NDX', 'Nasdaq 100 Index', 'indices', 'us', true, 'polygon')
    """)

    conn.execute("""
        INSERT INTO sic_to_naics VALUES
        (3571, 'Electronic Computers', 334111, 'Electronic Computer Manufacturing')
    """)

    conn.commit()

    yield conn

    # Cleanup
    conn.close()
    if original_db_path:
        os.environ["DB_PATH"] = original_db_path
    else:
        os.environ.pop("DB_PATH", None)


@pytest.fixture(scope="function")
def client(test_db: ddb.DuckDBPyConnection) -> TestClient:
    """
    Create a test client for the FastAPI application.

    Args:
        test_db: Test database connection fixture

    Returns:
        TestClient for making API requests
    """
    return TestClient(app)


@pytest.fixture
def sample_company_data() -> dict:
    """Sample company data for testing."""
    return {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "market_cap": 3000000000000,
        "active": True,
        "sic_code": 3571,
    }


@pytest.fixture
def sample_indice_data() -> dict:
    """Sample indice data for testing."""
    return {
        "indice": "DJI",
        "name": "Dow Jones Industrial Average",
        "market": "indices",
        "locale": "us",
        "active": True,
        "source_feed": "polygon",
    }
