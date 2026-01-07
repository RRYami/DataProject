"""
Shared dependencies for API endpoints.

This module contains reusable dependencies like database connections,
authentication, and other common functionality.
"""

import os
from typing import Generator

import duckdb as ddb
from dotenv import load_dotenv
from fastapi import HTTPException

from logger import logger as logger_module

load_dotenv("./secret/.env")
loggers = logger_module.get_logger(__name__)


def get_db_connection() -> Generator[ddb.DuckDBPyConnection, None, None]:
    """
    Dependency that provides a DuckDB connection.

    Yields:
        ddb.DuckDBPyConnection: Database connection

    Raises:
        HTTPException: If database path is not configured
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_db_path() -> str:
    """
    Get the database path from environment variables.

    Returns:
        str: Database path

    Raises:
        HTTPException: If database path is not configured
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )
    return db_path
