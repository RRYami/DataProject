"""
Company-related API endpoints.

This module handles all company and ticker-related operations including:
- Company details
- Price history
- Available tickers list
"""

from typing import Union

import duckdb as ddb
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_connection, loggers

router = APIRouter(
    prefix="/company",
    tags=["companies"],
    responses={404: {"description": "Not found"}},
)


@router.get("/{ticker}/priceHistory")
async def get_price_history(
    ticker: str,
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    """
    Return price history for the given ticker from the database.

    Parameters:
    - ticker: Stock ticker symbol
    - start_date: Optional start date (format: YYYY-MM-DD)
    - end_date: Optional end date (format: YYYY-MM-DD)

    Returns:
    Dict containing ticker and price history results
    """
    query = """
        SELECT *
        FROM price_data
        WHERE UPPER(ticker) = UPPER(?)
    """
    params = [ticker]

    if start_date:
        query += f" AND date >= '{start_date}'"

    if end_date:
        query += f" AND date <= '{end_date}'"

    try:
        df = conn.execute(query, params).pl()
        data = df.to_dicts()
    except Exception:
        loggers.exception("Failed to query price history")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found")

    return {"ticker": ticker.upper(), "results": data}


@router.get("/{ticker}")
async def get_company(
    ticker: str,
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    """
    Return company details for the given ticker from the database.

    Parameters:
    - ticker: Stock ticker symbol

    Returns:
    Dict containing ticker and company details including NAICS/SIC codes
    """
    query = """
        SELECT cd.*, sc.naics_code, sc.naics_description, sc.sic_code ,sc.sic_description
        FROM company_details cd
        LEFT JOIN sic_to_naics sc ON cd.sic_code = sc.sic_code
        WHERE UPPER(cd.ticker) = UPPER(?)
    """
    try:
        df = conn.execute(query, (ticker,)).pl()
        data = df.to_dicts()
    except Exception:
        loggers.exception("Failed to query company details")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found")

    return {"ticker": ticker.upper(), "results": data}


@router.get("/list/available_tickers", name="List Available Tickers")
async def list_available_tickers(
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    """
    Return a list of all available tickers in the company_details table.

    Returns:
    Dict containing list of available ticker symbols
    """
    query = "SELECT DISTINCT ticker FROM company_details"
    try:
        df = conn.execute(query).pl()
        tickers = df["ticker"].to_list()
    except Exception:
        loggers.exception("Failed to query available tickers")
        raise HTTPException(status_code=500, detail="Database query failed")

    return {"available_tickers": tickers}
