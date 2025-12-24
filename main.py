import os
from logging import Logger
from typing import Optional, Union

import duckdb as ddb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import logger.logger as logger

load_dotenv("./secret/.env")
loggers: Logger = logger.get_logger(__name__)


app = FastAPI()


@app.get("/company/{ticker}/priceHistory")
async def get_price_history(
    ticker: str,
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
) -> dict:
    """
    Return price history for the given ticker from the database.
    Optional start_date and end_date can be provided to filter the results.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
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
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found")

    return {"ticker": ticker.upper(), "results": data}


@app.get("/company/{ticker}")
async def get_company(ticker: str) -> dict:
    """
    Return company details for the given ticker from the database.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
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
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found")

    return {"ticker": ticker.upper(), "results": data}


@app.get("/list_available_tickers")
async def list_available_tickers() -> dict:
    """
    Return a list of all available tickers in the company_details table.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
    query = "SELECT DISTINCT ticker FROM company_details"
    try:
        df = conn.execute(query).pl()
        tickers = df["ticker"].to_list()
    except Exception:
        loggers.exception("Failed to query available tickers")
        raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"available_tickers": tickers}


@app.get("/list_available_indices")
async def list_available_indices() -> dict:
    """
    Return a list of all available indices in the tickers table.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
    query = "FROM tickers"
    try:
        df = conn.execute(query).pl()
    except Exception:
        loggers.exception("Failed to query available indices")
        raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"available_indices": df.to_dicts()}


@app.post("/post_indice")
async def post_indice(
    indice: str,
    name: str,
    market: str,
    locale: str,
    active: bool,
    source_feed: str,
) -> dict:
    """
    Add a new indice to the tickers table.

    Parameters:
    - indice: The ticker symbol of the indice.
    - name: The name of the indice.
    - market: The market where the indice is listed.
    - locale: The locale of the indice.
    - active: Boolean indicating if the indice is active.
    - source_feed: The source feed of the indice.

    Returns:
    Success message upon successful insertion.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)
    query = "INSERT INTO tickers (ticker, name, market, locale, active, source_feed) VALUES (?, ?, ?, ?, ?, ?)"
    try:
        conn.execute(query, (indice, name, market, locale, active, source_feed))
    except Exception:
        loggers.exception("Failed to insert new indice")
        raise HTTPException(status_code=500, detail="Database insert failed")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"message": f"Indice {indice} added successfully"}


@app.get("/curves/US_treasury_yield")
async def get_us_treasury_yield_curve(
    date: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    latest_only: bool = False,
) -> dict:
    """
    Return US Treasury yield curve data from the database.

    Parameters:
    - date: Optional specific date to query (format: YYYY-MM-DD)
    - limit: Maximum number of records to return. If None, returns all records.
    - offset: Number of records to skip for pagination (default: 0)
    - latest_only: If True, returns only the most recent date's data (default: False)

    Note: Requesting all records without a limit may return a large dataset.
    Consider using pagination (limit/offset) for better performance.
    """
    db_path = os.getenv("DB_PATH")
    if not db_path:
        loggers.error("DB_PATH not found in environment variables")
        raise HTTPException(
            status_code=500, detail="Database path not configured"
        )

    conn = ddb.connect(db_path)

    if date:
        # Query specific date
        query = """
            SELECT *
            FROM treasury_curves
            WHERE date = ?
        """
        params = (date,)
    elif latest_only:
        # Query only the most recent date
        query = """
            SELECT *
            FROM treasury_curves
            WHERE date = (SELECT MAX(date) FROM treasury_curves)
            ORDER BY date DESC
        """
        params = None
    else:
        # Query with optional pagination
        query = """
            SELECT *
            FROM treasury_curves
            ORDER BY date DESC
        """
        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset}"
        params = None

    try:
        df = conn.execute(query, params).pl()
        data = df.to_dicts()

        # Get total count for pagination info
        if not date and limit is not None:
            total_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM treasury_curves"
            ).pl()["cnt"][0]
        else:
            total_count = len(data)

    except Exception:
        loggers.exception("Failed to query US Treasury yield curve")
        raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not data:
        raise HTTPException(status_code=404, detail="No yield curve data found")

    return {
        "results": data,
        "count": len(data),
        "total_count": total_count,
        "offset": offset if limit is not None else 0,
    }
