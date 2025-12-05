import os
from logging import Logger
from typing import Union

import duckdb as ddb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import logger

load_dotenv("./secret/.env")
loggers: Logger = logger.get_logger(__name__)


app = FastAPI()


@app.get("/priceHistory/{ticker}")
async def get_price_history(
    ticker: str,
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
):
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
async def get_company(ticker: str):
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
        SELECT cd.*, sc.office as sic_office, sc.industry as sic_industry
        FROM company_details cd
        LEFT JOIN sic_codes sc ON cd.sic_code = sc.sic_code
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
