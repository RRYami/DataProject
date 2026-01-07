"""
Ticker and indices API endpoints.

This module handles all ticker/indices-related operations including:
- Listing available indices
- Adding new indices
"""

import duckdb as ddb
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_connection, loggers
from api.models.ticker import IndiceCreate, IndiceResponse

router = APIRouter(
    prefix="/tickers",
    tags=["tickers"],
    responses={404: {"description": "Not found"}},
)


@router.get("/indices/available", name="List Available Indices")
async def list_available_indices(
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    """
    Return a list of all available indices in the tickers table.

    Returns:
    Dict containing list of available indices with their details
    """
    query = "FROM tickers"
    try:
        df = conn.execute(query).pl()
    except Exception:
        loggers.exception("Failed to query available indices")
        raise HTTPException(status_code=500, detail="Database query failed")

    return {"available_indices": df.to_dicts()}


@router.post("/indices", name="Add New Indice", response_model=IndiceResponse)
async def post_indice(
    indice_data: IndiceCreate,
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> IndiceResponse:
    """
    Add a new indice to the tickers table.

    Parameters:
    - indice_data: IndiceCreate model containing all required fields

    Returns:
    Success message upon successful insertion
    """
    query = "INSERT INTO tickers (ticker, name, market, locale, active, source_feed) VALUES (?, ?, ?, ?, ?, ?)"
    try:
        conn.execute(
            query,
            (
                indice_data.indice,
                indice_data.name,
                indice_data.market,
                indice_data.locale,
                indice_data.active,
                indice_data.source_feed,
            ),
        )
    except Exception:
        loggers.exception("Failed to insert new indice")
        raise HTTPException(status_code=500, detail="Database insert failed")

    return IndiceResponse(
        message=f"Indice {indice_data.indice} added successfully"
    )
