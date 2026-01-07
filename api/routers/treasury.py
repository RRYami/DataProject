"""
Treasury yield curve API endpoints.

This module handles all US Treasury yield curve-related operations.
"""

from typing import Optional

import duckdb as ddb
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_connection, loggers

router = APIRouter(
    prefix="/curves",
    tags=["treasury"],
    responses={404: {"description": "Not found"}},
)


@router.get("/US_treasury_yield", name="Get US Treasury Yield Curve")
async def get_us_treasury_yield_curve(
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    latest_only: bool = False,
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    """
    Return US Treasury yield curve data from the database.

    Parameters:
    - date: Optional specific date to query (format: YYYY-MM-DD)
    - start_date: Optional start date for range query (format: YYYY-MM-DD)
    - end_date: Optional end date for range query (format: YYYY-MM-DD)
    - limit: Maximum number of records to return. If None, returns all records.
    - offset: Number of records to skip for pagination (default: 0)
    - latest_only: If True, returns only the most recent date's data (default: False)

    Note:
    - If 'date' is provided, it takes precedence over 'start_date' and 'end_date'
    - You can use 'start_date' alone, 'end_date' alone, or both together for range queries
    - Requesting all records without a limit may return a large dataset.
    - Consider using pagination (limit/offset) for better performance.

    Returns:
    Dict containing results, count, total_count, and offset
    """
    if date:
        # Query specific date (takes precedence)
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
    elif start_date or end_date:
        # Query date range
        query = "SELECT * FROM treasury_curves WHERE 1=1"
        params = []

        if start_date:
            query += " AND date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND date <= ?"
            params.append(end_date)

        query += " ORDER BY date DESC"

        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset}"

        params = tuple(params) if params else None
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
            # Build count query with same filters
            count_query = (
                "SELECT COUNT(*) as cnt FROM treasury_curves WHERE 1=1"
            )
            count_params = []

            if start_date:
                count_query += " AND date >= ?"
                count_params.append(start_date)

            if end_date:
                count_query += " AND date <= ?"
                count_params.append(end_date)

            total_count = conn.execute(
                count_query, tuple(count_params) if count_params else None
            ).pl()["cnt"][0]
        else:
            total_count = len(data)

    except Exception:
        loggers.exception("Failed to query US Treasury yield curve")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not data:
        raise HTTPException(status_code=404, detail="No yield curve data found")

    return {
        "results": data,
        "count": len(data),
        "total_count": total_count,
        "offset": offset if limit is not None else 0,
    }
