"""
Monitoring and metrics API endpoints.

This module provides endpoints for exposing Prometheus metrics
and application health/status information.
"""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.monitoring.metrics import (
    http_requests_total,
    http_request_duration_seconds,
    db_queries_total,
)

router = APIRouter(
    prefix="/monitoring",
    tags=["monitoring"],
)


@router.get("/metrics")
async def get_metrics() -> Response:
    """
    Expose Prometheus metrics.

    This endpoint returns metrics in Prometheus format that can be
    scraped by a Prometheus server.

    Returns:
        Prometheus metrics in text format
    """
    metrics_data = generate_latest()
    return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)


@router.get("/health/detailed")
async def detailed_health() -> dict:
    """
    Detailed health check with metrics summary.

    Returns:
        Dict with health status and key metrics
    """
    # Get current metric values (simplified for demo)
    return {
        "status": "healthy",
        "metrics": {
            "total_requests": "See /monitoring/metrics for detailed counts",
            "active_connections": "Tracked via Prometheus",
        },
        "endpoints": {
            "metrics": "/monitoring/metrics",
            "health": "/health",
            "detailed_health": "/monitoring/health/detailed",
        },
    }


@router.get("/stats")
async def get_stats() -> dict:
    """
    Get application statistics.

    Returns:
        Dict with application stats and metrics summary
    """
    return {
        "message": "Statistics available at /monitoring/metrics endpoint",
        "format": "Prometheus format",
        "available_metrics": [
            "http_requests_total - Total HTTP requests by method, endpoint, status",
            "http_request_duration_seconds - Request latency histogram",
            "http_requests_in_progress - Current in-progress requests",
            "db_queries_total - Total database queries by operation and table",
            "db_query_duration_seconds - Database query latency",
            "db_connections_active - Active database connections",
            "http_exceptions_total - Total exceptions by type",
            "tickers_queried_total - Total ticker queries",
            "price_history_requests_total - Price history requests",
            "treasury_curve_requests_total - Treasury curve requests",
        ],
    }
