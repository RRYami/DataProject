"""
Prometheus metrics configuration for DataProject API.

This module sets up Prometheus metrics for monitoring API performance,
request counts, latencies, and errors.
"""

from prometheus_client import Counter, Histogram, Gauge, Info

# API Request metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
)

http_requests_in_progress = Gauge(
    "http_requests_in_progress",
    "HTTP requests currently in progress",
    ["method", "endpoint"],
)

# Database metrics
db_queries_total = Counter(
    "db_queries_total", "Total database queries", ["operation", "table"]
)

db_query_duration_seconds = Histogram(
    "db_query_duration_seconds",
    "Database query latency in seconds",
    ["operation", "table"],
)

db_connections_active = Gauge(
    "db_connections_active", "Active database connections"
)

# Application metrics
app_info = Info("app_info", "Application information")

# Error metrics
http_exceptions_total = Counter(
    "http_exceptions_total",
    "Total HTTP exceptions",
    ["exception_type", "endpoint"],
)

# Business metrics
tickers_queried_total = Counter(
    "tickers_queried_total", "Total ticker queries", ["ticker"]
)

price_history_requests_total = Counter(
    "price_history_requests_total", "Total price history requests", ["ticker"]
)

treasury_curve_requests_total = Counter(
    "treasury_curve_requests_total",
    "Total treasury yield curve requests",
    ["query_type"],  # 'all', 'date', 'range', 'latest'
)

# Set application info
app_info.info(
    {
        "version": "1.0.0",
        "name": "DataProject API",
        "environment": "development",
    }
)
