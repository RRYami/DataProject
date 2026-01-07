"""
Monitoring middleware for FastAPI application.

This module provides middleware for tracking HTTP request metrics,
including request counts, latencies, and errors.
"""

import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from api.monitoring.metrics import (
    http_requests_total,
    http_request_duration_seconds,
    http_requests_in_progress,
    http_exceptions_total,
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware to collect Prometheus metrics for HTTP requests.

    Tracks request counts, latencies, and in-progress requests.
    """

    def __init__(self, app: ASGIApp):
        """
        Initialize Prometheus middleware.

        Args:
            app: ASGI application
        """
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and collect metrics.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response
        """
        # Get request details
        method = request.method
        path = request.url.path

        # Track in-progress requests
        http_requests_in_progress.labels(method=method, endpoint=path).inc()

        # Track request duration
        start_time = time.time()

        try:
            # Process request
            response = await call_next(request)
            status_code = response.status_code

        except Exception as exc:
            # Track exceptions
            http_exceptions_total.labels(
                exception_type=type(exc).__name__, endpoint=path
            ).inc()
            raise

        finally:
            # Calculate duration
            duration = time.time() - start_time

            # Record metrics
            http_requests_total.labels(
                method=method, endpoint=path, status=status_code
            ).inc()

            http_request_duration_seconds.labels(
                method=method, endpoint=path
            ).observe(duration)

            # Decrement in-progress counter
            http_requests_in_progress.labels(method=method, endpoint=path).dec()

        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log detailed request information.

    Logs request method, path, duration, and status code.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and log details.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response
        """
        start_time = time.time()

        response = await call_next(request)

        duration = time.time() - start_time

        # Log request (could be enhanced with logger)
        print(
            f"{request.method} {request.url.path} "
            f"completed in {duration:.3f}s with status {response.status_code}"
        )

        return response
