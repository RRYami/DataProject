"""
DataProject FastAPI Application.

Main entry point for the FastAPI application.
All API endpoints are organized in separate routers under the api/ directory.
"""

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import companies, tickers, treasury

# Load environment variables
load_dotenv("./secret/.env")

# Create FastAPI application
app = FastAPI(
    title="DataProject API",
    description="API for financial data including company details, price history, and treasury yields",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS (adjust origins as needed for production)
app.add_middleware(
    CORSMiddleware,  # type: ignore
    allow_origins=["*"],  # Change to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(companies.router)
app.include_router(tickers.router)
app.include_router(treasury.router)


@app.get("/", tags=["root"])
async def root() -> dict:
    """
    Root endpoint providing API information.

    Returns:
    Dict with welcome message and documentation links
    """
    return {
        "message": "Welcome to DataProject API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
    }


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    """
    Health check endpoint.

    Returns:
    Dict with status
    """
    return {"status": "healthy"}
