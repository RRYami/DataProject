# AGENTS.md - Development Guidelines for AI Coding Agents

## Overview

This document provides comprehensive guidelines for AI coding agents working on the DataProject codebase. Follow these conventions to maintain code quality, consistency, and functionality.

## Build, Lint, and Test Commands

### Package Management
```bash
# Install dependencies (preferred)
uv sync

# Alternative with pip
pip install -e .
```

### Linting and Code Quality
```bash
# Run linting and formatting checks
ruff check .

# Auto-fix linting issues
ruff check . --fix

# Format code
ruff format .
```

### Running the Application
```bash
# Start FastAPI development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Production deployment
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Testing
```bash
# Run all tests (if pytest is configured)
pytest

# Run tests for specific module
pytest tests/test_module.py

# Run single test
pytest tests/test_module.py::test_function_name

# Run tests with coverage
pytest --cov=src --cov-report=html
```

### Database Operations
```bash
# Initialize classification tables
python -m database.classification_table

# Clean database
python -m database.clean_db

# Run SQL scripts
python database/run.sql
```

## Code Style Guidelines

### Python Version and Dependencies
- **Python Version**: >= 3.13
- **Package Manager**: UV (preferred) or pip
- **Line Length**: 80 characters (enforced by ruff)
- **Type Hints**: Required for all function parameters and return values

### Import Organization
```python
# Standard library imports
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional, Union

# Third-party imports (alphabetical)
import duckdb as ddb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from polars import DataFrame

# Local imports (relative)
from logger.logger import get_logger, setup_logging
import logger.logger as logger
```

### Naming Conventions

#### Classes
- **PascalCase**: `PolygonClient`, `TickerDetailsExtractor`, `BatchTickerExtractor`
- Single responsibility principle - each class has one clear purpose
- Factory pattern: `PolygonExtractorFactory`

#### Functions and Methods
- **snake_case**: `get_price_history`, `extract_ticker_details`, `load_price_data`
- Descriptive names that indicate purpose
- Private methods: `_load_checkpoint`, `_save_checkpoint`

#### Variables
- **snake_case**: `api_key`, `ticker_symbol`, `start_date`
- Descriptive names, avoid single letters except in loops
- Constants: `UPPER_SNAKE_CASE`

#### Files and Modules
- **snake_case**: `extract_polygon.py`, `load_fred.py`, `main.py`
- Clear, descriptive names

### Type Hints
```python
# Function signatures
def get_price_history(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict[str, Any]:

# Complex types
from typing import Union, Optional, dict, list

def process_data(
    data: Union[list[dict[str, Any]], dict[str, Any]],
    config: Optional[dict[str, str]] = None
) -> list[dict[str, Any]]:
```

### Docstrings
```python
def extract_ticker_details(
    self,
    ticker: str,
    type: str = "stocks"
) -> dict[str, Any]:
    """
    Extract ticker details for a single ticker.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        type: Type of ticker ('stocks' or 'indices')

    Returns:
        dict containing raw ticker details from the API

    Raises:
        Exception: If API request fails
    """
```

### Error Handling
```python
try:
    # Operation that might fail
    result = client.get_ticker_details(ticker)
    logger.info(f"Successfully extracted data for {ticker}")
except Exception as e:
    logger.error(f"Error extracting data for {ticker}: {e}")
    raise  # Re-raise or handle appropriately
finally:
    # Cleanup operations
    if 'conn' in locals():
        conn.close()
```

### Logging
- Use structured logging with appropriate levels
- Include contextual information
- Use the custom logger setup

```python
from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

logger.info(f"Processing {len(tickers)} tickers")
logger.debug(f"Extracted fields: {list(data.keys())}")
logger.error(f"Failed to extract {ticker}: {e}")
logger.warning(f"Skipping {ticker} due to error: {e}")
```

### Database Operations
```python
# Use parameterized queries to prevent SQL injection
query = """
    SELECT *
    FROM price_data
    WHERE UPPER(ticker) = UPPER(?)
"""
params = [ticker]

# Always close connections in finally blocks
conn = ddb.connect(db_path)
try:
    df = conn.execute(query, params).pl()
    return df.to_dicts()
finally:
    conn.close()
```

### API Design (FastAPI)
```python
@app.get("/company/{ticker}")
async def get_company(ticker: str) -> dict:
    """
    Return company details for the given ticker.
    """
    # Validate inputs
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker is required")

    # Handle business logic with proper error handling
    try:
        # ... implementation
    except Exception as e:
        logger.exception("Failed to query company details")
        raise HTTPException(status_code=500, detail="Database query failed")
```

### Environment Configuration
- Use `.env` files for sensitive configuration
- Load with `python-dotenv`
- Never commit sensitive data

```python
from dotenv import load_dotenv
import os

load_dotenv("./secret/.env")

db_path = os.getenv("DB_PATH")
api_key = os.getenv("POLYGON_API_KEY")

if not db_path:
    raise ValueError("DB_PATH environment variable is required")
```

### Path Handling
```python
from pathlib import Path

# Use pathlib for cross-platform compatibility
root_path = Path(__file__).parent.parent
log_dir = root_path / "logs"
log_dir.mkdir(exist_ok=True)

config_path = root_path / "log_config" / "config.json"
```

### Control Flow
- Use `match/case` for pattern matching (Python 3.10+)
- Prefer explicit conditions over implicit

```python
match ticker_type.lower():
    case "stocks":
        # Handle stocks
        pass
    case "indices":
        # Handle indices
        pass
    case _:
        raise ValueError(f"Unsupported ticker type: {ticker_type}")
```

### Data Processing
- Use Polars for DataFrame operations (preferred over pandas)
- Leverage DuckDB for analytical queries
- Handle data types explicitly

```python
import polars as pl

# Convert DuckDB result to Polars DataFrame
df = conn.execute(query, params).pl()

# Process data
data = df.to_dicts()
```

## Architecture Patterns

### Factory Pattern
```python
class PolygonExtractorFactory:
    @staticmethod
    def create_ticker_extractor(api_key: str | None = None) -> TickerDetailsExtractor:
        if api_key is None:
            api_key = get_api_key()
        polygon_client = PolygonClient(api_key)
        return TickerDetailsExtractor(polygon_client.get_client())
```

### Single Responsibility Principle
- Each class should have one reason to change
- Separate extraction, loading, and API concerns
- Clear separation between data access and business logic

### Dependency Injection
```python
# Inject extractors into loaders
def __init__(self, extractor: TickerDetailsExtractor):
    self.extractor = extractor
```

## Security Best Practices

- ✅ Store API keys in `.env` files, never in code
- ✅ Use parameterized queries to prevent SQL injection
- ✅ Validate all inputs
- ✅ Log errors without exposing sensitive information
- ✅ Use HTTPS in production
- ⚠️ Implement authentication for production APIs

## File Structure Conventions

```
DataProject/
├── ELT/                    # Extract, Load, Transform operations
│   ├── extract_*.py       # Data extraction logic
│   ├── load_*.py          # Data loading logic
│   └── main.py            # ELT orchestration
├── database/              # Database utilities and schemas
├── log_config/            # Logging configuration
├── logger/                # Custom logging implementation
├── data/                  # Static data files
├── secret/                # Environment variables (.env)
├── main.py                # FastAPI application
└── pyproject.toml         # Project configuration
```

## Development Workflow

1. **Before starting work**: Run `ruff check .` to ensure clean baseline
2. **During development**: Use type hints and follow naming conventions
3. **Before committing**: Run full linting and formatting
4. **Testing**: Run relevant tests and verify functionality
5. **Logging**: Add appropriate log statements for debugging

## Common Patterns in This Codebase

### API Response Format
```python
return {
    "ticker": ticker.upper(),
    "results": data,
    "count": len(data),
    "total_count": total_count,
}
```

### Batch Processing with Rate Limiting
```python
batch_size = 5  # API rate limit
for i in range(0, len(items), batch_size):
    batch = items[i:i + batch_size]
    # Process batch
    time.sleep(60)  # Rate limit delay
```

### Checkpoint/Restart Logic
```python
# Save progress for resumable operations
checkpoint_data = self._load_checkpoint(checkpoint_file)
# ... processing logic ...
self._save_checkpoint(checkpoint_file, results, processed)
```

## Performance Considerations

- Use batch operations for multiple API calls
- Implement rate limiting to respect API limits
- Reuse database connections when possible
- Use appropriate data structures (Polars for analytics)
- Add database indexes for frequently queried columns

## Error Messages

- Be descriptive but don't expose sensitive information
- Include context when possible
- Use appropriate HTTP status codes in APIs

```python
# Good
raise HTTPException(status_code=404, detail="Ticker not found")

# Bad - exposes implementation details
raise HTTPException(status_code=500, detail="KeyError: 'results'")
```</content>
<parameter name="filePath">C:\Users\renar\OneDrive\Bureau\PythonPoject\DataProject\AGENTS.md