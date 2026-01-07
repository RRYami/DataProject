# AGENTS.md - Development Guidelines for AI Coding Agents

## Build, Lint, and Test Commands

### Package Management
```bash
uv sync                    # Install dependencies
uv add <package>           # Add dependency
uv run fastapi dev main.py # Start dev server
```

### Linting and Formatting
```bash
uv run ruff check .        # Check linting
uv run ruff check . --fix  # Auto-fix issues
uv run ruff format .       # Format code
```

### Testing
```bash
uv run pytest                              # Run all tests
uv run pytest tests/test_module.py         # Run specific file
uv run pytest tests/test_module.py::test_name  # Run single test
uv run pytest --cov=. --cov-report=html    # With coverage
```

### Database Operations
```bash
uv run python -m database.classification_table  # Init tables
uv run python -m database.clean_db              # Clean DB
```

---

## Code Style Guidelines

### Configuration
- **Python**: >= 3.13
- **Line Length**: 80 characters (ruff)
- **Type Hints**: Required for all public functions
- **Docstrings**: Google style for all public functions/classes

### Import Order
```python
# 1. Standard library (alphabetical)
import json
import os
from pathlib import Path
from typing import Any, Optional, Union

# 2. Third-party (alphabetical)
import duckdb as ddb
import polars as pl
from fastapi import APIRouter, Depends, HTTPException

# 3. Local imports
from api.dependencies import get_db_connection
from logger.logger import get_logger
```

### Naming Conventions
- **Classes**: `PascalCase` → `PolygonClient`, `TickerDetailsExtractor`
- **Functions/Methods**: `snake_case` → `get_price_history`, `load_data`
- **Variables**: `snake_case` → `ticker_symbol`, `start_date`
- **Constants**: `UPPER_SNAKE_CASE` → `MAX_RETRIES`, `API_BASE_URL`
- **Private**: `_prefix` → `_load_checkpoint`, `_api_key`
- **Files**: `snake_case` → `extract_polygon.py`, `companies.py`

### Type Hints (Required)
```python
def get_price_history(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Brief description."""
    pass

# Use Union or | for multiple types
def process(data: Union[list, dict]) -> list[dict[str, Any]]:
    pass

# Factory pattern with type hints
def create_extractor(api_key: str | None = None) -> TickerDetailsExtractor:
    pass
```

### Docstrings (Google Style)
```python
def extract_ticker_details(
    self,
    ticker: str,
    type: str = "stocks"
) -> dict[str, Any]:
    """
    Extract ticker details for a single ticker.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')
        type: Type of ticker ('stocks' or 'indices')

    Returns:
        dict containing raw ticker details from API

    Raises:
        Exception: If API request fails
    """
```

---

## Architecture Patterns

### Factory Pattern
```python
class PolygonExtractorFactory:
    @staticmethod
    def create_ticker_extractor(api_key: str | None = None):
        if api_key is None:
            api_key = get_api_key()
        client = PolygonClient(api_key)
        return TickerDetailsExtractor(client.get_client())
```

### Dependency Injection (FastAPI)
```python
@router.get("/{ticker}")
async def get_company(
    ticker: str,
    conn: ddb.DuckDBPyConnection = Depends(get_db_connection),
) -> dict:
    pass
```

### Single Responsibility
- `PolygonClient` → API connection only
- `TickerDetailsExtractor` → Extract data only
- `PolygonDataLoader` → Load to DB only

---

## FastAPI Guidelines

### Router Structure
```python
router = APIRouter(
    prefix="/company",
    tags=["companies"],
    responses={404: {"description": "Not found"}},
)

@router.get("/{ticker}/priceHistory")
async def get_price_history(...) -> dict:
    """Endpoint docstring."""
```

### Response Format
```python
return {
    "ticker": ticker.upper(),
    "results": data,
    "count": len(data),
    "total_count": total_count,
}
```

### Error Handling
```python
try:
    df = conn.execute(query, params).pl()
    data = df.to_dicts()
except Exception:
    loggers.exception("Failed to query")
    raise HTTPException(status_code=500, detail="Query failed")

if not data:
    raise HTTPException(status_code=404, detail="Not found")
```

---

## Database Patterns

### DuckDB Connection
```python
# Always use try-finally
db_path = os.getenv("DB_PATH")
if not db_path:
    raise HTTPException(status_code=500, detail="DB not configured")

conn = ddb.connect(db_path)
try:
    df = conn.execute(query, params).pl()
    data = df.to_dicts()
finally:
    try:
        conn.close()
    except Exception:
        pass
```

### Parameterized Queries (Required)
```python
# ✅ CORRECT - Use parameters
query = "SELECT * FROM price_data WHERE UPPER(ticker) = UPPER(?)"
params = [ticker]
df = conn.execute(query, params).pl()

# ❌ WRONG - Never use f-strings for SQL
query = f"SELECT * FROM price_data WHERE ticker = '{ticker}'"
```

### Polars DataFrames
```python
df = conn.execute(query, params).pl()  # DuckDB → Polars
data = df.to_dicts()                   # → List of dicts
tickers = df["ticker"].to_list()       # Extract column
```

---

## Logging

```python
from logger.logger import get_logger, setup_logging

setup_logging()  # In main/entry point
logger = get_logger(__name__)

logger.info(f"Processing {len(tickers)} tickers")
logger.debug(f"Data: {data.keys()}")
logger.warning(f"Skipping {ticker}: {e}")
logger.error(f"Failed: {e}")
logger.exception("Error with traceback")  # In except blocks
```

---

## Environment Configuration

```python
from dotenv import load_dotenv
import os

load_dotenv("./secret/.env")

# Always validate required vars
db_path = os.getenv("DB_PATH")
if not db_path:
    raise ValueError("DB_PATH required")
```

**Never commit**: `.env` files, API keys, credentials

---

## Security Checklist

✅ **DO**:
- Store secrets in `.env`
- Use parameterized SQL queries
- Validate all inputs
- Log without sensitive data

❌ **DON'T**:
- Commit `.env` files
- Use f-strings for SQL
- Expose stack traces to users
- Log passwords/keys

---

## File Structure

```
DataProject/
├── main.py              # FastAPI entry point
├── api/                 # API layer
│   ├── dependencies.py  # Shared dependencies
│   ├── routers/        # Domain routers
│   │   ├── companies.py
│   │   ├── tickers.py
│   │   └── treasury.py
│   └── models/         # Pydantic models
├── ELT/                # ETL operations
│   ├── extract_*.py
│   └── load_*.py
├── database/           # DB utilities
├── logger/             # Logging
└── secret/             # .env files
```
