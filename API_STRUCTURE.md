# API Structure Documentation

## Overview

The API has been restructured following professional FastAPI best practices. All endpoints are now organized into separate modules by domain.

## Directory Structure

```
DataProject/
├── main.py                      # FastAPI app entry point (NEW)
├── main_backup.py               # Backup of old main.py
├── api/                         # API layer (NEW)
│   ├── __init__.py
│   ├── dependencies.py          # Shared dependencies (DB connections, etc.)
│   ├── routers/                 # Route groupings by domain
│   │   ├── __init__.py
│   │   ├── companies.py         # Company & ticker endpoints
│   │   ├── tickers.py           # Indices management endpoints
│   │   └── treasury.py          # Treasury yield curve endpoints
│   └── models/                  # Pydantic models for validation
│       ├── __init__.py
│       └── ticker.py            # Ticker request/response models
├── ELT/                         # Your ETL logic (unchanged)
├── database/                    # Database utilities (unchanged)
└── logger/                      # Logging (unchanged)
```

## API Endpoints

### Root & Health
- `GET /` - API information
- `GET /health` - Health check

### Companies (`/company`)
- `GET /company/{ticker}` - Get company details
- `GET /company/{ticker}/priceHistory` - Get price history
  - Query params: `start_date`, `end_date`
- `GET /company/list/available_tickers` - List all available tickers

### Tickers & Indices (`/tickers`)
- `GET /tickers/indices/available` - List all available indices
- `POST /tickers/indices` - Add a new indice (JSON body required)

### Treasury Curves (`/curves`)
- `GET /curves/US_treasury_yield` - Get US Treasury yield curve data
  - Query params: `date`, `start_date`, `end_date`, `limit`, `offset`, `latest_only`

## Running the API

### Development Mode
```bash
uv run fastapi dev main.py
```

### Production Mode
```bash
uv run fastapi run main.py
```

## API Documentation

Once the server is running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Key Changes

### 1. Modular Structure
- Endpoints grouped by domain (companies, tickers, treasury)
- Each router is self-contained and easier to maintain

### 2. Shared Dependencies
- Database connection logic centralized in `api/dependencies.py`
- No code duplication across endpoints

### 3. Pydantic Models
- Request validation for POST endpoints
- Better API documentation
- Type safety

### 4. URL Changes
Some URLs have changed slightly for better organization:

**OLD → NEW**
- `/list_available_tickers` → `/company/list/available_tickers`
- `/list_available_indices` → `/tickers/indices/available`
- `/post_indice` → `/tickers/indices` (POST)
- `/curves/US_treasury_yield` → `/curves/US_treasury_yield` (unchanged)
- `/company/{ticker}` → `/company/{ticker}` (unchanged)
- `/company/{ticker}/priceHistory` → `/company/{ticker}/priceHistory` (unchanged)

### 5. Enhanced Features
- CORS middleware configured
- Better error handling
- Improved logging
- Professional API documentation

## Benefits

1. **Scalability**: Easy to add new routers/endpoints
2. **Maintainability**: Easier to find and modify code
3. **Testing**: Simpler to test individual routers
4. **Collaboration**: Multiple developers can work on different routers
5. **Documentation**: Auto-generated docs are better organized with tags

## Migration Notes

- Your original `main.py` is backed up as `main_backup.py`
- All functionality is preserved
- Database connection logic is now reusable
- Environment variables are loaded in the same way

## Example Usage

### Get Treasury Yield Curve for Date Range
```bash
curl "http://localhost:8000/curves/US_treasury_yield?start_date=2024-01-01&end_date=2024-12-31"
```

### Add New Indice (JSON body)
```bash
curl -X POST "http://localhost:8000/tickers/indices" \
  -H "Content-Type: application/json" \
  -d '{
    "indice": "SPX",
    "name": "S&P 500 Index",
    "market": "indices",
    "locale": "us",
    "active": true,
    "source_feed": "polygon"
  }'
```

### Get Company Price History
```bash
curl "http://localhost:8000/company/AAPL/priceHistory?start_date=2024-01-01&end_date=2024-12-31"
```
