# DataProject

A comprehensive Python-based ELT (Extract, Load, Transform) pipeline with a FastAPI REST API for fetching, storing, and querying financial and economic data from multiple sources including Polygon.io and FRED (Federal Reserve Economic Data).

## Overview

This project provides a modular system for extracting financial data, loading it into a DuckDB analytical database, and exposing it through a professional RESTful API. Key features include:

- 🔄 **Multi-Source ELT Pipeline**: Extract data from Polygon.io and FRED APIs
- 💾 **DuckDB Storage**: Efficient analytical database for financial data
- 🌐 **FastAPI REST API**: Modular router architecture with domain-separated endpoints
- 📊 **Batch Processing**: Support for single or batch ticker extraction
- 📈 **Price History**: Store and retrieve historical stock prices with date range filtering
- 📉 **Treasury Yield Curves**: US Treasury yield data from FRED with flexible date queries
- 📝 **Comprehensive Logging**: JSON-formatted logging with queue-based handlers
- 🏗️ **Clean Architecture**: Factory pattern, dependency injection, and single responsibility
- 🔗 **Industry Classification**: SIC to NAICS code mapping for industry analysis
- 🎯 **Professional Structure**: Organized API routers, Pydantic models, and shared dependencies
- 📚 **Auto Documentation**: Interactive Swagger UI and ReDoc with organized domain tags
- 🧪 **Comprehensive Testing**: Full test suite with pytest covering all API endpoints
- 📊 **Prometheus Monitoring**: Built-in metrics collection for requests, database queries, and performance

## Features

### Data Extraction
- **Company Details**: Extract ticker details (market cap, employees, exchange info, etc.)
- **Price History**: Historical stock price data with date range filtering
- **Treasury Yields**: US Treasury yield curves from FRED (1M, 3M, 6M, 1Y, 2Y, 5Y, 10Y, 30Y)
- **Ticker Lists**: Available tickers and indices from Polygon.io
- **Batch Processing**: Efficient batch extraction for multiple tickers

### Data Storage
- Automatic database schema creation
- Conflict resolution (upsert functionality)
- SIC to NAICS code mapping
- Optimized indexing for fast queries

### REST API
- **Modular Router Architecture**: Endpoints organized by domain (companies, tickers, treasury)
- **Company Lookup**: Get company details by ticker symbol
- **Price History**: Query historical prices with flexible date range filtering
- **Treasury Curves**: Access US Treasury yield curve data with start_date/end_date support
- **Ticker Management**: List and add indices/tickers with Pydantic validation
- **Interactive Documentation**: Built-in Swagger UI and ReDoc with domain tags
- **Health Checks**: Built-in health check and API info endpoints
- **CORS Support**: Configured CORS middleware for cross-origin requests

## Project Structure

```
DataProject/
├── main.py                    # FastAPI application entry point
├── main_backup.py             # Backup of legacy main.py
├── load_test.py               # Load testing tool for generating traffic
├── Dockerfile                 # Docker container definition
├── docker-compose.yml         # Multi-container orchestration
├── .dockerignore             # Docker build exclusions
├── monitoring-stack.sh       # Monitoring stack manager (Linux/macOS)
├── monitoring-stack.bat      # Monitoring stack manager (Windows)
├── api/                       # FastAPI API layer (modular structure)
│   ├── __init__.py
│   ├── dependencies.py        # Shared dependencies (DB connections, etc.)
│   ├── monitoring/           # Monitoring and metrics
│   │   ├── __init__.py
│   │   ├── metrics.py        # Prometheus metrics definitions
│   │   ├── middleware.py     # Metrics collection middleware
│   │   └── monitoring.py     # Monitoring endpoints
│   ├── routers/              # Domain-specific API routers
│   │   ├── __init__.py
│   │   ├── companies.py      # Company & price history endpoints
│   │   ├── tickers.py        # Ticker/indices management endpoints
│   │   ├── treasury.py       # Treasury yield curve endpoints
│   │   └── monitoring.py     # Monitoring/health endpoints
│   └── models/               # Pydantic models for request/response
│       ├── __init__.py
│       └── ticker.py         # Ticker data models
├── ELT/                      # Extract, Load, Transform pipeline
│   ├── __init__.py
│   ├── extract_polygon.py    # Polygon.io data extraction
│   ├── extract_fred.py       # FRED API data extraction
│   ├── extract_http.py       # Alpha Vantage fundamentals (legacy)
│   ├── load_polygon.py       # Load data into DuckDB
│   ├── load_fred.py          # Load FRED data into DuckDB
│   └── main.py               # ELT pipeline examples
├── database/                 # Database files and utilities
│   ├── polygon.duckdb        # DuckDB database file
│   ├── classification_table.py # SIC/NAICS table management
│   ├── clean_db.py           # Database cleanup utilities
│   ├── indexing.sql          # Database index definitions
│   └── run.sql               # SQL helper scripts
├── data/                     # Static data files
│   └── sic_naics.parquet     # SIC to NAICS mapping data
├── log_config/               # Logging configuration
│   └── config.json
├── logger/                   # Custom logging implementation
│   ├── __init__.py
│   └── logger.py
├── logs/                     # Application logs directory
├── monitoring/               # Monitoring stack configuration
│   ├── prometheus/
│   │   └── prometheus.yml    # Prometheus configuration
│   └── grafana/
│       ├── provisioning/     # Auto-provisioning configs
│       └── dashboards/       # Pre-built dashboards
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures
│   └── routers/             # Router tests
│       ├── test_companies.py
│       ├── test_tickers.py
│       └── test_treasury.py
├── secret/                   # Sensitive configuration
│   └── .env                  # Environment variables (API keys, DB path)
├── get_api_keys.py           # Centralized API key management
├── pyproject.toml            # Project dependencies and configuration
├── AGENTS.md                 # Development guidelines for AI agents
├── API_STRUCTURE.md          # Detailed API structure documentation
├── MONITORING.md             # Monitoring stack setup guide
├── LOAD_TESTING.md           # Load testing tool documentation
└── README.md                 # This file
```

## Requirements

- Python >= 3.13
- UV package manager (recommended) or pip
- Docker & Docker Compose (optional, for monitoring stack)

### Dependencies

- `duckdb` - Analytical database
- `polars` - Fast DataFrame library
- `polygon-api-client` - Polygon.io API wrapper
- `fastapi[standard]` - Modern web framework for APIs
- `python-dotenv` - Environment variable management
- `requests` - HTTP library
- `pendulum` - DateTime handling
- `pyarrow` - Apache Arrow support
- `pytest` - Testing framework (dev dependency)
- `pytest-asyncio` - Async test support (dev dependency)
- `pytest-cov` - Code coverage (dev dependency)
- `httpx` - HTTP client for testing (dev dependency)
- `prometheus-client` - Prometheus metrics collection

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd DataProject
   ```

2. **Create virtual environment** (if using venv)
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   
   Using UV (recommended):
   ```bash
   uv sync
   ```
   
   Or using pip:
   ```bash
   pip install -e .
   ```

4. **Set up environment variables**
   
   Create a `.env` file in the `secret/` directory:
   ```env
   POLYGON_API_KEY=your_polygon_api_key_here
   FRED_KEY=your_fred_api_key_here
   DB_PATH=./database/polygon.duckdb
   ```

## Configuration

### API Keys

You'll need API keys from:
- **Polygon.io**: Sign up at [Polygon.io](https://polygon.io/) for stock market data
- **FRED**: Get a free API key at [FRED API](https://fred.stlouisfed.org/docs/api/api_key.html) for economic data

### Logging Configuration

The logging system uses a queue-based handler for thread-safe logging. Configuration is in `log_config/config.json`. Logs are stored in the `logs/` directory with JSON formatting.

## Usage

### Option 1: Full Stack with Docker Compose (Recommended)

The easiest way to run everything including monitoring:

**Windows:**
```cmd
monitoring-stack.bat start
```

**Linux/macOS:**
```bash
chmod +x monitoring-stack.sh
./monitoring-stack.sh start
```

This starts:
- FastAPI application with all endpoints
- Prometheus metrics collection
- Grafana dashboards

Access:
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Grafana Dashboard: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

See [MONITORING.md](MONITORING.md) for details.

### Option 2: Running API Only (Local Development)

Start just the FastAPI REST service:

**Using UV (recommended):**
```bash
uv run uvicorn main:app --reload
```

**Or using FastAPI CLI:**
```bash
uv run fastapi dev main.py
```

**Or using uvicorn directly:**
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:
- **Base URL**: http://127.0.0.1:8000
- **Root Endpoint**: http://127.0.0.1:8000/ (API info)
- **Health Check**: http://127.0.0.1:8000/health
- **Interactive Docs (Swagger UI)**: http://127.0.0.1:8000/docs
- **Alternative Docs (ReDoc)**: http://127.0.0.1:8000/redoc

### API Endpoints

The API is now organized into modular routers by domain. All endpoints are documented in the interactive Swagger UI at `/docs`.

#### Root & Health

**Root Endpoint**
```
GET /
```
Returns API information and documentation links.

**Health Check**
```
GET /health
```
Returns API health status.

#### Monitoring Endpoints

**Prometheus Metrics**
```
GET /monitoring/metrics
```
Returns all Prometheus metrics in text exposition format for scraping.

**Metrics Summary**
```
GET /monitoring/stats
```
Returns available metrics and their descriptions.

**Detailed Health**
```
GET /monitoring/health/detailed
```
Returns detailed health information including metric endpoints.

**Example:**
```bash
# Get Prometheus metrics
curl http://127.0.0.1:8000/monitoring/metrics

# Get metrics summary
curl http://127.0.0.1:8000/monitoring/stats
```

Available metrics include:
- `http_requests_total` - Total HTTP requests by method, endpoint, status
- `http_request_duration_seconds` - Request latency histogram
- `http_requests_in_progress` - Current in-progress requests
- `db_queries_total` - Total database queries by operation and table
- `db_query_duration_seconds` - Database query latency
- `db_connections_active` - Active database connections
- `http_exceptions_total` - Total exceptions by type
- `tickers_queried_total` - Total ticker queries
- `price_history_requests_total` - Price history requests
- `treasury_curve_requests_total` - Treasury curve requests

#### 1. Get Company Details
```
GET /company/{ticker}
```

Returns company information including SIC and NAICS codes.

**Example:**
```bash
curl http://127.0.0.1:8000/company/AAPL
```

**Response:**
```json
{
  "ticker": "AAPL",
  "results": [
    {
      "ticker": "AAPL",
      "name": "Apple Inc.",
      "market_cap": 2000000000000,
      "sic_code": 3571,
      "naics_code": 334111,
      "sic_description": "Electronic Computers",
      "naics_description": "Electronic Computer Manufacturing",
      ...
    }
  ]
}
```

#### 2. Get Price History
```
GET /company/{ticker}/priceHistory?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

Returns historical price data with optional date filtering.

**Example:**
```bash
curl "http://127.0.0.1:8000/company/AAPL/priceHistory?start_date=2024-01-01&end_date=2024-12-31"
```

**Response:**
```json
{
  "ticker": "AAPL",
  "results": [
    {
      "date": "2024-01-02",
      "open": 184.35,
      "high": 186.95,
      "low": 183.89,
      "close": 185.64,
      "volume": 58414460,
      ...
    }
  ]
}
```

#### 3. Get US Treasury Yield Curve
```
GET /curves/US_treasury_yield?date=YYYY-MM-DD&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&limit=100&offset=0&latest_only=false
```

Returns US Treasury yield curve data from FRED with flexible date filtering.

**Query Parameters:**
- `date`: Specific date (optional, takes precedence)
- `start_date`: Start date for range query (optional)
- `end_date`: End date for range query (optional)
- `limit`: Maximum records to return (optional)
- `offset`: Pagination offset (default: 0)
- `latest_only`: Return only most recent data (default: false)

**Examples:**
```bash
# Get latest yield curve
curl "http://127.0.0.1:8000/curves/US_treasury_yield?latest_only=true"

# Get yield curve for date range
curl "http://127.0.0.1:8000/curves/US_treasury_yield?start_date=2024-01-01&end_date=2024-12-31"

# Get all data from a specific date onwards
curl "http://127.0.0.1:8000/curves/US_treasury_yield?start_date=2024-06-01"

# Get paginated results
curl "http://127.0.0.1:8000/curves/US_treasury_yield?limit=100&offset=0"
```

**Response:**
```json
{
  "results": [
    {
      "date": "2024-01-15",
      "DGS1MO": 5.45,
      "DGS3MO": 5.38,
      "DGS6MO": 5.25,
      "DGS1": 4.95,
      "DGS2": 4.42,
      "DGS5": 4.12,
      "DGS10": 4.05,
      "DGS30": 4.25
    }
  ],
  "count": 1,
  "total_count": 5000,
  "offset": 0
}
```

#### 4. List Available Tickers
```
GET /company/list/available_tickers
```

Returns all tickers stored in the database.

**Example:**
```bash
curl http://127.0.0.1:8000/company/list/available_tickers
```

#### 5. List Available Indices
```
GET /tickers/indices/available
```

Returns all indices/tickers metadata.

**Example:**
```bash
curl http://127.0.0.1:8000/tickers/indices/available
```

#### 6. Add New Indice
```
POST /tickers/indices
```

Add a new indice to the tickers table. Uses Pydantic model validation.

**Request Body:**
```json
{
  "indice": "SPX",
  "name": "S&P 500 Index",
  "market": "indices",
  "locale": "us",
  "active": true,
  "source_feed": "polygon"
}
```

**Example:**
```bash
curl -X POST "http://127.0.0.1:8000/tickers/indices" \
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

### Running the ELT Pipeline

You can run extraction and loading scripts directly from `ELT/main.py`:

```bash
python -m ELT.main
```

**Example: Load Company Details**
```python
from ELT.extract_polygon import PolygonExtractorFactory
from ELT.load_polygon import PolygonDataLoader

# Single ticker
single_extractor = PolygonExtractorFactory.create_ticker_extractor()
loader = PolygonDataLoader()
loader.load_ticker_details("AAPL", single_extractor)

# Batch tickers
batch_extractor = PolygonExtractorFactory.create_batch_extractor()
tickers = ["META", "AMZN", "NFLX", "GOOGL"]
loader.load_batch_ticker_details(tickers, batch_extractor)
```

**Example: Load Price Data**
```python
from ELT.extract_polygon import PolygonExtractorFactory
from ELT.load_polygon import PolygonDataLoader

price_extractor = PolygonExtractorFactory.create_price_extractor()
loader = PolygonDataLoader()

tickers = ["AAPL", "MSFT"]
data = price_extractor.extract_range(tickers, "2024-01-01", "2024-12-31")
loader.load_price_data(data)
```

**Example: Load Treasury Yield Data**
```python
from ELT.extract_fred import FredExtractor
from ELT.load_fred import YieldLoader

extractor = FredExtractor()
maturities = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5", "DGS10", "DGS30"]

yield_data = extractor.get_series_observations(series_id=maturities)
loader = YieldLoader()
loader.load_yield_data(yield_data)
```

### Querying the Database Directly

You can query DuckDB directly using Python:

```python
import duckdb as ddb

conn = ddb.connect('./database/polygon.duckdb')

# Query company details with industry classification
result = conn.execute("""
    SELECT cd.*, sn.naics_description, sn.sic_description
    FROM company_details cd
    LEFT JOIN sic_to_naics sn ON cd.sic_code = sn.sic_code
    WHERE cd.ticker = 'AAPL'
""").pl()

print(result)
```

## Database Schema

### company_details Table

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Primary key, stock ticker symbol |
| name | VARCHAR | Company name |
| market_cap | BIGINT | Market capitalization |
| active | BOOLEAN | Whether ticker is actively traded |
| composite_figi | VARCHAR | Financial Instrument Global Identifier |
| base_currency | VARCHAR | Base currency for trading |
| list_date | DATE | Date company was listed |
| primary_exchange | VARCHAR | Primary stock exchange |
| shares_outstanding | BIGINT | Number of shares outstanding |
| total_employees | BIGINT | Total number of employees |
| sic_code | INT | Standard Industrial Classification code |

### price_data Table

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Stock ticker symbol |
| date | DATE | Trading date |
| open | DOUBLE | Opening price |
| high | DOUBLE | Highest price |
| low | DOUBLE | Lowest price |
| close | DOUBLE | Closing price |
| volume | BIGINT | Trading volume |
| vwap | DOUBLE | Volume weighted average price |
| transactions | BIGINT | Number of transactions |

### treasury_curves Table

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Primary key, observation date |
| DGS1MO | FLOAT | 1-Month Treasury yield |
| DGS3MO | FLOAT | 3-Month Treasury yield |
| DGS6MO | FLOAT | 6-Month Treasury yield |
| DGS1 | FLOAT | 1-Year Treasury yield |
| DGS2 | FLOAT | 2-Year Treasury yield |
| DGS5 | FLOAT | 5-Year Treasury yield |
| DGS10 | FLOAT | 10-Year Treasury yield |
| DGS30 | FLOAT | 30-Year Treasury yield |

### sic_to_naics Table

| Column | Type | Description |
|--------|------|-------------|
| sic_code | INT | SIC code |
| sic_description | VARCHAR | SIC industry description |
| naics_code | INT | NAICS code |
| naics_description | VARCHAR | NAICS industry description |

### tickers Table

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Ticker symbol |
| name | VARCHAR | Security name |
| market | VARCHAR | Market type (stocks, indices, etc.) |
| locale | VARCHAR | Locale (us, global, etc.) |
| active | BOOLEAN | Whether actively traded |
| source_feed | VARCHAR | Data source |

## Architecture

### Design Patterns

- **Factory Pattern**: `PolygonExtractorFactory` creates appropriate extractor instances
- **Single Responsibility Principle**: Each class/module has one clear purpose
- **Dependency Injection**: Used throughout API routers and extractors
- **Separation of Concerns**: Clear layers for extraction, loading, and API
- **Modular Router Architecture**: API endpoints organized by domain

### Key Components

#### API Layer (NEW Modular Structure)
1. **FastAPI Application** (`main.py`): Entry point with router registration
2. **Routers** (`api/routers/`):
   - `companies.py`: Company details and price history endpoints
   - `tickers.py`: Ticker/indices management endpoints
   - `treasury.py`: Treasury yield curve endpoints
3. **Dependencies** (`api/dependencies.py`): Shared DB connections and utilities
4. **Models** (`api/models/`): Pydantic models for validation and documentation
5. **Error Handling**: Proper HTTP status codes and error messages
6. **Auto-generated Documentation**: Swagger UI and ReDoc with domain tags

#### Extraction Layer (ELT)
1. **ApiKeyProvider**: Centralized API key retrieval from environment
2. **PolygonClient**: Wrapper for Polygon.io API client
3. **TickerDetailsExtractor**: Extracts single ticker data
4. **BatchTickerExtractor**: Extracts multiple tickers efficiently
5. **PriceExtractor**: Extracts historical price data with rate limiting
6. **FredExtractor**: Extracts economic data from FRED API
7. **YieldDataExtractor**: Extracts treasury yield data

#### Loading Layer (ELT)
1. **PolygonDataLoader**: Loads company and price data into DuckDB
2. **YieldLoader**: Loads treasury yield data into DuckDB
3. Automatic schema creation and conflict resolution

#### Logging Layer
1. **Custom Logger**: JSON formatter with queue-based handling
2. **Structured Logging**: Consistent log format across all modules
3. **Error Tracking**: Comprehensive exception logging

## Development

### Testing

The project includes a comprehensive test suite covering all API endpoints.

#### Running Tests

Run all tests:
```bash
uv run pytest -v
```

Run specific test file:
```bash
uv run pytest tests/routers/test_companies.py -v
```

Run specific test:
```bash
uv run pytest tests/routers/test_companies.py::TestGetCompany::test_get_company_success -v
```

#### Code Coverage

Generate coverage report:
```bash
# Run tests with coverage
uv run pytest --cov=api --cov=ELT --cov-report=html

# Open coverage report
# Windows: start htmlcov/index.html
# macOS: open htmlcov/index.html
# Linux: xdg-open htmlcov/index.html
```

Current test coverage:
- **35 tests** covering all API endpoints
- Company endpoints: 12 tests
- Ticker endpoints: 11 tests  
- Treasury endpoints: 14 tests
- Test fixtures with isolated database per test
- Validation testing for Pydantic models

#### Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test database setup
└── routers/
    ├── test_companies.py    # Company & price history endpoint tests
    ├── test_tickers.py      # Ticker/indices endpoint tests
    └── test_treasury.py     # Treasury yield curve endpoint tests
```

Each test file includes:
- **Success cases**: Valid requests with expected responses
- **Error cases**: Invalid inputs and 404 scenarios
- **Data validation**: Schema and type checking
- **Edge cases**: Boundary conditions and special cases

### Monitoring & Observability

The API includes built-in Prometheus metrics collection for production monitoring.

#### Available Metrics

**HTTP Metrics:**
- Request counts by method, endpoint, and status code
- Request duration histograms
- In-progress request gauges
- Exception counts by type

**Database Metrics:**
- Query counts by operation and table
- Query duration histograms
- Active connection counts

**Business Metrics:**
- Ticker query counts
- Price history request counts
- Treasury curve request counts

#### Full Monitoring Stack (Docker Compose)

We provide a complete monitoring stack with Prometheus and Grafana pre-configured:

**Quick Start:**
```bash
# Windows
monitoring-stack.bat start

# Linux/macOS
./monitoring-stack.sh start
```

This will start:
- **FastAPI API** on http://localhost:8000
- **Prometheus** on http://localhost:9090
- **Grafana** on http://localhost:3000 (admin/admin)

**Important: Ephemeral Metrics**
- ✅ Prometheus metrics are **fresh on every restart** (perfect for testing)
- ✅ Grafana settings **persist** (passwords, dashboards saved)
- ✅ Database data **persists** (your financial data is safe)

**Access the Dashboard:**
Open http://localhost:3000/d/dataproject-api to see:
- Real-time request rates and latencies
- Database query performance
- Error rates and exceptions
- Business metrics (ticker queries, price requests, etc.)

See [MONITORING.md](MONITORING.md) for complete setup instructions and troubleshooting.

#### Manual Setup (Without Docker)

1. **Start the API** with monitoring enabled:
   ```bash
   uv run uvicorn main:app --host 0.0.0.0 --port 8000
   ```

2. **Access metrics endpoint**:
   ```bash
   curl http://localhost:8000/monitoring/metrics
   ```

3. **Install and configure Prometheus** manually:
   - Download from https://prometheus.io/download/
   - Use `monitoring/prometheus/prometheus.yml` config
   - Start Prometheus pointing to your API

4. **Install and configure Grafana** manually:
   - Download from https://grafana.com/grafana/download
   - Add Prometheus as datasource
   - Import dashboard from `monitoring/grafana/dashboards/`

#### Load Testing

Generate realistic traffic to populate your Grafana dashboards with metrics:

```bash
uv run python load_test.py
```

**Test Scenarios:**
- **Standard Load Test**: 5 minutes with varying intensity (low → medium → high → burst)
- **Quick Test**: 1 minute test for quick validation
- **Morning Rush**: Custom scenario simulating realistic traffic pattern
- **Quick Burst**: 100 concurrent requests to test spike handling
- **Long Test**: 15 minutes for comprehensive metrics

The load tester automatically:
- Generates requests to all API endpoints with realistic distribution
- Cycles through different intensity levels (2-1000 req/sec)
- Displays real-time statistics in terminal
- Creates beautiful metrics in Grafana

See [LOAD_TESTING.md](LOAD_TESTING.md) for detailed documentation.

**Example:**
```bash
# Start monitoring stack
monitoring-stack.bat start

# In another terminal, run load test
uv run python load_test.py

# Choose option 1 for standard test
# Open Grafana: http://localhost:3000/d/dataproject-api
# Watch metrics populate in real-time!
```

#### Metrics Middleware

All requests automatically track:
- Request latency
- Response status codes
- Exception types
- Database operation timing

No additional instrumentation needed in route handlers.

### Code Quality

The project uses Ruff for linting and formatting with an 80-character line length limit:

```bash
# Check for linting issues
uv run ruff check .

# Auto-fix linting issues
uv run ruff check . --fix

# Format code
uv run ruff format .
```

See `AGENTS.md` for comprehensive development guidelines.

### Database Management

Initialize classification tables:
```bash
python -m database.classification_table
```

Clean database:
```bash
python -m database.clean_db
```

### Adding New Features

1. **New Extractor**: Add to `ELT/extract_*.py` and register in factory
2. **New Loader**: Add to `ELT/load_*.py` with schema creation
3. **New API Endpoint**: 
   - Add to appropriate router in `api/routers/` (or create new router)
   - Define Pydantic models in `api/models/` if needed
   - Register new router in `main.py` if created
4. **Update Schema**: Modify table definitions and run migrations
5. **Add Logging**: Use the logger for observability

See `API_STRUCTURE.md` for detailed information about the API architecture.

## Troubleshooting

### Common Issues

1. **API Key Not Found**
   - Ensure `.env` file exists in `secret/` directory
   - Verify `POLYGON_API_KEY` and `FRED_KEY` are set correctly
   - Check environment variable names match exactly

2. **Database Connection Failed**
   - Check `DB_PATH` in `.env` file
   - Ensure `database/` directory exists
   - Verify DuckDB file has proper permissions

3. **Import Errors**
   - Verify virtual environment is activated
   - Run `uv sync` or `pip install -e .`
   - Check Python version >= 3.13

4. **API Rate Limiting**
   - Polygon.io free tier has rate limits
   - Use batch extractors to minimize API calls
   - Implement delays between requests

5. **FastAPI Not Starting**
   - Check port 8000 is not in use
   - Verify all dependencies are installed
   - Check logs for detailed error messages

### Error Responses

The API returns standard HTTP status codes:
- `200`: Success
- `404`: Ticker/data not found
- `500`: Server error (check logs for details)

## Output Files

The application can generate:
- `result.csv`: Query results with company details
- `price_data_result.csv`: Price history data
- `tickers_result.csv`: Available tickers list
- Log files in `logs/` directory (JSON format)
- `polygon.duckdb`: DuckDB database file

## Performance Considerations

- **Batch Processing**: Use batch extractors for multiple tickers
- **Database Indexing**: Indexes on ticker and date columns
- **Query Optimization**: Use date range filters to limit result sets
- **Rate Limiting**: Built-in rate limit handling for API calls
- **Connection Pooling**: Reuse database connections where possible

## Security Best Practices

- ✅ API keys stored in `.env` file (not in code)
- ✅ `.gitignore` includes `secret/` directory
- ✅ Environment variables for sensitive data
- ✅ No hardcoded credentials
- ⚠️ Use HTTPS in production
- ⚠️ Implement authentication for production APIs

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting (`ruff check .`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Future Enhancements

- [ ] Authentication and authorization for API endpoints
- [ ] WebSocket support for real-time data streaming
- [ ] Additional data sources (Yahoo Finance, Alpha Vantage)
- [ ] Data visualization dashboard
- [ ] Automated data refresh scheduler
- [ ] PostgreSQL support for larger datasets
- [ ] Docker containerization
- [ ] CI/CD pipeline
- [x] Unit and integration tests
- [x] Performance monitoring and metrics (Prometheus)

## License

[Add your license here]

## Acknowledgments

- [Polygon.io](https://polygon.io/) - Financial market data API
- [FRED](https://fred.stlouisfed.org/) - Federal Reserve Economic Data
- [DuckDB](https://duckdb.org/) - High-performance analytical database
- [Polars](https://www.pola.rs/) - Lightning-fast DataFrame library
- [FastAPI](https://fastapi.tiangolo.com/) - Modern web framework
- [UV](https://github.com/astral-sh/uv) - Fast Python package installer

---

**Note**: This project is for educational/development purposes. Ensure compliance with all API terms of service and rate limits. Market data may be delayed or require paid subscriptions for real-time access.