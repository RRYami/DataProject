# DataProject

A comprehensive Python-based ELT (Extract, Load, Transform) pipeline with a FastAPI REST API for fetching, storing, and querying financial and economic data from multiple sources including Polygon.io and FRED (Federal Reserve Economic Data).

## Overview

This project provides a modular system for extracting financial data, loading it into a DuckDB analytical database, and exposing it through a RESTful API. Key features include:

- 🔄 **Multi-Source ELT Pipeline**: Extract data from Polygon.io and FRED APIs
- 💾 **DuckDB Storage**: Efficient analytical database for financial data
- 🌐 **FastAPI REST API**: Query financial data via HTTP endpoints
- 📊 **Batch Processing**: Support for single or batch ticker extraction
- 📈 **Price History**: Store and retrieve historical stock prices
- 📉 **Treasury Yield Curves**: US Treasury yield data from FRED
- 📝 **Comprehensive Logging**: JSON-formatted logging with queue-based handlers
- 🏗️ **Factory Pattern**: Clean architecture with factory pattern for extractor creation
- 🔗 **Industry Classification**: SIC to NAICS code mapping for industry analysis

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
- **Company Lookup**: Get company details by ticker symbol
- **Price History**: Query historical prices with date filtering
- **Treasury Curves**: Access US Treasury yield curve data
- **Ticker Management**: List and add indices/tickers
- **Interactive Documentation**: Built-in Swagger UI and ReDoc

## Project Structure

```
DataProject/
├── ELT/
│   ├── __init__.py
│   ├── extract_polygon.py     # Polygon.io data extraction
│   ├── extract_fred.py        # FRED API data extraction
│   ├── extract_http.py        # Alpha Vantage fundamentals (legacy)
│   ├── load_polygon.py        # Load data into DuckDB
│   ├── load_fred.py           # Load FRED data into DuckDB
│   └── main.py                # ELT pipeline examples
├── database/
│   ├── polygon.duckdb         # DuckDB database file
│   ├── classification_table.py # SIC/NAICS table management
│   ├── clean_db.py            # Database cleanup utilities
│   ├── indexing.sql           # Database index definitions
│   └── run.sql                # SQL helper scripts
├── data/
│   └── sic_naics.parquet      # SIC to NAICS mapping data
├── log_config/
│   └── config.json            # Logging configuration
├── logger/
│   ├── __init__.py
│   └── logger.py              # Custom logging setup
├── logs/                      # Application logs directory
├── secret/
│   └── .env                   # Environment variables (API keys, DB path)
├── get_api_keys.py            # Centralized API key management
├── main.py                    # FastAPI application entry point
├── pyproject.toml             # Project dependencies
└── README.md                  # This file
```

## Requirements

- Python >= 3.13
- UV package manager (recommended) or pip

### Dependencies

- `duckdb` - Analytical database
- `polars` - Fast DataFrame library
- `polygon-api-client` - Polygon.io API wrapper
- `fastapi[standard]` - Modern web framework for APIs
- `python-dotenv` - Environment variable management
- `requests` - HTTP library
- `pendulum` - DateTime handling
- `pyarrow` - Apache Arrow support

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

### Running the FastAPI Application

Start the FastAPI REST service from the project root:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:
- **Base URL**: http://127.0.0.1:8000
- **Interactive Docs (Swagger UI)**: http://127.0.0.1:8000/docs
- **Alternative Docs (ReDoc)**: http://127.0.0.1:8000/redoc

### API Endpoints

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
GET /curves/US_treasury_yield?date=YYYY-MM-DD&limit=100&offset=0&latest_only=false
```

Returns US Treasury yield curve data from FRED.

**Query Parameters:**
- `date`: Specific date (optional)
- `limit`: Maximum records to return (optional)
- `offset`: Pagination offset (default: 0)
- `latest_only`: Return only most recent data (default: false)

**Example:**
```bash
curl "http://127.0.0.1:8000/curves/US_treasury_yield?latest_only=true"
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
GET /list_available_tickers
```

Returns all tickers stored in the database.

**Example:**
```bash
curl http://127.0.0.1:8000/list_available_tickers
```

#### 5. List Available Indices
```
GET /list_available_indices
```

Returns all indices/tickers metadata.

#### 6. Add New Indice
```
POST /post_indice
```

Add a new indice to the tickers table.

**Request Body:**
```json
{
  "indice": "SPY",
  "name": "SPDR S&P 500 ETF Trust",
  "market": "stocks",
  "locale": "us",
  "active": true,
  "source_feed": "polygon"
}
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
- **Single Responsibility Principle**: Each class has one clear purpose
- **Dependency Injection**: Extractors are injected into loaders
- **Separation of Concerns**: Clear separation between extraction, loading, and API layers

### Key Components

#### Extraction Layer
1. **ApiKeyProvider**: Centralized API key retrieval from environment
2. **PolygonClient**: Wrapper for Polygon.io API client
3. **TickerDetailsExtractor**: Extracts single ticker data
4. **BatchTickerExtractor**: Extracts multiple tickers efficiently
5. **PriceExtractor**: Extracts historical price data with rate limiting
6. **FredExtractor**: Extracts economic data from FRED API
7. **YieldDataExtractor**: Extracts treasury yield data

#### Loading Layer
1. **PolygonDataLoader**: Loads company and price data into DuckDB
2. **YieldLoader**: Loads treasury yield data into DuckDB
3. Automatic schema creation and conflict resolution

#### API Layer
1. **FastAPI Application**: RESTful API with automatic documentation
2. **Error Handling**: Proper HTTP status codes and error messages
3. **Query Optimization**: Efficient database queries with filtering

#### Logging Layer
1. **Custom Logger**: JSON formatter with queue-based handling
2. **Structured Logging**: Consistent log format across all modules
3. **Error Tracking**: Comprehensive exception logging

## Development

### Code Quality

The project uses Ruff for linting with an 80-character line length limit:

```bash
ruff check .
```

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
3. **New Endpoint**: Add to `main.py` with proper error handling
4. **Update Schema**: Modify table definitions and run migrations
5. **Add Logging**: Use the logger for observability

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
- [ ] Unit and integration tests
- [ ] Performance monitoring and metrics

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