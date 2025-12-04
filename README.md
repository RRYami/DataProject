# DataProject

A Python-based ELT (Extract, Load, Transform) pipeline for fetching and storing financial data from the Polygon.io API into a DuckDB database.

## Overview

This project provides a modular system for extracting company ticker details from Polygon.io, loading them into a DuckDB database, and querying the data. It features:

- 🔄 **ELT Pipeline**: Extract financial data from Polygon.io API
- 💾 **DuckDB Storage**: Efficient analytical database for company details
- 📊 **Batch Processing**: Support for single or batch ticker extraction
- 📝 **Comprehensive Logging**: JSON-formatted logging with queue-based handlers
- 🏗️ **Factory Pattern**: Clean architecture with factory pattern for extractor creation

## Features

- Extract ticker details (market cap, employees, exchange info, etc.)
- Batch processing for multiple tickers
- Automatic database schema creation
- Conflict resolution (upsert functionality)
- Structured JSON logging
- SIC code integration for industry classification

## Project Structure

```
DataProject/
├── ELT/
│   ├── __init__.py
│   ├── extract_polygon.py    # Data extraction from Polygon.io
│   └── load_polygon.py        # Data loading into DuckDB
├── database/
│   ├── polygon.duckdb         # DuckDB database file
│   └── sic_table.py           # SIC codes management
├── log_config/
│   └── config.json            # Logging configuration
├── logs/                      # Application logs
├── secret/
│   └── .env                   # Environment variables (API keys, DB path)
├── logger.py                  # Custom logging setup
├── main.py                    # Application entry point
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
   DB_PATH=./database/polygon.duckdb
   ```

## Configuration

### API Keys

You'll need a Polygon.io API key:
- Sign up at [Polygon.io](https://polygon.io/)
- Get your API key from the dashboard
- Add it to `secret/.env`

### Logging Configuration

The logging system uses a queue-based handler for thread-safe logging. Configuration is in `log_config/config.json`. Logs are stored in the `logs/` directory.

## Usage

### Basic Usage

The `main.py` file contains examples for both single and batch ticker extraction:

```python
from ELT.extract_polygon import PolygonExtractorFactory
from ELT.load_polygon import PolygonDataLoader

# Single ticker example
single_extractor = PolygonExtractorFactory.create_ticker_extractor()
loader = PolygonDataLoader()
loader.load_ticker_details("AAPL", single_extractor)

# Batch ticker example
batch_extractor = PolygonExtractorFactory.create_batch_extractor()
tickers_to_load = ["META", "AMZN", "NFLX", "PLTR"]
loader.load_batch_ticker_details(tickers_to_load, batch_extractor)
```

### Running the Application

```bash
python main.py
```

The application will:
1. Extract ticker data from Polygon.io
2. Load data into DuckDB
3. Join company details with SIC codes
4. Export results to `result.csv`

### Querying the Database

The project uses DuckDB for data storage. You can query the database directly:

```python
import duckdb as ddb

conn = ddb.connect('./database/polygon.duckdb')
result = conn.execute("""
    SELECT * FROM company_details 
    JOIN sic_codes ON company_details.sic_code = sic_codes.sic_code
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

## Architecture

### Design Patterns

- **Factory Pattern**: `PolygonExtractorFactory` creates appropriate extractor instances
- **Single Responsibility**: Each class has one clear purpose
- **Dependency Injection**: Extractors are injected into loaders

### Components

1. **ApiKeyProvider**: Manages API key retrieval from environment
2. **PolygonClient**: Wrapper for Polygon.io API client
3. **TickerDetailsExtractor**: Extracts single ticker data
4. **BatchTickerExtractor**: Extracts multiple tickers efficiently
5. **PolygonDataLoader**: Loads data into DuckDB with upsert logic
6. **Logger**: Custom JSON formatter with queue-based handling

## Development

### Code Quality

The project uses Ruff for linting with an 80-character line length limit:

```bash
ruff check .
```

### Adding New Features

1. Create extractors in `ELT/extract_polygon.py`
2. Create loaders in `ELT/load_polygon.py`
3. Update database schema as needed
4. Add logging statements for observability

## Troubleshooting

### Common Issues

1. **API Key Not Found**
   - Ensure `.env` file exists in `secret/` directory
   - Verify `POLYGON_API_KEY` is set correctly

2. **Database Connection Failed**
   - Check `DB_PATH` in `.env` file
   - Ensure `database/` directory exists

3. **Import Errors**
   - Verify virtual environment is activated
   - Run `uv sync` or `pip install -e .`

## Output

The application generates:
- `result.csv`: Query results with company details and SIC codes
- Log files in `logs/` directory
- `polygon.duckdb`: DuckDB database file

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

[Add your license here]

## Acknowledgments

- [Polygon.io](https://polygon.io/) for financial data API
- [DuckDB](https://duckdb.org/) for the analytical database
- [Polars](https://www.pola.rs/) for DataFrame operations

---

**Note**: This project is for educational/development purposes. Ensure you comply with Polygon.io's terms of service and rate limits.