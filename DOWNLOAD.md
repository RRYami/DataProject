# 📥 Data Download Tool

Command-line interface for downloading financial data into the DataProject database.

## Overview

The download tool provides a simple, command-line interface to extract and load financial data from:

- **Polygon.io** - Stock price data (OHLCV) and company details
- **FRED** - US Treasury yield curves

Data is automatically validated, loaded into the DuckDB database, and verified after download.

---

## Quick Start

### Prerequisites

- UV package manager installed
- API keys configured in `secret/.env`:
  - `POLYGON_API_KEY` - For price and company data
  - `FRED_KEY` - For treasury yields

### Basic Examples

```bash
# Download 30 days of price data for Apple
download.bat price AAPL

# Download company details for multiple tickers
download.bat company AAPL,MSFT,GOOGL

# Download 90 days of treasury yields
download.bat treasury
```

---

## Commands

### 1. Price Data

Download historical OHLCV (Open, High, Low, Close, Volume) price data.

#### Syntax

```bash
download.bat price <tickers> [start_date] [end_date]
```

#### Parameters

- `<tickers>` - **Required.** Comma-separated ticker symbols (no spaces)
  - Examples: `AAPL`, `AAPL,MSFT,GOOGL`
- `[start_date]` - **Optional.** Start date in `YYYY-MM-DD` format
  - Default: 30 days ago
- `[end_date]` - **Optional.** End date in `YYYY-MM-DD` format
  - Default: Today

#### Examples

```bash
# Single ticker with specific date range
download.bat price AAPL 2025-01-01 2025-01-07

# Multiple tickers with specific date range
download.bat price AAPL,MSFT,GOOGL 2024-01-01 2024-12-31

# Single ticker with default dates (last 30 days)
download.bat price AAPL

# Multiple tickers with default dates
download.bat price AAPL,MSFT,GOOGL
```

#### Notes

- **Rate Limit**: Polygon free tier allows 5 API calls per minute
- **Date Range Limit**: Maximum 2 years per request (Polygon free tier)
- **Errors**: If a ticker fails, it will be skipped and reported in the summary
- **Database Table**: Data is stored in `price_data` table

#### Output Example

```
[Downloading Price Data]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tickers: AAPL, MSFT, GOOGL
Date Range: 2025-01-01 to 2025-01-07

Processing...
  → Fetching AAPL... ✓ 5 days extracted
  → Fetching MSFT... ✓ 5 days extracted
  → Fetching GOOGL... ✓ 5 days extracted

┏━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ Summary       ┃       ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━┩
│ Total Tickers │ 3     │
│ Successful    │ 3     │
│ Failed        │ 0     │
│ Total Records │ 15    │
└───────────────┴───────┘

┏━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Database Verification  ┃
┡━━━━━━━━┳━━━━━┳━━━━━━━━━━┩
│ Ticker │ Days│ From     │
├────────┼─────┼──────────┤
│ AAPL   │ 5   │ 2025-01-01│
│ MSFT   │ 5   │ 2025-01-01│
│ GOOGL  │ 5   │ 2025-01-01│
└────────┴─────┴──────────┘
```

---

### 2. Company Details

Download company/ticker details including name, market cap, exchange, and more.

#### Syntax

```bash
download.bat company <tickers>
download.bat company --all
```

#### Parameters

- `<tickers>` - **Required.** Comma-separated ticker symbols (no spaces)
  - Examples: `AAPL`, `AAPL,MSFT,GOOGL`
- `--all` - Download details for all tickers in the `tickers` table

#### Examples

```bash
# Single ticker
download.bat company AAPL

# Multiple tickers
download.bat company AAPL,MSFT,GOOGL

# All tickers from database
download.bat company --all
```

#### Notes

- **Rate Limit**: Polygon free tier allows 5 API calls per minute (handled automatically)
- **Batch Processing**: Uses batch extractor for efficient processing
- **Errors**: If a ticker fails, it will be skipped and reported in the summary
- **Database Table**: Data is stored in `company_details` table

#### Output Example

```
[Downloading Company Details]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tickers: AAPL, MSFT, GOOGL

Processing...
  ✓ AAPL - Apple Inc.
  ✓ MSFT - Microsoft Corporation
  ✓ GOOGL - Alphabet Inc. Class A

┏━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ Summary       ┃       ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━┩
│ Total Tickers │ 3     │
│ Successful    │ 3     │
│ Failed        │ 0     │
└───────────────┴───────┘

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Database Verification       ┃
┡━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┩
│ Ticker │ Name               │
├────────┼────────────────────┤
│ AAPL   │ Apple Inc.         │
│ MSFT   │ Microsoft Corp.    │
│ GOOGL  │ Alphabet Inc.      │
└────────┴────────────────────┘
```

---

### 3. Treasury Yields

Download US Treasury yield curve data from FRED (Federal Reserve Economic Data).

#### Syntax

```bash
download.bat treasury [start_date] [end_date]
```

#### Parameters

- `[start_date]` - **Optional.** Start date in `YYYY-MM-DD` format
  - Default: 90 days ago
- `[end_date]` - **Optional.** End date in `YYYY-MM-DD` format
  - Default: Today

#### Examples

```bash
# With specific date range
download.bat treasury 2024-01-01 2024-12-31

# With default dates (last 90 days)
download.bat treasury
```

#### Notes

- **Data Source**: FRED API (Federal Reserve Bank of St. Louis)
- **Maturities Included**: 1M, 3M, 6M, 1Y, 2Y, 5Y, 10Y, 30Y
- **Format**: Wide format (one row per date, one column per maturity)
- **Business Days Only**: FRED only publishes data on business days
- **Database Table**: Data is stored in `treasury_curves` table

#### Output Example

```
[Downloading Treasury Yields (FRED)]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Date Range: 2024-01-01 to 2024-12-31
Series: DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS10, DGS30

Fetching data...
  ✓ 8 maturities fetched
  ✓ 252 business days

┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Summary        ┃                       ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
│ Records Loaded │ 252                   │
│ Date Range     │ 2024-01-01 to 2024-12-31│
└────────────────┴───────────────────────┘

┏━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Database Verification   ┃
┡━━━━━━━━━━━━━┳━━━━━━━━━━━┩
│ Records in DB│ 252       │
│ Date Range   │ 2024-01-01│
│              │ to        │
│              │ 2024-12-31│
└──────────────┴───────────┘
```

---

## Date Defaults

| Command   | Default Start Date | Default End Date |
|-----------|--------------------|------------------|
| `price`   | 30 days ago        | Today            |
| `treasury`| 90 days ago        | Today            |

---

## Error Handling

### Continue on Error

When downloading data for multiple tickers:

- **If one ticker fails**, the script continues processing remaining tickers
- **Failed tickers are tracked** and reported in the summary
- **Exit code is 0** if at least one ticker succeeds

### Error Types

#### API Errors
```
✗ TSLA - API rate limit exceeded
✗ INVALID - Ticker not found
```

#### Validation Errors
```
Error: Invalid date format. Use YYYY-MM-DD
Error: Start date must be before end date
Error: No valid tickers provided
```

#### System Errors
```
Error: UV is not installed or not in PATH
Error: DB_PATH not found in environment variables
Error: POLYGON_API_KEY not configured
```

---

## Input Validation

### Tickers

- Must be non-empty strings
- Comma-separated for multiple tickers (no spaces)
- Automatically converted to uppercase
- Format: Letters, numbers, dots, and hyphens allowed
  - Valid: `AAPL`, `BRK.B`, `AAPL,MSFT,GOOGL`
  - Invalid: `AAPL MSFT` (spaces not allowed)

### Dates

- Format: `YYYY-MM-DD` (ISO 8601)
- Start date must be before end date
- End date cannot be in the future (warning shown, adjusted to today)
- For price data: Max 2-year range (Polygon free tier limit)

---

## Database Tables

### `price_data`

Historical OHLCV price data.

```sql
CREATE TABLE price_data (
    ticker VARCHAR,
    date TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
);
```

### `company_details`

Company/ticker details.

```sql
CREATE TABLE company_details (
    ticker VARCHAR PRIMARY KEY,
    name VARCHAR,
    market_cap BIGINT,
    active BOOLEAN,
    composite_figi VARCHAR,
    base_currency VARCHAR,
    list_date DATE,
    primary_exchange VARCHAR,
    shares_outstanding BIGINT,
    total_employees BIGINT,
    sic_code INT
);
```

### `treasury_curves`

US Treasury yield curves (wide format).

```sql
CREATE TABLE treasury_curves (
    date DATE PRIMARY KEY,
    DGS1MO FLOAT,  -- 1-month
    DGS3MO FLOAT,  -- 3-month
    DGS6MO FLOAT,  -- 6-month
    DGS1 FLOAT,    -- 1-year
    DGS2 FLOAT,    -- 2-year
    DGS5 FLOAT,    -- 5-year
    DGS10 FLOAT,   -- 10-year
    DGS30 FLOAT    -- 30-year
);
```

---

## API Rate Limits

### Polygon.io (Free Tier)

- **Rate**: 5 API calls per minute
- **Handled by**: Built-in rate limiting in extractors
- **Wait behavior**: Automatic 60-second pause between batches
- **Visual feedback**: Progress messages show when waiting

### FRED API

- **Rate**: Generally permissive (no strict limits for free tier)
- **Handled by**: Not rate-limited by extractors

---

## Tips & Best Practices

### 1. Batch Downloads

For downloading many tickers, use comma-separated lists:

```bash
# Good: Single command for multiple tickers
download.bat price AAPL,MSFT,GOOGL,TSLA,AMZN

# Less efficient: Multiple commands
download.bat price AAPL
download.bat price MSFT
download.bat price GOOGL
```

### 2. Date Ranges

Be mindful of API limits:

```bash
# Good: Within 2-year limit
download.bat price AAPL 2023-01-01 2024-12-31

# Warning: Exceeds 2-year limit
download.bat price AAPL 2020-01-01 2024-12-31
```

### 3. Company Details Before Price Data

Download company details first to populate the database:

```bash
# Step 1: Get company info
download.bat company AAPL,MSFT,GOOGL

# Step 2: Get price data
download.bat price AAPL,MSFT,GOOGL 2024-01-01 2024-12-31
```

### 4. Using `--all` Flag

If you have many tickers in your database:

```bash
# First, ensure tickers table is populated
# (this requires manual population or ticker list extraction)

# Then download all company details
download.bat company --all
```

### 5. Regular Updates

Set up scheduled downloads for regular updates:

```bash
# Daily: Download yesterday's price data
download.bat price AAPL,MSFT,GOOGL

# Weekly: Download treasury yields
download.bat treasury
```

---

## Troubleshooting

### Problem: "UV is not installed"

**Solution**: Install UV package manager:
```bash
# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Problem: "DB_PATH not found"

**Solution**: Ensure `secret/.env` contains:
```env
DB_PATH=./database/polygon.duckdb
```

### Problem: "API key not configured"

**Solution**: Add API keys to `secret/.env`:
```env
POLYGON_API_KEY=your_polygon_key_here
FRED_KEY=your_fred_key_here
```

### Problem: "Rate limit exceeded"

**Solution**: Wait 60 seconds or use smaller batches:
```bash
# Instead of: AAPL,MSFT,GOOGL,TSLA,AMZN,NFLX
# Use smaller batches:
download.bat price AAPL,MSFT,GOOGL,TSLA,AMZN
# Wait 60 seconds
download.bat price NFLX
```

### Problem: "No data returned"

**Possible causes**:
- Invalid ticker symbol
- Date range has no trading days (weekends/holidays)
- Ticker was not traded during that period

**Solution**: Check ticker validity and date range.

### Problem: Script fails silently

**Solution**: Check logs in `logs/` directory for detailed error messages.

---

## Advanced Usage

### Linux/macOS

Use `download.sh` instead of `download.bat`:

```bash
# Make executable (first time only)
chmod +x download.sh

# Run commands
./download.sh price AAPL 2025-01-01 2025-01-07
./download.sh company AAPL,MSFT,GOOGL
./download.sh treasury
```

### Help & Documentation

Show help message:

```bash
download.bat
download.bat --help
download.bat help
```

### Scripting & Automation

Create custom scripts for complex workflows:

```bash
@echo off
REM daily-update.bat - Download daily data

echo Downloading price data...
download.bat price AAPL,MSFT,GOOGL

echo Downloading treasury yields...
download.bat treasury

echo Done!
```

---

## Related Documentation

- [README.md](README.md) - Main project documentation
- [MONITORING.md](MONITORING.md) - Monitoring stack setup
- [LOAD_TESTING.md](LOAD_TESTING.md) - Load testing guide
- [API_STRUCTURE.md](API_STRUCTURE.md) - API architecture

---

## Technical Details

### Implementation

- **CLI Framework**: Python `argparse`
- **Terminal UI**: `rich` library for progress and tables
- **Extractors**: `ELT/extract_polygon.py`, `ELT/extract_fred.py`
- **Loaders**: `ELT/load_polygon.py`, `ELT/load_fred.py`
- **Database**: DuckDB with Polars integration
- **Logging**: Structured logging to `logs/` directory

### File Structure

```
DataProject/
├── download.bat           # Windows wrapper
├── download.sh            # Linux/macOS wrapper
├── ELT/
│   ├── download_cli.py    # Main CLI script
│   ├── extract_polygon.py # Polygon extractors
│   ├── extract_fred.py    # FRED extractor
│   ├── load_polygon.py    # Polygon loaders
│   └── load_fred.py       # FRED loader
└── secret/
    └── .env               # API keys
```

---

## Support

For issues or questions:

1. Check [Troubleshooting](#troubleshooting) section
2. Review logs in `logs/` directory
3. Check API key configuration in `secret/.env`
4. Verify database path and permissions

---

**Last Updated**: 2025-01-07
