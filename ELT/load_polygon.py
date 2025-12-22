"""Load polygon data into the database."""

import os
import time

import duckdb as ddb
import polars as pl
from dotenv import load_dotenv

from ELT.extract_polygon import (
    BatchTickerExtractor,
    TickerDetailsExtractor,
    TickerListExtractor,
)
from logger.logger import get_logger


class PolygonDataLoader:
    """
    Load polygon data into the database.

    Single Responsibility: Data loading and transformation.
    """

    def __init__(self):
        self.logger = get_logger(__name__)
        load_dotenv("./secret/.env")
        db_path = os.getenv("DB_PATH")
        if db_path is not None:
            self.db_connection = ddb.connect(db_path)
        else:
            self.logger.error("DB_PATH not found in environment variables")
            raise ValueError("DB_PATH not found in environment variables")

    def load_tickers(self, extractor: TickerListExtractor):
        """
        Load a list of tickers into the database.

        Args:
            tickers: List of ticker symbols
        """

        self.logger.info("Loading tickers into the database.")

        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                ticker VARCHAR PRIMARY KEY,
                name VARCHAR,
                market VARCHAR,
                locale VARCHAR,
                active BOOLEAN,
                source_feed VARCHAR
            )
        """)

        try:
            details = extractor.extract()

            start_time = time.time()
            self.logger.info(
                f"Starting database insert for {len(details)} tickers..."
            )

            # Convert to Polars DataFrame - DuckDB can read it directly!
            df = pl.DataFrame(details)

            # DuckDB can query the DataFrame directly
            self.db_connection.execute("""
                INSERT INTO tickers
                SELECT * FROM df
                ON CONFLICT (ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    market = EXCLUDED.market,
                    locale = EXCLUDED.locale,
                    active = EXCLUDED.active,
                    source_feed = EXCLUDED.source_feed
            """)

            elapsed = time.time() - start_time
            self.logger.info(
                f"Successfully loaded {len(details)} tickers into the database in {elapsed:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Failed to load tickers: {e}")
            raise

    def load_ticker_details(
        self, ticker: str, extractor: TickerDetailsExtractor
    ):
        """
        Load ticker details into the database.

        Args:
            ticker: The ticker symbol
            extractor: The TickerDetailsExtractor instance
        """
        self.logger.info(f"Loading details for ticker: {ticker}")

        # Create table if it doesn't exist
        self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS company_details (
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
                )
            """)

        details = extractor.extract(ticker)

        # For single ticker, traditional insert is fine
        self.db_connection.execute(
            """
                INSERT INTO company_details (ticker, name, market_cap, active, composite_figi, base_currency, list_date, primary_exchange,
                shares_outstanding, total_employees, sic_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    market_cap = EXCLUDED.market_cap,
                    active = EXCLUDED.active,
                    composite_figi = EXCLUDED.composite_figi,
                    base_currency = EXCLUDED.base_currency,
                    list_date = EXCLUDED.list_date,
                    primary_exchange = EXCLUDED.primary_exchange,
                    shares_outstanding = EXCLUDED.shares_outstanding,
                    total_employees = EXCLUDED.total_employees,
                    sic_code = EXCLUDED.sic_code
            """,
            (
                ticker,
                details.get("name"),
                details.get("market_cap"),
                details.get("active"),
                details.get("composite_figi"),
                details.get("currency_name"),
                details.get("list_date"),
                details.get("primary_exchange"),
                details.get("share_class_shares_outstanding"),
                details.get("total_employees"),
                details.get("sic_code"),
            ),
        )

        self.logger.info(f"Successfully loaded details for ticker: {ticker}")

    def load_batch_ticker_details(
        self, tickers: list[str], batch_extractor: BatchTickerExtractor
    ):
        """
        Load ticker details for multiple tickers in batch.

        Args:
            tickers: List of ticker symbols
            batch_extractor: The BatchTickerExtractor instance
        """
        self.logger.info(f"Starting batch load for {len(tickers)} tickers")

        # Extract all tickers in batch
        batch_data = batch_extractor.extract(tickers)

        # Create table if it doesn't exist
        self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS company_details (
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
                )
            """)

        start_time = time.time()
        self.logger.info(
            f"Starting database insert for {len(batch_data)} company details..."
        )

        try:
            # Convert batch_data dict to list of dicts for DataFrame
            records = []
            for ticker, details in batch_data.items():
                records.append(
                    {
                        "ticker": ticker,
                        "name": details.get("name"),
                        "market_cap": details.get("market_cap"),
                        "active": details.get("active"),
                        "composite_figi": details.get("composite_figi"),
                        "base_currency": details.get("currency_name"),
                        "list_date": details.get("list_date"),
                        "primary_exchange": details.get("primary_exchange"),
                        "shares_outstanding": details.get(
                            "share_class_shares_outstanding"
                        ),
                        "total_employees": details.get("total_employees"),
                        "sic_code": details.get("sic_code"),
                    }
                )

            # Convert to Polars DataFrame
            df = pl.DataFrame(records)

            # Bulk insert using DataFrame
            self.db_connection.execute("""
                    INSERT INTO company_details
                    SELECT * FROM df
                    ON CONFLICT (ticker) DO UPDATE SET
                        name = EXCLUDED.name,
                        market_cap = EXCLUDED.market_cap,
                        active = EXCLUDED.active,
                        composite_figi = EXCLUDED.composite_figi,
                        base_currency = EXCLUDED.base_currency,
                        list_date = EXCLUDED.list_date,
                        primary_exchange = EXCLUDED.primary_exchange,
                        shares_outstanding = EXCLUDED.shares_outstanding,
                        total_employees = EXCLUDED.total_employees,
                        sic_code = EXCLUDED.sic_code
                """)

            elapsed = time.time() - start_time
            self.logger.info(
                f"Batch load complete: {len(records)} tickers loaded successfully in {elapsed:.2f}s"
            )
        except Exception as e:
            self.logger.error(f"Failed to load batch ticker details: {e}")
            raise

    def load_price_data(self, price_data: dict[str, list[dict]]):
        """
        Load price data into the database.

        Args:
            price_data: Dictionary mapping ticker symbols to their price data
        """
        self.logger.info("Starting price data load")

        self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS price_data (
                    ticker VARCHAR,
                    date TIMESTAMP,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume BIGINT,
                    PRIMARY KEY (ticker, date)
                )
            """)

        start_time = time.time()

        # Flatten the nested dict structure into a list of records
        records = []
        for ticker, prices in price_data.items():
            for price in prices:
                records.append(
                    {
                        "ticker": ticker,
                        "timestamp_ms": price.get("timestamp"),
                        "open": price.get("open"),
                        "high": price.get("high"),
                        "low": price.get("low"),
                        "close": price.get("close"),
                        "volume": price.get("volume"),
                    }
                )

        self.logger.info(
            f"Starting database insert for {len(records)} price records..."
        )

        try:
            # Convert to Polars DataFrame
            df = pl.DataFrame(records)

            # Bulk insert using DataFrame with timestamp conversion
            self.db_connection.execute("""
                    INSERT INTO price_data (ticker, date, open, high, low, close, volume)
                    SELECT ticker, epoch_ms(timestamp_ms), open, high, low, close, volume
                    FROM df
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """)

            elapsed = time.time() - start_time
            self.logger.info(
                f"Price data load complete: {len(records)} records loaded in {elapsed:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Failed to load price data: {e}")
            raise

    def load_yield_data(self, yield_data: list, curve_id: str = "US_TREASURY"):
        """
        Load treasury yield curve data into the database.

        Args:
            yield_data: List of TreasuryYield objects from the API
            curve_id: Identifier for the yield curve (default: "US_TREASURY")
        """
        self.logger.info(f"Starting yield data load for curve: {curve_id}")

        # Create table with curve_id to support multiple curves
        self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS treasury_yields (
                    curve_id VARCHAR,
                    date DATE,
                    maturity VARCHAR,
                    yield FLOAT,
                    PRIMARY KEY (curve_id, date, maturity)
                )
            """)

        start_time = time.time()

        # Flatten the TreasuryYield objects into normalized records
        records = []

        # Define maturity mappings
        maturity_fields = {
            "yield_1_month": "1M",
            "yield_3_month": "3M",
            "yield_6_month": "6M",
            "yield_1_year": "1Y",
            "yield_2_year": "2Y",
            "yield_3_year": "3Y",
            "yield_5_year": "5Y",
            "yield_7_year": "7Y",
            "yield_10_year": "10Y",
            "yield_20_year": "20Y",
            "yield_30_year": "30Y",
        }

        for yield_obj in yield_data:
            date = (
                yield_obj.date
                if hasattr(yield_obj, "date")
                else yield_obj.get("date")
            )

            # Extract each maturity yield
            for field_name, maturity_label in maturity_fields.items():
                yield_value = (
                    getattr(yield_obj, field_name, None)
                    if hasattr(yield_obj, field_name)
                    else yield_obj.get(field_name)
                )

                # Only add records where yield value is not None
                if yield_value is not None:
                    records.append(
                        {
                            "curve_id": curve_id,
                            "date": date,
                            "maturity": maturity_label,
                            "yield": yield_value,
                        }
                    )

        self.logger.info(
            f"Starting database insert for {len(records)} yield records..."
        )

        try:
            # Convert to Polars DataFrame
            df = pl.DataFrame(records)

            # Bulk insert using DataFrame
            self.db_connection.execute("""
                    INSERT INTO treasury_yields (curve_id, date, maturity, yield)
                    SELECT curve_id, date, maturity, yield
                    FROM df
                    ON CONFLICT (curve_id, date, maturity) DO UPDATE SET
                        yield = EXCLUDED.yield
                """)

            elapsed = time.time() - start_time
            self.logger.info(
                f"Yield data load complete for {curve_id}: "
                f"{len(records)} records loaded in {elapsed:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Failed to load yield data: {e}")
            raise
