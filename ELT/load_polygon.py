"""Load polygon data into the database."""

import os

import duckdb as ddb
from dotenv import load_dotenv

from ELT.extract_polygon import BatchTickerExtractor, TickerDetailsExtractor
from logger import get_logger


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
        details = extractor.extract(ticker)

        # Example of inserting data into a DuckDB table
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

        # Load each ticker's data
        successful_loads = 0
        for ticker, details in batch_data.items():
            try:
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
                successful_loads += 1
            except Exception as e:
                self.logger.error(f"Failed to load {ticker}: {e}")
                continue

        self.logger.info(
            f"Batch load complete: {successful_loads}/{len(batch_data)} tickers loaded successfully"
        )

    def load_price_data(self, price_data: dict[str, list[dict]]):
        """
        Load price data into the database.

        Args:
            price_data: Dictionary mapping ticker symbols to their price data
        """
        self.logger.info("Starting price data load")

        # Delete Table to start fresh
        # self.db_connection.execute("DROP TABLE IF EXISTS price_data")

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

        # Load price data for each ticker
        for ticker, prices in price_data.items():
            for price in prices:
                try:
                    # Convert Unix timestamp (milliseconds) to TIMESTAMP
                    timestamp_ms = price.get("timestamp")

                    self.db_connection.execute(
                        """
                        INSERT INTO price_data (ticker, date, open, high, low, close, volume)
                        VALUES (?, epoch_ms(?), ?, ?, ?, ?, ?)
                        ON CONFLICT (ticker, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """,
                        (
                            ticker,
                            timestamp_ms,
                            price.get("open"),
                            price.get("high"),
                            price.get("low"),
                            price.get("close"),
                            price.get("volume"),
                        ),
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to load price data for {ticker} on {price.get('timestamp')}: {e}"
                    )
                    continue

        self.logger.info("Price data load complete")
