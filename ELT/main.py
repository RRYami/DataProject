import os
import time
from logging import Logger

import duckdb as ddb
import polars as pl
from dotenv import load_dotenv

import logger.logger as logger
from ELT.extract_fred import FredExtractor
from ELT.extract_polygon import PolygonExtractorFactory
from ELT.load_fred import YieldLoader
from ELT.load_polygon import PolygonDataLoader

load_dotenv("./secret/.env")
loggers: Logger = logger.get_logger(__name__)


def run_pipeline() -> None:
    loggers.info("Starting the extraction process.")
    # Example: Load single ticker
    # single_extractor = PolygonExtractorFactory.create_ticker_extractor()
    # loader = PolygonDataLoader()
    # loader.load_ticker_details("AAPL", single_extractor)

    # Example: Load batch of tickers
    # batch_extractor = (
    #     ELT.extract_polygon.PolygonExtractorFactory.create_batch_extractor()
    # )
    # loader = ELT.load_polygon.PolygonDataLoader()
    # tickers_to_load: list[str] = ["TDG", "D"]
    # loader.load_batch_ticker_details(tickers_to_load, batch_extractor)

    # Query to verify data load
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    result = db_connection.execute("FROM company_details").pl()
    # Convert result to Polars DataFrame for better visualization
    result.write_csv("result.csv")
    print(result["ticker"].to_list())

    time.sleep(60)
    # Example: Load Price Data
    price_extractor = PolygonExtractorFactory.create_price_extractor()
    loader = PolygonDataLoader()
    tickers_to_load: list[str] = result["ticker"].to_list()
    data = price_extractor.extract_range(
        tickers_to_load, "2025-01-01", "2025-10-28"
    )
    loader.load_price_data(data)
    # Query to verify data load
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
        db_connection.execute("DROP TABLE IF EXISTS price_data")
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    # Convert result to Polars DataFrame for better visualization
    result = db_connection.execute("FROM price_data").pl()
    result.write_csv("price_data_result.csv")
    print(result)

    time.sleep(60)
    # Example: load list of tickers
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
        db_connection.execute("DROP TABLE IF EXISTS tickers")
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    indices_list_extractor = (
        PolygonExtractorFactory.create_ticker_list_extractor()
    )
    indices_loader = PolygonDataLoader()
    indices_loader.load_tickers(indices_list_extractor)
    result = db_connection.execute("FROM tickers").pl()
    result.write_csv("tickers_result.csv")
    print(result)


def test_Fred_extractor() -> None:
    extractor = FredExtractor()
    maturities = ["DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5", "DGS10", "DGS30"]

    # Fetch raw data for all series (add date range if you want to limit data volume)
    raw_data = extractor.get_series_observations(
        series_id=maturities,
        # observation_start="2000-01-01",  # Optional: uncomment to restrict range
    )

    # Latest curve
    if isinstance(raw_data, pl.DataFrame):
        latest_curve = raw_data.tail(10)
    else:
        raise ValueError("Expected Polars DataFrame from FRED extractor")

    print("\nLatest US Treasury Yield Curve:")
    print(latest_curve.select(["date"] + maturities))
    loader = YieldLoader()
    loader.load_yield_data(raw_data)
    # Verify data load
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    result = db_connection.execute(
        "FROM treasury_curves order by date desc"
    ).pl()
    print("\nData in treasury_curves table:")
    print(result.head(10).select(["date"] + maturities))


if __name__ == "__main__":
    # run_pipeline()
    # test_polygon_yield()
    test_Fred_extractor()
    pass
