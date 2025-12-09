import os
from logging import Logger

import duckdb as ddb
from dotenv import load_dotenv

import ELT.extract_polygon
import ELT.load_polygon
import logger.logger as logger

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
    # tickers_to_load: list[str] = ["RDDT", "LEU", "LMT", "TXN"]
    # loader.load_batch_ticker_details(tickers_to_load, batch_extractor)

    # # Query to verify data load
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    result = db_connection.execute(
        "FROM company_details Join sic_codes on company_details.sic_code = sic_codes.sic_code"
    ).pl()
    # Convert result to Polars DataFrame for better visualization
    result.write_csv("result.csv")
    print(result["ticker"].to_list())

    # Example: Load Price Data
    price_extractor = (
        ELT.extract_polygon.PolygonExtractorFactory.create_price_extractor()
    )
    loader = ELT.load_polygon.PolygonDataLoader()
    tickers_to_load: list[str] = result["ticker"].to_list()
    data = price_extractor.extract_range(
        tickers_to_load, "2025-01-01", "2025-10-28"
    )
    loader.load_price_data(data)
    # Query to verify data load
    db_path = os.getenv("DB_PATH")
    if db_path is not None:
        db_connection = ddb.connect(db_path)
    else:
        loggers.error("DB_PATH not found in environment variables")
        raise ValueError("DB_PATH not found in environment variables")
    # Convert result to Polars DataFrame for better visualization
    result = db_connection.execute("FROM price_data").pl()
    result.write_csv("price_data_result.csv")
    print(result)


if __name__ == "__main__":
    run_pipeline()
