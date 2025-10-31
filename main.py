import os

import duckdb as ddb
from dotenv import load_dotenv

from logger import get_logger

load_dotenv("./secret/.env")
logger = get_logger(__name__)


def main():
    logger.info("Starting the extraction process.")
    if __name__ == "__main__":
        # Example: Load single ticker
        # single_extractor = PolygonExtractorFactory.create_ticker_extractor()
        # loader = PolygonDataLoader()
        # loader.load_ticker_details("AAPL", single_extractor)

        # # Example: Load batch of tickers
        # batch_extractor = PolygonExtractorFactory.create_batch_extractor()
        # loader = PolygonDataLoader()
        # tickers_to_load = ["GOOGL", "MSFT", "NVDA", "AMD", "META"]
        # loader.load_batch_ticker_details(tickers_to_load, batch_extractor)

        # Query to verify data load
        db_path = os.getenv("DB_PATH")
        if db_path is not None:
            db_connection = ddb.connect(db_path)
        else:
            logger.error("DB_PATH not found in environment variables")
            raise ValueError("DB_PATH not found in environment variables")
        result = db_connection.execute("FROM company_details Join sic_codes on company_details.sic_code = sic_codes.sic_code").pl()
        # Convert result to Polars DataFrame for better visualization
        result.write_csv("result.csv")
        print(result)


if __name__ == "__main__":
    main()
