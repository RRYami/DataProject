"""Extract data from Polygon.io API for ELT pipeline."""
import os
from typing import Any

from dotenv import load_dotenv
from polygon import RESTClient

from logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


class ApiKeyProvider:
    """
    Responsible for providing API keys from environment variables.

    Single Responsibility: API key management and retrieval.
    """

    @staticmethod
    def get_api_key(key_name: str = "API_KEY") -> str:
        """
        Retrieve API key from environment variables.

        Args:
            key_name: Name of the environment variable containing the API key

        Returns:
            API key string

        Raises:
            ValueError: If API key is not found in environment
        """
        try:
            load_dotenv("./secret/.env")
            api_key = os.getenv(key_name)
            if api_key is None:
                logger.warning(f"{key_name} not found in environment variables")
                raise ValueError(f"{key_name} not found in environment variables")
        except Exception as e:
            logger.error(f"Error retrieving API key: {e}")
            raise
        return api_key


class PolygonClient:
    """
    Responsible for managing the Polygon API client connection.

    Single Responsibility: Client initialization and lifecycle management.
    """

    def __init__(self, api_key: str):
        """
        Initialize Polygon REST client.

        Args:
            api_key: Polygon API key
        """
        self.client = RESTClient(api_key)
        logger.info("Polygon API client initialized")

    def get_client(self) -> RESTClient:
        """Return the underlying REST client."""
        return self.client


class TickerDetailsExtractor:
    """
    Responsible for extracting ticker details from Polygon API.

    Single Responsibility: Ticker details extraction logic.
    """

    def __init__(self, client: RESTClient):
        """
        Initialize extractor with a Polygon client.

        Args:
            client: Initialized Polygon REST client
        """
        self.client = client

    def extract(self, ticker: str) -> dict[str, Any]:
        """
        Extract ticker details for a single ticker.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')

        Returns:
            dict containing raw ticker details from the API

        Raises:
            Exception: If API request fails
        """
        logger.info(f"Extracting ticker details for: {ticker}")
        try:
            details = self.client.get_ticker_details(ticker)
            data = details.__dict__.copy()
            logger.info(f"Successfully extracted data for {ticker}")
            logger.debug(f"Extracted fields: {list(data.keys())}")
        except Exception as e:
            logger.error(f"Error extracting data for {ticker}: {e}")
            raise
        return data


class BatchTickerExtractor:
    """
    Responsible for orchestrating batch extraction of multiple tickers.

    Single Responsibility: Batch processing and error handling for multiple tickers.
    """

    def __init__(self, extractor: TickerDetailsExtractor):
        """
        Initialize batch extractor.

        Args:
            extractor: Single ticker extractor instance
        """
        self.extractor = extractor

    def extract(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """
        Extract ticker details for multiple tickers.

        Args:
            tickers: List of ticker symbols

        Returns:
            dict mapping ticker symbols to their details
        """
        logger.info(f"Starting batch extraction for {len(tickers)} tickers")
        results = {}

        for ticker in tickers:
            try:
                results[ticker] = self.extractor.extract(ticker)
            except Exception as e:
                logger.warning(f"Skipping {ticker} due to error: {e}")
                continue

        logger.info(f"Batch extraction complete: {len(results)}/{len(tickers)} successful")
        return results


class PolygonExtractorFactory:
    """
    Responsible for creating properly configured extractor instances.

    Single Responsibility: Object creation and dependency injection.
    """

    @staticmethod
    def create_ticker_extractor(api_key: str | None = None) -> TickerDetailsExtractor:
        """
        Create a configured TickerDetailsExtractor.

        Args:
            api_key: Optional API key. If None, will load from environment.

        Returns:
            Configured TickerDetailsExtractor instance
        """
        if api_key is None:
            api_key = ApiKeyProvider.get_api_key()

        polygon_client = PolygonClient(api_key)
        return TickerDetailsExtractor(polygon_client.get_client())

    @staticmethod
    def create_batch_extractor(api_key: str | None = None) -> BatchTickerExtractor:
        """
        Create a configured BatchTickerExtractor.

        Args:
            api_key: Optional API key. If None, will load from environment.

        Returns:
            Configured BatchTickerExtractor instance
        """
        ticker_extractor = PolygonExtractorFactory.create_ticker_extractor(api_key)
        return BatchTickerExtractor(ticker_extractor)


# extractor = PolygonExtractorFactory.create_ticker_extractor()
# apple_data = extractor.extract('AAPL')
# print(apple_data)
