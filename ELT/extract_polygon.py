"""Extract data from Polygon.io API for ELT pipeline."""

import json
import os
import time
from pathlib import Path
from typing import Any, Union

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
                raise ValueError(
                    f"{key_name} not found in environment variables"
                )
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

        logger.info(
            f"Batch extraction complete: {len(results)}/{len(tickers)} successful"
        )
        return results


class PriceExtractor:
    """
    Responsible for extracting batch price data from Polygon API.

    Single Responsibility: Batch price data extraction logic.
    """

    def __init__(self, client: RESTClient):
        """
        Initialize extractor with a Polygon client.

        Args:
            client: Initialized Polygon REST client
        """
        self.client = client

    def extract_range(
        self,
        tickers: Union[list[str], str],
        start_date: str,
        end_date: str,
        checkpoint_file: str = "data/extraction_checkpoint.json",
    ) -> dict[str, Any]:
        """
        Extract price data for multiple tickers between dates.
        PS: with Polygon free tier, the maximum date range is 2 years.
        Rate limit: 5 API calls per minute.

        Supports resuming from checkpoint if interrupted.

        Args:
            tickers: List of stock ticker symbols (e.g., 'AAPL', 'MSFT')
            start_date: Start date for extraction (YYYY-MM-DD)
            end_date: End date for extraction (YYYY-MM-DD)
            checkpoint_file: Path to save progress (default: data/extraction_checkpoint.json)

        Returns:
            dict mapping ticker symbols to their price data

        Raises:
            Exception: If API request fails
        """
        logger.info(
            f"Extracting price data for tickers between {start_date} and {end_date}"
        )

        if isinstance(tickers, str):
            tickers = [tickers]

        # Load checkpoint if exists
        checkpoint_data = self._load_checkpoint(checkpoint_file)
        results = checkpoint_data.get("results", {})
        processed_tickers = set(checkpoint_data.get("processed", []))

        # Filter out already processed tickers
        remaining_tickers = [t for t in tickers if t not in processed_tickers]

        if processed_tickers:
            logger.info(
                f"Resuming: {len(processed_tickers)} tickers already processed, {len(remaining_tickers)} remaining"
            )

        # Rate limiting: 5 calls per minute
        batch_size = 5
        total_remaining = len(remaining_tickers)

        try:
            for i in range(0, total_remaining, batch_size):
                batch = remaining_tickers[i : i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_remaining + batch_size - 1) // batch_size

                logger.info(
                    f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)"
                )

                for ticker in batch:
                    max_retries = 3
                    retry_delay = 15  # seconds

                    for attempt in range(max_retries):
                        try:
                            bars = self.client.get_aggs(
                                ticker,
                                1,
                                "day",
                                start_date,
                                end_date,
                            )
                            results[ticker] = [bar.__dict__ for bar in bars]
                            processed_tickers.add(ticker)
                            logger.info(
                                f"Successfully extracted price data for {ticker}"
                            )

                            # Save checkpoint after each successful extraction
                            self._save_checkpoint(
                                checkpoint_file,
                                results,
                                list(processed_tickers),
                            )
                            break  # Success, exit retry loop

                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(
                                    f"Error extracting {ticker} (attempt {attempt + 1}/{max_retries}): {e}"
                                )
                                logger.info(
                                    f"Retrying in {retry_delay} seconds..."
                                )
                                time.sleep(retry_delay)
                            else:
                                logger.error(
                                    f"Failed to extract {ticker} after {max_retries} attempts: {e}"
                                )
                                # Mark as processed to skip on next run
                                processed_tickers.add(ticker)
                                self._save_checkpoint(
                                    checkpoint_file,
                                    results,
                                    list(processed_tickers),
                                )

                # Wait 60 seconds before next batch (unless this is the last batch)
                if i + batch_size < total_remaining:
                    logger.info(
                        "Rate limit: waiting 60 seconds before next batch..."
                    )
                    time.sleep(60)

            logger.info(
                f"Extraction complete: {len(results)}/{len(tickers)} tickers successful"
            )

            # Clean up checkpoint file on successful completion
            self._cleanup_checkpoint(checkpoint_file)

        except KeyboardInterrupt:
            logger.warning(
                "Extraction interrupted by user. Progress saved to checkpoint."
            )
            raise
        except Exception as e:
            logger.error(
                f"Extraction failed: {e}. Progress saved to checkpoint."
            )
            raise

        return results

    def _load_checkpoint(self, checkpoint_file: str) -> dict:
        """Load checkpoint data from file."""
        checkpoint_path = Path(checkpoint_file)
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path, "r") as f:
                    data = json.load(f)
                    logger.info(f"Loaded checkpoint from {checkpoint_file}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return {"results": {}, "processed": []}

    def _save_checkpoint(
        self, checkpoint_file: str, results: dict, processed: list
    ):
        """Save checkpoint data to file."""
        checkpoint_path = Path(checkpoint_file)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(checkpoint_path, "w") as f:
                json.dump(
                    {
                        "results": results,
                        "processed": processed,
                        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _cleanup_checkpoint(self, checkpoint_file: str):
        """Remove checkpoint file after successful completion."""
        checkpoint_path = Path(checkpoint_file)
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
                logger.info(f"Checkpoint file {checkpoint_file} cleaned up")
            except Exception as e:
                logger.warning(f"Failed to cleanup checkpoint: {e}")

    def extract_day(self, tickers: list[str], date: str) -> dict[str, Any]:
        """
        Extract price data for multiple tickers on a specific date.

        Args:
            tickers: List of stock ticker symbols (e.g., 'AAPL', 'MSFT')
            date: Date for which to extract price data (YYYY-MM-DD)
        Returns:
            dict mapping ticker symbols to their price data
        Raises:
            Exception: If API request fails
        """
        logger.info(f"Extracting price data for tickers on {date}")
        results = {}
        for ticker in tickers:
            try:
                bars = self.client.get_daily_open_close_agg(
                    ticker,
                    date,
                    adjusted=True,
                )
                logger.info(f"Successfully extracted price data for {ticker}")
            except Exception as e:
                logger.error(f"Error extracting price data for {ticker}: {e}")
                continue
                results[ticker] = bars.__dict__
        return results


class PolygonExtractorFactory:
    """
    Responsible for creating properly configured extractor instances.

    Single Responsibility: Object creation and dependency injection.
    """

    @staticmethod
    def create_ticker_extractor(
        api_key: str | None = None,
    ) -> TickerDetailsExtractor:
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
    def create_batch_extractor(
        api_key: str | None = None,
    ) -> BatchTickerExtractor:
        """
        Create a configured BatchTickerExtractor.

        Args:
            api_key: Optional API key. If None, will load from environment.

        Returns:
            Configured BatchTickerExtractor instance
        """
        ticker_extractor: TickerDetailsExtractor = (
            PolygonExtractorFactory.create_ticker_extractor(api_key)
        )
        return BatchTickerExtractor(ticker_extractor)

    @staticmethod
    def create_price_extractor(
        api_key: str | None = None,
    ) -> PriceExtractor:
        """
        Create a configured PriceExtractor.

        Args:
            api_key: Optional API key. If None, will load from environment.

        Returns:
            Configured PriceExtractor instance
        """
        if api_key is None:
            api_key = ApiKeyProvider.get_api_key()

        polygon_client = PolygonClient(api_key)
        return PriceExtractor(polygon_client.get_client())


# extractor = PolygonExtractorFactory.create_ticker_extractor()
# apple_data = extractor.extract('AAPL')
# print(apple_data)
