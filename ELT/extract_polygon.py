"""Extract data from Polygon.io API for ELT pipeline."""

import json
import os
from datetime import datetime
from typing import Any

import pendulum as p
import requests
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


class AlphaVantageClient:
    """
    Responsible for managing the Alpha Vantage API client connection.

    Single Responsibility: Client initialization and lifecycle management.
    """

    def __init__(self):
        """
        Initialize Alpha Vantage REST client.

        Args:
            api_key: Alpha Vantage API key
        """
        self.api_key = ApiKeyProvider.get_api_key("ALPHA_VANTAGE_KEY")
        self.destination_folder = os.getenv("FUNDAMENTALS_DATA_PATH")
        logger.info("Alpha Vantage API client initialized")

    def get_api_key(self) -> str:
        """Return the API key."""
        return self.api_key


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


class FundamentalsDataExtractor:
    def __str__(self) -> str:
        return f"FundamentalsDataDownloader({self.statements!r})"

    def __repr__(self) -> str:
        return f"FundamentalsDataDownloader({self.statements!r})"

    def __init__(
        self, client: AlphaVantageClient, tickers: list[str], statements="ALL"
    ):
        self.statements = statements
        self.api_key = client.api_key
        self.destination_folder: str = client.destination_folder
        self.tickers = tickers
        if self.destination_folder is None:
            logger.error("FUNDAMENTALS_DATA_PATH not found in environment")
            raise ValueError("FUNDAMENTALS_DATA_PATH not found in environment")

    def get_financial_statement(self):
        if self.statements == "ALL":
            self.statements = [
                "INCOME_STATEMENT",
                "BALANCE_SHEET",
                "CASH_FLOW",
                "EARNINGS",
                "OVERVIEW",
            ]
            try:
                logger.info("Starting download of all financial statements")
                for j in self.tickers:
                    for i in self.statements:
                        logger.info(f"Downloading {i} for {j}")
                        url = (
                            "https://www.alphavantage.co/query?function="
                            + i
                            + "&symbol="
                            + j
                            + "&apikey="
                            + self.api_key
                        )
                        try:
                            r = requests.get(url)
                            r.raise_for_status()
                        except requests.exceptions.HTTPError as e:
                            print(e)
                        match i:
                            case "INCOME_STATEMENT":
                                with open(
                                    self.destination_folder
                                    + "\\Income Statement\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(
                                        f"Saved Income Statement for {j}"
                                    )
                            case "BALANCE_SHEET":
                                with open(
                                    self.destination_folder
                                    + "\\Balance Sheet\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Balance Sheet for {j}")
                            case "CASH_FLOW":
                                with open(
                                    self.destination_folder
                                    + "\\Cash Flow\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Cash Flow for {j}")
                            case "EARNINGS":
                                with open(
                                    self.destination_folder
                                    + "\\Earnings\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Earnings for {j}")
                            case "OVERVIEW":
                                with open(
                                    self.destination_folder
                                    + "\\Overview\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Overview for {j}")
                            case _:
                                logger.warning(
                                    f"Invalid statement or not added ({self.statements})"
                                )
            except Exception as e:
                logger.error(f"Error getting data: {e}")
        else:
            try:
                for i in self.tickers:
                    url = (
                        "https://www.alphavantage.co/query?function="
                        + str(self.statements)
                        + "&symbol="
                        + i
                        + "&apikey="
                        + self.api_key
                    )
                    r = requests.get(url)
                    match self.statements:
                        case "INCOME_STATEMENT":
                            with open(
                                self.destination_folder
                                + "\\Income Statement\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Income Statement for {i}")
                        case "BALANCE_SHEET":
                            with open(
                                self.destination_folder
                                + "\\Balance Sheet\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Balance Sheet for {i}")
                        case "CASH_FLOW":
                            with open(
                                self.destination_folder
                                + "\\Cash Flow\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Cash Flow for {i}")
                        case "EARNINGS":
                            with open(
                                self.destination_folder
                                + "\\Earnings\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Earnings for {i}")
                        case "OVERVIEW":
                            with open(
                                self.destination_folder
                                + "\\Overview\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Overview for {i}")
                        case _:
                            logger.warning(
                                f"Invalid statement or not added ({self.statements})"
                            )
            except Exception as e:
                logger.error(f"Error getting data: {e}")

    def get_list_of_reports_dates(self, file_path: str) -> list[datetime]:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"No file found at {file_path}: {e}")
        try:
            dates_list = []
            for i in range(len(data["quarterlyReports"])):
                dates_list.append(
                    data["quarterlyReports"][i]["fiscalDateEnding"]
                )
        except Exception as e:
            logger.error(f"Error parsing dates from JSON: {e}")
        try:
            datetime_list = []
            for j in dates_list:
                coverted_string = [datetime.strptime(j, "%Y-%m-%d")]
                datetime_list.append(coverted_string)
        except Exception as e:
            logger.error(f"Error converting strings to datetime: {e}")
        return datetime_list


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


# extractor = PolygonExtractorFactory.create_ticker_extractor()
# apple_data = extractor.extract('AAPL')
# print(apple_data)
