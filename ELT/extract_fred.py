from typing import Optional, Union

import polars as pl
import requests

from get_api_keys import get_api_key
from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredExtractor:
    """
    A class to extract data from the FRED (Federal Reserve Economic Data) API.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = get_api_key("FRED_KEY")

    def get_series_observations(
        self,
        series_id: Union[list, str],
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        frequency: Optional[str] = "d",
        file_type: Optional[str] = "json",
        limit: Optional[int] = 100000,
    ) -> Union[dict, pl.DataFrame]:
        """
        Fetch series observations from FRED API.

        Args:
            series_id: The ID of the FRED series to fetch.
            observation_start: The start date for observations (YYYY-MM-DD).
            observation_end: The end date for observations (YYYY-MM-DD).
            frequency: Frequency of the data ('d' for daily, 'm' for monthly, etc.).
            file_type: The format of the response ('json' or 'xml').

        Returns:
            A dictionary containing the series observations.
        """
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": file_type,
            "frequency": frequency,
            "limit": limit,
        }

        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end

        try:
            if isinstance(series_id, list):
                all_data = {}
                for sid in series_id:
                    params["series_id"] = sid
                    response = requests.get(BASE_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                    all_data[sid] = data
                    data_dfs = {}
                    for series, response in all_data.items():
                        observations = response.get("observations", [])
                        if observations:
                            df = pl.DataFrame(observations).select(
                                [
                                    pl.col("date").str.to_date(
                                        "%Y-%m-%d"
                                    ),  # Convert to proper Date type
                                    pl.col("value")
                                    .replace(
                                        ".", None
                                    )  # "." means no data → null
                                    .cast(pl.Float64)
                                    .alias(series),  # Rename to series ID
                                ]
                            )
                            data_dfs[series] = df

                    # Long format
                    long_list = []
                    for series in data_dfs:
                        long_list.append(
                            data_dfs[series]
                            .rename({series: "yield"})
                            .with_columns(pl.lit(series).alias("maturity"))
                        )

                    long = pl.concat(long_list)

                    # Pivot to wide
                    combined = long.pivot(
                        index="date",
                        on="maturity",
                        values="yield",
                        aggregate_function=None,  # Or "first" if duplicates possible
                    ).sort("date")
                return combined
            else:
                response = requests.get(BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                return data
        except requests.RequestException as e:
            logger.error(f"Error fetching data from FRED API: {e}")
            raise
