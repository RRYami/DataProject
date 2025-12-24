import os
import time

import duckdb as ddb
import polars as pl

from logger.logger import get_logger


class YieldLoader:
    def __init__(self):
        db_path = os.getenv("DB_PATH")
        if db_path is not None:
            self.db_connection = ddb.connect(db_path)
        else:
            self.logger.error("DB_PATH not found in environment variables")
            raise ValueError("DB_PATH not found in environment variables")
        self.logger = get_logger(__name__)

    def load_yield_data(
        self, yield_df: pl.DataFrame, table_name: str = "treasury_curves"
    ):
        """
        Load treasury yield curve data into DuckDB in wide format.
        Args:
            yield_df: Polars DataFrame with columns [date, DGS1MO, DGS3MO, DGS6MO, ..., DGS30]
            table_name: Name of the table to store the data
        """
        self.logger.info(f"Starting yield data load into {table_name}")

        # Create table with wide format
        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS treasury_curves (
                date DATE PRIMARY KEY,
                DGS1MO FLOAT,
                DGS3MO FLOAT,
                DGS6MO FLOAT,
                DGS1 FLOAT,
                DGS2 FLOAT,
                DGS5 FLOAT,
                DGS10 FLOAT,
                DGS30 FLOAT
            )
        """)

        start_time = time.time()

        try:
            # Register Polars DataFrame with DuckDB
            self.db_connection.register("yield_df", yield_df)

            # Insert data, updating on conflict
            self.db_connection.execute("""
                INSERT INTO treasury_curves (date, DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS10, DGS30)
                SELECT date, DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS10, DGS30
                FROM yield_df
                ON CONFLICT (date) DO UPDATE SET
                    DGS1MO = EXCLUDED.DGS1MO,
                    DGS3MO = EXCLUDED.DGS3MO,
                    DGS6MO = EXCLUDED.DGS6MO,
                    DGS1 = EXCLUDED.DGS1,
                    DGS2 = EXCLUDED.DGS2,
                    DGS5 = EXCLUDED.DGS5,
                    DGS10 = EXCLUDED.DGS10,
                    DGS30 = EXCLUDED.DGS30
            """)

            elapsed = time.time() - start_time
            self.logger.info(
                f"Yield data load complete: {yield_df.height} records loaded in {elapsed:.2f}s"
            )
        except Exception as e:
            self.logger.error(f"Failed to load yield data: {e}")
            raise
        finally:
            self.db_connection.unregister("yield_df")
