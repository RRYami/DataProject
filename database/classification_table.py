"""Create the sic code lookup table in the database."""

import os

import duckdb as ddb
import polars as pl
from dotenv import load_dotenv

from logger.logger import get_logger, setup_logging


def drop_tables(db_connection: ddb.DuckDBPyConnection):
    """
    Drop existing classification tables if they exist.
    """
    db_connection.execute("DROP TABLE IF EXISTS sic_to_naics")
    db_connection.execute("DROP TABLE IF EXISTS naics_codes")
    db_connection.execute("DROP TABLE IF EXISTS sic_codes")


class NaicsTableCreator:
    """
    Responsible for creating the NAICS code lookup table.

    Single Responsibility: Database table creation.
    """

    def __init__(self, db_connection: ddb.DuckDBPyConnection):
        self.db_connection = db_connection
        self.logger = get_logger(__name__)

    def create_naics_table(self):
        """
        Create the NAICS code lookup table in the database.
        """
        # Delete table if already exist
        self.db_connection.execute("DROP TABLE IF EXISTS naics_codes")

        self.logger.info("Creating NAICS code lookup table.")
        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS naics_codes (
                naics_code INT PRIMARY KEY,
                description VARCHAR
            )
        """)
        self.logger.info("NAICS code lookup table created successfully.")

    def insert_naics_codes(self, naics_data: pl.DataFrame):
        """
        Insert NAICS codes into the lookup table.

        Args:
            naics_data: Dictionary mapping NAICS codes to descriptions
        """
        self.logger.info(
            f"Inserting {len(naics_data)} NAICS codes into the lookup table."
        )
        try:
            for row in naics_data.iter_rows(named=True):
                self.db_connection.execute(
                    """
                    INSERT INTO naics_codes (naics_code, description)
                    VALUES (?, ?)
                    ON CONFLICT (naics_code) DO NOTHING
                """,
                    (row["2022 NAICS US Code"], row["2022 NAICS US Title"]),
                )
        except Exception as e:
            self.logger.exception("Failed to insert NAICS codes.")
            self.logger.error(f"Error details: {e}")
            raise
        else:
            self.logger.info("NAICS codes inserted successfully.")


class SicTableCreator:
    """
    Responsible for creating the SIC code lookup table.

    Single Responsibility: Database table creation.
    """

    def __init__(self, db_connection: ddb.DuckDBPyConnection):
        self.db_connection = db_connection
        self.logger = get_logger(__name__)

    def create_sic_table(self):
        """
        Create the SIC code lookup table in the database.
        """
        # Delete table if already exist
        self.db_connection.execute("DROP TABLE IF EXISTS sic_codes")

        self.logger.info("Creating SIC code lookup table.")
        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS sic_codes (
                sic_code INT PRIMARY KEY,
                office VARCHAR,
                industry VARCHAR
            )
        """)
        self.logger.info("SIC code lookup table created successfully.")

    def insert_sic_codes(self, sic_data: dict[int, tuple[str, str]]):
        """
        Insert SIC codes into the lookup table.

        Args:
            sic_data: Dictionary mapping SIC codes to descriptions
        """
        self.logger.info(
            f"Inserting {len(sic_data)} SIC codes into the lookup table."
        )
        try:
            for sic_code, (office, industry) in sic_data.items():
                self.db_connection.execute(
                    """
                    INSERT INTO sic_codes (sic_code, office, industry)
                    VALUES (?, ?, ?)
                    ON CONFLICT (sic_code) DO NOTHING
                """,
                    (sic_code, office, industry),
                )
        except Exception as e:
            self.logger.exception("Failed to insert SIC codes.")
            self.logger.error(f"Error details: {e}")
            raise
        else:
            self.logger.info("SIC codes inserted successfully.")


class SicToNaicsCreator:
    """
    Responsible for creating the SIC to NAICS mapping table.
    Single Responsibility: Database table creation.
    """

    def __init__(self, db_connection: ddb.DuckDBPyConnection):
        self.db_connection = db_connection
        self.logger = get_logger(__name__)

    def create_sic_to_naics_table(self):
        """
        Create the SIC to NAICS mapping table in the database.
        """
        # Delete table if already exist
        self.db_connection.execute("DROP TABLE IF EXISTS sic_to_naics")

        self.logger.info("Creating SIC to NAICS mapping table.")
        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS sic_to_naics (
                sic_code INT,
                sic_description VARCHAR,
                naics_code INT,
                naics_description VARCHAR,
                PRIMARY KEY (sic_code, naics_code)
            )
        """)
        self.logger.info("SIC to NAICS mapping table created successfully.")

    def insert_sic_to_naics_mappings(self, mappings: pl.DataFrame):
        """
        Insert SIC to NAICS mappings into the table.

        Args:
            mappings: Polars DataFrame with 'sic_code' and 'naics_code' columns
        """
        self.logger.info(
            f"Inserting {len(mappings)} SIC to NAICS mappings into the table."
        )
        try:
            for row in mappings.iter_rows(named=True):
                self.db_connection.execute(
                    """
                    INSERT INTO sic_to_naics (sic_code, sic_description, naics_code, naics_description)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (sic_code, naics_code) DO NOTHING
                """,
                    (
                        row["SIC Code"],
                        row["SIC_Description"],
                        row["NAICS Code"],
                        row["NAICS_Description"],
                    ),
                )
        except ddb.ConstraintException as ce:
            self.logger.error(
                "Constraint violation while inserting SIC to NAICS mappings."
            )
            self.logger.error(f"Error details: {ce}")
            raise
        except ddb.BinderException as de:
            self.logger.error(
                "Data binding error while inserting SIC to NAICS mappings. Too many Primary Keys in the On Conflict clause?"
            )
            self.logger.error(f"Error details: {de}")
            raise
        except Exception as e:
            self.logger.exception("Failed to insert SIC to NAICS mappings.")
            self.logger.error(f"Error details: {e}")
            raise
        else:
            self.logger.info("SIC to NAICS mappings inserted successfully.")


# Example usage:
if __name__ == "__main__":
    setup_logging()
    load_dotenv("./secret/.env")
    db_connection = ddb.connect(os.getenv("DB_PATH"))  # type: ignore
    drop_tables(db_connection)

    # Sic to Naics mapping table creation
    sic_naics_creator = SicToNaicsCreator(db_connection)
    sic_naics_data = pl.read_parquet("data/sic_naics.parquet")
    sic_naics_creator.create_sic_to_naics_table()
    sic_naics_creator.insert_sic_to_naics_mappings(sic_naics_data)
    result = db_connection.execute("FROM sic_to_naics").pl()
    print(result.head())
