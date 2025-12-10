"""Check and fix duplicates in company_details table."""

import os

import duckdb as ddb
from dotenv import load_dotenv

load_dotenv("./secret/.env")


def check_duplicates():
    """Check for duplicate tickers in company_details table."""
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH not found in environment variables")

    conn = ddb.connect(db_path)

    # Check for duplicates
    result = conn.execute("""
        SELECT ticker, COUNT(*) as count
        FROM company_details
        GROUP BY ticker
        HAVING COUNT(*) > 1
    """).pl()

    if len(result) > 0:
        print(f"Found {len(result)} duplicate tickers:")
        print(result)
        return True
    else:
        print("No duplicates found in company_details table.")
        return False


def fix_duplicates():
    """Remove duplicates keeping the most recent record."""
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH not found in environment variables")

    conn = ddb.connect(db_path)

    # This will recreate the table with only unique tickers
    # keeping the first occurrence of each ticker
    conn.execute("""
        CREATE TABLE company_details_temp AS
        SELECT DISTINCT ON (ticker) *
        FROM company_details
        ORDER BY ticker
    """)

    conn.execute("DROP TABLE company_details")
    conn.execute("ALTER TABLE company_details_temp RENAME TO company_details")

    print("Duplicates removed. Table cleaned up.")


if __name__ == "__main__":
    # display the table
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH not found in environment variables")

    conn = ddb.connect(db_path)
    data = conn.execute("SELECT * FROM company_details").pl()
    data.write_csv("data/company_details_before_cleanup.csv")

    if check_duplicates():
        response = input("Do you want to fix duplicates? (yes/no): ")
        if response.lower() == "yes":
            fix_duplicates()
