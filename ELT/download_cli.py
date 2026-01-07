"""Command-line interface for downloading financial data into DataProject database."""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb as ddb
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ELT.extract_fred import FredExtractor
from ELT.extract_polygon import PolygonExtractorFactory
from ELT.load_fred import YieldLoader
from ELT.load_polygon import PolygonDataLoader
from logger.logger import get_logger

# Setup
load_dotenv("./secret/.env")
console = Console()
logger = get_logger(__name__)


def main():
    """Main entry point for the download CLI."""
    parser = create_argument_parser()

    # Show help if no arguments
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # Route to appropriate handler
    try:
        if args.command == "price":
            download_price_data(args)
        elif args.command == "company":
            download_company_data(args)
        elif args.command == "treasury":
            download_treasury_data(args)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Download failed: {e}")
        sys.exit(1)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="download",
        description="Download financial data into DataProject database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Price data:
    download price AAPL 2025-01-01 2025-01-07
    download price AAPL,MSFT,GOOGL
    download price AAPL

  Company details:
    download company AAPL
    download company AAPL,MSFT,GOOGL
    download company --all

  Treasury yields (FRED):
    download treasury 2024-01-01 2024-12-31
    download treasury
        """,
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute"
    )

    # Price command
    price_parser = subparsers.add_parser(
        "price", help="Download price data (OHLCV)"
    )
    price_parser.add_argument(
        "tickers",
        help="Ticker symbol(s) - comma-separated, no spaces (e.g., AAPL,MSFT)",
    )
    price_parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date in YYYY-MM-DD format (default: 30 days ago)",
    )
    price_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date in YYYY-MM-DD format (default: today)",
    )

    # Company command
    company_parser = subparsers.add_parser(
        "company", help="Download company/ticker details"
    )
    company_parser.add_argument(
        "tickers",
        help="Ticker symbol(s) - comma-separated, or --all for all tickers in database",
    )

    # Treasury command
    treasury_parser = subparsers.add_parser(
        "treasury", help="Download treasury yields from FRED"
    )
    treasury_parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date in YYYY-MM-DD format (default: 90 days ago)",
    )
    treasury_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date in YYYY-MM-DD format (default: today)",
    )

    return parser


def download_price_data(args):
    """Download price data for specified tickers."""
    console.print(Panel.fit("[bold cyan]Downloading Price Data[/bold cyan]"))

    # Parse tickers
    tickers = parse_tickers(args.tickers)
    if not tickers:
        console.print("[red]Error: No valid tickers provided[/red]")
        sys.exit(1)

    # Apply date defaults and validate
    start_date = args.start_date or get_default_start_date(days=30)
    end_date = args.end_date or get_default_end_date()

    if not validate_date_format(start_date) or not validate_date_format(
        end_date
    ):
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        sys.exit(1)

    if not validate_date_range(start_date, end_date):
        console.print("[red]Error: Start date must be before end date[/red]")
        sys.exit(1)

    # Check for 2-year limit (Polygon free tier)
    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)
    days_diff = (end_dt - start_dt).days

    if days_diff > 730:  # ~2 years
        console.print(
            "[yellow]Warning: Date range exceeds 2 years. Polygon free tier limits apply.[/yellow]"
        )

    # Display parameters
    console.print(f"[bold]Tickers:[/bold] {', '.join(tickers)}")
    console.print(f"[bold]Date Range:[/bold] {start_date} to {end_date}")
    console.print()

    # Create extractors and loaders
    try:
        price_extractor = PolygonExtractorFactory.create_price_extractor()
        loader = PolygonDataLoader()
    except Exception as e:
        console.print(f"[red]Error initializing extractors: {e}[/red]")
        sys.exit(1)

    # Download data
    success_count = 0
    failed_tickers = []
    total_records = 0

    console.print("[bold]Processing...[/bold]")

    for ticker in tickers:
        try:
            console.print(f"  [cyan]→[/cyan] Fetching {ticker}...", end=" ")

            # Extract price data (rate limiting handled by extractor)
            data = price_extractor.extract_range(
                ticker,
                start_date,
                end_date,
                checkpoint_file=f"data/.checkpoint_{ticker}.json",
            )

            if data and ticker in data:
                record_count = len(data[ticker])
                loader.load_price_data(data)

                console.print(f"[green]✓[/green] {record_count} days extracted")
                success_count += 1
                total_records += record_count
            else:
                console.print("[yellow]✗[/yellow] No data returned")
                failed_tickers.append(ticker)

        except Exception as e:
            console.print(f"[red]✗[/red] {str(e)[:50]}")
            logger.warning(f"Failed to download {ticker}: {e}")
            failed_tickers.append(ticker)
            continue

    # Display summary
    console.print()
    summary_table = Table(title="Summary", show_header=False)
    summary_table.add_row("[bold]Total Tickers[/bold]", str(len(tickers)))
    summary_table.add_row(
        "[bold green]Successful[/bold green]", str(success_count)
    )
    summary_table.add_row(
        "[bold red]Failed[/bold red]", str(len(failed_tickers))
    )
    summary_table.add_row("[bold]Total Records[/bold]", str(total_records))
    console.print(summary_table)

    if failed_tickers:
        console.print(
            f"[yellow]Failed tickers: {', '.join(failed_tickers)}[/yellow]"
        )

    # Verify data
    if success_count > 0:
        verify_price_data(
            [t for t in tickers if t not in failed_tickers],
            start_date,
            end_date,
        )


def download_company_data(args):
    """Download company details for specified tickers."""
    console.print(
        Panel.fit("[bold cyan]Downloading Company Details[/bold cyan]")
    )

    # Parse tickers
    if args.tickers == "--all":
        # Load all tickers from database
        tickers = get_tickers_from_database()
        if not tickers:
            console.print(
                "[yellow]No tickers found in database. Run 'download tickers' first.[/yellow]"
            )
            sys.exit(0)
        console.print(
            f"[bold]Loading details for {len(tickers)} tickers from database[/bold]"
        )
    else:
        tickers = parse_tickers(args.tickers)
        if not tickers:
            console.print("[red]Error: No valid tickers provided[/red]")
            sys.exit(1)
        console.print(f"[bold]Tickers:[/bold] {', '.join(tickers)}")

    console.print()

    # Create extractor and loader
    try:
        batch_extractor = PolygonExtractorFactory.create_batch_extractor()
        loader = PolygonDataLoader()
    except Exception as e:
        console.print(f"[red]Error initializing extractors: {e}[/red]")
        sys.exit(1)

    # Download data
    success_count = 0
    failed_tickers = []

    console.print("[bold]Processing...[/bold]")

    # Use batch extractor (handles rate limiting internally)
    try:
        batch_data = batch_extractor.extract(tickers)

        # Display results as we go
        for ticker in tickers:
            if ticker in batch_data:
                details = batch_data[ticker]
                name = details.get("name", "Unknown")
                console.print(f"  [green]✓[/green] {ticker} - {name}")
                success_count += 1
            else:
                console.print(f"  [red]✗[/red] {ticker} - Not found")
                failed_tickers.append(ticker)

        # Load into database
        if batch_data:
            loader.load_batch_ticker_details(
                list(batch_data.keys()), batch_extractor
            )

    except Exception as e:
        console.print(f"[red]Error during batch extraction: {e}[/red]")
        logger.error(f"Batch extraction failed: {e}")
        sys.exit(1)

    # Display summary
    console.print()
    summary_table = Table(title="Summary", show_header=False)
    summary_table.add_row("[bold]Total Tickers[/bold]", str(len(tickers)))
    summary_table.add_row(
        "[bold green]Successful[/bold green]", str(success_count)
    )
    summary_table.add_row(
        "[bold red]Failed[/bold red]", str(len(failed_tickers))
    )
    console.print(summary_table)

    if failed_tickers:
        console.print(
            f"[yellow]Failed tickers: {', '.join(failed_tickers)}[/yellow]"
        )

    # Verify data
    if success_count > 0:
        verify_company_data([t for t in tickers if t not in failed_tickers])


def download_treasury_data(args):
    """Download treasury yields from FRED."""
    console.print(
        Panel.fit("[bold cyan]Downloading Treasury Yields (FRED)[/bold cyan]")
    )

    # Apply date defaults and validate
    start_date = args.start_date or get_default_start_date(days=90)
    end_date = args.end_date or get_default_end_date()

    if not validate_date_format(start_date) or not validate_date_format(
        end_date
    ):
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        sys.exit(1)

    if not validate_date_range(start_date, end_date):
        console.print("[red]Error: Start date must be before end date[/red]")
        sys.exit(1)

    # Display parameters
    maturities = [
        "DGS1MO",
        "DGS3MO",
        "DGS6MO",
        "DGS1",
        "DGS2",
        "DGS5",
        "DGS10",
        "DGS30",
    ]
    console.print(f"[bold]Date Range:[/bold] {start_date} to {end_date}")
    console.print(f"[bold]Series:[/bold] {', '.join(maturities)}")
    console.print()

    # Create extractor and loader
    try:
        extractor = FredExtractor()
        loader = YieldLoader()
    except Exception as e:
        console.print(f"[red]Error initializing extractors: {e}[/red]")
        sys.exit(1)

    # Download data
    console.print("[bold]Fetching data...[/bold]")

    try:
        raw_data = extractor.get_series_observations(
            series_id=maturities,
            observation_start=start_date,
            observation_end=end_date,
        )

        # Check if data is a DataFrame (expected for list of series)
        import polars as pl

        if (
            raw_data is not None
            and isinstance(raw_data, pl.DataFrame)
            and len(raw_data) > 0
        ):
            console.print(
                f"  [green]✓[/green] {len(maturities)} maturities fetched"
            )
            console.print(f"  [green]✓[/green] {len(raw_data)} business days")

            # Load into database
            loader.load_yield_data(raw_data)

            # Display summary
            console.print()
            summary_table = Table(title="Summary", show_header=False)
            summary_table.add_row(
                "[bold]Records Loaded[/bold]", str(len(raw_data))
            )
            summary_table.add_row(
                "[bold]Date Range[/bold]", f"{start_date} to {end_date}"
            )
            console.print(summary_table)

            # Verify data
            verify_treasury_data(start_date, end_date)

        else:
            console.print("[yellow]No data returned from FRED API[/yellow]")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Treasury data download failed: {e}")
        sys.exit(1)


# Helper functions


def parse_tickers(tickers_str: str) -> list[str]:
    """Parse comma-separated ticker string into list."""
    if not tickers_str or tickers_str == "--all":
        return []

    tickers = [t.strip().upper() for t in tickers_str.split(",")]
    return [t for t in tickers if t]  # Remove empty strings


def get_tickers_from_database() -> list[str]:
    """Get all tickers from the tickers table."""
    try:
        db_path = os.getenv("DB_PATH")
        if not db_path:
            raise ValueError("DB_PATH not found in environment variables")

        db_connection = ddb.connect(db_path)
        result = db_connection.execute(
            "SELECT ticker FROM tickers WHERE active = true ORDER BY ticker"
        ).fetchall()
        return [row[0] for row in result]
    except Exception as e:
        logger.warning(f"Failed to load tickers from database: {e}")
        return []


def get_default_start_date(days: int = 30) -> str:
    """Get default start date (N days ago)."""
    return (date.today() - timedelta(days=days)).isoformat()


def get_default_end_date() -> str:
    """Get default end date (today)."""
    return date.today().isoformat()


def validate_date_format(date_str: str) -> bool:
    """Validate date string is in YYYY-MM-DD format."""
    try:
        date.fromisoformat(date_str)
        return True
    except ValueError:
        return False


def validate_date_range(start_date: str, end_date: str) -> bool:
    """Validate start date is before end date and not in future."""
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        today = date.today()

        if start > end:
            return False
        if end > today:
            console.print(
                "[yellow]Warning: End date is in the future. Using today instead.[/yellow]"
            )
            return True
        return True
    except ValueError:
        return False


def verify_price_data(tickers: list[str], start_date: str, end_date: str):
    """Verify price data was loaded successfully."""
    try:
        db_path = os.getenv("DB_PATH")
        if not db_path:
            return

        db_connection = ddb.connect(db_path)

        # Build ticker list for SQL
        ticker_list = ", ".join([f"'{t}'" for t in tickers])

        result = db_connection.execute(
            f"""
            SELECT ticker, COUNT(*) as days, MIN(date) as from_date, MAX(date) as to_date
            FROM price_data
            WHERE ticker IN ({ticker_list})
              AND date >= '{start_date}'
              AND date <= '{end_date}'
            GROUP BY ticker
            ORDER BY ticker
        """
        ).fetchall()

        if result:
            console.print()
            verify_table = Table(
                title="Database Verification", show_header=True
            )
            verify_table.add_column("Ticker", style="cyan")
            verify_table.add_column("Days", justify="right")
            verify_table.add_column("From", style="dim")
            verify_table.add_column("To", style="dim")

            for row in result:
                verify_table.add_row(
                    row[0], str(row[1]), str(row[2]), str(row[3])
                )

            console.print(verify_table)

    except Exception as e:
        logger.warning(f"Failed to verify price data: {e}")


def verify_company_data(tickers: list[str]):
    """Verify company data was loaded successfully."""
    try:
        db_path = os.getenv("DB_PATH")
        if not db_path:
            return

        db_connection = ddb.connect(db_path)

        # Build ticker list for SQL
        ticker_list = ", ".join([f"'{t}'" for t in tickers])

        result = db_connection.execute(
            f"""
            SELECT ticker, name, market_cap, primary_exchange
            FROM company_details
            WHERE ticker IN ({ticker_list})
            ORDER BY ticker
        """
        ).fetchall()

        if result:
            console.print()
            verify_table = Table(
                title="Database Verification", show_header=True
            )
            verify_table.add_column("Ticker", style="cyan")
            verify_table.add_column("Name", style="green")
            verify_table.add_column("Market Cap", justify="right")
            verify_table.add_column("Exchange", style="dim")

            for row in result:
                market_cap = f"${row[2]:,.0f}" if row[2] else "N/A"
                verify_table.add_row(
                    row[0], row[1] or "N/A", market_cap, row[3] or "N/A"
                )

            console.print(verify_table)

    except Exception as e:
        logger.warning(f"Failed to verify company data: {e}")


def verify_treasury_data(start_date: str, end_date: str):
    """Verify treasury data was loaded successfully."""
    try:
        db_path = os.getenv("DB_PATH")
        if not db_path:
            return

        db_connection = ddb.connect(db_path)

        result = db_connection.execute(
            f"""
            SELECT COUNT(*) as records, MIN(date) as from_date, MAX(date) as to_date
            FROM treasury_curves
            WHERE date >= '{start_date}' AND date <= '{end_date}'
        """
        ).fetchone()

        if result and result[0] > 0:
            console.print()
            verify_table = Table(
                title="Database Verification", show_header=False
            )
            verify_table.add_row("[bold]Records in DB[/bold]", str(result[0]))
            verify_table.add_row(
                "[bold]Date Range[/bold]", f"{result[1]} to {result[2]}"
            )
            console.print(verify_table)

    except Exception as e:
        logger.warning(f"Failed to verify treasury data: {e}")


if __name__ == "__main__":
    main()
