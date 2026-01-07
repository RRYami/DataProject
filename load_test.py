"""
Load Testing Script for DataProject API

This script generates random API calls with varying intensity to populate
Grafana dashboards with realistic metrics data.

Features:
- Random intensity (bursts and quiet periods)
- Multiple endpoint types
- Realistic traffic patterns
- Error injection for testing exception metrics
- Configurable duration and intensity levels
"""

import asyncio
import random
import time
from typing import Dict

import httpx
from rich.console import Console
from rich.live import Live
from rich.table import Table

console = Console()

# API Configuration
API_BASE_URL = "http://localhost:8000"

# Sample data for realistic requests
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD"]
START_DATES = ["2024-01-01", "2024-06-01", "2023-01-01", "2024-03-01"]
END_DATES = ["2024-12-31", "2024-11-30", "2024-06-30"]

# Traffic patterns
INTENSITY_LEVELS = {
    "low": (0.1, 0.5),  # 0.1-0.5 seconds between requests
    "medium": (0.05, 0.2),  # 0.05-0.2 seconds
    "high": (0.01, 0.1),  # 0.01-0.1 seconds (10-100 req/s)
    "burst": (0.001, 0.01),  # 0.001-0.01 seconds (100-1000 req/s)
}

# Statistics tracking
stats = {
    "total_requests": 0,
    "successful": 0,
    "failed": 0,
    "by_endpoint": {},
    "by_status": {},
    "start_time": None,
}


class EndpointGenerator:
    """Generate random API endpoint requests."""

    @staticmethod
    def get_company(ticker: str | None = None) -> str:
        """Generate company details endpoint."""
        ticker = ticker or random.choice(TICKERS)
        return f"/company/{ticker}"

    @staticmethod
    def get_price_history(ticker: str | None = None) -> str:
        """Generate price history endpoint with random date range."""
        ticker = ticker or random.choice(TICKERS)

        # 50% chance to include date filters
        if random.random() < 0.5:
            start = random.choice(START_DATES)
            end = random.choice(END_DATES)
            return f"/company/{ticker}/priceHistory?start_date={start}&end_date={end}"
        else:
            return f"/company/{ticker}/priceHistory"

    @staticmethod
    def get_treasury_curve() -> str:
        """Generate treasury yield curve endpoint."""
        choice = random.random()

        if choice < 0.3:
            # Latest only
            return "/curves/US_treasury_yield?latest_only=true"
        elif choice < 0.6:
            # Date range
            start = random.choice(START_DATES)
            return f"/curves/US_treasury_yield?start_date={start}"
        elif choice < 0.8:
            # With pagination
            limit = random.choice([10, 50, 100])
            return f"/curves/US_treasury_yield?limit={limit}"
        else:
            # All data
            return "/curves/US_treasury_yield"

    @staticmethod
    def get_available_tickers() -> str:
        """Generate available tickers endpoint."""
        return "/company/list/available_tickers"

    @staticmethod
    def get_indices() -> str:
        """Generate indices endpoint."""
        return "/tickers/indices/available"

    @staticmethod
    def get_health() -> str:
        """Generate health check endpoint."""
        return "/health"

    @staticmethod
    def get_monitoring_stats() -> str:
        """Generate monitoring stats endpoint."""
        return "/monitoring/stats"

    @staticmethod
    def get_random_endpoint() -> tuple[str, str]:
        """Generate a weighted random endpoint."""
        # Weighted endpoint selection (more realistic traffic)
        choices = [
            (EndpointGenerator.get_company, "company", 30),
            (EndpointGenerator.get_price_history, "price_history", 40),
            (EndpointGenerator.get_treasury_curve, "treasury", 15),
            (EndpointGenerator.get_available_tickers, "tickers", 5),
            (EndpointGenerator.get_indices, "indices", 5),
            (EndpointGenerator.get_health, "health", 3),
            (EndpointGenerator.get_monitoring_stats, "monitoring", 2),
        ]

        # Create weighted list
        weighted = []
        for func, name, weight in choices:
            weighted.extend([(func, name)] * weight)

        func, name = random.choice(weighted)
        return func(), name


async def make_request(
    client: httpx.AsyncClient, endpoint: str, endpoint_name: str
) -> Dict:
    """Make a single API request and track statistics."""
    url = f"{API_BASE_URL}{endpoint}"

    try:
        response = await client.get(url, timeout=10.0)

        # Update statistics
        stats["total_requests"] += 1
        stats["by_endpoint"][endpoint_name] = (
            stats["by_endpoint"].get(endpoint_name, 0) + 1
        )
        stats["by_status"][response.status_code] = (
            stats["by_status"].get(response.status_code, 0) + 1
        )

        if response.status_code < 400:
            stats["successful"] += 1
            return {
                "success": True,
                "status": response.status_code,
                "endpoint": endpoint_name,
            }
        else:
            stats["failed"] += 1
            return {
                "success": False,
                "status": response.status_code,
                "endpoint": endpoint_name,
            }

    except Exception as e:
        stats["total_requests"] += 1
        stats["failed"] += 1
        return {"success": False, "error": str(e), "endpoint": endpoint_name}


def get_current_intensity() -> str:
    """Get current intensity level based on time-based pattern."""
    # Create a wave pattern that changes over time
    elapsed = time.time() - stats["start_time"]

    # Change intensity every 30 seconds
    cycle = int(elapsed / 30) % 4

    if cycle == 0:
        return "medium"
    elif cycle == 1:
        return "high"
    elif cycle == 2:
        return "burst"
    else:
        return "low"


def generate_statistics_table() -> Table:
    """Generate a rich table with current statistics."""
    table = Table(title="Load Testing Statistics", title_style="bold magenta")

    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")

    # Calculate elapsed time
    elapsed = time.time() - stats["start_time"]

    # Calculate requests per second
    rps = stats["total_requests"] / elapsed if elapsed > 0 else 0

    # Overall stats
    table.add_row("Total Requests", str(stats["total_requests"]))
    table.add_row(
        "Successful",
        f"{stats['successful']} ({stats['successful'] / max(stats['total_requests'], 1) * 100:.1f}%)",
    )
    table.add_row(
        "Failed",
        f"{stats['failed']} ({stats['failed'] / max(stats['total_requests'], 1) * 100:.1f}%)",
    )
    table.add_row("Elapsed Time", f"{elapsed:.1f}s")
    table.add_row("Requests/sec", f"{rps:.2f}")
    table.add_row("", "")  # Separator

    # Current intensity
    current_intensity = get_current_intensity()
    table.add_row("Current Intensity", current_intensity.upper())
    table.add_row("", "")  # Separator

    # Requests by endpoint
    table.add_row("[bold]By Endpoint[/bold]", "")
    for endpoint, count in sorted(
        stats["by_endpoint"].items(), key=lambda x: x[1], reverse=True
    ):
        percentage = (count / stats["total_requests"]) * 100
        table.add_row(f"  {endpoint}", f"{count} ({percentage:.1f}%)")

    table.add_row("", "")  # Separator

    # Requests by status code
    table.add_row("[bold]By Status Code[/bold]", "")
    for status, count in sorted(stats["by_status"].items()):
        percentage = (count / stats["total_requests"]) * 100
        table.add_row(f"  {status}", f"{count} ({percentage:.1f}%)")

    return table


async def run_load_test(duration_seconds: int = 300):
    """
    Run load test with varying intensity.

    Args:
        duration_seconds: How long to run the test (default 5 minutes)
    """
    console.print(f"\n[bold green]Starting Load Test[/bold green]")
    console.print(
        f"Duration: {duration_seconds} seconds ({duration_seconds / 60:.1f} minutes)"
    )
    console.print(f"Target API: {API_BASE_URL}")
    console.print(
        f"Intensity Pattern: Cycles through low → medium → high → burst every 30s\n"
    )

    stats["start_time"] = time.time()
    end_time = stats["start_time"] + duration_seconds

    async with httpx.AsyncClient() as client:
        with Live(generate_statistics_table(), refresh_per_second=4) as live:
            while time.time() < end_time:
                # Get current intensity
                intensity = get_current_intensity()
                min_delay, max_delay = INTENSITY_LEVELS[intensity]

                # Generate random endpoint
                endpoint, endpoint_name = (
                    EndpointGenerator.get_random_endpoint()
                )

                # Make request
                await make_request(client, endpoint, endpoint_name)

                # Random delay based on intensity
                delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(delay)

                # Update display
                live.update(generate_statistics_table())

    console.print("\n[bold green]✓ Load Test Completed![/bold green]\n")
    console.print(generate_statistics_table())


async def run_custom_scenario():
    """Run a custom scenario with specific traffic pattern."""
    console.print("\n[bold yellow]Custom Scenario: Morning Rush[/bold yellow]")
    console.print("Simulating morning traffic with gradual increase...\n")

    stats["start_time"] = time.time()

    async with httpx.AsyncClient() as client:
        # Phase 1: Slow start (30 seconds)
        console.print("[cyan]Phase 1: Slow Start (30s)[/cyan]")
        for _ in range(50):
            endpoint, name = EndpointGenerator.get_random_endpoint()
            await make_request(client, endpoint, name)
            await asyncio.sleep(random.uniform(0.5, 1.0))

        # Phase 2: Ramp up (30 seconds)
        console.print("[cyan]Phase 2: Ramping Up (30s)[/cyan]")
        for _ in range(200):
            endpoint, name = EndpointGenerator.get_random_endpoint()
            await make_request(client, endpoint, name)
            await asyncio.sleep(random.uniform(0.1, 0.2))

        # Phase 3: Peak traffic (60 seconds)
        console.print("[cyan]Phase 3: Peak Traffic (60s)[/cyan]")
        for _ in range(1000):
            endpoint, name = EndpointGenerator.get_random_endpoint()
            await make_request(client, endpoint, name)
            await asyncio.sleep(random.uniform(0.01, 0.05))

        # Phase 4: Cool down (30 seconds)
        console.print("[cyan]Phase 4: Cool Down (30s)[/cyan]")
        for _ in range(100):
            endpoint, name = EndpointGenerator.get_random_endpoint()
            await make_request(client, endpoint, name)
            await asyncio.sleep(random.uniform(0.2, 0.5))

    console.print("\n[bold green]✓ Custom Scenario Completed![/bold green]\n")
    console.print(generate_statistics_table())


async def quick_burst():
    """Quick burst test - 100 requests as fast as possible."""
    console.print("\n[bold red]Quick Burst: 100 Requests[/bold red]\n")

    stats["start_time"] = time.time()

    async with httpx.AsyncClient() as client:
        tasks = []
        for _ in range(100):
            endpoint, name = EndpointGenerator.get_random_endpoint()
            tasks.append(make_request(client, endpoint, name))

        await asyncio.gather(*tasks)

    console.print("\n[bold green]✓ Burst Completed![/bold green]\n")
    console.print(generate_statistics_table())


def main():
    """Main entry point with menu."""
    console.print(
        "\n[bold magenta]═══════════════════════════════════════════════[/bold magenta]"
    )
    console.print(
        "[bold magenta]  DataProject API Load Testing Tool[/bold magenta]"
    )
    console.print(
        "[bold magenta]═══════════════════════════════════════════════[/bold magenta]\n"
    )

    console.print("Choose a test scenario:\n")
    console.print(
        "  [bold cyan]1[/bold cyan] - Standard Load Test (5 minutes, varying intensity)"
    )
    console.print(
        "  [bold cyan]2[/bold cyan] - Quick Load Test (1 minute, varying intensity)"
    )
    console.print(
        "  [bold cyan]3[/bold cyan] - Custom Scenario (morning rush pattern)"
    )
    console.print(
        "  [bold cyan]4[/bold cyan] - Quick Burst (100 concurrent requests)"
    )
    console.print("  [bold cyan]5[/bold cyan] - Long Test (15 minutes)")
    console.print("  [bold cyan]6[/bold cyan] - Exit\n")

    choice = console.input("[bold yellow]Enter choice (1-6): [/bold yellow]")

    if choice == "1":
        asyncio.run(run_load_test(300))  # 5 minutes
    elif choice == "2":
        asyncio.run(run_load_test(60))  # 1 minute
    elif choice == "3":
        asyncio.run(run_custom_scenario())
    elif choice == "4":
        asyncio.run(quick_burst())
    elif choice == "5":
        asyncio.run(run_load_test(900))  # 15 minutes
    elif choice == "6":
        console.print("\n[bold green]Goodbye![/bold green]\n")
        return
    else:
        console.print("\n[bold red]Invalid choice![/bold red]\n")
        return

    # Ask if user wants to run another test
    console.print("\n")
    again = console.input(
        "[bold yellow]Run another test? (y/n): [/bold yellow]"
    )
    if again.lower() == "y":
        main()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n\n[bold red]Test interrupted by user[/bold red]")
        if stats["total_requests"] > 0:
            console.print(generate_statistics_table())
