# Load Testing Tool

Generate realistic traffic to your DataProject API to populate Grafana dashboards with metrics.

## Features

- **Multiple Intensity Levels**: Low, Medium, High, and Burst traffic patterns
- **Realistic Endpoint Distribution**: Weighted random selection mimicking real usage
- **Automatic Intensity Cycling**: Changes intensity every 30 seconds
- **Real-time Statistics**: Live terminal dashboard showing metrics
- **Multiple Scenarios**: Standard tests, custom patterns, and burst tests
- **Rich Terminal UI**: Beautiful, colorful output with progress tracking

## Quick Start

### 1. Make sure your API is running

**Option A: Using Docker Compose (with monitoring)**
```bash
# Start the full stack
monitoring-stack.bat start

# The API will be available at http://localhost:8000
```

**Option B: Running locally**
```bash
# Start just the API
uv run uvicorn main:app --reload
```

### 2. Run the load tester

```bash
uv run python load_test.py
```

### 3. Choose a scenario

The tool will present a menu:

```
1 - Standard Load Test (5 minutes, varying intensity)
2 - Quick Load Test (1 minute, varying intensity)
3 - Custom Scenario (morning rush pattern)
4 - Quick Burst (100 concurrent requests)
5 - Long Test (15 minutes)
6 - Exit
```

## Test Scenarios

### 1. Standard Load Test (5 minutes)
- Cycles through 4 intensity levels every 30 seconds
- **Low**: 2-10 requests/second
- **Medium**: 5-20 requests/second  
- **High**: 10-100 requests/second
- **Burst**: 100-1000 requests/second

Perfect for seeing how metrics change over time in Grafana!

### 2. Quick Load Test (1 minute)
- Same as standard but shorter
- Good for quick validation

### 3. Custom Scenario (Morning Rush)
Simulates realistic morning traffic pattern:
- **Phase 1**: Slow start (30s) - Users waking up
- **Phase 2**: Ramp up (30s) - Morning activity
- **Phase 3**: Peak traffic (60s) - Everyone online
- **Phase 4**: Cool down (30s) - Activity normalizes

### 4. Quick Burst
- 100 concurrent requests as fast as possible
- Tests API under sudden load spike
- Great for testing exception handling

### 5. Long Test (15 minutes)
- Extended standard test
- Best for monitoring long-term stability
- Generates comprehensive metrics

## Endpoints Tested

The load tester hits all your API endpoints with realistic distribution:

| Endpoint | Weight | Purpose |
|----------|--------|---------|
| `GET /company/{ticker}` | 30% | Company details lookup |
| `GET /company/{ticker}/priceHistory` | 40% | Price history queries |
| `GET /curves/US_treasury_yield` | 15% | Treasury yield curves |
| `GET /company/list/available_tickers` | 5% | List all tickers |
| `GET /tickers/indices/available` | 5% | List indices |
| `GET /health` | 3% | Health checks |
| `GET /monitoring/stats` | 2% | Monitoring stats |

## Viewing Results

### In Terminal
The tool displays real-time statistics:
- Total requests and success rate
- Requests per second
- Current intensity level
- Breakdown by endpoint
- Breakdown by status code

### In Grafana
While the test runs, open your Grafana dashboard:

```
http://localhost:3000/d/dataproject-api
```

You'll see:
- Request rate graphs spiking and dropping
- Latency percentiles (p50, p95) responding to load
- Database query rates increasing
- Business metrics accumulating
- Exception counts (if any errors occur)

## Sample Tickers Used

The load tester uses these realistic ticker symbols:
- AAPL (Apple)
- MSFT (Microsoft)
- GOOGL (Google)
- AMZN (Amazon)
- META (Meta/Facebook)
- TSLA (Tesla)
- NVDA (NVIDIA)
- AMD (AMD)

## Traffic Intensity Details

### Low Intensity
- Delay: 0.1 - 0.5 seconds between requests
- ~2-10 requests/second
- Simulates: Light usage

### Medium Intensity  
- Delay: 0.05 - 0.2 seconds between requests
- ~5-20 requests/second
- Simulates: Normal business hours

### High Intensity
- Delay: 0.01 - 0.1 seconds between requests
- ~10-100 requests/second
- Simulates: Peak traffic

### Burst Intensity
- Delay: 0.001 - 0.01 seconds between requests
- ~100-1000 requests/second
- Simulates: Traffic spike or attack

## Tips for Best Results

1. **Start with Quick Test** - Verify everything works (1 minute)
2. **Run Standard Test** - Get varied metrics (5 minutes)
3. **Watch Grafana Live** - Open dashboard before starting test
4. **Try Custom Scenario** - See realistic traffic patterns
5. **Test Burst** - Verify API handles spikes

## Stopping a Test

Press `Ctrl+C` to stop any test early. The tool will display final statistics.

## Example Output

```
═══════════════════════════════════════════════
  DataProject API Load Testing Tool
═══════════════════════════════════════════════

Starting Load Test
Duration: 300 seconds (5.0 minutes)
Target API: http://localhost:8000
Intensity Pattern: Cycles through low → medium → high → burst every 30s

┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Metric                 ┃ Value           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ Total Requests         │ 1547            │
│ Successful             │ 1523 (98.4%)    │
│ Failed                 │ 24 (1.6%)       │
│ Elapsed Time           │ 45.3s           │
│ Requests/sec           │ 34.15           │
│                        │                 │
│ Current Intensity      │ HIGH            │
│                        │                 │
│ By Endpoint            │                 │
│   price_history        │ 618 (40.0%)     │
│   company              │ 464 (30.0%)     │
│   treasury             │ 232 (15.0%)     │
│   tickers              │ 77 (5.0%)       │
│   indices              │ 77 (5.0%)       │
│   health               │ 46 (3.0%)       │
│   monitoring           │ 33 (2.0%)       │
```

## Troubleshooting

### "Connection refused" errors
- Make sure the API is running
- Check if it's on `http://localhost:8000`
- Try `curl http://localhost:8000/health`

### High failure rate
- Your database might not have data for the test tickers
- Load some data first using the ELT pipeline
- Or modify `TICKERS` list in `load_test.py`

### Not seeing metrics in Grafana
- Wait 10-15 seconds for Prometheus to scrape
- Refresh the Grafana dashboard
- Check time range is set to "Last 5 minutes"

## Customization

Edit `load_test.py` to customize:

```python
# Change test duration
asyncio.run(run_load_test(600))  # 10 minutes

# Change API URL
API_BASE_URL = "http://your-server:8000"

# Add your own tickers
TICKERS = ["AAPL", "MSFT", "YOUR_TICKER"]

# Adjust intensity levels
INTENSITY_LEVELS = {
    "low": (0.2, 1.0),     # Even slower
    "medium": (0.1, 0.3),  # Customized
}
```

## Advanced Usage

### Run specific test programmatically

```python
import asyncio
from load_test import run_load_test, quick_burst, run_custom_scenario

# Run 10-minute test
asyncio.run(run_load_test(600))

# Run burst test
asyncio.run(quick_burst())

# Run custom scenario
asyncio.run(run_custom_scenario())
```

### Continuous testing

```bash
# Run tests in a loop (Linux/macOS)
while true; do
    uv run python load_test.py << EOF
2
y
EOF
    sleep 60
done
```

## Integration with CI/CD

Use the load tester in your CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run load test
  run: |
    uv run python load_test.py << EOF
    4
    6
    EOF
```

---

**Happy Load Testing!** 🚀

Watch your Grafana dashboards come alive with realistic metrics!
