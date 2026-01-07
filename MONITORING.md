# Monitoring Stack Quick Start Guide

This guide will help you set up and run the DataProject monitoring stack with Prometheus and Grafana.

## Important: Ephemeral Metrics Behavior

**🔄 Fresh Metrics on Every Restart**

This monitoring stack is configured for **development and testing** with the following behavior:

- ✅ **Prometheus metrics are EPHEMERAL** - Cleared on every restart
- ✅ **Grafana settings are PERSISTENT** - Passwords and dashboards are kept
- ✅ **Database data is PERSISTENT** - Your financial data is safe

**What this means:**
- Every time you stop and start the stack, you get **fresh metrics starting from zero**
- Perfect for testing load scenarios without historical clutter
- Grafana customizations (password changes, dashboard edits) persist
- Your API database (companies, prices, treasury data) remains intact

**Use Cases:**
- 🎯 Load testing with fresh metrics each session
- 🧪 Testing monitoring setup without old data
- 📊 Demonstrating metrics from scratch
- 🔄 Iterative development with clean slate

**Not Recommended For:**
- ❌ Production environments (use persistent Prometheus storage)
- ❌ Long-term trend analysis across restarts
- ❌ Historical metric comparison

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed (included with Docker Desktop)
- Environment variables configured in `secret/.env`

## Architecture

```
┌─────────────────┐
│  Your Browser   │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌──────┐  ┌─────────┐
│ API  │  │ Grafana │
│:8000 │  │  :3000  │
└──┬───┘  └────┬────┘
   │           │
   │      ┌────▼────────┐
   └─────►│ Prometheus  │
          │    :9090    │
          └─────────────┘
```

## Quick Start

### Option 1: Using the Helper Script (Windows)

```cmd
# Start the entire stack
monitoring-stack.bat start

# View logs
monitoring-stack.bat logs

# Stop the stack
monitoring-stack.bat stop
```

### Option 2: Using Docker Compose Directly

```bash
# Start all services
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

## Accessing the Services

Once the stack is running:

| Service | URL | Credentials |
|---------|-----|-------------|
| **FastAPI API** | http://localhost:8000 | - |
| **API Documentation** | http://localhost:8000/docs | - |
| **Metrics Endpoint** | http://localhost:8000/monitoring/metrics | - |
| **Prometheus** | http://localhost:9090 | - |
| **Grafana** | http://localhost:3000 | admin / admin |

### Grafana Dashboard

1. Open http://localhost:3000
2. Login with `admin` / `admin` (you'll be prompted to change password)
3. Go to **Dashboards** → **DataProject** → **DataProject API Dashboard**

Or access directly: http://localhost:3000/d/dataproject-api

## What You'll See in Grafana

The pre-configured dashboard includes:

### HTTP Metrics
- **Request Rate**: Requests per second by endpoint and status
- **Total Requests**: Cumulative request count
- **Request Latency**: p50 and p95 latency percentiles
- **Total Exceptions**: Error count

### Database Metrics
- **Query Rate**: Database operations per second
- **Query Latency**: p95 database query duration

### Business Metrics
- **Ticker Queries**: Number of ticker lookups
- **Price History Requests**: Price data requests
- **Treasury Curve Requests**: Yield curve data requests

## Testing the Metrics

Generate some traffic to see metrics:

```bash
# Get company info
curl http://localhost:8000/company/AAPL

# Get price history
curl "http://localhost:8000/company/AAPL/priceHistory?start_date=2024-01-01"

# Get treasury yields
curl "http://localhost:8000/curves/US_treasury_yield?latest_only=true"

# List tickers
curl http://localhost:8000/company/list/available_tickers
```

Then refresh the Grafana dashboard to see the metrics update!

## Prometheus Query Examples

You can run these queries in Prometheus (http://localhost:9090):

```promql
# Request rate
rate(http_requests_total[5m])

# Average request duration
rate(http_request_duration_seconds_sum[5m]) / rate(http_request_duration_seconds_count[5m])

# Error rate
rate(http_requests_total{status=~"5.."}[5m])

# Database query rate
rate(db_queries_total[5m])
```

## Troubleshooting

### Container won't start

```bash
# Check container logs
docker-compose logs api
docker-compose logs prometheus
docker-compose logs grafana

# Check container status
docker-compose ps
```

### Port already in use

If you get "port already allocated" errors:

```bash
# Stop the stack
docker-compose down

# Check what's using the port
netstat -ano | findstr :8000   # Windows
lsof -i :8000                   # macOS/Linux

# Kill the process or change ports in docker-compose.yml
```

### Grafana dashboard not showing data

1. Check if Prometheus is scraping: http://localhost:9090/targets
2. The target `dataproject-api` should show as "UP"
3. Verify metrics are available: http://localhost:8000/monitoring/metrics

### Cannot connect to API from Prometheus

This is a networking issue. Make sure all services are on the same Docker network:

```bash
docker network inspect dataproject_monitoring
```

## Stopping the Stack

```bash
# Stop containers (metrics will be cleared on next start)
docker-compose down

# Stop and remove ALL data (including Grafana settings)
docker-compose down -v
```

**Note:** 
- `docker-compose down` stops containers and clears Prometheus metrics automatically (they're ephemeral)
- `docker-compose down -v` also removes Grafana settings (passwords, dashboard edits)
- Use the helper scripts for clearer messaging about what's being cleared

## Understanding Data Persistence

### What Gets Cleared on Restart

**Prometheus Metrics** (Ephemeral):
- Request counts
- Latency histograms
- Error rates
- Database query metrics
- All time-series data

**Result:** Fresh start for testing!

### What Persists Across Restarts

**Grafana Settings** (Persistent):
- Admin password changes
- Dashboard customizations
- Panel configurations
- User preferences

**API Database** (Persistent):
- Company details
- Price history
- Treasury yield curves
- All financial data

**Result:** No need to reconfigure!

### Converting to Production Setup

If you need **persistent Prometheus metrics** for production:

1. Edit `docker-compose.yml`
2. Uncomment the Prometheus volume:
   ```yaml
   volumes:
     - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
     - prometheus-data:/prometheus  # Add this line
   ```
3. Add volume definition at bottom:
   ```yaml
   volumes:
     prometheus-data:
       driver: local
     grafana-data:
       driver: local
   ```
4. Configure retention policies in `prometheus.yml`
5. Set up regular backups

## Production Notes

**⚠️ Current Configuration: Development/Testing Mode**

This stack uses **ephemeral Prometheus metrics** perfect for development but NOT recommended for production.

### For Production Deployment:

1. **Enable Persistent Prometheus Storage** (see "Converting to Production Setup" above)
2. **Change Grafana password** in docker-compose.yml or via environment variable
3. **Add authentication** to your API endpoints
4. **Use proper secrets management** (vault, AWS Secrets Manager, not .env files)
5. **Configure retention policies** in Prometheus (default: 15 days)
6. **Set up alerting** in Grafana for critical thresholds
7. **Use reverse proxy** (nginx/traefik) with HTTPS/TLS
8. **Configure backup** for Grafana dashboards and Prometheus data
9. **Monitor disk usage** for Prometheus TSDB storage
10. **Set resource limits** in docker-compose.yml (CPU, memory)
11. **Use external volume drivers** for better performance
12. **Implement high availability** (Prometheus HA, Grafana clustering)

## Next Steps

- Customize the Grafana dashboard
- Add more panels for specific metrics
- Set up alerts for critical thresholds
- Export metrics to external monitoring systems
- Create custom Prometheus rules

## Useful Commands

```bash
# Rebuild and restart a specific service
docker-compose up -d --build api

# View resource usage
docker stats

# Access container shell
docker exec -it dataproject-api /bin/bash

# View Prometheus config
docker exec dataproject-prometheus cat /etc/prometheus/prometheus.yml

# Backup Grafana dashboards
docker exec dataproject-grafana grafana-cli admin export-dashboards > backup.json
```
