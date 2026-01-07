@echo off
REM Helper script to manage the DataProject monitoring stack on Windows

setlocal EnableDelayedExpansion

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not running. Please start Docker Desktop first.
    exit /b 1
)

REM Parse command
if "%1"=="" goto usage
if /i "%1"=="start" goto start
if /i "%1"=="stop" goto stop
if /i "%1"=="restart" goto restart
if /i "%1"=="logs" goto logs
if /i "%1"=="status" goto status
if /i "%1"=="cleanup" goto cleanup
goto usage

:start
echo =========================================================
echo Starting DataProject Monitoring Stack
echo =========================================================
echo.
echo Building and starting containers...
docker-compose up -d --build
if errorlevel 1 (
    echo Error: Failed to start the stack
    exit /b 1
)
echo.
echo Stack started successfully!
echo.
echo Services are available at:
echo   - API:        http://localhost:8000
echo   - API Docs:   http://localhost:8000/docs
echo   - Metrics:    http://localhost:8000/monitoring/metrics
echo   - Prometheus: http://localhost:9090
echo   - Grafana:    http://localhost:3000 (admin/admin)
echo.
echo Grafana dashboard: http://localhost:3000/d/dataproject-api
echo.
echo Run 'monitoring-stack.bat logs' to view logs
goto end

:stop
echo =========================================================
echo Stopping DataProject Monitoring Stack
echo =========================================================
docker-compose down
echo Stack stopped successfully!
goto end

:restart
echo =========================================================
echo Restarting DataProject Monitoring Stack
echo =========================================================
call :stop
timeout /t 2 /nobreak >nul
call :start
goto end

:logs
if "%2"=="" (
    docker-compose logs -f
) else (
    docker-compose logs -f %2
)
goto end

:status
echo =========================================================
echo DataProject Monitoring Stack Status
echo =========================================================
docker-compose ps
goto end

:cleanup
echo =========================================================
echo Cleaning Up DataProject Monitoring Stack
echo =========================================================
echo Stopping and removing containers, networks, and volumes...
docker-compose down -v
echo Cleanup completed!
goto end

:usage
echo DataProject Monitoring Stack Manager
echo.
echo Usage: %0 {start^|stop^|restart^|logs^|status^|cleanup}
echo.
echo Commands:
echo   start    - Start the monitoring stack (API + Prometheus + Grafana)
echo   stop     - Stop the monitoring stack
echo   restart  - Restart the monitoring stack
echo   logs     - View logs (use 'logs api', 'logs prometheus', or 'logs grafana')
echo   status   - Show status of all containers
echo   cleanup  - Stop and remove all containers, networks, and volumes
exit /b 1

:end
endlocal
