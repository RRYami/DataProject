@echo off
REM DataProject - Download CLI Wrapper for Windows
REM Usage: download.bat [command] [arguments]

setlocal enabledelayedexpansion

REM Change to script directory
cd /d "%~dp0"

REM Check if UV is installed
where uv >nul 2>&1
if errorlevel 1 (
    echo Error: UV is not installed or not in PATH
    echo Please install UV: https://docs.astral.sh/uv/getting-started/installation/
    exit /b 1
)

REM Pass all arguments to Python CLI
uv run python ELT/download_cli.py %*

endlocal
