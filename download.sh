#!/bin/bash
# DataProject - Download CLI Wrapper for Linux/macOS
# Usage: ./download.sh [command] [arguments]

# Change to script directory
cd "$(dirname "$0")"

# Check if UV is installed
if ! command -v uv &> /dev/null; then
    echo "Error: UV is not installed or not in PATH"
    echo "Please install UV: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Pass all arguments to Python CLI
uv run python ELT/download_cli.py "$@"
