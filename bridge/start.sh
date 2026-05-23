#!/bin/bash
# Start google-harness-bridge as a managed daemon
# Uses the Antigravity venv packages via PYTHONPATH.

export HOME="/Users/chen"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/tmp/agy-env/lib/python3.14/site-packages"
export HARNESS_ADAPTER="${HARNESS_ADAPTER:-real}"
export HARNESS_PORT="${HARNESS_PORT:-8080}"
export HARNESS_MODEL="${HARNESS_MODEL:-gemini-3.5-flash}"

# Kill existing if any
pkill -f "bridge/server.py" 2>/dev/null
sleep 1

cd /tmp/google-harness-bridge
exec /opt/homebrew/bin/python3 -u bridge/server.py
