#!/bin/bash
# Start google-harness-bridge as a managed daemon
# Uses /opt/homebrew/bin/python3 for compatibility

export HOME="/Users/chen"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/tmp/agy-env/lib/python3.14/site-packages"

# Kill existing if any
pkill -f "bridge/server.py" 2>/dev/null
sleep 1

cd /tmp/google-harness-bridge
exec /opt/homebrew/bin/python3 -u bridge/server.py
