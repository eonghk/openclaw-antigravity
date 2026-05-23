#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${HARNESS_TEST_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
)}"
LOG="${TMPDIR:-/tmp}/google-harness-bridge-test-${PORT}.log"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

cd "$ROOT"
HARNESS_ADAPTER=fake \
HARNESS_PORT="$PORT" \
PYTHONPATH="$ROOT" \
python3 -u bridge/server.py >"$LOG" 2>&1 &
SERVER_PID=$!

for _ in {1..50}; do
  if python3 - <<PY >/dev/null 2>&1
import json, urllib.request
with urllib.request.urlopen("http://127.0.0.1:${PORT}/health", timeout=0.2) as r:
    assert json.loads(r.read()).get("status") == "ok"
PY
  then
    break
  fi
  sleep 0.1
done

BRIDGE_URL="http://127.0.0.1:${PORT}" python3 bridge/test-bridge.py
