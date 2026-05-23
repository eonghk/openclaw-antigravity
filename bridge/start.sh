#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export HARNESS_ADAPTER="${HARNESS_ADAPTER:-real}"
export HARNESS_PORT="${HARNESS_PORT:-8080}"
export HARNESS_MODEL="${HARNESS_MODEL:-gemini-3.5-flash}"

cd "$ROOT"
exec "${PYTHON:-python3}" -u bridge/server.py
