#!/bin/bash
# Start the Google Harness Bridge
# Requires: GEMINI_API_KEY set
cd "$(dirname "$0")"
python3 server.py
