# Google Harness Bridge — OpenClaw Plugin

OpenClaw provider plugin for Google's native agent runtime via Antigravity localharness.
Enables Gemini models with optimized system prompt, native tool execution, and agent loop.

## Architecture

```
OpenClaw
  └── google-harness provider ── HTTP/JSON ──► bridge/server.py ── WebSocket ──► localharness ──► Gemini API
                                                (Python)                      (100MB binary)
```

## Structure

```
├── openclaw.plugin.json      # OpenClaw plugin manifest
├── package.json              # npm package
├── dist/
│   ├── index.mjs             # Plugin entry point
│   └── provider.mjs          # Provider catalog registration
└── bridge/
    ├── server.py             # Python HTTP + WebSocket bridge
    ├── start.sh              # Bridge startup script
    └── pyproject.toml        # Python dependencies
```

## Quick Start

```bash
# 1. Start the bridge
cd /tmp/google-harness-bridge
HARNESS_ADAPTER=fake HARNESS_PORT=18080 PYTHONPATH=/tmp/google-harness-bridge python3 -u bridge/server.py

# 2. Run the OpenClaw-compatible test client
BRIDGE_URL=http://127.0.0.1:18080 python3 bridge/test-bridge.py

# 3. For real Antigravity/localharness smoke tests
PYTHONPATH=/tmp/google-harness-bridge:/tmp/agy-env/lib/python3.14/site-packages \
  HARNESS_ADAPTER=real HARNESS_PORT=8080 \
  /opt/homebrew/bin/python3 -u bridge/server.py

# 4. Configure OpenClaw
# openclaw.json:
# {
#   "models": {
#     "providers": {
#       "google-harness": { "baseUrl": "http://127.0.0.1:8080" }
#     }
#   }
# }

# 5. Use the model
# openclaw models set google-harness/gemini-3.5-flash
```

## Status

Bridge refactor in progress. The Python bridge now has:

- OpenAI-compatible non-streaming and SSE responses
- per-session harness isolation
- FIFO queueing within a session
- cross-session concurrency
- deterministic fake adapter for regression tests
- real localharness adapter smoke-tested outside OpenClaw

Next: finish non-invasive OpenClaw provider/plugin wiring and tighten tool permissions.

## License

MIT
