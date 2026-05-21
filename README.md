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
cd bridge
pip install google-antigravity
export GEMINI_API_KEY="your_key"
python3 server.py

# 2. Configure OpenClaw
# openclaw.json:
# {
#   "models": {
#     "providers": {
#       "google-harness": { "baseUrl": "http://127.0.0.1:8080" }
#     }
#   }
# }

# 3. Use the model
# openclaw models set google-harness/gemini-3.5-flash
```

## Status

MVP — plugin format structure ready, Python bridge functional.
Next: Native Node.js bridge, tool call routing, PR to OpenClaw.

## License

MIT
