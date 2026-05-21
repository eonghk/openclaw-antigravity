# Google Harness Bridge

An OpenAI-compatible HTTP bridge for Google's native agent runtime (localharness).
Enables Gemini 3.5 Flash and future Google AI models to run through OpenClaw 
with native agent loop optimization — similar to how Codex runtime works for OpenAI.

## Architecture

```
OpenClaw → HTTP :8080 → Bridge → WebSocket → localharness → Gemini API
                              └── agent loop
                              └── native tool execution
                              └── optimized system prompt
```

## Quick Start

```bash
pip install google-antigravity
export GEMINI_API_KEY="your_api_key"
python3 src/harness_bridge/server.py
```

OpenClaw config:

```json
{
  "models": {
    "providers": {
      "google-harness": {
        "baseUrl": "http://127.0.0.1:8080"
      }
    }
  }
}
```

## Status

MVP — Proof of concept. Single-session, no tool call routing, no plugin registration yet.

## License

MIT
