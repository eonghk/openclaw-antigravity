# OpenClaw Antigravity

OpenClaw provider plugin for Google Antigravity localharness.

This project packages one OpenClaw plugin plus a Python sidecar bridge. The bridge depends on Google's official `google-antigravity` Python package and does not redistribute the `localharness` binary.

## Status

Experimental, macOS-first, and intended for local OpenClaw installations.

What works today:

- OpenAI-compatible `/v1/chat/completions` endpoint
- non-streaming and SSE responses
- per-session harness isolation
- FIFO processing inside one session
- cross-session concurrency
- OpenAI tool-call bridge
- deterministic fake adapter regression tests
- OpenClaw provider plugin entry point

## Architecture

```
OpenClaw provider plugin
  -> http://127.0.0.1:8080/v1/chat/completions
  -> Python bridge sidecar
  -> google-antigravity localharness
  -> Gemini / Antigravity runtime
```

The npm package owns the OpenClaw provider and CLI. The Python bridge remains a separate process so Python dependencies, localharness lifecycle, crash recovery, and logs stay isolated from the OpenClaw Node.js process.

## Install

```bash
npm install -g openclaw-antigravity
python3 -m venv ~/.openclaw-antigravity
~/.openclaw-antigravity/bin/pip install google-antigravity
PYTHON=~/.openclaw-antigravity/bin/python openclaw-antigravity doctor
```

Set `GEMINI_API_KEY` in your environment or in `~/.openclaw/openclaw.json` under `env.vars.GEMINI_API_KEY`.

## Run

```bash
PYTHON=~/.openclaw-antigravity/bin/python openclaw-antigravity start
```

Default bridge URL:

```
http://127.0.0.1:8080/v1/chat/completions
```

Useful environment variables:

- `HARNESS_PORT`: bridge port, default `8080`
- `HARNESS_ADAPTER`: `real` or `fake`, default `real`
- `HARNESS_BINARY`: optional explicit path to `localharness`
- `HARNESS_SESSION_IDLE_SEC`: session idle timeout
- `HARNESS_MAX_ACTIVE_SESSIONS`: active session cap
- `HARNESS_REQUEST_TIMEOUT_SEC`: request timeout

## Test

```bash
openclaw-antigravity test
```

The test command uses the fake adapter and does not require `google-antigravity` or a Gemini API key.

## OpenClaw Provider

The plugin registers provider id:

```
google-antigravity
```

Models:

- `gemini-3.5-flash`
- `gemini-3.1-pro`
- `gemini-3.1-flash`

The provider uses OpenAI-compatible completions against the local bridge. OpenClaw session metadata is forwarded as headers so the bridge can isolate sessions.

## Security

- The bridge binds to `127.0.0.1` only.
- Missing session ids are rejected for chat requests.
- This package does not include Google's `localharness` binary.
- Do not publish local `.env`, OpenClaw config, service account JSON, or API keys.
- Run a secret scan before publishing releases.

## Development

```bash
git clone git@github-personal:eonghk/openclaw-antigravity.git
cd openclaw-antigravity
npm test
```

Run the bridge directly:

```bash
HARNESS_ADAPTER=fake HARNESS_PORT=18080 PYTHONPATH=$PWD python3 -u bridge/server.py
BRIDGE_URL=http://127.0.0.1:18080 python3 bridge/test-bridge.py
```

## License

MIT
