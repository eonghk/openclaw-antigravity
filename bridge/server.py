#!/usr/bin/env python3
"""
google-harness-bridge: OpenAI-compatible HTTP → harness WebSocket proxy.
Session-aware: each session gets its own harness instance.
"""
import asyncio, json, os, struct, subprocess, sys, time
import websockets

with open("/Users/chen/.openclaw/openclaw.json") as f:
    _cfg = json.load(f)
_GEMINI_API_KEY = _cfg.get("env", {}).get("vars", {}).get("GEMINI_API_KEY", "")

from bridge.session_manager import SessionManager

# Initialize global session manager
session_manager = None

# Active SSE streams (so we don't block on non-chat requests)
_active_sse = {}  # session_id -> set of writer futures

# (HarnessSession moved to session_manager.py)
async def handle(reader, writer):
    """Single HTTP request handler."""
    """Single HTTP request handler."""
    request_data = b""
    while True:
        line = await reader.readline()
        request_data += line
        if line == b"\r\n":
            break
    
    req_line = request_data.decode("utf-8", errors="replace").split("\r\n")[0]
    parts = req_line.split(" ")
    method = parts[0] if len(parts) > 0 else ""
    path = parts[1] if len(parts) > 1 else ""
    
    if method == "GET" and path == "/health":
        await _respond(writer, 200, {"status": "ok"})
        return
    
    if method != "POST" or (path != "/v1/chat/completions" and path != "/chat/completions"):
        await _respond(writer, 404, {"error": "not found"})
        return
    
    # Parse all HTTP headers
    headers = {}
    content_length = 0
    for line in request_data.decode().split("\r\n"):
        if ":" in line:
            key, _, val = line.partition(":")
            key_s = key.strip().lower()
            val_s = val.strip()
            if key_s:
                headers[key_s] = headers.get(key_s, val_s) if key_s not in headers else headers[key_s]  # keep first
        if line.lower().startswith("content-length:"):
            content_length = int(line.split(":")[1].strip())
    
    body = b""
    if content_length > 0:
        body = await reader.readexactly(content_length)
    
    try:
        req = json.loads(body)
    except:
        await _respond(writer, 400, {"error": "invalid json"})
        return
    
    import sys as _sys
    print(f"[bridge] REQ: model={req.get('model','?')} stream={req.get('stream',False)} msgs={len(req.get('messages',[]))}", flush=True)
    
    stream_mode = req.get("stream", False)
    resp_id = f"chatcmpl-{os.urandom(8).hex()}"
    now = int(time.time())
    
    # Collect response
    collected_text = ""
    collected_tool_calls = []
    response_tracker = set()

    if stream_mode:
        # Write SSE HTTP headers before streaming
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: keep-alive\r\n"
            b"Access-Control-Allow-Origin: *\r\n"
            b"\r\n"
        )
        await writer.drain()

    
    # Session-aware: derive key from X-Session-Id header (injected by OpenClaw), then user field, then env var
    session_key = headers.get("x-session-id", req.get("session_id", req.get("user", os.environ.get("SESSION_NAME", "default"))))
    harness = await session_manager.get_or_create(session_key)
    collected_text = ""

    # Extract last user message and send to session's harness
    for m in req.get("messages", []):
        if m.get("role") == "user":
            last_msg = m.get("content", "")
            if last_msg:
                collected_text = await harness.queue_message(last_msg, timeout=60.0)

    # Build and send response
    if collected_text:
        if stream_mode:
            # Send content chunk (role chunk was sent before harness call)
            content_chunk = {
                "id": resp_id, "object": "chat.completion.chunk",
                "created": now, "model": "gemini-3.5-flash",
                "choices": [{"index": 0, "delta": {"content": collected_text}, "finish_reason": None}],
            }
            await _write_sse(writer, json.dumps(content_chunk))
            # Send final
            final_chunk = {
                "id": resp_id, "object": "chat.completion.chunk",
                "created": now, "model": "gemini-3.5-flash",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }
            await _write_sse(writer, json.dumps(final_chunk))
            writer.write(b"data: [DONE]\n\n")
            await writer.drain()
            writer.close()
        else:
            resp = {
                "id": resp_id,
                "object": "chat.completion",
                "created": now,
                "model": "gemini-3.5-flash",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": collected_text}, "finish_reason": "stop"}],
            }
            await _respond(writer, 200, resp)
    else:
        await _respond(writer, 200, {"choices": [{"message": {"content": "No response"}}]})


async def _write_sse(writer, data: str):
    """Write SSE data chunk."""
    try:
        writer.write(f"data: {data}\n\n".encode())
        await writer.drain()
    except:
        pass


async def _respond(writer, status, data):
    body = json.dumps(data).encode()
    status_text = {200: "OK", 400: "Bad Request", 404: "Not Found"}
    writer.write(
        f"HTTP/1.1 {status} {status_text.get(status, '')}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"\r\n".encode()
    )
    writer.write(body)
    await writer.drain()
    writer.close()


async def main():
    global session_manager
    
    port = int(os.environ.get("HARNESS_PORT", "8080"))
    
    print("🚀 Starting Google Harness Bridge (session-aware)...", flush=True)
    session_manager = await SessionManager(api_key=_GEMINI_API_KEY).start()
    
    server = await asyncio.start_server(handle, "127.0.0.1", port)
    print(f"🌐 http://127.0.0.1:{port}/v1/chat/completions", flush=True)
    print(f"📝 session-aware | per-session harness instances", flush=True)
    
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
        if session_manager:
            try:
                loop.run_until_complete(session_manager.stop_all())
            except:
                pass
