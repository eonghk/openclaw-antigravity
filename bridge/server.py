#!/usr/bin/env python3
"""OpenAI-compatible HTTP bridge for Google localharness."""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

from bridge.session_manager import MAX_REQUEST_TIMEOUT_SEC, MIN_REQUEST_TIMEOUT_SEC, REQUEST_TIMEOUT_SEC, SessionManager

MAX_BODY_BYTES = int(os.environ.get("HARNESS_MAX_BODY_BYTES", str(32 * 1024 * 1024)))
REQUEST_READ_TIMEOUT_SEC = float(os.environ.get("HARNESS_REQUEST_READ_TIMEOUT_SEC", "10"))
HARNESS_CONTEXT_MAX_TOKENS = int(os.environ.get("HARNESS_CONTEXT_MAX_TOKENS", "1048576"))
HARNESS_CONTEXT_CHARS_PER_TOKEN = float(os.environ.get("HARNESS_CONTEXT_CHARS_PER_TOKEN", "4"))
HARNESS_CONTEXT_MAX_CHARS = int(
    os.environ.get(
        "HARNESS_CONTEXT_MAX_CHARS",
        str(int(HARNESS_CONTEXT_MAX_TOKENS * HARNESS_CONTEXT_CHARS_PER_TOKEN)),
    )
)
HARNESS_RECENT_MESSAGE_COUNT = int(os.environ.get("HARNESS_RECENT_MESSAGE_COUNT", "32"))
HARNESS_TOOL_OUTPUT_FULL_CHARS = int(os.environ.get("HARNESS_TOOL_OUTPUT_FULL_CHARS", str(256 * 1024)))
HARNESS_TOOL_OUTPUT_EXCERPT_CHARS = int(os.environ.get("HARNESS_TOOL_OUTPUT_EXCERPT_CHARS", "8192"))
CONTEXT_WRAPPER_CHARS = 2048


@dataclass
class ContextPackStats:
    total_messages: int
    packed_messages: int = 0
    raw_chars: int = 0
    packed_chars: int = 0
    compacted_tools: int = 0
    truncated_messages: int = 0

    @property
    def estimated_tokens(self) -> int:
        return int(self.packed_chars / max(HARNESS_CONTEXT_CHARS_PER_TOKEN, 1))


def _load_gemini_api_key() -> str:
    env_key = os.environ.get("GEMINI_API_KEY")
    if env_key:
        return env_key
    config_path = os.path.expanduser("~/.openclaw/openclaw.json")
    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except OSError:
        return ""
    return cfg.get("env", {}).get("vars", {}).get("GEMINI_API_KEY", "")


session_manager: SessionManager | None = None


class BridgeHttpError(Exception):
    def __init__(self, status: int, message: str, error_type: str = "invalid_request_error"):
        super().__init__(message)
        self.status = status
        self.message = message
        self.error_type = error_type


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        method, path, headers, body = await asyncio.wait_for(
            _read_http_request(reader),
            timeout=REQUEST_READ_TIMEOUT_SEC,
        )
        if method == "GET" and path == "/health":
            await _respond_json(writer, 200, {"status": "ok"})
            return
        if method != "POST" or path not in {"/v1/chat/completions", "/chat/completions"}:
            await _respond_json(writer, 404, {"error": {"message": "not found", "type": "not_found"}})
            return
        try:
            req = json.loads(body or b"{}")
        except json.JSONDecodeError:
            await _respond_json(writer, 400, {"error": {"message": "invalid json", "type": "invalid_request_error"}})
            return
        await _handle_chat_completion(writer, headers, req)
    except BridgeHttpError as exc:
        if not writer.is_closing():
            await _respond_json(writer, exc.status, {"error": {"message": exc.message, "type": exc.error_type}})
    except Exception as exc:
        if not writer.is_closing():
            await _respond_json(writer, 500, {"error": {"message": str(exc), "type": "server_error"}})


async def _read_http_request(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str], bytes]:
    header_bytes = await reader.readuntil(b"\r\n\r\n")
    header_text = header_bytes.decode("utf-8", errors="replace")
    lines = header_text.split("\r\n")
    request_line = lines[0].split()
    method = request_line[0] if len(request_line) > 0 else ""
    path = request_line[1] if len(request_line) > 1 else ""
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    try:
        content_length = int(headers.get("content-length", "0") or "0")
    except ValueError as exc:
        raise BridgeHttpError(400, "invalid content-length") from exc
    if content_length > MAX_BODY_BYTES:
        raise BridgeHttpError(413, "request body too large", "request_too_large")
    body = await reader.readexactly(content_length) if content_length else b""
    return method, path, headers, body


async def _handle_chat_completion(
    writer: asyncio.StreamWriter,
    headers: dict[str, str],
    req: dict[str, Any],
) -> None:
    global session_manager
    if session_manager is None:
        raise RuntimeError("session manager is not initialized")

    model = str(req.get("model") or os.environ.get("HARNESS_MODEL") or "gemini-3.5-flash")
    messages = _normalize_messages(req.get("messages", []))
    last_user_text = _last_user_text(messages)
    last_tool_message = _last_tool_message(messages)
    harness_input, pack_stats = _build_harness_input(messages, last_user_text)
    session_id = _resolve_session_id(headers, req)
    workspace_dir = _resolve_workspace_dir(headers, req)
    tools = _normalize_tools(req.get("tools", []))
    stream = bool(req.get("stream", False))
    resp_id = f"chatcmpl-{os.urandom(8).hex()}"
    created = int(time.time())
    timeout = _parse_timeout(req.get("timeout", REQUEST_TIMEOUT_SEC))

    print(
        f"[bridge] REQ model={model} stream={stream} session={session_id} workspace={workspace_dir or '-'} messages={len(messages)} roles={','.join(m.get('role','?') for m in messages[-5:])} raw_chars={pack_stats.raw_chars} input_chars={len(harness_input)} packed_messages={pack_stats.packed_messages}/{pack_stats.total_messages} compacted_tools={pack_stats.compacted_tools} truncated={pack_stats.truncated_messages} est_tokens={pack_stats.estimated_tokens}/{HARNESS_CONTEXT_MAX_TOKENS} tool_response={'yes' if last_tool_message else 'no'} tool_id={(last_tool_message or {}).get('tool_call_id','-')} tool_chars={len((last_tool_message or {}).get('content',''))}",
        flush=True,
    )

    async def run_harness() -> dict[str, Any]:
        harness = await session_manager.get_or_create(
            session_id,
            workspace_dir=workspace_dir,
            tools=tools,
            model_name=model,
        )
        if last_tool_message:
            return await harness.queue_tool_response(
                last_tool_message["tool_call_id"],
                _tool_result_to_json(last_tool_message.get("content", "")),
                messages=messages,
                timeout=timeout,
            )
        if not last_user_text.strip():
            return {"text": "No user content was provided.", "tool_calls": []}
        return await harness.queue_message(harness_input, messages=messages, timeout=timeout)

    response = await run_harness()

    response_text = str(response.get("text") or "")
    tool_calls = response.get("tool_calls") if isinstance(response.get("tool_calls"), list) else []

    if stream:
        await _start_sse(writer)
        await _write_sse(
            writer,
            {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
            },
        )
        if response_text:
            await _write_sse(
                writer,
                {
                    "id": resp_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": response_text}, "finish_reason": None}],
                },
            )
        if tool_calls:
            await _write_tool_call_sse(writer, resp_id, created, model, tool_calls)
        await _write_sse(
            writer,
            {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if tool_calls else "stop"}],
            },
        )
        writer.write(b"data: [DONE]\n\n")
        await writer.drain()
        writer.close()
        return

    await _respond_json(
        writer,
        200,
        {
            "id": resp_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": response_text,
                        **({"tool_calls": [_openai_tool_call(call) for call in tool_calls]} if tool_calls else {}),
                    },
                    "finish_reason": "tool_calls" if tool_calls else "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        },
    )


def _normalize_messages(raw_messages: Any) -> list[dict[str, str]]:
    if not isinstance(raw_messages, list):
        return []
    normalized: list[dict[str, str]] = []
    for msg in raw_messages:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "")
        content = _content_to_text(msg.get("content"))
        item = {"role": role, "content": content}
        tool_call_id = msg.get("tool_call_id") or msg.get("toolCallId") or msg.get("toolUseId") or msg.get("tool_use_id")
        if tool_call_id:
            item["tool_call_id"] = str(tool_call_id)
        normalized.append(item)
    return normalized


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text") or ""))
            elif isinstance(item, dict):
                for key in ("text", "content", "output", "result"):
                    value = item.get(key)
                    if isinstance(value, str) and value:
                        parts.append(value)
                        break
        return "\n".join(part for part in parts if part)
    if content is None:
        return ""
    return str(content)


def _last_user_text(messages: list[dict[str, str]]) -> str:
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def _last_tool_message(messages: list[dict[str, str]]) -> dict[str, str] | None:
    if not messages:
        return None
    msg = messages[-1]
    if msg.get("role") in {"tool", "toolResult", "function"} and msg.get("tool_call_id"):
        return msg
    return None


def _normalize_tools(raw_tools: Any) -> list[dict[str, str]]:
    if not isinstance(raw_tools, list):
        return []
    tools: list[dict[str, str]] = []
    for raw_tool in raw_tools:
        if not isinstance(raw_tool, dict) or raw_tool.get("type") != "function":
            continue
        fn = raw_tool.get("function")
        if not isinstance(fn, dict):
            continue
        name = str(fn.get("name") or "").strip()
        if not name:
            continue
        parameters = fn.get("parameters") if isinstance(fn.get("parameters"), dict) else {"type": "object", "properties": {}}
        tools.append({
            "name": name,
            "description": str(fn.get("description") or ""),
            "parameters_json_schema": json.dumps(parameters, separators=(",", ":")),
            "response_json_schema": "{}",
        })
    return tools


def _tool_result_to_json(content: str) -> str:
    try:
        parsed = json.loads(content)
        return json.dumps(parsed, separators=(",", ":"))
    except Exception:
        return json.dumps({"content": content}, separators=(",", ":"))


def _openai_tool_call(call: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(call.get("id") or f"call_{os.urandom(4).hex()}"),
        "type": "function",
        "function": {
            "name": str(call.get("name") or ""),
            "arguments": str(call.get("arguments") or "{}"),
        },
    }


def _build_harness_input(messages: list[dict[str, str]], last_user_text: str) -> tuple[str, ContextPackStats]:
    """Package OpenClaw's turn context for a stateful localharness.

    The provider advertises a large context window, so the bridge should not
    hide that behind a tiny fixed prompt cap. It still has to assemble context
    structurally because replaying unbounded channel history and old tool dumps
    as one user event is wasteful and can time out before localharness responds.
    """
    stats = ContextPackStats(
        total_messages=len(messages),
        raw_chars=sum(len(msg.get("content") or "") for msg in messages),
    )
    if len(messages) <= 1:
        stats.packed_messages = len(messages)
        stats.packed_chars = len(last_user_text)
        return last_user_text, stats
    context_messages = _select_context_messages(messages, last_user_text, stats)
    parts = [
        "You are receiving the packed OpenClaw turn context for this request.",
        "Use system/developer/context messages as current instructions and answer only the latest user message.",
        "The bridge preserves high-value structure first and compacts old oversized tool outputs when needed.",
        "",
        "<openclaw_messages>",
    ]
    for msg in context_messages:
        role = msg.get("role") or "unknown"
        content = msg.get("content") or ""
        if not content:
            continue
        parts.append(f"[{role}]\n{content}")
    parts.extend([
        "</openclaw_messages>",
        "",
        "Latest user message:",
        last_user_text,
    ])
    harness_input = "\n\n".join(parts)
    stats.packed_chars = len(harness_input)
    return harness_input, stats


def _select_context_messages(
    messages: list[dict[str, str]],
    last_user_text: str,
    stats: ContextPackStats,
) -> list[dict[str, str]]:
    """Pack by message structure, never by keyword or guessed intent."""
    if not messages:
        return []

    latest_user_idx = _latest_user_index(messages)
    budget = max(0, HARNESS_CONTEXT_MAX_CHARS - len(last_user_text) - CONTEXT_WRAPPER_CHARS)
    selected: dict[int, dict[str, str]] = {}
    used_chars = 0

    def add_message(index: int, mode: str = "auto") -> None:
        nonlocal used_chars
        if index in selected or index == latest_user_idx:
            return
        remaining = budget - used_chars
        if remaining <= 0:
            return
        packed = _render_context_message(messages[index], remaining, mode)
        if packed is None:
            return
        selected[index] = packed
        used_chars += len(packed.get("content") or "")
        if packed.get("_compacted") == "true":
            stats.compacted_tools += 1
        if packed.get("_truncated") == "true":
            stats.truncated_messages += 1

    # Current instructions and injected runtime context have the highest value.
    for index, msg in enumerate(messages):
        if msg.get("role") in {"system", "developer"}:
            add_message(index, "preserve")

    # Preserve the active tool-result chain at the tail so tool continuation works.
    for index in range(_current_tool_chain_start(messages, latest_user_idx), len(messages)):
        add_message(index, "preserve")

    # Keep the newest ordinary turns before considering older history.
    recent_start = max(0, len(messages) - max(HARNESS_RECENT_MESSAGE_COUNT, 0))
    for index in range(recent_start, len(messages)):
        add_message(index, "recent")

    # Fill the remaining hard window with older turns, newest first.
    for index in range(recent_start - 1, -1, -1):
        add_message(index, "old")

    stats.packed_messages = len(selected)
    return [selected[index] for index in sorted(selected)]


def _latest_user_index(messages: list[dict[str, str]]) -> int:
    for index in range(len(messages) - 1, -1, -1):
        if messages[index].get("role") == "user":
            return index
    return -1


def _current_tool_chain_start(messages: list[dict[str, str]], latest_user_idx: int) -> int:
    if not messages or messages[-1].get("role") not in {"tool", "toolResult", "function"}:
        return len(messages)
    start = max(latest_user_idx + 1, 0)
    for index in range(len(messages) - 1, start - 1, -1):
        if messages[index].get("role") == "assistant":
            return index
    return start


def _render_context_message(msg: dict[str, str], remaining_chars: int, mode: str) -> dict[str, str] | None:
    role = msg.get("role") or "unknown"
    content = msg.get("content") or ""
    if not content:
        return None

    tool_like = role in {"tool", "toolResult", "function"}
    if tool_like and (mode == "old" or len(content) > HARNESS_TOOL_OUTPUT_FULL_CHARS):
        return _compact_tool_message(msg, remaining_chars)

    return _fit_message(msg, remaining_chars)


def _fit_message(msg: dict[str, str], remaining_chars: int) -> dict[str, str] | None:
    content = msg.get("content") or ""
    if remaining_chars <= 0:
        return None
    if len(content) <= remaining_chars:
        return dict(msg)
    if remaining_chars < 512:
        return None
    packed = dict(msg)
    packed["content"] = _middle_excerpt(content, remaining_chars, "message truncated")
    packed["_truncated"] = "true"
    return packed


def _compact_tool_message(msg: dict[str, str], remaining_chars: int) -> dict[str, str] | None:
    content = msg.get("content") or ""
    if remaining_chars < 512:
        return None
    tool_call_id = msg.get("tool_call_id") or "-"
    header = f"[tool output compacted; original_chars={len(content)}; tool_call_id={tool_call_id}]"
    excerpt_budget = min(HARNESS_TOOL_OUTPUT_EXCERPT_CHARS, remaining_chars - len(header) - 32)
    if excerpt_budget < 256:
        return None
    packed = dict(msg)
    packed["content"] = f"{header}\n{_middle_excerpt(content, excerpt_budget, 'tool output excerpt')}"
    packed["_compacted"] = "true"
    if len(content) > excerpt_budget:
        packed["_truncated"] = "true"
    return packed


def _middle_excerpt(text: str, max_chars: int, label: str) -> str:
    if len(text) <= max_chars:
        return text
    marker = f"\n[...{label}; omitted_chars={len(text) - max_chars}...]\n"
    if max_chars <= len(marker) + 2:
        return text[:max_chars]
    edge = max(1, (max_chars - len(marker)) // 2)
    tail = max_chars - len(marker) - edge
    return text[:edge] + marker + text[-tail:]


def _resolve_session_id(headers: dict[str, str], req: dict[str, Any]) -> str:
    for key in ("x-openclaw-session-id", "x-session-id", "x-request-session-id"):
        if headers.get(key):
            return headers[key]
    for key in ("session_id", "sessionId", "conversation_id", "conversationId"):
        value = req.get(key)
        if value:
            return str(value)
    raise BridgeHttpError(400, "missing required session id", "invalid_request_error")


def _resolve_workspace_dir(headers: dict[str, str], req: dict[str, Any]) -> str | None:
    value = headers.get("x-openclaw-workspace-dir") or req.get("workspace_dir") or req.get("workspaceDir")
    if not value:
        return None
    path = os.path.abspath(os.path.expanduser(str(value)))
    if not os.path.isdir(path):
        return None
    return path


def _parse_timeout(raw_timeout: Any) -> float:
    try:
        timeout = float(raw_timeout)
    except (TypeError, ValueError) as exc:
        raise BridgeHttpError(400, "invalid timeout", "invalid_request_error") from exc
    if timeout != timeout:
        raise BridgeHttpError(400, "invalid timeout", "invalid_request_error")
    return min(max(timeout, MIN_REQUEST_TIMEOUT_SEC), MAX_REQUEST_TIMEOUT_SEC)


def _stream_error_chunk(resp_id: str, created: int, model: str, message: str) -> dict[str, Any]:
    if not message:
        message = "request failed or timed out"
    return {
        "id": resp_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {"content": f"Bridge error: {message}"}, "finish_reason": "stop"}],
    }


async def _start_sse(writer: asyncio.StreamWriter) -> None:
    writer.write(
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/event-stream\r\n"
        b"Cache-Control: no-cache, no-transform\r\n"
        b"Connection: keep-alive\r\n"
        b"X-Accel-Buffering: no\r\n"
        b"Access-Control-Allow-Origin: *\r\n"
        b"\r\n"
    )
    await writer.drain()


async def _write_sse(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    writer.write(f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode())
    await writer.drain()


async def _write_tool_call_sse(
    writer: asyncio.StreamWriter,
    resp_id: str,
    created: int,
    model: str,
    tool_calls: list[dict[str, Any]],
) -> None:
    for index, call in enumerate(tool_calls):
        openai_call = _openai_tool_call(call)
        await _write_sse(writer, {
            "id": resp_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"tool_calls": [{
                    "index": index,
                    "id": openai_call["id"],
                    "type": "function",
                    "function": {"name": openai_call["function"]["name"], "arguments": ""},
                }]},
                "finish_reason": None,
            }],
        })
        arguments = openai_call["function"]["arguments"]
        for start in range(0, len(arguments), 256):
            await _write_sse(writer, {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {"tool_calls": [{
                        "index": index,
                        "function": {"arguments": arguments[start:start + 256]},
                    }]},
                    "finish_reason": None,
                }],
            })


async def _respond_json(writer: asyncio.StreamWriter, status: int, data: dict[str, Any]) -> None:
    body = json.dumps(data, separators=(",", ":")).encode()
    phrase = HTTPStatus(status).phrase if status in HTTPStatus._value2member_map_ else ""
    writer.write(
        f"HTTP/1.1 {status} {phrase}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n".encode()
    )
    writer.write(body)
    await writer.drain()
    writer.close()


async def main() -> None:
    global session_manager
    port = int(os.environ.get("HARNESS_PORT", "8080"))
    adapter_kind = os.environ.get("HARNESS_ADAPTER", "real")
    model = os.environ.get("HARNESS_MODEL", "gemini-3.5-flash")
    print("Starting Google Harness Bridge", flush=True)
    print(f"Adapter: {adapter_kind} | Model: {model}", flush=True)
    session_manager = await SessionManager(
        api_key=_load_gemini_api_key(),
        model_name=model,
        adapter_kind=adapter_kind,
    ).start()
    server = await asyncio.start_server(handle, "127.0.0.1", port)
    print(f"Listening: http://127.0.0.1:{port}/v1/chat/completions", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
    finally:
        if session_manager:
            loop.run_until_complete(session_manager.stop_all())
