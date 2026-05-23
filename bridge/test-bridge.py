#!/usr/bin/env python3
"""OpenClaw-style bridge test client.

By default this targets a bridge running with HARNESS_ADAPTER=fake so protocol,
session, queueing, and SSE regressions are quick to catch.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import time
import urllib.error
import urllib.request


BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8080")


def chat(messages, stream=False, session_id="default", timeout=20, request_timeout=None):
    payload = {
        "model": "gemini-3.5-flash",
        "messages": messages,
        "stream": stream,
        "session_id": session_id,
    }
    if request_timeout is not None:
        payload["timeout"] = request_timeout
    req = urllib.request.Request(
        f"{BRIDGE_URL}/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "X-OpenClaw-Session-Id": session_id,
        },
        method="POST",
    )
    started = time.monotonic()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if stream:
            text = _read_sse(resp)
            return {"text": text, "time": time.monotonic() - started}
        body = resp.read().decode()
    result = json.loads(body)
    choice = result.get("choices", [{}])[0]
    return {
        "text": choice.get("message", {}).get("content", ""),
        "finish_reason": choice.get("finish_reason", ""),
        "time": time.monotonic() - started,
    }


def raw_chat(payload, headers=None, timeout=20):
    req = urllib.request.Request(
        f"{BRIDGE_URL}/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read())


def _read_sse(resp) -> str:
    chunks: list[str] = []
    for raw in resp:
        line = raw.decode().strip()
        if not line.startswith("data: "):
            continue
        data = line[6:]
        if data == "[DONE]":
            break
        event = json.loads(data)
        delta = event["choices"][0].get("delta", {})
        chunks.append(delta.get("content", ""))
    return "".join(chunks)


def read_sse_events(messages, session_id="sse-events", request_timeout=None):
    payload = {
        "model": "gemini-3.5-flash",
        "messages": messages,
        "stream": True,
        "session_id": session_id,
    }
    if request_timeout is not None:
        payload["timeout"] = request_timeout
    req = urllib.request.Request(
        f"{BRIDGE_URL}/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "X-OpenClaw-Session-Id": session_id},
        method="POST",
    )
    events = []
    with urllib.request.urlopen(req, timeout=20) as resp:
        assert resp.headers.get_content_type() == "text/event-stream"
        for raw in resp:
            line = raw.decode().strip()
            if not line.startswith("data: "):
                continue
            data = line[6:]
            if data == "[DONE]":
                events.append("[DONE]")
                break
            events.append(json.loads(data))
    return events


def health():
    with urllib.request.urlopen(f"{BRIDGE_URL}/health", timeout=5) as resp:
        return json.loads(resp.read())


passed = 0
failed = 0


def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS {name}")
        passed += 1
    except Exception as exc:
        print(f"  FAIL {name}: {exc}")
        failed += 1


def test_health():
    assert health().get("status") == "ok"


def test_non_streaming():
    result = chat([{"role": "user", "content": "Reply with exactly: OK"}], session_id="basic")
    assert result["text"] == "OK", result


def test_streaming():
    result = chat(
        [{"role": "user", "content": "Reply with exactly: STREAM_OK"}],
        stream=True,
        session_id="stream",
    )
    assert result["text"] == "STREAM_OK", result


def test_sse_event_shape():
    events = read_sse_events([{"role": "user", "content": "Reply with exactly: SSE_SHAPE"}])
    assert events[0]["choices"][0]["delta"]["role"] == "assistant", events
    assert events[1]["choices"][0]["delta"]["content"] == "SSE_SHAPE", events
    assert events[2]["choices"][0]["finish_reason"] == "stop", events
    assert events[-1] == "[DONE]", events


def test_session_memory_and_isolation():
    chat([{"role": "user", "content": "My name is Alice."}], session_id="session-A")
    recall = chat([{"role": "user", "content": "What is my name?"}], session_id="session-A")
    assert "Alice" in recall["text"], recall
    isolated = chat([{"role": "user", "content": "What is my name?"}], session_id="session-B")
    assert "Alice" not in isolated["text"], isolated


def test_header_session_id_wins():
    payload = {
        "model": "gemini-3.5-flash",
        "messages": [{"role": "user", "content": "My name is Bob."}],
        "stream": False,
        "session_id": "body-session",
    }
    _, result_body = raw_chat(payload, headers={"X-OpenClaw-Session-Id": "header-session"})
    result = {"text": result_body["choices"][0]["message"]["content"]}
    assert "Bob" in result["text"], result
    recall_header = chat([{"role": "user", "content": "What is my name?"}], session_id="header-session")
    recall_body = chat([{"role": "user", "content": "What is my name?"}], session_id="body-session")
    assert "Bob" in recall_header["text"], recall_header
    assert "Bob" not in recall_body["text"], recall_body


def test_same_session_queue_and_cross_session_concurrency():
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        same = [
            pool.submit(chat, [{"role": "user", "content": f"Reply with exactly: SAME_{i}"}], False, "same-session")
            for i in range(3)
        ]
        other = pool.submit(chat, [{"role": "user", "content": "Reply with exactly: OTHER"}], False, "other-session")
        same_results = [f.result(timeout=30)["text"] for f in same]
        other_result = other.result(timeout=30)["text"]
    assert same_results == ["SAME_0", "SAME_1", "SAME_2"], same_results
    assert other_result == "OTHER", other_result


def test_slow_same_session_does_not_block_other_session():
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        slow = pool.submit(
            chat,
            [{"role": "user", "content": "sleep:0.3 Reply with exactly: SLOW"}],
            False,
            "queue-slow",
        )
        queued = pool.submit(chat, [{"role": "user", "content": "Reply with exactly: QUEUED"}], False, "queue-slow")
        other = pool.submit(chat, [{"role": "user", "content": "Reply with exactly: FAST_OTHER"}], False, "queue-other")
        other_result = other.result(timeout=5)
        slow_result = slow.result(timeout=5)
        queued_result = queued.result(timeout=5)
    assert other_result["text"] == "FAST_OTHER", other_result
    assert other_result["time"] < slow_result["time"], (other_result, slow_result)
    assert slow_result["text"] == "SLOW", slow_result
    assert queued_result["text"] == "QUEUED", queued_result


def test_content_parts_and_empty():
    parts = chat(
        [{"role": "user", "content": [{"type": "text", "text": "Reply with exactly: PARTS_OK"}]}],
        session_id="parts",
    )
    assert parts["text"] == "PARTS_OK", parts
    empty = chat([{"role": "user", "content": ""}], session_id="empty")
    assert "No user content" in empty["text"], empty


def test_full_context_is_forwarded():
    result = chat(
        [
            {"role": "system", "content": "OpenClaw injected memory: favorite color is green."},
            {"role": "user", "content": "Earlier turn that should be visible."},
            {"role": "assistant", "content": "Earlier assistant turn."},
            {"role": "user", "content": "Reply with exactly: CONTEXT_OK"},
        ],
        session_id="full-context",
    )
    assert result["text"] == "CONTEXT_OK", result


def test_timeout_does_not_poison_session():
    timed_out = chat(
        [{"role": "user", "content": "sleep:1.2 Reply with exactly: TOO_LATE"}],
        session_id="timeout-session",
        timeout=10,
        request_timeout=1,
    )
    assert "Harness timeout" in timed_out["text"], timed_out
    recovered = chat(
        [{"role": "user", "content": "Reply with exactly: RECOVERED"}],
        session_id="timeout-session",
        timeout=5,
    )
    assert recovered["text"] == "RECOVERED", recovered


def test_missing_session_id_rejected():
    payload = {
        "model": "gemini-3.5-flash",
        "messages": [{"role": "user", "content": "Reply with exactly: NO_SESSION"}],
        "stream": False,
    }
    try:
        raw_chat(payload)
    except urllib.error.HTTPError as exc:
        assert exc.code == 400, exc
        body = json.loads(exc.read())
        assert "missing required session id" in body["error"]["message"], body
    else:
        raise AssertionError("missing session id was accepted")


def test_invalid_timeout_rejected():
    payload = {
        "model": "gemini-3.5-flash",
        "messages": [{"role": "user", "content": "Reply with exactly: BAD_TIMEOUT"}],
        "stream": False,
        "session_id": "bad-timeout",
        "timeout": "nope",
    }
    try:
        raw_chat(payload)
    except urllib.error.HTTPError as exc:
        assert exc.code == 400, exc
    else:
        raise AssertionError("invalid timeout was accepted")


if __name__ == "__main__":
    tests = [
        test_health,
        test_non_streaming,
        test_streaming,
        test_sse_event_shape,
        test_session_memory_and_isolation,
        test_header_session_id_wins,
        test_same_session_queue_and_cross_session_concurrency,
        test_slow_same_session_does_not_block_other_session,
        test_content_parts_and_empty,
        test_full_context_is_forwarded,
        test_timeout_does_not_poison_session,
        test_missing_session_id_rejected,
        test_invalid_timeout_rejected,
    ]
    for fn in tests:
        test(fn.__name__, fn)
    print(f"Results: {passed}/{passed + failed} passed")
    if failed:
        sys.exit(1)
