#!/usr/bin/env python3
"""
Test the Google Harness Bridge end-to-end simulation demodev agent requests.
Usage: python3 test-bridge.py
"""
import json, sys, time, urllib.request

BRIDGE_URL = "http://127.0.0.1:8080"

def chat(messages, stream=False, session_id="default"):
    """Send a chat request to the bridge, return response text."""
    data = json.dumps({
        "model": "gemini-3.5-flash",
        "messages": messages,
        "stream": stream,
        "session_id": session_id,
    }).encode()
    
    req = urllib.request.Request(
        f"{BRIDGE_URL}/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        body = resp.read().decode()
        result = json.loads(body)
        choice = result.get("choices", [{}])[0]
        msg = choice.get("message", {})
        return {
            "text": msg.get("content", ""),
            "finish_reason": choice.get("finish_reason", ""),
            "usage": result.get("usage", {}),
            "time": 0,
        }
    except Exception as e:
        return {"error": str(e), "text": ""}

def health():
    """Check bridge health."""
    try:
        resp = urllib.request.urlopen(f"{BRIDGE_URL}/health", timeout=5)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

# ── Tests ─────────────────────────────────────────────────

passed = 0
failed = 0

def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  ✅ {name}")
        passed += 1
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        failed += 1

# Health check
h = health()
assert h.get("status") == "ok", f"Bridge not ready: {h}"
print(f"✅ Bridge health OK\n")

# Test 1: Simple chat
print("Test 1: Simple chat")
def t1():
    result = chat([{"role": "user", "content": "Reply with exactly: OK"}])
    assert "error" not in result, f"Error: {result.get('error')}"
    assert len(result["text"]) > 0, f"Empty response"
    print(f"   Response: {result['text'][:80]}")
test("Simple chat", t1)

# Test 2: Multi-turn context
print("\nTest 2: Multi-turn session A")
session_a = []
def t2a():
    result = chat([{"role": "user", "content": "My name is Alice."}], session_id="test-A")
    assert "error" not in result
    session_a.append({"role": "user", "content": "My name is Alice."})
    session_a.append({"role": "assistant", "content": result["text"]})
    print(f"   Set name: {result['text'][:60]}")
test("Session A: set name", t2a)

def t2b():
    result = chat(session_a + [{"role": "user", "content": "What is my name?"}], session_id="test-A")
    assert "error" not in result
    assert "Alice" in result["text"], f"Session A forgot name: {result['text']}"
    print(f"   Recall: {result['text'][:60]}")
test("Session A: recall name", t2b)

# Test 3: Session isolation
print("\nTest 3: Session isolation")
def t3():
    result = chat([{"role": "user", "content": "What is my name? I never told you."}], session_id="test-B")
    assert "error" not in result
    assert "Alice" not in result["text"], f"Session B leaked from A: {result['text']}"
    print(f"   Session B doesn't know Alice: {result['text'][:80]}")
test("Session B isolation", t3)

# Test 4: Long context (many messages)
print("\nTest 4: Long context")
def t4():
    msgs = [{"role": "user", "content": f"Fact {i}: The sky is blue."} for i in range(5)]
    msgs.append({"role": "user", "content": "What color is the sky?"})
    result = chat(msgs, session_id="test-long")
    assert "error" not in result
    assert "blue" in result["text"].lower(), f"Lost context: {result['text']}"
    print(f"   {result['text'][:60]}")
test("Long context", t4)

# Test 5: Error handling - empty message
print("\nTest 5: Empty message")
def t5():
    result = chat([{"role": "user", "content": ""}], session_id="test-empty")
    # Should not crash
    if "error" in result:
        print(f"   Expected error for empty msg (acceptable): {result['error'][:60]}")
    else:
        print(f"   Got response for empty msg: {result['text'][:60]}")
test("Empty message", t5)

# ── Summary ────────────────────────────────────────────────
print(f"\n{'='*40}")
print(f"Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"❌ {failed} test(s) FAILED")
    sys.exit(1)
else:
    print("✅ All tests passed!")
