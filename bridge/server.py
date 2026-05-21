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

def _oai_to_harness_tool(tool_def: dict) -> pb.Tool | None:
    """Convert OpenAI tool format to harness Tool proto."""
    if tool_def.get("type") != "function":
        return None
    fn = tool_def.get("function", {})
    return pb.Tool(
        name=fn.get("name", ""),
        description=fn.get("description", ""),
        parameters_json_schema=json.dumps(fn.get("parameters", {})),
        response_json_schema="",
    )

class HarnessSession:
    def __init__(self, api_key: str, model: str = "gemini-3.5-flash", skill_dirs: list[str] = None):
        self.api_key = api_key
        self.model = model
        self.skill_dirs = skill_dirs or []
        self.process = None
        self.ws = None
        self._pending_tool_calls = {}  # tool_call_id -> proto ToolCall response
    
    async def start(self, tool_defs: list[dict] = None):
        """Launch harness with optional tool definitions."""
        self.process = subprocess.Popen(
            [BINARY], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        ic = pb.InputConfig(storage_directory="/tmp/harness-bridge-output")
        raw = ic.SerializeToString()
        self.process.stdin.write(struct.pack("<I", len(raw)) + raw)
        self.process.stdin.flush()
        
        raw_len = self.process.stdout.read(4)
        length = struct.unpack("<I", raw_len)[0]
        oc = pb.OutputConfig()
        oc.ParseFromString(self.process.stdout.read(length))
        
        self.ws = await websockets.connect(
            f"ws://localhost:{oc.port}/",
            additional_headers={"x-goog-api-key": oc.api_key},
        )
        
        # Convert OpenAI tools to harness tools
        harness_tools = []
        if tool_defs:
            for td in tool_defs:
                t = _oai_to_harness_tool(td)
                if t:
                    harness_tools.append(t)
        
        hc = pb.HarnessConfig(
            cascade_id="",
            gemini_config=pb.GeminiConfig(api_key=self.api_key, model_name=self.model),
            system_instructions=pb.SystemInstructions(
                appended=pb.AppendedSystemInstructions(
                    appended_sections=[pb.AppendedSystemInstructions.Section(
                        title="user_system_instructions",
                        content=(
                            "You are a helpful AI assistant powered by Gemini 3.5 Flash.\n"
                            "Reply concisely and accurately.\n"
                            "When you need to use a tool, call it with the correct arguments.\n"
                            "After receiving tool results, incorporate them into your response."
                        ),
                    )]
                )
            ),
            tools=harness_tools,
            harness_side_tools=pb.HarnessSideTools(
                subagents=pb.SubagentsConfig(enabled=True),
                find=pb.FindToolConfig(enabled=True),
                run_command=pb.RunCommandToolConfig(enabled=True),
                user_questions=pb.UserQuestionsConfig(enabled=True),
                file_edit=pb.FileEditToolConfig(enabled=True),
                view_file=pb.ViewFileToolConfig(enabled=True),
                write_to_file=pb.WriteToFileToolConfig(enabled=True),
                grep_search=pb.GrepSearchToolConfig(enabled=True),
                list_dir=pb.ListDirToolConfig(enabled=True),
                generate_image=pb.GenerateImageToolConfig(enabled=True, model_name="gemini-3.1-flash-image-preview"),
            ),
            workspaces=[],
            skills_paths=self.skill_dirs,
        )
        await self.ws.send(json_format.MessageToJson(pb.InitializeConversationEvent(config=hc)))
        
        # Drain init
        try:
            while True:
                await asyncio.wait_for(self.ws.recv(), timeout=2.0)
        except (asyncio.TimeoutError, websockets.ConnectionClosed):
            pass
        
        try:
            pong = await self.ws.ping()
            await asyncio.wait_for(pong, timeout=2)
        except:
            raise RuntimeError("Harness died after init")
        
        tool_count = len(harness_tools)
        print(f"Harness ready ({self.model}) with {tool_count} tools", flush=True)
        return self
    
    async def chat(self, messages: list[dict]):
        """
        Handle one round of chat. May return text or tool calls.
        Yields: (type, data) where type is "text", "tool_call", or "done"
        """
        # Find last user message OR tool result to send
        last_tool_result = None
        last_user = None
        
        for m in messages:
            if m.get("role") == "user":
                last_user = m.get("content", "")
            elif m.get("role") == "tool":
                last_tool_result = {
                    "tool_call_id": m.get("tool_call_id", ""),
                    "content": m.get("content", ""),
                }
        
        # If there's a pending tool result to send, use tool_response
        if last_tool_result:
            tc_id = last_tool_result["tool_call_id"]
            tc_response = self._pending_tool_calls.get(tc_id, {})
            if tc_response:
                tool_resp = pb.InputEvent(
                    tool_response=pb.ToolResponse(
                        id=tc_response.get("harness_tool_call_id", tc_id),
                        response_json=last_tool_result["content"],
                    )
                )
                await self.ws.send(json_format.MessageToJson(tool_resp))
                del self._pending_tool_calls[tc_id]
        elif last_user:
            await self.ws.send(json_format.MessageToJson(pb.InputEvent(user_input=str(last_user))))
        else:
            yield "done", ""
            return
        
        # Stream responses
        try:
            while True:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=30.0)
                data = json.loads(msg)
                su = data.get("stepUpdate")
                tsu = data.get("trajectoryStateUpdate")
                tc_out = data.get("toolCall")
                usage = data.get("usageMetadata")
                
                if su:
                    state = su.get("state", "")
                    source = su.get("source", "")
                    
                    # Harness native tool approval (auto-approve off, skip instead)
                    if state == "STATE_WAITING_FOR_USER":
                        # Skip built-in tool requests
                        tc_skip = pb.InputEvent(
                            tool_confirmation=pb.ToolConfirmation(
                                trajectory_id=su.get("trajectoryId", ""),
                                step_index=su.get("stepIndex", 0),
                                accepted=True,
                            )
                        )
                        await self.ws.send(json_format.MessageToJson(tc_skip))
                    
                    # Forward assistant text
                    if source == "SOURCE_MODEL":
                        text = su.get("text", "")
                        text_delta = su.get("textDelta", "")
                        # state check for tool calls
                        if state == "STATE_WAITING_FOR_USER":
                            pass  # already handled above
                        elif text_delta:
                            yield "text", text_delta
                        elif text and not text_delta:
                            yield "text", text
                
                # Custom tool call from harness
                if tc_out:
                    tc_id = tc_out.get("id", "")
                    self._pending_tool_calls[tc_id] = {
                        "harness_tool_call_id": tc_id,
                        "name": tc_out.get("name", ""),
                        "arguments": tc_out.get("argumentsJson", "{}"),
                }
                    yield "tool_call", {
                        "id": tc_id,
                        "type": "function",
                        "function": {
                            "name": tc_out.get("name", ""),
                            "arguments": tc_out.get("argumentsJson", "{}"),
                    }
                }
                
                # Trajectory idle = conversation turn complete
                if tsu and tsu.get("state") == "STATE_IDLE":
                    yield "done", ""
                    return
                    
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            yield "done", ""
    
    async def stop(self):
        if self.ws:
            await self.ws.close()
        if self.process:
            self.process.kill()
            try:
                self.process.wait(timeout=3)
            except:
                pass


# ── HTTP Server ─────────────────────────────────────────────

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
    
    content_length = 0
    for line in request_data.decode().split("\r\n"):
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

    # Session-aware: each session_id gets its own harness
    session_key = req.get("session_id", req.get("user", "default"))
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
