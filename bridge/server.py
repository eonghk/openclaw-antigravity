#!/usr/bin/env python3
"""
google-harness-bridge: OpenAI-compatible HTTP → harness WebSocket proxy.
Supports both streaming (SSE) and non-streaming responses.
"""
import asyncio, json, os, struct, subprocess, sys
import websockets
from google.protobuf import json_format
import google.antigravity.connections.local.localharness_pb2 as pb

BINARY = os.path.expanduser(
    "/tmp/agy-env/lib/python3.14/site-packages/google/antigravity/bin/localharness"
)

class HarnessSession:
    """Manages localharness process + WebSocket."""
    
    def __init__(self, api_key: str, model: str = "gemini-3.5-flash"):
        self.api_key = api_key
        self.model = model
        self.process = None
        self.ws = None
    
    async def start(self):
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
        
        hc = pb.HarnessConfig(
            cascade_id="",
            gemini_config=pb.GeminiConfig(api_key=self.api_key, model_name=self.model),
            system_instructions=pb.SystemInstructions(
                appended=pb.AppendedSystemInstructions(
                    appended_sections=[pb.AppendedSystemInstructions.Section(
                        title="user_system_instructions",
                        content="You are a helpful AI assistant. Reply concisely.",
                    )]
                )
            ),
            tools=[],
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
            workspaces=[pb.Workspace(filesystem_workspace=pb.FilesystemWorkspace(directory="/tmp"))],
        )
        await self.ws.send(json_format.MessageToJson(pb.InitializeConversationEvent(config=hc)))
        
        # Drain init
        try:
            while True:
                await asyncio.wait_for(self.ws.recv(), timeout=2.0)
        except (asyncio.TimeoutError, websockets.ConnectionClosed):
            pass
        
        # Verify alive
        try:
            pong = await self.ws.ping()
            await asyncio.wait_for(pong, timeout=2)
        except:
            raise RuntimeError("Harness died after init")
        
        print(f"Harness ready ({self.model})", flush=True)
        return self
    
    async def chat(self, messages: list[dict], stream: bool = False):
        """Send messages to harness. If stream=True, yields (delta_text, is_final, usage)."""
        last_user = ""
        for m in messages:
            if m.get("role") == "user":
                last_user = m.get("content", "")
        
        if not last_user:
            yield "", True, None
            return
        
        await self.ws.send(json_format.MessageToJson(pb.InputEvent(user_input=str(last_user))))
        
        response_text = ""
        try:
            while True:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=30.0)
                data = json.loads(msg)
                su = data.get("stepUpdate")
                tsu = data.get("trajectoryStateUpdate")
                usage = data.get("usageMetadata")
                
                if su:
                    state = su.get("state", "")
                    source = su.get("source", "")
                    
                    # Auto-approve tool calls
                    if state == "STATE_WAITING_FOR_USER":
                        tc = pb.InputEvent(
                            tool_confirmation=pb.ToolConfirmation(
                                trajectory_id=su.get("trajectoryId", ""),
                                step_index=su.get("stepIndex", 0),
                                accepted=True,
                            )
                        )
                        await self.ws.send(json_format.MessageToJson(tc))
                    
                    # Streaming text from assistant
                    if source == "SOURCE_MODEL":
                        delta = su.get("textDelta", "")
                        full = su.get("text", "")
                        if delta:
                            response_text += delta
                            yield delta, False, None
                        elif full and not response_text:
                            response_text = full
                            yield full, False, None
                
                # Trajectory idle = conversation complete
                if tsu and tsu.get("state") == "STATE_IDLE":
                    yield "", True, usage
                    return
                    
        except asyncio.TimeoutError:
            yield response_text or "No response", True, None
    
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
    """Single HTTP request handler with SSE streaming support."""
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
    
    if method != "POST" or path != "/v1/chat/completions":
        await _respond(writer, 404, {"error": "not found"})
        return
    
    # Read body
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
    
    stream_mode = req.get("stream", False)
    
    if stream_mode:
        # SSE streaming response
        resp_id = f"chatcmpl-{os.urandom(8).hex()}"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: keep-alive\r\n"
            b"Access-Control-Allow-Origin: *\r\n"
            b"\r\n"
        )
        await writer.drain()
        
        async for delta, is_final, usage in session.chat(req.get("messages", []), stream=True):
            if delta:
                chunk = {
                    "id": resp_id,
                    "object": "chat.completion.chunk",
                    "created": int(__import__("time").time()),
                    "model": "gemini-3.5-flash",
                    "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": None}],
                }
                writer.write(f"data: {json.dumps(chunk)}\n\n".encode())
                await writer.drain()
            
            if is_final:
                # Final chunk with finish reason
                chunk = {
                    "id": resp_id,
                    "object": "chat.completion.chunk",
                    "created": int(__import__("time").time()),
                    "model": "gemini-3.5-flash",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                }
                writer.write(f"data: {json.dumps(chunk)}\n\n".encode())
                writer.write(b"data: [DONE]\n\n")
                await writer.drain()
        
        writer.close()
    else:
        # Non-streaming: accumulate full response
        full_text = ""
        async for delta, is_final, usage in session.chat(req.get("messages", [])):
            if delta:
                full_text += delta
            if is_final:
                break
        
        resp = {
            "id": "chatcmpl-harness",
            "object": "chat.completion",
            "created": int(__import__("time").time()),
            "model": "gemini-3.5-flash",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": full_text},
                "finish_reason": "stop",
            }],
            "usage": {"total_tokens": 0},
        }
        await _respond(writer, 200, resp)

async def _respond(writer, status, data):
    body = json.dumps(data).encode()
    status_text = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Error"}
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
    global session
    
    api_key = os.environ.get("GEMINI_API_KEY") or ""
    if not api_key:
        with open("/Users/chen/.openclaw/openclaw.json") as f:
            cfg = json.load(f)
        api_key = cfg.get("env", {}).get("vars", {}).get("GEMINI_API_KEY", "")
    if not api_key:
        print("ERROR: No GEMINI_API_KEY", flush=True)
        sys.exit(1)
    
    port = int(os.environ.get("HARNESS_PORT", "8080"))
    
    print("🚀 Starting Google Harness Bridge...", flush=True)
    session = await HarnessSession(api_key=api_key).start()
    
    server = await asyncio.start_server(handle, "127.0.0.1", port)
    print(f"🌐 Bridge: http://127.0.0.1:{port}/v1/chat/completions", flush=True)
    print(f"📝 Supports: streaming (SSE) + non-streaming", flush=True)
    
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    session = None
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
        if session:
            try:
                loop.run_until_complete(session.stop())
            except:
                pass
