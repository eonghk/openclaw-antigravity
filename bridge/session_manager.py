"""Session-aware harness pool manager.

Each active session gets its own localharness process + WebSocket.
Same-session messages are queued (FIFO). Idle sessions are reaped.
"""
import asyncio, json, os, struct, subprocess, time
import websockets
from google.protobuf import json_format
import google.antigravity.connections.local.localharness_pb2 as pb

BINARY = os.path.expanduser(
    "/tmp/agy-env/lib/python3.14/site-packages/google/antigravity/bin/localharness"
)

SESSION_IDLE_TIMEOUT_SEC = 300  # 5 min idle → reap

class HarnessInstance:
    """Single localharness process + WebSocket for one session."""
    
    def __init__(self, session_id: str, api_key: str):
        self.session_id = session_id
        self.api_key = api_key
        self.process = None
        self.ws = None
        self.last_used = time.time()
        self._lock = asyncio.Lock()
        self._queue = asyncio.Queue()
        self._worker_task = None
    
    async def start(self):
        """Start harness and initialize conversation."""
        self.process = subprocess.Popen(
            [BINARY], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        ic = pb.InputConfig(storage_directory=f"/tmp/harness-{self.session_id[:8]}")
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
            gemini_config=pb.GeminiConfig(api_key=self.api_key, model_name="gemini-3.5-flash"),
            system_instructions=pb.SystemInstructions(
                appended=pb.AppendedSystemInstructions(
                    appended_sections=[pb.AppendedSystemInstructions.Section(
                        title="user_system_instructions",
                        content="You are a helpful AI assistant powered by Gemini 3.5 Flash. Reply concisely.",
                    )]
                )
            ),
            skills_paths=[],
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
        
        # Start queue worker
        self._worker_task = asyncio.create_task(self._process_queue())
        return self
    
    async def send_message(self, user_text: str):
        """Send a message to this harness and wait for response."""
        self.last_used = time.time()
        
        await self.ws.send(json_format.MessageToJson(pb.InputEvent(user_input=str(user_text))))
        
        response_text = ""
        try:
            while True:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=60.0)
                data = json.loads(msg)
                su = data.get("stepUpdate")
                tsu = data.get("trajectoryStateUpdate")
                
                if su:
                    source = su.get("source", "")
                    state = su.get("state", "")
                    
                    # Auto-approve built-in tool calls
                    if state == "STATE_WAITING_FOR_USER":
                        tc = pb.InputEvent(
                            tool_confirmation=pb.ToolConfirmation(
                                trajectory_id=su.get("trajectoryId", ""),
                                step_index=su.get("stepIndex", 0),
                                accepted=True,
                            )
                        )
                        await self.ws.send(json_format.MessageToJson(tc))
                    
                    if source == "SOURCE_MODEL":
                        response_text += su.get("textDelta", "") or su.get("text", "") or ""
                
                if tsu and tsu.get("state") == "STATE_IDLE":
                    break
        except (asyncio.TimeoutError, websockets.ConnectionClosed):
            pass
        
        return response_text or "No response"
    
    async def _process_queue(self):
        """Process queued messages for this session."""
        while True:
            event = await self._queue.get()
            if event is None:  # shutdown signal
                break
            result_key, user_text = event["key"], event["text"]
            response = await self.send_message(user_text)
            # Store result (HTTP handler will retrieve it)
            event["future"].set_result(response)
    
    async def queue_message(self, user_text: str, timeout: float = 30.0):
        """Queue a message and wait for response. Serializes within session."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put({
            "text": user_text,
            "key": os.urandom(8).hex(),
            "future": future,
        })
        result = await asyncio.wait_for(future, timeout=timeout)
        return result
    
    async def stop(self):
        if self._worker_task:
            await self._queue.put(None)  # shutdown signal
        if self.ws:
            await self.ws.close()
        if self.process:
            self.process.kill()
            try:
                self.process.wait(timeout=3)
            except:
                pass
    
    @property
    def is_idle(self) -> bool:
        return time.time() - self.last_used > SESSION_IDLE_TIMEOUT_SEC


class SessionManager:
    """Manages harness instances keyed by session_id."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._instances: dict[str, HarnessInstance] = {}
        self._reaper_task = None
    
    async def start(self):
        self._reaper_task = asyncio.create_task(self._reaper_loop())
        return self
    
    async def get_or_create(self, session_id: str) -> HarnessInstance:
        if session_id in self._instances:
            inst = self._instances[session_id]
            if inst.process and inst.process.poll() is None:
                return inst
            # Dead harness, remove and recreate
            del self._instances[session_id]
        
        inst = HarnessInstance(session_id, self.api_key)
        await inst.start()
        self._instances[session_id] = inst
        print(f"[session] Created harness for {session_id[:40]}...", flush=True)
        return inst
    
    async def _reaper_loop(self):
        """Reap idle sessions."""
        while True:
            await asyncio.sleep(60)
            dead = [sid for sid, inst in self._instances.items() if inst.is_idle]
            for sid in dead:
                inst = self._instances.pop(sid)
                await inst.stop()
                print(f"[session] Reaped idle {sid[:40]}...", flush=True)
    
    async def stop_all(self):
        for inst in self._instances.values():
            await inst.stop()
        self._instances.clear()
