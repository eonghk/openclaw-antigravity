"""Session-aware harness pool manager.

Each active session gets its own harness adapter. Requests for the same session
are processed FIFO by one worker; different sessions can run concurrently.
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import re
import shutil
import struct
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import websockets


def _default_localharness_binary() -> str:
    spec = importlib.util.find_spec("google.antigravity")
    if spec and spec.origin:
        candidate = Path(spec.origin).parent / "bin" / "localharness"
        if candidate.exists():
            return str(candidate)
    return ""


DEFAULT_BINARY = _default_localharness_binary()
SESSION_IDLE_TIMEOUT_SEC = int(os.environ.get("HARNESS_SESSION_IDLE_SEC", "300"))
REQUEST_TIMEOUT_SEC = float(os.environ.get("HARNESS_REQUEST_TIMEOUT_SEC", "300"))
MIN_REQUEST_TIMEOUT_SEC = float(os.environ.get("HARNESS_MIN_REQUEST_TIMEOUT_SEC", "1"))
MAX_REQUEST_TIMEOUT_SEC = float(os.environ.get("HARNESS_MAX_REQUEST_TIMEOUT_SEC", "300"))
MAX_ACTIVE_SESSIONS = int(os.environ.get("HARNESS_MAX_ACTIVE_SESSIONS", "16"))
MAX_STARTING_SESSIONS = int(os.environ.get("HARNESS_MAX_STARTING_SESSIONS", "4"))


class HarnessCrashed(RuntimeError):
    """Raised when a harness adapter cannot complete a request."""


class HarnessAdapter(Protocol):
    async def start(self) -> None: ...
    async def chat(self, user_text: str, messages: list[dict] | None = None) -> dict: ...
    async def tool_response(self, tool_call_id: str, response_json: str) -> dict: ...
    async def stop(self) -> None: ...
    def is_alive(self) -> bool: ...


@dataclass
class QueueItem:
    text: str
    messages: list[dict]
    timeout: float
    future: asyncio.Future[dict]
    tool_response_id: str | None = None
    tool_response_json: str | None = None

    @property
    def abandoned(self) -> bool:
        return self.future.cancelled() or self.future.done()


def tools_fingerprint(tools: list[dict] | None) -> str:
    payload = json.dumps(tools or [], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _safe_session_fragment(session_id: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.-]+", "-", session_id).strip("-")
    digest = hashlib.sha256(session_id.encode()).hexdigest()[:12]
    if clean:
        return f"{clean[:48]}-{digest}"
    return digest


def _join_response_steps(order: list[tuple[str, int]], steps: dict[tuple[str, int], str]) -> str:
    return "".join(steps.get(key, "") for key in order)


def _collapse_repeated_text(text: str) -> str:
    if not text:
        return text
    normalized = text.strip()
    if not normalized:
        return text
    midpoint = len(normalized) // 2
    if len(normalized) % 2 == 0 and normalized[:midpoint] == normalized[midpoint:]:
        return normalized[:midpoint]
    for sep in ("\n\n", "\n"):
        parts = normalized.split(sep)
        if len(parts) % 2 == 0:
            half = len(parts) // 2
            if parts[:half] == parts[half:]:
                return sep.join(parts[:half])
    return text


class FakeHarnessAdapter:
    """Fast deterministic adapter for bridge/client tests."""

    def __init__(self, session_id: str, **_: object):
        self.session_id = session_id
        self._alive = False
        self._facts: dict[str, str] = {}

    async def start(self) -> None:
        self._alive = True

    async def chat(self, user_text: str, messages: list[dict] | None = None) -> dict:
        delay = float(os.environ.get("HARNESS_FAKE_DELAY_SEC", "0"))
        sleep_match = re.search(r"sleep:(\d+(?:\.\d+)?)", user_text)
        if sleep_match:
            delay = float(sleep_match.group(1))
        await asyncio.sleep(delay)
        if not self._alive:
            raise HarnessCrashed("fake harness is stopped")
        lowered = user_text.lower()
        current_text = user_text.rsplit("Latest user message:", 1)[-1]
        current_lowered = current_text.lower()
        name_match = re.search(r"my name is ([a-zA-Z][a-zA-Z0-9_-]*)", current_text, re.I)
        if name_match:
            self._facts["name"] = name_match.group(1)
            return {"text": f"I will remember your name is {self._facts['name']}.", "tool_calls": []}
        if "what is my name" in current_lowered:
            if "name" in self._facts:
                return {"text": f"Your name is {self._facts['name']}.", "tool_calls": []}
            return {"text": "You have not told me your name in this session.", "tool_calls": []}
        if "exactly:" in current_lowered:
            return {"text": current_text.rsplit("exactly:", 1)[1].strip(), "tool_calls": []}
        if "color is the sky" in current_lowered:
            return {"text": "The sky is blue.", "tool_calls": []}
        if current_text.strip():
            return {"text": f"Echo[{self.session_id}]: {current_text.strip()}", "tool_calls": []}
        return {"text": "No user content was provided.", "tool_calls": []}

    async def tool_response(self, tool_call_id: str, response_json: str) -> dict:
        return {"text": f"Tool response received for {tool_call_id}.", "tool_calls": []}

    async def stop(self) -> None:
        self._alive = False

    def is_alive(self) -> bool:
        return self._alive


class LocalHarnessAdapter:
    """Adapter around Google's localharness binary and WebSocket protocol."""

    def __init__(self, session_id: str, api_key: str, model_name: str, binary: str = DEFAULT_BINARY, workspace_dir: str | None = None, tools: list[dict] | None = None):
        self.session_id = session_id
        self.api_key = api_key
        self.model_name = model_name
        self.binary = binary
        self.workspace_dir = workspace_dir
        self.tools = tools or []
        self.process: subprocess.Popen | None = None
        self.ws = None
        self.pb = None
        self.json_format = None
        self.storage_dir: str | None = None
        self._tool_id_aliases: dict[str, str] = {}

    async def start(self) -> None:
        from google.protobuf import json_format
        import google.antigravity.connections.local.localharness_pb2 as pb

        self.pb = pb
        self.json_format = json_format
        binary = Path(self.binary).expanduser()
        if not binary.exists():
            raise FileNotFoundError(f"localharness binary not found: {binary}")
        self.process = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        try:
            if not self.process.stdin or not self.process.stdout:
                raise HarnessCrashed("failed to open localharness stdio")

            self.storage_dir = f"/tmp/harness-{_safe_session_fragment(self.session_id)}"
            ic = self.pb.InputConfig(storage_directory=self.storage_dir)
            raw = ic.SerializeToString()
            self.process.stdin.write(struct.pack("<I", len(raw)) + raw)
            self.process.stdin.flush()

            raw_len = await asyncio.wait_for(asyncio.to_thread(self.process.stdout.read, 4), timeout=10)
            if len(raw_len) != 4:
                raise HarnessCrashed("localharness exited before output config")
            length = struct.unpack("<I", raw_len)[0]
            oc = self.pb.OutputConfig()
            output_config = await asyncio.wait_for(asyncio.to_thread(self.process.stdout.read, length), timeout=10)
            oc.ParseFromString(output_config)

            self.ws = await websockets.connect(
                f"ws://localhost:{oc.port}/",
                additional_headers={"x-goog-api-key": oc.api_key},
                ping_interval=20,
                ping_timeout=10,
            )
            workspaces = []
            if self.workspace_dir:
                workspaces.append(
                    self.pb.Workspace(
                        filesystem_workspace=self.pb.FilesystemWorkspace(directory=self.workspace_dir)
                    )
                )
            tools = [
                self.pb.Tool(
                    name=str(tool.get("name") or ""),
                    description=str(tool.get("description") or ""),
                    parameters_json_schema=str(tool.get("parameters_json_schema") or "{}"),
                    response_json_schema=str(tool.get("response_json_schema") or "{}"),
                )
                for tool in self.tools
                if tool.get("name")
            ]
            hc = self.pb.HarnessConfig(
            cascade_id="",
            gemini_config=self.pb.GeminiConfig(api_key=self.api_key, model_name=self.model_name),
            system_instructions=self.pb.SystemInstructions(
                appended=self.pb.AppendedSystemInstructions(
                    appended_sections=[
                        self.pb.AppendedSystemInstructions.Section(
                            title="user_system_instructions",
                            content="You are a helpful AI assistant. Reply concisely.",
                        )
                    ]
                )
            ),
            tools=tools,
            skills_paths=[],
            harness_side_tools=self.pb.HarnessSideTools(
                subagents=self.pb.SubagentsConfig(enabled=False),
                find=self.pb.FindToolConfig(enabled=False),
                run_command=self.pb.RunCommandToolConfig(enabled=False),
                user_questions=self.pb.UserQuestionsConfig(enabled=False),
                file_edit=self.pb.FileEditToolConfig(enabled=False),
                view_file=self.pb.ViewFileToolConfig(enabled=False),
                write_to_file=self.pb.WriteToFileConfig(enabled=False) if hasattr(self.pb, "WriteToFileConfig") else self.pb.WriteToFileToolConfig(enabled=False),
                grep_search=self.pb.GrepSearchToolConfig(enabled=False),
                list_dir=self.pb.ListDirToolConfig(enabled=False),
                generate_image=self.pb.GenerateImageToolConfig(
                    enabled=False,
                    model_name="gemini-3.1-flash-image-preview",
                ),
            ),
            workspaces=workspaces,
            )
            await self.ws.send(self.json_format.MessageToJson(self.pb.InitializeConversationEvent(config=hc)))
            await self._drain_init()
        except Exception:
            await self.stop()
            raise

    async def _drain_init(self) -> None:
        if not self.ws:
            raise HarnessCrashed("websocket not connected")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                await asyncio.wait_for(self.ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                break
        pong = await self.ws.ping()
        await asyncio.wait_for(pong, timeout=2)

    async def chat(self, user_text: str, messages: list[dict] | None = None) -> dict:
        if not self.ws:
            raise HarnessCrashed("websocket not connected")
        try:
            await self.ws.send(self.json_format.MessageToJson(self.pb.InputEvent(user_input=str(user_text))))
            return await self._receive_response()
        except websockets.ConnectionClosed as exc:
            raise HarnessCrashed(f"websocket closed: {exc}") from exc

    async def tool_response(self, tool_call_id: str, response_json: str) -> dict:
        if not self.ws:
            raise HarnessCrashed("websocket not connected")
        try:
            response_id = self._resolve_tool_response_id(tool_call_id)
            event = self.pb.InputEvent(tool_response=self.pb.ToolResponse(id=response_id, response_json=response_json))
            await self.ws.send(self.json_format.MessageToJson(event))
            return await self._receive_response()
        except websockets.ConnectionClosed as exc:
            raise HarnessCrashed(f"websocket closed: {exc}") from exc

    async def _receive_response(self) -> dict:
        response_steps: dict[tuple[str, int], str] = {}
        response_order: list[tuple[str, int]] = []
        while True:
            msg = await asyncio.wait_for(self.ws.recv(), timeout=REQUEST_TIMEOUT_SEC)
            data = json.loads(msg)
            tool_call = data.get("toolCall")
            if tool_call:
                tool_call_id = str(tool_call.get("id", ""))
                self._remember_tool_id(tool_call_id)
                return {
                    "text": _collapse_repeated_text(_join_response_steps(response_order, response_steps)),
                    "tool_calls": [{
                        "id": tool_call_id,
                        "name": tool_call.get("name", ""),
                        "arguments": tool_call.get("argumentsJson", "{}"),
                    }],
                }
            su = data.get("stepUpdate")
            tsu = data.get("trajectoryStateUpdate")
            if su:
                if su.get("state") == "STATE_WAITING_FOR_USER":
                    await self._approve_tool_call(su)
                if su.get("source") == "SOURCE_MODEL" and su.get("target") == "TARGET_USER":
                    key = (str(su.get("trajectoryId", "")), int(su.get("stepIndex", 0)))
                    if key not in response_steps:
                        response_order.append(key)
                        response_steps[key] = ""
                    text = su.get("text") or ""
                    text_delta = su.get("textDelta") or ""
                    if text:
                        response_steps[key] = text
                    elif text_delta:
                        response_steps[key] += text_delta
            if tsu and tsu.get("state") == "STATE_IDLE":
                response_text = _collapse_repeated_text(_join_response_steps(response_order, response_steps))
                return {"text": response_text or "No response", "tool_calls": []}

    def _remember_tool_id(self, tool_call_id: str) -> None:
        if not tool_call_id:
            return
        self._tool_id_aliases[tool_call_id] = tool_call_id
        self._tool_id_aliases[re.sub(r"[^a-zA-Z0-9]", "", tool_call_id)] = tool_call_id

    def _resolve_tool_response_id(self, tool_call_id: str) -> str:
        if not tool_call_id:
            return tool_call_id
        if tool_call_id in self._tool_id_aliases:
            return self._tool_id_aliases[tool_call_id]
        stripped = re.sub(r"[^a-zA-Z0-9]", "", tool_call_id)
        if stripped in self._tool_id_aliases:
            return self._tool_id_aliases[stripped]
        for alias, original in sorted(self._tool_id_aliases.items(), key=lambda item: len(item[0]), reverse=True):
            if stripped.startswith(alias) or alias.startswith(stripped):
                return original
        return tool_call_id

    async def _approve_tool_call(self, step_update: dict) -> None:
        if not self.ws:
            return
        if not self.pb or not self.json_format:
            return
        tc = self.pb.InputEvent(
            tool_confirmation=self.pb.ToolConfirmation(
                trajectory_id=step_update.get("trajectoryId", ""),
                step_index=step_update.get("stepIndex", 0),
                accepted=True,
            )
        )
        await self.ws.send(self.json_format.MessageToJson(tc))

    async def stop(self) -> None:
        if self.ws:
            await self.ws.close()
            self.ws = None
        if self.process:
            if self.process.poll() is None:
                self.process.kill()
            try:
                await asyncio.to_thread(self.process.wait, 3)
            except Exception:
                pass
            self.process = None

    async def discard_storage(self) -> None:
        if self.storage_dir:
            await asyncio.to_thread(shutil.rmtree, self.storage_dir, True)

    def is_alive(self) -> bool:
        return bool(self.process and self.process.poll() is None and self.ws)


class HarnessInstance:
    """One session-scoped queue and harness adapter."""

    def __init__(self, session_id: str, adapter: HarnessAdapter, model_name: str, tools_hash: str):
        self.session_id = session_id
        self.adapter = adapter
        self.model_name = model_name
        self.tools_hash = tools_hash
        self.last_used = time.time()
        self._queue: asyncio.Queue[QueueItem | None] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._closed = False
        self._current_item: QueueItem | None = None

    async def start(self) -> "HarnessInstance":
        await self.adapter.start()
        self._worker_task = asyncio.create_task(self._process_queue(), name=f"harness-{self.session_id}")
        return self

    async def queue_message(
        self,
        user_text: str,
        messages: list[dict] | None = None,
        timeout: float = REQUEST_TIMEOUT_SEC,
    ) -> dict:
        if self._closed:
            raise HarnessCrashed("session is closed")
        self.last_used = time.time()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict] = loop.create_future()
        await self._queue.put(QueueItem(user_text, messages or [], timeout, future))
        try:
            return await asyncio.wait_for(future, timeout=timeout + 5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            future.cancel()
            raise

    async def queue_tool_response(
        self,
        tool_call_id: str,
        response_json: str,
        messages: list[dict] | None = None,
        timeout: float = REQUEST_TIMEOUT_SEC,
    ) -> dict:
        if self._closed:
            raise HarnessCrashed("session is closed")
        self.last_used = time.time()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict] = loop.create_future()
        await self._queue.put(QueueItem("", messages or [], timeout, future, tool_call_id, response_json))
        try:
            return await asyncio.wait_for(future, timeout=timeout + 5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            future.cancel()
            raise

    async def _process_queue(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            if item.abandoned:
                self._queue.task_done()
                continue
            self._current_item = item
            try:
                if item.tool_response_id is not None:
                    result = await asyncio.wait_for(
                        self.adapter.tool_response(item.tool_response_id, item.tool_response_json or "{}"),
                        timeout=item.timeout,
                    )
                else:
                    result = await asyncio.wait_for(
                        self.adapter.chat(item.text, item.messages),
                        timeout=item.timeout,
                    )
            except asyncio.TimeoutError:
                result = {"text": f"Harness timeout after {item.timeout:.1f}s", "tool_calls": []}
                self._closed = True
                await self.adapter.stop()
                discard = getattr(self.adapter, "discard_storage", None)
                if discard:
                    await discard()
            except Exception as exc:
                result = {"text": f"Harness error: {exc}", "tool_calls": []}
                self._closed = True
                await self.adapter.stop()
                discard = getattr(self.adapter, "discard_storage", None)
                if discard:
                    await discard()
            if not item.future.done():
                item.future.set_result(result)
            self.last_used = time.time()
            self._current_item = None
            self._queue.task_done()
            if self._closed:
                await self._fail_pending("Harness session closed")
                return

    async def _fail_pending(self, message: str) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if item is None:
                self._queue.task_done()
                return
            if not item.future.done():
                item.future.set_result({"text": message, "tool_calls": []})
            self._queue.task_done()

    async def stop(self) -> None:
        self._closed = True
        if self._current_item and not self._current_item.future.done():
            self._current_item.future.set_result({"text": "Harness session closed", "tool_calls": []})
        if self._worker_task:
            await self._queue.put(None)
            try:
                await asyncio.wait_for(self._worker_task, timeout=5)
            except asyncio.TimeoutError:
                self._worker_task.cancel()
        await self.adapter.stop()

    @property
    def is_idle(self) -> bool:
        return time.time() - self.last_used > SESSION_IDLE_TIMEOUT_SEC and self._queue.empty()

    def is_alive(self) -> bool:
        return not self._closed and self.adapter.is_alive()

    def matches(self, model_name: str, tools_hash: str) -> bool:
        return self.model_name == model_name and self.tools_hash == tools_hash


class SessionManager:
    """Manages harness instances keyed by session_id."""

    def __init__(self, api_key: str, model_name: str = "gemini-3.5-flash", adapter_kind: str = "real"):
        self.api_key = api_key
        self.model_name = model_name
        self.adapter_kind = adapter_kind
        self._instances: dict[str, HarnessInstance] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._reaper_task: asyncio.Task | None = None
        self._startup_semaphore = asyncio.Semaphore(MAX_STARTING_SESSIONS)

    async def start(self) -> "SessionManager":
        self._reaper_task = asyncio.create_task(self._reaper_loop(), name="harness-reaper")
        return self

    async def get_or_create(
        self,
        session_id: str,
        workspace_dir: str | None = None,
        tools: list[dict] | None = None,
        model_name: str | None = None,
    ) -> HarnessInstance:
        selected_model = model_name or self.model_name
        selected_tools_hash = tools_fingerprint(tools)
        lock = self._locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            inst = self._instances.get(session_id)
            if inst and inst.is_alive() and inst.matches(selected_model, selected_tools_hash):
                return inst
            if inst:
                await inst.stop()
                self._instances.pop(session_id, None)

            if len(self._instances) >= MAX_ACTIVE_SESSIONS:
                await self._evict_one_idle_or_raise()

            adapter = self._new_adapter(session_id, workspace_dir=workspace_dir, tools=tools, model_name=selected_model)
            async with self._startup_semaphore:
                inst = await HarnessInstance(session_id, adapter, selected_model, selected_tools_hash).start()
            self._instances[session_id] = inst
            print(f"[session] Created harness for {session_id[:80]}", flush=True)
            return inst

    def _new_adapter(
        self,
        session_id: str,
        workspace_dir: str | None = None,
        tools: list[dict] | None = None,
        model_name: str | None = None,
    ) -> HarnessAdapter:
        if self.adapter_kind == "fake":
            return FakeHarnessAdapter(session_id)
        return LocalHarnessAdapter(
            session_id=session_id,
            api_key=self.api_key,
            model_name=model_name or self.model_name,
            binary=os.environ.get("HARNESS_BINARY", DEFAULT_BINARY),
            workspace_dir=workspace_dir,
            tools=tools,
        )

    async def _evict_one_idle_or_raise(self) -> None:
        for sid, inst in sorted(self._instances.items(), key=lambda item: item[1].last_used):
            if inst.is_idle:
                self._instances.pop(sid, None)
                await inst.stop()
                return
        raise RuntimeError(f"too many active sessions: max {MAX_ACTIVE_SESSIONS}")

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            for sid, inst in list(self._instances.items()):
                if inst.is_idle:
                    lock = self._locks.setdefault(sid, asyncio.Lock())
                    async with lock:
                        if self._instances.get(sid) is inst and inst.is_idle:
                            self._instances.pop(sid, None)
                            await inst.stop()
                            print(f"[session] Reaped idle {sid[:80]}", flush=True)

    async def stop_all(self) -> None:
        if self._reaper_task:
            self._reaper_task.cancel()
        for inst in list(self._instances.values()):
            await inst.stop()
        self._instances.clear()
