#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");
const bridgeServer = join(root, "bridge", "server.py");
const runTests = join(root, "bridge", "run-tests.sh");

function python() {
  return process.env.PYTHON || process.env.PYTHON3 || "python3";
}

function run(cmd, args, options = {}) {
  return spawnSync(cmd, args, {
    stdio: options.capture ? "pipe" : "inherit",
    encoding: "utf8",
    env: { ...process.env, ...(options.env || {}) },
    cwd: options.cwd || root,
  });
}

function printHelp() {
  console.log([
    "openclaw-antigravity",
    "",
    "Usage:",
    "  openclaw-antigravity doctor",
    "  openclaw-antigravity start",
    "  openclaw-antigravity test",
    "",
    "Environment:",
    "  HARNESS_PORT             Bridge port, default 8080",
    "  HARNESS_ADAPTER          real or fake, default real",
    "  HARNESS_BINARY           Optional path to google-antigravity localharness",
    "  GEMINI_API_KEY           Gemini API key used by the real harness adapter",
    "  PYTHON                   Python executable, default python3",
  ].join("\n"));
}

function doctor() {
  let ok = true;
  const py = python();
  const pyCheck = run(py, ["--version"], { capture: true });
  if (pyCheck.status === 0) {
    console.log("ok python: " + (pyCheck.stdout || pyCheck.stderr).trim());
  } else {
    console.log("fail python: " + py + " not found");
    ok = false;
  }

  const importCheck = run(
    py,
    [
      "-c",
      "import importlib.util, pathlib; spec=importlib.util.find_spec('google.antigravity'); print('missing' if not spec else pathlib.Path(spec.origin).parent / 'bin' / 'localharness')",
    ],
    { capture: true },
  );
  const harness = (importCheck.stdout || "").trim();
  if (importCheck.status === 0 && harness && harness !== "missing") {
    console.log("ok google-antigravity: " + harness);
  } else {
    console.log("warn google-antigravity: not importable; install it in the Python environment used to start the bridge");
  }

  const syntaxCheck = run(py, ["-m", "py_compile", bridgeServer], {
    capture: true,
    env: { PYTHONPATH: root },
  });
  if (syntaxCheck.status === 0) {
    console.log("ok bridge syntax");
  } else {
    console.log((syntaxCheck.stderr || syntaxCheck.stdout).trim());
    ok = false;
  }

  if (!process.env.GEMINI_API_KEY) {
    console.log("warn GEMINI_API_KEY is not set; bridge may still read ~/.openclaw/openclaw.json");
  }

  process.exit(ok ? 0 : 1);
}

function start() {
  const child = spawn(python(), ["-u", bridgeServer], {
    stdio: "inherit",
    cwd: root,
    env: {
      ...process.env,
      PYTHONPATH: [root, process.env.PYTHONPATH].filter(Boolean).join(":"),
      HARNESS_ADAPTER: process.env.HARNESS_ADAPTER || "real",
      HARNESS_PORT: process.env.HARNESS_PORT || "8080",
    },
  });
  child.on("exit", (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    process.exit(code ?? 1);
  });
}

function test() {
  const result = run("bash", [runTests]);
  process.exit(result.status ?? 1);
}

const command = process.argv[2] || "help";
if (command === "doctor") doctor();
else if (command === "start") start();
else if (command === "test") test();
else {
  printHelp();
  process.exit(command === "help" || command === "--help" || command === "-h" ? 0 : 1);
}
