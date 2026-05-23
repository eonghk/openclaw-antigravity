#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");
const bridgeServer = join(root, "bridge", "server.py");
const runTests = join(root, "bridge", "run-tests.sh");
const appDir = join(homedir(), ".openclaw-antigravity");
const venvDir = join(appDir, "venv");
const logsDir = join(appDir, "logs");
const label = "com.openclaw.antigravity";
const plistPath = join(homedir(), "Library", "LaunchAgents", label + ".plist");

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
    "  openclaw-antigravity install",
    "  openclaw-antigravity uninstall",
    "  openclaw-antigravity status",
    "  openclaw-antigravity logs",
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

function plistXml() {
  const node = process.execPath;
  const cli = fileURLToPath(import.meta.url);
  const py = join(venvDir, "bin", "python");
  const outLog = join(logsDir, "bridge.out.log");
  const errLog = join(logsDir, "bridge.err.log");
  return [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
    "<plist version=\"1.0\">",
    "<dict>",
    "  <key>Label</key>",
    "  <string>" + label + "</string>",
    "  <key>ProgramArguments</key>",
    "  <array>",
    "    <string>" + node + "</string>",
    "    <string>" + cli + "</string>",
    "    <string>start</string>",
    "  </array>",
    "  <key>EnvironmentVariables</key>",
    "  <dict>",
    "    <key>PYTHON</key>",
    "    <string>" + py + "</string>",
    "    <key>HARNESS_PORT</key>",
    "    <string>8080</string>",
    "    <key>HARNESS_ADAPTER</key>",
    "    <string>real</string>",
    "  </dict>",
    "  <key>RunAtLoad</key>",
    "  <true/>",
    "  <key>KeepAlive</key>",
    "  <true/>",
    "  <key>WorkingDirectory</key>",
    "  <string>" + root + "</string>",
    "  <key>StandardOutPath</key>",
    "  <string>" + outLog + "</string>",
    "  <key>StandardErrorPath</key>",
    "  <string>" + errLog + "</string>",
    "</dict>",
    "</plist>",
    "",
  ].join("\n");
}

function ensureVenv() {
  mkdirSync(appDir, { recursive: true });
  mkdirSync(logsDir, { recursive: true });
  const py = join(venvDir, "bin", "python");
  if (!existsSync(py)) {
    const create = run(python(), ["-m", "venv", venvDir]);
    if (create.status !== 0) process.exit(create.status ?? 1);
  }
  const pip = join(venvDir, "bin", "pip");
  const install = run(pip, ["install", "--upgrade", "pip", "google-antigravity"]);
  if (install.status !== 0) process.exit(install.status ?? 1);
}

function launchctl(args, options = {}) {
  return run("launchctl", args, options);
}

function installService() {
  if (process.platform !== "darwin") {
    console.error("install currently supports macOS LaunchAgents only.");
    process.exit(1);
  }
  ensureVenv();
  mkdirSync(dirname(plistPath), { recursive: true });
  writeFileSync(plistPath, plistXml(), { mode: 0o644 });
  launchctl(["bootout", "gui/" + process.getuid(), plistPath], { capture: true });
  const result = launchctl(["bootstrap", "gui/" + process.getuid(), plistPath]);
  if (result.status !== 0) process.exit(result.status ?? 1);
  launchctl(["kickstart", "-k", "gui/" + process.getuid() + "/" + label], { capture: true });
  console.log("installed " + label);
  console.log("plist: " + plistPath);
  console.log("logs: " + logsDir);
}

function uninstallService() {
  if (process.platform !== "darwin") {
    console.error("uninstall currently supports macOS LaunchAgents only.");
    process.exit(1);
  }
  launchctl(["bootout", "gui/" + process.getuid(), plistPath], { capture: true });
  try {
    if (existsSync(plistPath)) {
      writeFileSync(plistPath + ".removed", readFileSync(plistPath));
      run("rm", ["-f", plistPath]);
    }
  } catch (err) {
    console.error(String(err));
    process.exit(1);
  }
  console.log("uninstalled " + label);
}

function statusService() {
  if (process.platform !== "darwin") {
    console.error("status currently supports macOS LaunchAgents only.");
    process.exit(1);
  }
  const result = launchctl(["print", "gui/" + process.getuid() + "/" + label]);
  process.exit(result.status ?? 1);
}

function logsService() {
  const outLog = join(logsDir, "bridge.out.log");
  const errLog = join(logsDir, "bridge.err.log");
  console.log("==> " + outLog);
  run("tail", ["-n", "80", outLog]);
  console.log("\n==> " + errLog);
  run("tail", ["-n", "80", errLog]);
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
else if (command === "install") installService();
else if (command === "uninstall") uninstallService();
else if (command === "status") statusService();
else if (command === "logs") logsService();
else if (command === "start") start();
else if (command === "test") test();
else {
  printHelp();
  process.exit(command === "help" || command === "--help" || command === "-h" ? 0 : 1);
}
