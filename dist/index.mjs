// Google Harness OpenClaw plugin entry point
// Registers the google-antigravity provider and runtime

import providerCatalog from "./provider.mjs";

// Lifecycle hooks
let bridgeProcess = null;

export function onActivate(ctx) {
  ctx.registerProvider(providerCatalog);
}

export function onDeactivate() {
  if (bridgeProcess) {
    bridgeProcess.kill();
    bridgeProcess = null;
  }
}

// Tell OpenClaw which harness types this plugin handles
export const harnessRuntime = "google-antigravity";

// When OpenClaw routes a request through google-antigravity,
// it hits the bridge HTTP API
export function createRuntimeConfig(provider, model, config) {
  const bridgePort = config?.plugins?.entries?.["google-antigravity"]?.port ?? 8080;
  return {
    baseUrl: `http://127.0.0.1:${bridgePort}`,
    models: [model],
  };
}
