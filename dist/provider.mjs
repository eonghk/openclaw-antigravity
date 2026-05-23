export const PROVIDER_ID = "google-antigravity";
export const PROVIDER_LABEL = "Google Harness";
export const DEFAULT_MODEL = "gemini-3.5-flash";
export const DEFAULT_PORT = 8080;
export const LOCAL_API_KEY = "custom-local";

export const MODELS = [
  {
    id: "gemini-3.5-flash",
    name: "Gemini 3.5 Flash (Harness)",
    contextTokens: 1048576,
    maxTokens: 65536,
  },
  {
    id: "gemini-3.1-pro",
    name: "Gemini 3.1 Pro (Harness)",
    contextTokens: 1048576,
    maxTokens: 65536,
  },
  {
    id: "gemini-3.1-flash",
    name: "Gemini 3.1 Flash (Harness)",
    contextTokens: 1048576,
    maxTokens: 65536,
  },
];

export function resolveBridgePort(config) {
  const raw = config?.plugins?.entries?.[PROVIDER_ID]?.port;
  return Number.isInteger(raw) && raw > 0 ? raw : DEFAULT_PORT;
}

export function resolveBridgeBaseUrl(config) {
  const configured = config?.models?.providers?.[PROVIDER_ID]?.baseUrl;
  if (typeof configured === "string" && configured.trim()) {
    return configured.trim().replace(/\/+$/, "");
  }
  return `http://127.0.0.1:${resolveBridgePort(config)}/v1`;
}

export function buildProviderConfig(config) {
  const baseUrl = resolveBridgeBaseUrl(config);
  return {
    baseUrl,
    api: "openai-completions",
    apiKey: LOCAL_API_KEY,
    models: MODELS.map((model) => ({
      ...model,
      api: "openai-completions",
      baseUrl,
    })),
  };
}
