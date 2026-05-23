import {
  DEFAULT_MODEL,
  LOCAL_API_KEY,
  PROVIDER_ID,
  PROVIDER_LABEL,
  buildProviderConfig,
} from "./provider.mjs";

function normalizeHeaderValue(value) {
  if (typeof value !== "string") return "";
  return value.trim().replace(/[\r\n]+/g, " ");
}

function resolveTransportTurnState(ctx) {
  const sessionId = normalizeHeaderValue(ctx.sessionId);
  if (!sessionId) return null;
  const turnId = normalizeHeaderValue(ctx.turnId);
  const attempt = String(Math.max(1, Number(ctx.attempt) || 1));
  return {
    headers: {
      "x-openclaw-session-id": sessionId,
      "x-openclaw-turn-id": turnId,
      "x-openclaw-turn-attempt": attempt,
    },
    metadata: {
      openclaw_session_id: sessionId,
      openclaw_turn_id: turnId,
      openclaw_turn_attempt: attempt,
      openclaw_transport: ctx.transport,
    },
  };
}

function buildTurnHeaders(options) {
  const sessionId = normalizeHeaderValue(options?.sessionId);
  if (!sessionId) return null;
  const turnId = normalizeHeaderValue(options?.turnId || options?.runId || "");
  const attempt = String(Math.max(1, Number(options?.attempt) || 1));
  const workspaceDir = normalizeHeaderValue(options?.workspaceDir || options?.cwd || "");
  return {
    "x-openclaw-session-id": sessionId,
    ...(turnId ? { "x-openclaw-turn-id": turnId } : {}),
    "x-openclaw-turn-attempt": attempt,
    ...(workspaceDir ? { "x-openclaw-workspace-dir": workspaceDir } : {}),
  };
}

function buildProvider() {
  return {
    id: PROVIDER_ID,
    label: PROVIDER_LABEL,
    docsPath: "/providers/google-antigravity",
    envVars: ["GEMINI_API_KEY"],
    auth: [
      {
        id: "local-bridge",
        label: "Local Google Harness bridge",
        hint: "Uses the local bridge; Gemini auth is resolved inside bridge/server.py",
        kind: "custom",
        run: async () => ({
          profiles: [
            {
              profileId: "local-bridge",
              credential: {
                type: "api_key",
                provider: PROVIDER_ID,
                key: LOCAL_API_KEY,
              },
            },
          ],
          configPatch: {
            models: {
              providers: {
                [PROVIDER_ID]: buildProviderConfig(),
              },
            },
          },
          defaultModel: DEFAULT_MODEL,
          notes: ["Google Harness bridge provider configured for local OpenAI-compatible chat completions."],
        }),
        runNonInteractive: async (ctx) => ({
          ...ctx.config,
          models: {
            ...(ctx.config.models ?? {}),
            providers: {
              ...(ctx.config.models?.providers ?? {}),
              [PROVIDER_ID]: buildProviderConfig(ctx.config),
            },
          },
        }),
      },
    ],
    catalog: {
      order: "simple",
      run: async (ctx) => ({
        provider: buildProviderConfig(ctx.config),
      }),
    },
    staticCatalog: {
      order: "simple",
      run: async (ctx) => ({
        provider: buildProviderConfig(ctx.config),
      }),
    },
    resolveSyntheticAuth: () => ({
      apiKey: LOCAL_API_KEY,
      source: "google-antigravity local bridge",
      mode: "api-key",
    }),
    shouldDeferSyntheticProfileAuth: ({ resolvedApiKey }) => resolvedApiKey === LOCAL_API_KEY,
    resolveTransportTurnState,
    wrapStreamFn: ({ streamFn }) => {
      if (!streamFn) return streamFn;
      return (model, context, options = {}) => {
        const turnHeaders = buildTurnHeaders(options);
        if (!turnHeaders) return streamFn(model, context, options);
        return streamFn(model, context, {
          ...options,
          headers: {
            ...(options.headers ?? {}),
            ...turnHeaders,
          },
        });
      };
    },
  };
}

export default {
  id: PROVIDER_ID,
  name: PROVIDER_LABEL,
  description: "Google Antigravity localharness provider for OpenClaw.",
  register(api) {
    api.registerProvider(buildProvider());
  },
};
