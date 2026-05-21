// Google Harness provider registration
// Registers google-antigravity as a custom provider backed by the Python bridge

export default {
  id: "google-antigravity",
  name: "Google Harness",
  description: "Gemini models via Antigravity localharness runtime",
  
  // Model catalog — models this provider supports
  catalog: [
    { id: "gemini-3.5-flash",   context: 1048576,  output: 65536,  aliases: [] },
    { id: "gemini-3.1-pro",     context: 1048576,  output: 65536,  aliases: ["gemini"] },
    { id: "gemini-3.1-flash",   context: 1048576,  output: 65536,  aliases: [] },
  ],
  
  // Default model when none specified
  defaultModel: "gemini-3.5-flash",
  
  // Auth: uses Gemini API key, passed through to bridge
  auth: {
    type: "apiKey",
    credentials: ["GEMINI_API_KEY"],
  },
  
  // Onboard config (for `openclaw onboard --google-antigravity`)
  onboard: {
    authChoice: "gemini-api-key",
    defaultModel: "gemini-3.5-flash",
  },
};
