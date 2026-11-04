// Deliberately passes raw Chat replies through. A gateway regression must
// not depend on the real WorkBuddy plugin classifying an empty reply first.
const id = process.env.FAKE_ID || "workbuddy"

export const EmptyReplyPlugin = async () => ({
  config: async (cfg) => {
    cfg.provider ??= {}
    cfg.provider[id] = {
      name: "WorkBuddy",
      npm: "@ai-sdk/openai-compatible",
      api: process.env.FAKE_BASE,
      models: Object.fromEntries(["gpt-5.6-sol", "claude-opus-5.5"].map((model) => [model, {
        name: model,
        reasoning: true,
        tool_call: true,
        limit: { context: 1000000, output: 65536 },
        variants: Object.fromEntries(["low", "medium", "high", "xhigh", "max"].map((effort) => [effort, { reasoningEffort: effort }])),
      }])),
    }
  },
  auth: {
    provider: id,
    methods: [{ type: "api", label: "Fixture key" }],
    loader: async () => ({
      baseURL: process.env.FAKE_BASE,
      apiKey: "fixture-only",
      fetch: (url, init) => fetch(url, init),
    }),
  },
})
