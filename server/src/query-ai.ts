import OpenAI from "openai";

const provider = (process.env.LLM_PROVIDER || "ollama").toLowerCase();
const ollamaBaseUrl = process.env.OLLAMA_BASE_URL || "http://127.0.0.1:11434";
const ollamaModel = process.env.OLLAMA_MODEL || "llama3.1:8b";
const openRouterModel = process.env.OPENROUTER_MODEL || "openai/gpt-4.1-mini";

async function queryOllama(systemPrompt: string, userPrompt: string, jsonMode: boolean): Promise<string> {
  const response = await fetch(`${ollamaBaseUrl}/api/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: ollamaModel,
      stream: false,
      format: jsonMode ? "json" : undefined,
      options: {
        temperature: 0.2,
      },
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Ollama request failed (${response.status}): ${text}`);
  }

  const payload = (await response.json()) as {
    message?: { content?: string };
  };

  const content = payload.message?.content?.trim();
  if (!content) {
    throw new Error("Ollama returned an empty response.");
  }

  return content;
}

async function queryOpenRouter(systemPrompt: string, userPrompt: string, jsonMode: boolean): Promise<string> {
  const apiKey = process.env.OPENROUTER_API_KEY || process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENROUTER_API_KEY (or OPENAI_API_KEY) is required when LLM_PROVIDER=openrouter.");
  }

  const openRouterClient = new OpenAI({
    apiKey,
    baseURL: "https://openrouter.ai/api/v1",
  });

  const completion = await openRouterClient.chat.completions.create(
    {
      model: openRouterModel,
      temperature: 0.2,
      response_format: jsonMode ? { type: "json_object" } : undefined,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    },
    {
      headers: {
        ...(process.env.OPENROUTER_HTTP_REFERER
          ? { "HTTP-Referer": process.env.OPENROUTER_HTTP_REFERER }
          : {}),
        ...(process.env.OPENROUTER_X_TITLE ? { "X-Title": process.env.OPENROUTER_X_TITLE } : {}),
      },
    },
  );

  const content = completion.choices?.[0]?.message?.content;
  if (!content || typeof content !== "string") {
    throw new Error("OpenRouter returned an empty response.");
  }

  return content;
}

export async function queryAI(systemPrompt: string, userPrompt: string, jsonMode: boolean = false): Promise<string> {
  if (provider === "ollama") {
    return queryOllama(systemPrompt, userPrompt, jsonMode);
  }

  if (provider === "openrouter") {
    return queryOpenRouter(systemPrompt, userPrompt, jsonMode);
  }

  throw new Error(`Unsupported LLM_PROVIDER "${provider}". Use "ollama" or "openrouter".`);
}
