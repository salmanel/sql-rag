import { AzureOpenAI } from "openai";

// Initialize Azure OpenAI client
let azureClient: AzureOpenAI | null = null;
if (process.env.LLM_PROVIDER === "azure") {
  azureClient = new AzureOpenAI({
    apiKey: process.env.AZURE_OPENAI_API_KEY,
    endpoint: process.env.AZURE_OPENAI_ENDPOINT,
    apiVersion: process.env.AZURE_OPENAI_API_VERSION,
  });
}

export async function queryAI(systemPrompt: string, userPrompt: string, jsonMode: boolean = false): Promise<string> {
  if (!azureClient) {
    throw new Error("Azure OpenAI client not initialized");
  }

  const completion = await azureClient.chat.completions.create({
    model: process.env.AZURE_OPENAI_DEPLOYMENT_NAME!,
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ],
    max_completion_tokens: 2000,
  });

  let content = completion.choices[0]?.message?.content || "";

  // o4-mini may wrap JSON in ```json ... ``` blocks since we can't use response_format
  if (jsonMode) {
    const fenced = content.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fenced) {
      content = fenced[1].trim();
    }
  }

  return content;
}
