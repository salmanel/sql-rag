import "dotenv/config";
import cors from "cors";
import express from "express";
import { chat } from "./chat-service";
import { initializeDb } from "./db";

interface ChatRequestBody {
  message?: unknown;
  language?: unknown;
}

async function startServer() {
  await initializeDb();

  const app = express();
  app.use(cors());
  app.use(express.json());

  const handleChatRequest = async (req: express.Request, res: express.Response) => {
    try {
      const { message, language } = (req.body ?? {}) as ChatRequestBody;
      if (typeof message !== "string" || message.trim().length === 0) {
        return res.status(400).json({ error: "message is required" });
      }

      const result = await chat(message, language);
      return res.json(result);
    } catch (error: any) {
      console.error("Chat error:", error);
      return res.status(500).json({ error: error.message || "Failed to process chat request" });
    }
  };

  // Backward-compatible route used by the current UI.
  app.post("/chat", handleChatRequest);
  // Versioned API route for external project integration.
  app.post("/api/v1/chat", handleChatRequest);

  // Lightweight health endpoint for API consumers and deploy checks.
  app.get("/api/v1/health", (_req, res) => {
    res.json({ ok: true });
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

startServer().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});
