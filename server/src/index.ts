import * as dotenv from "dotenv";
import path from "path";

// .env is in server/ folder, __dirname is server/src/
const envPath = path.resolve(__dirname, "..", ".env");
const result = dotenv.config({ path: envPath });

console.log("=== ENV DEBUG ===");
console.log("Looking for .env at:", envPath);
console.log("dotenv result:", result.error ? result.error.message : "loaded successfully");
console.log("DB_HOST:", process.env.DB_HOST || "(not set)");
console.log("DB_USER:", process.env.DB_USER || "(not set)");
console.log("=================");

import cors from "cors";
import express from "express";
import { chat } from "./chat-service";
import { initializeDb, closeDb } from "./db";

// Simple in-memory rate limiter (no external dependency)
function createRateLimiter(windowMs: number, maxRequests: number) {
  const hits = new Map<string, { count: number; resetAt: number }>();
  return (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const ip = req.ip || "unknown";
    const now = Date.now();
    const record = hits.get(ip);
    if (!record || now > record.resetAt) {
      hits.set(ip, { count: 1, resetAt: now + windowMs });
      return next();
    }
    record.count++;
    if (record.count > maxRequests) {
      return res.status(429).json({ error: "Too many requests, please try again later." });
    }
    return next();
  };
}

interface ChatRequestBody {
  message?: unknown;
  language?: unknown;
}

async function startServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  const limiter = createRateLimiter(60 * 1000, 20); // 20 req/min per IP
  app.use("/api/chat", limiter);
  app.use("/chat", limiter);

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

  app.post("/chat", handleChatRequest);
  app.post("/api/v1/chat", handleChatRequest);

  app.get("/api/v1/health", (_req, res) => {
    res.json({ ok: true });
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

async function main() {
  try {
    console.log("Connecting to database...");
    await initializeDb();
    console.log("Database connected successfully!");
    await startServer();
  } catch (err) {
    console.error("Database connection failed:", (err as Error).message);
    process.exit(1);
  }
}

process.on("SIGINT", async () => {
  console.log("\nClosing database connection...");
  await closeDb();
  process.exit(0);
});

main().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});
