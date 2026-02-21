import "dotenv/config";
import cors from "cors";
import express from "express";
import { chat } from "./chat-service";
import { initializeDb } from "./db";

async function startServer() {
  await initializeDb();

  const app = express();
  app.use(cors());
  app.use(express.json());

  app.post("/chat", async (req, res) => {
    try {
      const { message, language } = req.body ?? {};
      if (typeof message !== "string" || message.trim().length === 0) {
        return res.status(400).json({ error: "message is required" });
      }

      const result = await chat(message, language);
      res.json(result);
    } catch (error: any) {
      console.error("Chat error:", error);
      res.status(500).json({ error: error.message || "Failed to process chat request" });
    }
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  });
}

startServer().catch(error => {
  console.error('Failed to start server:', error);
  process.exit(1);
}); 
