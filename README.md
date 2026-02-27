# AI SQL Query RAG

Chat-first assistant to query SQL Server in natural language (English/French), with safe backend execution and a client-friendly UI.

## What This Project Does

- Chat-only UX (no CSV upload flow)
- Bilingual conversation (`en` / `fr`)
- LLM planner returns structured query intent (no raw SQL generation)
- Backend validates and executes parameterized SQL Server queries
- Client-friendly responses (hides technical IDs/coordinates unless requested)
- Clickable suggestions in chat
- Light/Dark theme switch in UI
- Session history sidebar with independent scrolling

## Stack

- `ui`: React + Vite + TypeScript
- `server`: Express + TypeScript + SQL Server (`mssql`)
- LLM provider:
  - default: Ollama (local)
  - optional: OpenRouter

## API Endpoints

### `POST /chat` (legacy-compatible)
### `POST /api/v1/chat` (versioned)

Request body:

```json
{
  "message": "Show me available projects in Casablanca",
  "language": "en"
}
```

`language` is optional (`en` or `fr`).

Typical response:

```json
{
  "language": "en",
  "status": "ok",
  "answer": "Friendly response text",
  "suggestions": [
    { "label": "Rabat", "payload": "projects in rabat", "type": "city" }
  ],
  "queryPlan": {},
  "results": []
}
```

### `GET /api/v1/health`

Response:

```json
{ "ok": true }
```

## Environment Setup

Create `server/.env`:

```env
DB_USER=your_sql_user
DB_PASSWORD=your_sql_password
DB_HOST=your_sql_host
DB_PORT=1433
DB_NAME=your_database
DB_ENCRYPT=true
DB_TRUST_SERVER_CERT=false
PORT=3000

LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://127.0.0.1:11434
OLLAMA_MODEL=mistral

# Optional troubleshooting
DEBUG_SQL=false
```

If using OpenRouter:

```env
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openai/gpt-4.1-mini
OPENROUTER_HTTP_REFERER=http://localhost:5173
OPENROUTER_X_TITLE=ai-sql-query-rag
```

## Run Locally

### Backend

```bash
cd server
npm install
npm run dev
```

### Frontend

```bash
cd ui
npm install
npm run dev
```

Open UI at `http://localhost:5173`.

## Try The API

Health:

```bash
curl http://localhost:3000/api/v1/health
```

Chat:

```bash
curl -X POST "http://localhost:3000/api/v1/chat" \
  -H "Content-Type: application/json" \
  -d "{\"message\":\"Show me available projects in Casablanca\",\"language\":\"en\"}"
```

Windows `cmd` (single line):

```bat
curl -X POST "http://localhost:3000/api/v1/chat" -H "Content-Type: application/json" -d "{\"message\":\"Show me available projects in Casablanca\",\"language\":\"en\"}"
```

## Prompt Examples

- `Show me available projects in Casablanca`
- `Show me projects currently in progress`
- `Tell me about 3-bedroom apartments`
- `Show me the latest listings`
- `Montre-moi les projets disponibles a Casablanca`

## Notes

- Project includes deterministic handling for key starter prompts to improve reliability.
- SQL debugging logs can be enabled with `DEBUG_SQL=true`.
- Keep `.env` and sensitive assets out of git.

## Requirements

See `REQUIREMENTS.md` for prerequisites and setup checklist.
