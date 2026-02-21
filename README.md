# AI SQL Query RAG (SQL Server + Bilingual Chat)

Chat-first assistant for querying SQL Server with natural language (English/French).

## Current capabilities

- Chat-only UI (CSV upload removed)
- Bilingual interactions (EN/FR)
- Backend `/chat` endpoint
- LLM outputs a structured query plan (not raw SQL)
- Backend validates plan and executes parameterized T-SQL
- Client-friendly answer formatting (technical fields hidden unless asked)

## Architecture

- `ui`: React + Vite chat interface
- `server`: Express + TypeScript + SQL Server (`mssql`)
- LLM provider:
  - default: Ollama (local/free)
  - optional: OpenRouter

## API

### `POST /chat`

Request:

```json
{
  "message": "do you have projects in casablanca?",
  "language": "en"
}
```

`language` is optional (`en` or `fr`).

Response:

```json
{
  "language": "en",
  "answer": "Friendly natural-language response",
  "queryPlan": {},
  "results": []
}
```

## Environment setup

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
```

If using OpenRouter instead of Ollama:

```env
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openai/gpt-4.1-mini
OPENROUTER_HTTP_REFERER=http://localhost:5173
OPENROUTER_X_TITLE=ai-sql-query-rag
```

## Run locally

1. Start backend

```bash
cd server
npm install
npm run dev
```

2. Start frontend

```bash
cd ui
npm install
npm run dev
```

3. Open the UI (usually `http://localhost:5173`)

## Ollama quick start

```bash
ollama pull mistral
ollama serve
```

If `ollama serve` says port already in use, Ollama is already running.

## Smoke tests

- EN: `do you have projects in casablanca?`
- FR: `est ce que vous avez des projets a casablanca ?`
- FR typo: `projets a casa?`
- EN: `count projects where city starts with casa`

## Requirements

See `REQUIREMENTS.md` for machine prerequisites and setup checklist.
