# Requirements

Use this checklist to run the project on a new machine.

## 1) Software

- Node.js `>= 18` (recommended: Node 20 LTS)
- npm `>= 9`
- Git
- SQL Server access (local SQL Server, Azure SQL, or SSMS-accessible instance)
- One LLM provider:
  - Ollama (default), or
  - OpenRouter (optional)

## 2) Network / Access

- Backend needs TCP access to SQL Server host/port (usually `1433`)
- If using Azure SQL, your machine IP must be allowlisted in SQL server firewall
- If using OpenRouter, outbound HTTPS to `https://openrouter.ai` must be allowed

## 3) Project setup

From repo root:

```bash
cd server
npm install
```

```bash
cd ../ui
npm install
```

## 4) Environment variables

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

OpenRouter alternative:

```env
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openai/gpt-4.1-mini
OPENROUTER_HTTP_REFERER=http://localhost:5173
OPENROUTER_X_TITLE=ai-sql-query-rag
```

## 5) LLM setup

For Ollama:

```bash
ollama pull mistral
ollama serve
```

If `ollama serve` reports port in use, Ollama is already running.

## 6) Run app

Backend:

```bash
cd server
npm run dev
```

Frontend:

```bash
cd ui
npm run dev
```

## 7) Verify

- Backend log shows: `Server running on port 3000`
- Open UI at `http://localhost:5173`
- Ask:
  - `do you have projects in casablanca?`
  - `est ce que vous avez des projets a casablanca ?`
