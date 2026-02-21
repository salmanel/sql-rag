import { loadSchemaAllowlist, renderAllowlistForPrompt } from "./allowlist";
import { queryAI } from "./query-ai";
import { query } from "./db";
import {
  ChatLanguage,
  QueryPlan,
  SchemaAllowlist,
  compileQueryPlan,
  validateAgainstAllowlist,
  validateQueryPlan,
} from "./query-plan";

interface ChatResponse {
  language: ChatLanguage;
  answer: string;
  queryPlan: QueryPlan;
  results: Record<string, unknown>[];
}

const TECHNICAL_COLUMN_PATTERNS = [
  /^id$/i,
  /_id$/i,
  /uuid/i,
  /guid/i,
  /lat/i,
  /lng/i,
  /lon/i,
  /coord/i,
];

function includesAny(text: string, terms: string[]): boolean {
  const lower = text.toLowerCase();
  return terms.some((term) => lower.includes(term));
}

function normalizeMessageForPlanner(message: string): string {
  let normalized = message.trim();

  // Light typo normalization for high-impact intent words.
  const replacements: Array<[RegExp, string]> = [
    [/\bwcount\b/gi, "count"],
    [/\bcoutn\b/gi, "count"],
    [/\bcout\b/gi, "count"],
    [/\bcmpt\b/gi, "count"],
    [/\bcombine\b/gi, "combien"],
    [/\bstart with\b/gi, "starts with"],
  ];

  for (const [pattern, value] of replacements) {
    normalized = normalized.replace(pattern, value);
  }

  return normalized;
}

function applyIntentAndOperatorHints(plan: QueryPlan, message: string): QueryPlan {
  const lower = message.toLowerCase();
  const countHint = includesAny(lower, ["count", "how many", "number of", "combien", "nombre"]);
  const startsWithHint = includesAny(lower, ["starts with", "start with", "commence par", "commen", "debute par"]);
  const containsHint = includesAny(lower, ["contains", "contain", "contient", "include", "includes"]);

  const next: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  if (countHint) {
    next.intent = {
      ...next.intent,
      type: "count",
      aggregation: undefined,
    };
  }

  if (startsWithHint || containsHint) {
    next.filters = next.filters.map((filter) => {
      const isTextLike = typeof filter.value === "string";
      if (!isTextLike) {
        return filter;
      }

      if (startsWithHint) {
        return { ...filter, operator: "starts_with" };
      }

      if (containsHint && filter.operator !== "starts_with") {
        return { ...filter, operator: "contains" };
      }

      return filter;
    });
  }

  return next;
}

function hasColumn(columns: Set<string>, candidates: string[]): boolean {
  return candidates.some((candidate) => columns.has(candidate.toLowerCase()));
}

function findColumn(columns: Set<string>, candidates: string[]): string | null {
  for (const candidate of candidates) {
    if (columns.has(candidate.toLowerCase())) {
      return candidate.toLowerCase();
    }
  }
  return null;
}

function normalizeSemanticFilters(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  if (plan.filters.length === 0) {
    return plan;
  }

  const normalized: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  const cityTerms = ["city", "ville"];
  const addressTerms = ["address", "adresse", "localisation", "location"];
  const cityColumn = findColumn(availableColumns, ["city", "ville"]);
  const addressColumn = findColumn(availableColumns, ["address", "adresse", "full_address", "street_address"]);

  // Prefer specific city fields over generic location/address when user asks city-level questions.
  if (cityColumn && includesAny(message, cityTerms)) {
    normalized.filters = normalized.filters.map((filter) => {
      const field = filter.field.toLowerCase();
      if (["address", "adresse", "location", "full_address", "street_address"].includes(field)) {
        return { ...filter, field: cityColumn };
      }
      return filter;
    });
  } else if (addressColumn && includesAny(message, addressTerms)) {
    // Prefer address fields when user explicitly asks for address.
    normalized.filters = normalized.filters.map((filter) => {
      if (filter.field.toLowerCase() === "city") {
        return { ...filter, field: addressColumn };
      }
      return filter;
    });
  }

  // Remove duplicate semantic filters like city=casablanca and address=casablanca.
  const seen = new Set<string>();
  normalized.filters = normalized.filters.filter((filter) => {
    const key = `${String(filter.value).toLowerCase()}::${filter.operator}`;
    const field = filter.field.toLowerCase();
    const semanticField =
      cityColumn && [cityColumn, "address", "adresse", "location", "full_address", "street_address"].includes(field)
        ? cityColumn
        : field;
    const semanticKey = `${semanticField}::${key}`;
    if (seen.has(semanticKey)) {
      return false;
    }
    seen.add(semanticKey);
    return true;
  });

  return normalized;
}

function applyClientFriendlyProjection(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  if (plan.intent.type !== "lookup") {
    return plan;
  }

  const explicitlyAsksTechnicalFields = includesAny(message, [
    "id",
    "uuid",
    "guid",
    "coord",
    "coordinate",
    "latitude",
    "longitude",
    "lat",
    "lng",
    "location",
  ]);

  if (explicitlyAsksTechnicalFields) {
    return plan;
  }

  const preferred = [
    "name",
    "project_name",
    "title",
    "address",
    "full_address",
    "street_address",
    "description",
    "details",
    "summary",
  ];

  const blockedPatterns = [
    /^id$/,
    /_id$/,
    /uuid/,
    /guid/,
    /lat/,
    /lng/,
    /lon/,
    /coord/,
    /^location$/,
  ];

  const selected = preferred.filter((column) => availableColumns.has(column));
  if (selected.length > 0) {
    return {
      ...plan,
      intent: {
        ...plan.intent,
        columns: selected,
      },
    };
  }

  const fallbackColumns = Array.from(availableColumns).filter(
    (column) => !blockedPatterns.some((pattern) => pattern.test(column)),
  );

  if (fallbackColumns.length === 0) {
    return plan;
  }

  return {
    ...plan,
    intent: {
      ...plan.intent,
      columns: fallbackColumns.slice(0, 6),
    },
  };
}

function dedupeRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length <= 1) {
    return rows;
  }

  const keys = Object.keys(rows[0] || {});
  const hasName = keys.some((key) => key.toLowerCase() === "name");
  const hasDescription = keys.some((key) => key.toLowerCase() === "description");
  const preferredKeys =
    hasName && hasDescription
      ? keys.filter((key) => ["name", "description"].includes(key.toLowerCase()))
      : keys.filter((key) => !TECHNICAL_COLUMN_PATTERNS.some((pattern) => pattern.test(key)));

  const stableKeys = preferredKeys.length > 0 ? preferredKeys : keys;
  const seen = new Set<string>();

  return rows.filter((row) => {
    const fingerprint = stableKeys
      .map((key) => String(row[key] ?? "").trim().toLowerCase())
      .join("|");
    if (!fingerprint) {
      return true;
    }
    if (seen.has(fingerprint)) {
      return false;
    }
    seen.add(fingerprint);
    return true;
  });
}

function isTechnicalKey(key: string): boolean {
  const lower = key.toLowerCase();
  return (
    lower === "id" ||
    lower.endsWith("_id") ||
    lower.includes("uuid") ||
    lower.includes("guid") ||
    lower.includes("coord") ||
    lower.includes("lat") ||
    lower.includes("lng") ||
    lower.includes("lon")
  );
}

function sanitizeRowsForResponder(
  rows: Record<string, unknown>[],
  message: string,
): Record<string, unknown>[] {
  if (rows.length === 0) {
    return rows;
  }

  const asksTechnical = includesAny(message, [
    "id",
    "uuid",
    "guid",
    "coordinate",
    "coordinates",
    "location",
    "latitude",
    "longitude",
    "lat",
    "lng",
  ]);

  if (asksTechnical) {
    return rows;
  }

  return rows.map((row) => {
    const clean: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (!isTechnicalKey(key)) {
        clean[key] = value;
      }
    }
    return Object.keys(clean).length > 0 ? clean : row;
  });
}

function coerceLanguage(language: unknown): ChatLanguage | null {
  if (language === "en" || language === "fr") {
    return language;
  }
  return null;
}

async function detectLanguage(message: string): Promise<ChatLanguage> {
  const response = await queryAI(
    `Detect if the user message is French or English.
Return JSON only: {"language":"fr"} or {"language":"en"}.
If uncertain, return {"language":"en"}.`,
    message,
    true,
  );

  try {
    const parsed = JSON.parse(response) as { language?: string };
    return parsed.language === "fr" ? "fr" : "en";
  } catch {
    return "en";
  }
}

async function generateQueryPlan(
  message: string,
  language: ChatLanguage,
  allowlist: SchemaAllowlist,
): Promise<QueryPlan> {
  const normalizedMessage = normalizeMessageForPlanner(message);
  const schemaPrompt = renderAllowlistForPrompt(allowlist);
  const operators = Array.from(allowlist.operators).join(", ");

  const response = await queryAI(
    `You generate strict QueryPlan JSON for SQL Server.
Always output a JSON object and nothing else.

Schema:
{
  "intent": {
    "type": "lookup" | "count" | "aggregate",
    "table": "string",
    "columns": ["string"],
    "aggregation": { "func": "sum" | "avg" | "min" | "max", "column": "string" }
  },
  "filters": [
    { "field": "string", "operator": "${operators}", "value": "string|number|boolean|array" }
  ],
  "limit": 1-500,
  "sort": { "field": "string", "direction": "asc" | "desc" }
}

Rules:
- Use semantic understanding for fields (e.g. city and address can refer to similar location concepts).
- Use only allowlisted table and column names.
- Keep filters empty when not needed.
- For lookup, include columns when possible.
- For count, do not include aggregation.
- For aggregate, include aggregation.
- Do not invent fields or tables.
- Use language "${language}" only for interpreting user intent, not for changing field names.`,
    `Allowlisted tables and columns:
${schemaPrompt}

User message:
${normalizedMessage}`,
    true,
  );

  const parsed = JSON.parse(response) as unknown;
  const shapeValidation = validateQueryPlan(parsed);
  if (!shapeValidation.ok) {
    throw new Error(`Invalid QueryPlan JSON: ${shapeValidation.errors.join(" ")}`);
  }

  const allowlistValidation = validateAgainstAllowlist(shapeValidation.plan, allowlist);
  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  return applyIntentAndOperatorHints(shapeValidation.plan, normalizedMessage);
}

async function generateAnswer(
  language: ChatLanguage,
  message: string,
  plan: QueryPlan,
  rows: Record<string, unknown>[],
): Promise<string> {
  const presentationRows = sanitizeRowsForResponder(rows, message);
  const response = await queryAI(
    `You are a client-facing real-estate assistant.
Reply in ${language === "fr" ? "French" : "English"}.
Use only the provided query results and never invent missing facts.
Keep a warm, concise, professional tone.
Do not mention SQL, query plans, allowlists, backend, internal validation, or system errors.
Do not expose technical identifiers (id/uuid/coordinates) unless the user explicitly asked for them.
When there are results:
- Prefer project name, address/city, and description when available.
- Summarize naturally instead of dumping every field value.
- If there are many rows, provide a compact summary plus 2-5 representative examples.
When there are no results:
- Say this politely.
- Suggest one concrete rephrasing in the same language.
Return JSON only with shape: {"answer":"string"}.`,
    `User question:
${message}

QueryPlan:
${JSON.stringify(plan)}

Rows:
${JSON.stringify(presentationRows)}`,
    true,
  );

  try {
    const parsed = JSON.parse(response) as { answer?: string };
    if (typeof parsed.answer === "string" && parsed.answer.trim().length > 0) {
      return parsed.answer;
    }
  } catch {
    // Fall through.
  }

  return language === "fr"
    ? "Je n'ai pas pu formater une reponse claire, mais les resultats sont fournis."
    : "I could not format a clear answer, but the query results are provided.";
}

export async function chat(message: string, requestedLanguage?: unknown): Promise<ChatResponse> {
  const resolvedLanguage = coerceLanguage(requestedLanguage) ?? (await detectLanguage(message));
  const allowlist = await loadSchemaAllowlist();
  const plan = await generateQueryPlan(message, resolvedLanguage, allowlist);
  const allowlistValidation = validateAgainstAllowlist(plan, allowlist);

  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  const normalizedPlan = normalizeSemanticFilters(plan, allowlistValidation.table.columns, message);
  const adjustedPlan = applyClientFriendlyProjection(normalizedPlan, allowlistValidation.table.columns, message);
  const { sql, params } = compileQueryPlan(adjustedPlan, allowlistValidation.table);
  const dbResult = await query(sql, params);
  const rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);
  const answer = await generateAnswer(resolvedLanguage, message, adjustedPlan, rows);

  return {
    language: resolvedLanguage,
    answer,
    queryPlan: adjustedPlan,
    results: rows,
  };
}
