import { loadSchemaAllowlist, renderAllowlistForPrompt } from "./allowlist";
import { queryAI } from "./query-ai";
import { query } from "./db";
import { buildChatSuggestions, SuggestionChip } from "./suggestions";
import {
  AllowlistTable,
  ChatLanguage,
  PlannerConcept,
  PlannerStatus,
  QueryPlan,
  SchemaAllowlist,
  compileQueryPlan,
  validatePlannerQueryPlan,
  validateAgainstAllowlist,
  validateQueryPlan,
} from "./query-plan";

interface ChatResponse {
  language: ChatLanguage;
  status?: PlannerStatus;
  answer: string;
  follow_up_question?: string;
  suggestions?: SuggestionChip[];
  queryPlan: QueryPlan;
  results: Record<string, unknown>[];
}

interface PlannerOutcome {
  executablePlan: QueryPlan;
  plannerLanguage: ChatLanguage;
  plannerStatus: PlannerStatus;
  askUser: string | null;
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

const KNOWN_CITY_TOKENS = new Set([
  "casablanca",
  "casa",
  "rabat",
  "zenata",
  "marrakech",
  "tanger",
  "tangier",
  "agadir",
]);

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

function shouldAutoListProjects(message: string): boolean {
  const lower = message.trim().toLowerCase();
  const projectOnlyPatterns = [
    /^projects?$/,
    /^project$/,
    /^projets?$/,
    /^projet$/,
    /^list( all)? projects?$/,
    /^show( me)? projects?$/,
    /^can you list( all)? projects?$/,
  ];
  return projectOnlyPatterns.some((pattern) => pattern.test(lower));
}

function applyConceptSelectionHints(
  plannerPlan: {
    select: PlannerConcept[];
    notes: string;
  },
  message: string,
): {
  select: PlannerConcept[];
  notes: string;
} {
  const lower = message.toLowerCase();
  const hinted = new Set<PlannerConcept>(plannerPlan.select);

  if (includesAny(lower, ["price", "prices", "prix", "tarif", "tarifs", "cost", "budget", "montant"])) {
    hinted.add("PRICE");
  }

  if (includesAny(lower, ["surface", "area", "sqm", "m2", "superficie"])) {
    hinted.add("SURFACE");
  }

  if (includesAny(lower, ["status", "state", "etat", "disponible", "disponibilite"])) {
    hinted.add("PROJECT_STATUS");
  }

  if (includesAny(lower, ["description", "details", "desc"])) {
    hinted.add("PROJECT_DESCRIPTION");
  }

  if (includesAny(lower, ["name", "nom", "project"])) {
    hinted.add("PROJECT_NAME");
  }

  return {
    select: Array.from(hinted),
    notes: `${plannerPlan.notes} | concept-hints-applied`,
  };
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

function normalizeLocationTerm(term: string): string {
  const map: Record<string, string> = {
    casa: "casablanca",
    casaa: "casablanca",
    casablanca: "casablanca",
    rabat: "rabat",
    zenata: "zenata",
    tanger: "tanger",
    tangier: "tanger",
    marrakech: "marrakech",
    agadir: "agadir",
  };
  const key = term.trim().toLowerCase();
  return map[key] || key;
}

function normalizeForParsing(text: string): string {
  return text
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function removeNoisyNameFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const nameCandidates = ["name", "project_name", "title", "nom"];
  const nameColumns = new Set(
    nameCandidates.filter((candidate) => availableColumns.has(candidate)).map((v) => v.toLowerCase()),
  );
  const genericValues = new Set(["project", "projects", "projet", "projets", "realestate", "residence", "residences"]);

  const cleaned = plan.filters.filter((filter) => {
    const isNameField = nameColumns.has(filter.field.toLowerCase());
    const isGenericValue = typeof filter.value === "string" && genericValues.has(filter.value.trim().toLowerCase());
    return !(isNameField && isGenericValue);
  });

  return {
    ...plan,
    filters: cleaned,
  };
}

function removeAmbiguousAvailabilityFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const statusCandidates = ["status", "state", "etat", "availability", "disponibilite"];
  const statusColumns = new Set(
    statusCandidates.filter((candidate) => availableColumns.has(candidate)).map((v) => v.toLowerCase()),
  );

  const ambiguousStatusValues = new Set([
    "available",
    "availability",
    "disponible",
    "disponibilite",
    "available projects",
  ]);

  const filtered = plan.filters.filter((filter) => {
    const isStatusField = statusColumns.has(filter.field.toLowerCase());
    if (!isStatusField || typeof filter.value !== "string") return true;
    const value = filter.value.trim().toLowerCase();
    return !ambiguousStatusValues.has(value);
  });

  return {
    ...plan,
    filters: filtered,
  };
}

function enforceLocationFilterFromMessage(plan: QueryPlan, availableColumns: Set<string>, message: string): QueryPlan {
  if (plan.intent.type !== "lookup") return plan;
  const rawTerm = extractLikelyLocationTerm(message);
  if (!rawTerm) return plan;
  const term = normalizeLocationTerm(rawTerm);

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return plan;
  const addressPriority = ["address", "adresse", "full_address", "street_address"];
  const addressColumn = addressPriority.find((col) => availableColumns.has(col));
  const primaryColumn = addressColumn || locationColumns[0];
  const useContains = addressPriority.includes(primaryColumn);

  const hasLocationFilter = plan.filters.some((f) => hasLocationLikeColumn(f.field));
  if (hasLocationFilter) {
    return {
      ...plan,
      filters: plan.filters.map((f) => {
        if (!hasLocationLikeColumn(f.field)) return f;
        if (typeof f.value !== "string") return f;
        return {
          ...f,
          field: primaryColumn,
          operator: useContains ? "contains" : "eq",
          value: normalizeLocationTerm(f.value),
        };
      }),
    };
  }

  return {
    ...plan,
    filters: [
      ...plan.filters,
      {
        field: primaryColumn,
        operator: useContains ? "contains" : "eq",
        value: term,
      },
    ],
  };
}

function applyDirectProjectLocationOverride(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  const lower = message.toLowerCase().trim();
  const looksLikeProjectLocationQuery =
    /(project|projects|projet|projets)/.test(lower) &&
    /\b(in|a|au|à|dans)\b/.test(lower);

  if (!looksLikeProjectLocationQuery) {
    return plan;
  }

  const rawTerm = extractLikelyLocationTerm(message);
  if (!rawTerm) {
    return plan;
  }

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) {
    return plan;
  }

  const addressPriority = ["address", "adresse", "full_address", "street_address"];
  const addressColumn = addressPriority.find((col) => availableColumns.has(col));
  const primaryColumn = addressColumn || locationColumns[0];
  const term = normalizeLocationTerm(rawTerm);

  // Deterministic override for broad queries: keep only location filter.
  return {
    ...plan,
    filters: [
      {
        field: primaryColumn,
        operator: "contains",
        value: term,
      },
    ],
  };
}

function hasLocationLikeColumn(column: string): boolean {
  return /(city|ville|address|adresse|location|quartier|zone|secteur|localisation)/i.test(column);
}

function isGeoLikeColumn(column: string): boolean {
  return /(lat|lng|lon|coord|latitude|longitude)/i.test(column) || /^location$/i.test(column);
}

function rankLocationColumns(columns: string[]): string[] {
  const score = (col: string): number => {
    const lower = col.toLowerCase();
    let s = 0;
    if (/city|ville/.test(lower)) s += 100;
    if (/address|adresse|street_address|full_address/.test(lower)) s += 90;
    if (/quartier|zone|secteur|localisation/.test(lower)) s += 75;
    if (/location/.test(lower)) s += 40;
    if (isGeoLikeColumn(lower)) s -= 60;
    return s;
  };
  return [...columns].sort((a, b) => score(b) - score(a));
}

function extractLikelyLocationTerm(message: string): string | null {
  const cleaned = normalizeForParsing(message)
    .replace(/[^a-z0-9\u00c0-\u017f\s-]/gi, " ")
    .trim();
  if (!cleaned) return null;

  const words = cleaned.split(/\s+/).filter(Boolean);
  for (const word of words) {
    if (KNOWN_CITY_TOKENS.has(word)) {
      return normalizeLocationTerm(word);
    }
  }

  const prepositions = new Set(["in", "a", "au", "dans", "near", "pres", "de"]);
  const stopwords = new Set([
    "what",
    "about",
    "location",
    "city",
    "ville",
    "where",
    "in",
    "at",
    "de",
    "des",
    "du",
    "la",
    "le",
    "les",
    "au",
    "aux",
    "dans",
    "projects",
    "project",
    "projets",
    "projet",
    "known",
    "available",
    "show",
    "list",
    "have",
    "you",
    "your",
    "the",
    "all",
    "are",
    "is",
  ]);

  for (let i = 0; i < words.length - 1; i++) {
    if (!prepositions.has(words[i])) continue;
    const candidate = words[i + 1];
    if (candidate.length > 1 && !stopwords.has(candidate)) {
      return normalizeLocationTerm(candidate);
    }
  }

  if (words.length === 1 && KNOWN_CITY_TOKENS.has(words[0])) {
    return normalizeLocationTerm(words[0]);
  }

  return null;
}

function extractCityTokenForForcedProjects(message: string): string | null {
  return extractLikelyLocationTerm(message);
}

function getRankedLocationColumns(columns: Set<string>): string[] {
  return rankLocationColumns(Array.from(columns).filter(hasLocationLikeColumn));
}

function buildForcedProjectsAddressPlan(message: string, allowlist: SchemaAllowlist): QueryPlan | null {
  const lower = normalizeForParsing(message);
  const mentionsProject = /(project|projects|projet|projets)/.test(lower);
  const mentionsCountIntent = /\b(count|combien|nombre|how many)\b/.test(lower);
  const cityToken = extractCityTokenForForcedProjects(message);

  if (!mentionsProject && !cityToken) return null;
  if (mentionsCountIntent) return null;

  const directProjects = allowlist.tables.get("projects");
  const projectCandidates = Array.from(allowlist.tables.entries()).filter(([key]) => /project|projet/i.test(key));
  const structuralCandidates = Array.from(allowlist.tables.entries()).filter(([, table]) => {
    const cols = table.columns;
    const hasLocationLike = getRankedLocationColumns(cols).length > 0;
    const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => cols.has(c));
    return hasLocationLike && hasNameLike;
  });

  const scoreTable = (key: string, table: AllowlistTable): number => {
    let score = 0;
    if (key === "projects") score += 100;
    if (key === "project") score += 90;
    if (/project|projet/.test(key)) score += 60;
    if (getRankedLocationColumns(table.columns).length > 0) score += 50;
    if (["name", "project_name", "title", "nom"].some((col) => table.columns.has(col))) score += 30;
    return score;
  };

  const allCandidates = [...projectCandidates, ...structuralCandidates].sort(
    (a, b) => scoreTable(b[0], b[1]) - scoreTable(a[0], a[1]),
  );
  const bestCandidateEntry = allCandidates[0];
  const projects = directProjects || (bestCandidateEntry ? bestCandidateEntry[1] : null);
  if (!projects) return null;

  const tableKey =
    directProjects
      ? "projects"
      : (bestCandidateEntry ? bestCandidateEntry[0] : "projects");

  const primaryLocationColumn = getRankedLocationColumns(projects.columns)[0];
  if (!primaryLocationColumn && cityToken) return null;

  const preferredColumns = [
    "name",
    "project_name",
    "title",
    ...(primaryLocationColumn ? [primaryLocationColumn] : []),
    "description",
    "status",
    "minprice",
    "maxprice",
  ].filter((col, idx, arr) => projects.columns.has(col) && arr.indexOf(col) === idx);

  return {
    intent: {
      type: "lookup",
      table: tableKey,
      columns: preferredColumns.length > 0 ? preferredColumns : undefined,
    },
    filters:
      cityToken && primaryLocationColumn
        ? [
            {
              field: primaryLocationColumn,
              operator: "contains",
              value: cityToken,
            },
          ]
        : [],
    limit: 50,
    sort: undefined,
  };
}

function buildCityFallbackPlans(message: string, allowlist: SchemaAllowlist): QueryPlan[] {
  const cityToken = extractCityTokenForForcedProjects(message);
  if (!cityToken) return [];

  const candidates = Array.from(allowlist.tables.entries())
    .filter(([, table]) => {
      const hasLocationLike = getRankedLocationColumns(table.columns).length > 0;
      const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c));
      return hasLocationLike && hasNameLike;
    })
    .sort(([aKey], [bKey]) => {
      const score = (key: string): number => {
        if (key === "projects") return 100;
        if (key === "project") return 90;
        if (/project|projet/i.test(key)) return 70;
        return 10;
      };
      return score(bKey) - score(aKey);
    });

  const plans: QueryPlan[] = [];
  for (const [tableKey, tableMeta] of candidates) {
    const locationColumns = getRankedLocationColumns(tableMeta.columns);
    for (const locationColumn of locationColumns) {
      const preferredColumns = [
        "name",
        "project_name",
        "title",
        locationColumn,
        "description",
        "status",
        "minprice",
        "maxprice",
      ].filter((col, idx, arr) => tableMeta.columns.has(col) && arr.indexOf(col) === idx);

      plans.push({
        intent: {
          type: "lookup" as const,
          table: tableKey,
          columns: preferredColumns.length > 0 ? preferredColumns : undefined,
        },
        filters: [
          {
            field: locationColumn,
            operator: "contains" as const,
            value: cityToken,
          },
        ],
        limit: 50,
        sort: undefined,
      });
    }
  }

  return plans;
}

function detectProjectCityRequest(message: string): { city: string | null; projectIntent: boolean } {
  const city = extractCityTokenForForcedProjects(message);
  const lower = normalizeForParsing(message);
  const projectIntent = /(project|projects|projet|projets)/.test(lower) || Boolean(city);
  return { city, projectIntent };
}

function quoteIdent(identifier: string): string {
  return `[${identifier.replace(/]/g, "]]")}]`;
}

function quoteTable(schema: string, table: string): string {
  return `${quoteIdent(schema)}.${quoteIdent(table)}`;
}

async function runProjectCityRescueLookup(
  message: string,
  allowlist: SchemaAllowlist,
): Promise<{ plan: QueryPlan; rows: Record<string, unknown>[] } | null> {
  const intent = detectProjectCityRequest(message);
  if (!intent.projectIntent || !intent.city) return null;

  const candidates = Array.from(allowlist.tables.entries())
    .filter(([, table]) => {
      const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c));
      const hasLocationLike = getRankedLocationColumns(table.columns).length > 0;
      return hasNameLike && hasLocationLike;
    })
    .sort(([aKey], [bKey]) => {
      const score = (key: string): number => {
        if (key === "projects") return 100;
        if (/project|projet/i.test(key)) return 70;
        return 10;
      };
      return score(bKey) - score(aKey);
    });

  for (const [tableKey, table] of candidates) {
    const locationCols = getRankedLocationColumns(table.columns).slice(0, 4);
    if (locationCols.length === 0) continue;

    const selectedColumns = [
      "name",
      "project_name",
      "title",
      ...locationCols,
      "description",
      "status",
      "minprice",
      "maxprice",
    ].filter((col, idx, arr) => table.columns.has(col) && arr.indexOf(col) === idx);

    const whereParts: string[] = [];
    const params: Array<string | number | boolean | Date | null> = [];
    for (const col of locationCols) {
      const ref = `@p${params.length}`;
      params.push(`%${intent.city}%`);
      whereParts.push(
        `LOWER(CAST(${quoteIdent(col)} AS NVARCHAR(4000))) LIKE LOWER(CAST(${ref} AS NVARCHAR(4000)))`,
      );
    }

    const sql = `SELECT TOP (50) ${selectedColumns.length > 0 ? selectedColumns.map(quoteIdent).join(", ") : "*"}
FROM ${quoteTable(table.schema, table.table)}
WHERE (${whereParts.join(" OR ")});`;

    const result = await query(sql, params);
    const rows = dedupeRows(result.rows as Record<string, unknown>[]);
    if (rows.length === 0) continue;

    return {
      plan: {
        intent: {
          type: "lookup",
          table: tableKey,
          columns: selectedColumns.length > 0 ? selectedColumns : undefined,
        },
        filters: [
          {
            field: locationCols[0],
            operator: "contains",
            value: intent.city,
          },
        ],
        limit: 50,
        sort: undefined,
      },
      rows,
    };
  }

  return null;
}

function buildRelaxedLocationPlan(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan | null {
  if (plan.intent.type !== "lookup") return null;

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return null;

  const relaxed: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  let changed = false;
  relaxed.filters = relaxed.filters.map((filter) => {
    if (hasLocationLikeColumn(filter.field) && filter.operator !== "contains") {
      changed = true;
      return { ...filter, operator: "contains" };
    }
    return filter;
  });

  if (relaxed.filters.length === 0) {
    const term = extractLikelyLocationTerm(message);
    if (term) {
      changed = true;
      relaxed.filters.push({
        field: locationColumns[0],
        operator: "contains",
        value: term,
      });
    }
  }

  return changed ? relaxed : null;
}

function getAlternativeLocationColumns(availableColumns: Set<string>, current: string): string[] {
  return rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn)).filter(
    (col) => col.toLowerCase() !== current.toLowerCase(),
  );
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
  const currentlySelected = new Set((plan.intent.columns || []).map((c) => c.toLowerCase()));
  const shouldKeepPricing = includesAny(message, ["price", "prix", "tarif", "budget", "cost", "montant"]);
  const shouldKeepSurface = includesAny(message, ["surface", "area", "sqm", "m2", "superficie"]);
  const keepExtra = Array.from(availableColumns).filter((column) => {
    if (currentlySelected.has(column)) return true;
    if (shouldKeepPricing && /(price|prix|cost|amount|montant|budget|tarif|min|max)/i.test(column)) return true;
    if (shouldKeepSurface && /(surface|area|sqm|sqft|m2|superficie|min|max)/i.test(column)) return true;
    return false;
  });

  if (selected.length > 0) {
    return {
      ...plan,
      intent: {
        ...plan.intent,
        columns: Array.from(new Set([...selected, ...keepExtra])).slice(0, 10),
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

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function findKeyCaseInsensitive(row: Record<string, unknown>, candidates: string[]): string | null {
  const keys = Object.keys(row);
  for (const candidate of candidates) {
    const hit = keys.find((key) => key.toLowerCase() === candidate.toLowerCase());
    if (hit) return hit;
  }
  return null;
}

function enrichRowsWithPriceRange(
  rows: Record<string, unknown>[],
  message: string,
): Record<string, unknown>[] {
  const asksPrice = includesAny(message, ["price", "prices", "prix", "tarif", "tarifs", "budget", "cost", "montant"]);
  if (!asksPrice || rows.length === 0) return rows;

  return rows.map((row) => {
    const minKey = findKeyCaseInsensitive(row, ["minprice", "min_price", "price_min", "prixmin"]);
    const maxKey = findKeyCaseInsensitive(row, ["maxprice", "max_price", "price_max", "prixmax"]);
    if (!minKey || !maxKey) return row;

    const minValue = toFiniteNumber(row[minKey]);
    const maxValue = toFiniteNumber(row[maxKey]);
    if (minValue === null || maxValue === null) return row;

    const low = Math.min(minValue, maxValue);
    const high = Math.max(minValue, maxValue);
    return {
      ...row,
      price_range: `between ${low} and ${high}`,
    };
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
): Promise<PlannerOutcome> {
  const normalizedMessage = normalizeMessageForPlanner(message);
  const schemaPrompt = renderAllowlistForPrompt(allowlist);
  const operators = Array.from(allowlist.operators).join(", ");
  const plannerSystemPrompt = `You are a planner for a bilingual (fr/en) real-estate assistant.
Return ONLY valid JSON and no extra text.

Planner output schema:
{
  "language": "fr|en",
  "status": "ok|need_clarification|out_of_scope",
  "intent": "lookup|count|aggregate",
  "table": "<allowlisted table key>",
  "select": ["LOCATION_CITY|LOCATION_TEXT|PROJECT_NAME|PROJECT_DESCRIPTION|PROJECT_STATUS|PRICE|SURFACE"],
  "filters": [{"concept":"<same concept set>","operator":"=|!=|LIKE|IN","value":"string|string[]"}],
  "sort": [{"concept":"<same concept set>","direction":"asc|desc"}],
  "limit": 1..50,
  "aggregation": null | {"func":"sum|avg|min|max","concept":"PRICE|SURFACE|PROJECT_STATUS|PROJECT_NAME|PROJECT_DESCRIPTION","group_by":["<concept>", "..."]},
  "ask_user": null | "friendly follow-up question",
  "notes": "short note"
}

Rules:
- Detect language from user text and set language to fr or en.
- Tolerate typos/grammar issues.
- Use concept-first planning only. Never output raw column names.
- Use schema ConceptMap to decide concepts.
- LOCATION_CITY synonyms include: ville, city, casablanca, rabat, tanger, marrakech, agadir.
- LOCATION_CITY → ["Ville", "City", "Location", "Adresse"]
- LOCATION_TEXT synonyms include: adresse, address, quartier, zone, localisation, location, secteur, pres de, near, by.
- Prefer LOCATION_CITY for city questions. Use LOCATION_TEXT for quartier/zone/near/by.
- Never invent table names or concepts.
- Don't mind case sensitivity
- filters must be [] when not needed.
- If the user mentions “prix”, “price”, “budget”, “coût”, “cost”:
    Intent remains LOOKUP
    Include MinPrice and MaxPrice (or PRICE_RANGE concept)
    Do NOT switch to COUNT or AGGREGATE
- status=ok only when plan is executable.
- status=need_clarification when key info is missing or ambiguous; include ask_user in same detected language.
- status=out_of_scope when user asks outside real-estate data; include ask_user in same detected language.
- For intent=count, aggregation must be null.
- For intent=aggregate, aggregation is required.
- Deduplicate select concepts.
- Only ignore values like “test”, “dummy”, “sample” WHEN they appear ALONE and NOT as part of a longer name.
  If “test” is part of a multi-word entity name (e.g., “lilas test”), treat it as a valid name.
- If both PROJECT_NAME and PROJECT_DESCRIPTION are redundant, keep PROJECT_NAME unless explicitly asked for description.
- Allowed SQL operators for later execution context are: ${operators}.`;

  const plannerUserPrompt = `Allowlisted tables, columns and concept maps:
${schemaPrompt}

User message:
${normalizedMessage}`;

  async function runPlanner(prompt: string): Promise<{ parsed: unknown; raw: string }> {
    const raw = await queryAI(plannerSystemPrompt, prompt, true);
    try {
      return { parsed: JSON.parse(raw), raw };
    } catch {
      // Let validation + repair flow handle malformed JSON.
      return { parsed: raw, raw };
    }
  }

  let plannerAttempt = await runPlanner(plannerUserPrompt);
  let plannerValidation = validatePlannerQueryPlan(plannerAttempt.parsed);

  let repairCount = 0;
  while (!plannerValidation.ok && repairCount < 2) {
    repairCount++;
    const repairPrompt = `REPAIR MODE.
Validation errors:
${plannerValidation.errors.join("\n")}

Original user message:
${normalizedMessage}

Allowlist + ConceptMap:
${schemaPrompt}

Invalid JSON:
${plannerAttempt.raw}

Return ONLY corrected JSON that satisfies the schema and constraints.`;

    plannerAttempt = await runPlanner(repairPrompt);
    plannerValidation = validatePlannerQueryPlan(plannerAttempt.parsed);
  }

  if (!plannerValidation.ok) {
    const fallbackLanguage = language;
    return {
      plannerLanguage: fallbackLanguage,
      plannerStatus: "need_clarification",
      askUser:
        fallbackLanguage === "fr"
          ? "Pouvez-vous preciser votre demande (ville, type de projet, ou statut) ?"
          : "Could you clarify your request (city, project type, or status)?",
      executablePlan: {
      intent: { type: "lookup", table: "projects", columns: ["name"] },
      filters: [],
      limit: 10,
      sort: undefined,
      },
    };
  }

  const plannerPlan = plannerValidation.plan;
  const effectivePlannerPlan =
    plannerPlan.status !== "ok" && shouldAutoListProjects(normalizedMessage)
      ? {
          ...plannerPlan,
          status: "ok" as const,
          intent: "lookup" as const,
          select: ["PROJECT_NAME", "LOCATION_CITY", "PROJECT_STATUS"] as PlannerConcept[],
          filters: [],
          sort: [],
          aggregation: null,
          ask_user: null,
          notes: `${plannerPlan.notes} | Auto-resolved broad project listing.`,
        }
      : plannerPlan;

  const hintedSelection = applyConceptSelectionHints(
    {
      select: effectivePlannerPlan.select,
      notes: effectivePlannerPlan.notes,
    },
    normalizedMessage,
  );
  effectivePlannerPlan.select = hintedSelection.select;
  effectivePlannerPlan.notes = hintedSelection.notes;

  const tableMeta = allowlist.tables.get(effectivePlannerPlan.table.toLowerCase());
  if (!tableMeta) {
    throw new Error(`Planner selected unknown table "${effectivePlannerPlan.table}".`);
  }

  const resolveConcept = (concept: PlannerConcept): string | null => {
    const candidates = tableMeta.conceptMap[concept] || [];
    if (concept === "LOCATION_CITY" || concept === "LOCATION_TEXT") {
      const ranked = rankLocationColumns(candidates);
      return ranked.length > 0 ? ranked[0] : null;
    }
    return candidates.length > 0 ? candidates[0] : null;
  };

  const resolveConceptForSelect = (concept: PlannerConcept): string[] => {
    const candidates = tableMeta.conceptMap[concept] || [];
    if (candidates.length === 0) return [];

    // For range-like business concepts (price/surface), include both min/max variants when present.
    if (concept === "PRICE" || concept === "SURFACE") {
      const ranged = candidates.filter((col) => /(min|max)/i.test(col));
      if (ranged.length > 0) {
        return Array.from(new Set([...ranged.slice(0, 2), candidates[0]]));
      }
    }

    return [candidates[0]];
  };

  const resolvedSelect = Array.from(
    new Set(
      effectivePlannerPlan.select.flatMap((concept) => resolveConceptForSelect(concept)),
    ),
  );

  const resolvedFilters = effectivePlannerPlan.filters
    .map((filter) => {
      const column = resolveConcept(filter.concept);
      if (!column) return null;
      const mappedOperator =
        filter.operator === "="
          ? "eq"
          : filter.operator === "!="
            ? "neq"
            : filter.operator === "IN"
              ? "in"
              : "contains";
      return {
        field: column,
        operator: mappedOperator as QueryPlan["filters"][number]["operator"],
        value: filter.value,
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const resolvedSort = effectivePlannerPlan.sort
    .map((item) => {
      const column = resolveConcept(item.concept);
      if (!column) return null;
      return {
        field: column,
        direction: item.direction,
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const aggregationColumn =
    effectivePlannerPlan.aggregation && effectivePlannerPlan.aggregation.concept
      ? resolveConcept(effectivePlannerPlan.aggregation.concept)
      : null;

  const executablePlan: QueryPlan = {
    intent: {
      type: effectivePlannerPlan.intent,
      table: effectivePlannerPlan.table,
      columns: resolvedSelect.length > 0 ? resolvedSelect : undefined,
      aggregation:
        effectivePlannerPlan.intent === "aggregate" && effectivePlannerPlan.aggregation && aggregationColumn
          ? {
              func: effectivePlannerPlan.aggregation.func,
              column: aggregationColumn,
            }
          : undefined,
    },
    filters: resolvedFilters,
    limit: Math.max(1, Math.min(50, effectivePlannerPlan.limit)),
    sort: resolvedSort.length > 0 ? resolvedSort[0] : undefined,
  };

  const shapeValidation = validateQueryPlan(executablePlan);
  if (!shapeValidation.ok) {
    throw new Error(`Invalid QueryPlan JSON: ${shapeValidation.errors.join(" ")}`);
  }

  const allowlistValidation = validateAgainstAllowlist(shapeValidation.plan, allowlist);
  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  return {
    plannerLanguage: plannerPlan.language,
    plannerStatus: effectivePlannerPlan.status,
    askUser: effectivePlannerPlan.ask_user,
    executablePlan: applyIntentAndOperatorHints(shapeValidation.plan, normalizedMessage),
  };
}

async function generateAnswer(
  language: ChatLanguage,
  message: string,
  plan: QueryPlan,
  rows: Record<string, unknown>[],
): Promise<string> {
  const presentationRows = enrichRowsWithPriceRange(sanitizeRowsForResponder(rows, message), message);
  const response = await queryAI(
    `You are a client-facing real-estate assistant.
Reply in ${language === "fr" ? "French" : "English"}.
Use only the provided query results and never invent missing facts.
Keep a warm, concise, professional tone.
Do not mention SQL, query plans, allowlists, backend, internal validation, or system errors.
Do not expose technical identifiers (id/uuid/coordinates) unless the user explicitly asked for them.
When there are results:
- Prefer project name, address/city, and description when available.
- If pricing columns include min/max variants (e.g. minprice/maxprice), always present price as a range:
  "between {min} and {max}" (or use provided price_range field if present).
- Present multiple items as a clean list with one item per line.
- Use bullets with "-" for each item, not one long paragraph.
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
  const forcedPlan = buildForcedProjectsAddressPlan(message, allowlist);
  const planning = forcedPlan
    ? {
        executablePlan: forcedPlan,
        plannerLanguage: resolvedLanguage,
        plannerStatus: "ok" as PlannerStatus,
        askUser: null,
      }
    : await generateQueryPlan(message, resolvedLanguage, allowlist);
  const plan = planning.executablePlan;
  const broadRequest = shouldAutoListProjects(message);

  if (planning.plannerStatus !== "ok" || broadRequest) {
    const lang = planning.plannerLanguage;
    const defaultCityQuestion =
      lang === "fr" ? "D'accord — vous cherchez des projets dans quelle ville ?" : "Sure — which city are you interested in?";
    const fallbackQuestion = lang === "fr" ? "Pouvez-vous reformuler votre demande ?" : "Could you rephrase your request?";
    const followUp = broadRequest ? defaultCityQuestion : planning.askUser || fallbackQuestion;
    const suggestions = await buildChatSuggestions({
      allowlist,
      message,
      language: lang,
      tableKey: plan.intent.table,
    });
    return {
      language: lang,
      status: broadRequest ? "need_clarification" : planning.plannerStatus,
      answer: followUp,
      follow_up_question: followUp,
      suggestions,
      queryPlan: plan,
      results: [],
    };
  }
  const allowlistValidation = validateAgainstAllowlist(plan, allowlist);

  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  const normalizedPlan = normalizeSemanticFilters(plan, allowlistValidation.table.columns, message);
  const denoisedPlan = removeNoisyNameFilters(normalizedPlan, allowlistValidation.table.columns);
  const deblockedPlan = removeAmbiguousAvailabilityFilters(denoisedPlan, allowlistValidation.table.columns);
  const locationEnforcedPlan = enforceLocationFilterFromMessage(deblockedPlan, allowlistValidation.table.columns, message);
  const directLocationPlan = applyDirectProjectLocationOverride(
    locationEnforcedPlan,
    allowlistValidation.table.columns,
    message,
  );
  const adjustedPlan = applyClientFriendlyProjection(directLocationPlan, allowlistValidation.table.columns, message);
  let executedPlan = adjustedPlan;
  let { sql, params } = compileQueryPlan(executedPlan, allowlistValidation.table);
  let dbResult = await query(sql, params);
  let rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);

  if (rows.length === 0) {
    const relaxed = buildRelaxedLocationPlan(executedPlan, allowlistValidation.table.columns, message);
    if (relaxed) {
      executedPlan = relaxed;
      const retry = compileQueryPlan(executedPlan, allowlistValidation.table);
      dbResult = await query(retry.sql, retry.params);
      rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);

      // Secondary fallback: try alternate textual location columns if still empty.
      if (rows.length === 0) {
        const firstLocationFilter = executedPlan.filters.find((f) => hasLocationLikeColumn(f.field));
        if (firstLocationFilter) {
          const alternatives = getAlternativeLocationColumns(allowlistValidation.table.columns, firstLocationFilter.field);
          for (const altCol of alternatives) {
            const altPlan: QueryPlan = {
              ...executedPlan,
              filters: executedPlan.filters.map((f) =>
                f === firstLocationFilter ? { ...f, field: altCol, operator: "contains" } : f,
              ),
            };
            const altQuery = compileQueryPlan(altPlan, allowlistValidation.table);
            const altResult = await query(altQuery.sql, altQuery.params);
            const altRows = dedupeRows(altResult.rows as Record<string, unknown>[]);
            if (altRows.length > 0) {
              executedPlan = altPlan;
              rows = altRows;
              break;
            }
          }
        }
      }
    }
  }

  if (rows.length === 0) {
    const cityFallbackPlans = buildCityFallbackPlans(message, allowlist);
    for (const candidatePlan of cityFallbackPlans) {
      const candidateAllowlist = validateAgainstAllowlist(candidatePlan, allowlist);
      if (!candidateAllowlist.ok) continue;
      const candidateQuery = compileQueryPlan(candidatePlan, candidateAllowlist.table);
      const candidateResult = await query(candidateQuery.sql, candidateQuery.params);
      const candidateRows = dedupeRows(candidateResult.rows as Record<string, unknown>[]);
      if (candidateRows.length > 0) {
        executedPlan = candidatePlan;
        rows = candidateRows;
        break;
      }
    }
  }

  if (rows.length === 0) {
    const rescued = await runProjectCityRescueLookup(message, allowlist);
    if (rescued) {
      executedPlan = rescued.plan;
      rows = rescued.rows;
    }
  }

  const answer = await generateAnswer(resolvedLanguage, message, executedPlan, rows);
  const suggestions =
    rows.length > 8
      ? await buildChatSuggestions({
          allowlist,
          message,
          language: resolvedLanguage,
          tableKey: executedPlan.intent.table,
        })
      : undefined;

  return {
    language: resolvedLanguage,
    status: "ok",
    answer,
    suggestions,
    queryPlan: executedPlan,
    results: rows,
  };
}
