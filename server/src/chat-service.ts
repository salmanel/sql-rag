import { randomUUID } from "crypto";
import { renderAllowlistForPrompt } from "./allowlist";
import { getCachedAllowlist } from "./allowlist-cache";
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
  conversation_id: string;
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

interface PresetExecution {
  plan: QueryPlan;
  rows: Record<string, unknown>[];
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

const DEBUG_SQL = process.env.DEBUG_SQL === "true";

function logSql(tag: string, sql: string, params: unknown[], rows?: number): void {
  if (!DEBUG_SQL) return;
  console.log(`[SQL:${tag}] ${sql}`);
  console.log(`[SQL:${tag}:params] ${JSON.stringify(params)}`);
  if (typeof rows === "number") {
    console.log(`[SQL:${tag}:rows] ${rows}`);
  }
}

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

type BroadEntity = "projects" | "annonces" | "immeubles" | "units" | null;

function detectBroadEntityRequest(message: string): BroadEntity {
  const lower = message.trim().toLowerCase();
  const normalized = normalizeForParsing(lower);

  const projectPatterns = [
    /^projects?$/,
    /^projets?$/,
    /^list( all)? (projects?|projets?)$/,
    /^show( me)?( all)?( the)? (projects?|projets?)$/,
    /^can you (list|show)( all)?( the)? (projects?|projets?)$/,
    /^(les |les derniers? |derniers? )?(projects?|projets?)$/,
    /^montre[- ]?moi les (projects?|projets?)$/,
  ];
  if (projectPatterns.some((p) => p.test(normalized))) return "projects";

  const annoncesPatterns = [
    /^annonces?$/,
    /^(les |les dernieres? |dernieres? )?annonces?$/,
    /^(list|show|montre)[- ]?(moi )?(les )?(dernieres? )?(annonces?|listings?)$/,
    /^listings?$/,
    /^(latest |recent )?(listings?|annonces?)$/,
    /^(les |les derniers? |derniers? )?listings?$/,
  ];
  if (annoncesPatterns.some((p) => p.test(normalized))) return "annonces";

  const immeublesPatterns = [
    /^immeubles?$/,
    /^buildings?$/,
    /^(les |les derniers? |derniers? )?immeubles?$/,
    /^(list|show|montre)[- ]?(moi )?(les )?(derniers? )?(immeubles?|buildings?)$/,
    /^(latest |recent )?(buildings?|immeubles?)$/,
  ];
  if (immeublesPatterns.some((p) => p.test(normalized))) return "immeubles";

  const unitsPatterns = [
    /^unites?$/,
    /^units?$/,
    /^(les |les dernieres? |dernieres? )?(unites?|units?)$/,
    /^(list|show|montre)[- ]?(moi )?(les )?(dernieres? )?(unites?|units?)$/,
    /^(latest |recent )?(units?|unites?)$/,
  ];
  if (unitsPatterns.some((p) => p.test(normalized))) return "units";

  return null;
}

async function runBroadEntityQuery(entity: BroadEntity): Promise<PresetExecution | null> {
  if (!entity) return null;

  if (entity === "projects") {
    const sql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
      FROM [dbo].[Projects]
      ORDER BY [Id] DESC;`;
    const result = await query(sql, []);
    logSql("broad:projects", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
        filters: [],
        limit: 50,
        sort: { field: "id", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (entity === "annonces") {
    const sql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
      FROM [dbo].[Projects]
      ORDER BY [Id] DESC;`;
    const result = await query(sql, []);
    logSql("broad:annonces", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
        filters: [],
        limit: 50,
        sort: { field: "id", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (entity === "immeubles") {
    const sql = `SELECT TOP (50)
      [Id], [Name], [MinPrice], [MaxPrice]
      FROM [dbo].[Immeubles]
      ORDER BY [Id] DESC;`;
    const result = await query(sql, []);
    logSql("broad:immeubles", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "immeubles", columns: ["name", "minprice", "maxprice"] },
        filters: [],
        limit: 50,
        sort: { field: "id", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (entity === "units") {
    const sql = `SELECT TOP (50)
      [Id], [NumberOfBedrooms], [NumberOfBathrooms], [TotalSurface], [LatestPrice]
      FROM [dbo].[Units]
      ORDER BY [Id] DESC;`;
    const result = await query(sql, []);
    logSql("broad:units", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "units", columns: ["numberofbedrooms", "numberofbathrooms", "totalsurface", "latestprice"] },
        filters: [],
        limit: 50,
        sort: { field: "id", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  return null;
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

  if (cityColumn && includesAny(message, cityTerms)) {
    normalized.filters = normalized.filters.map((filter) => {
      const field = filter.field.toLowerCase();
      if (["address", "adresse", "location", "full_address", "street_address"].includes(field)) {
        return { ...filter, field: cityColumn };
      }
      return filter;
    });
  } else if (addressColumn && includesAny(message, addressTerms)) {
    normalized.filters = normalized.filters.map((filter) => {
      if (filter.field.toLowerCase() === "city") {
        return { ...filter, field: addressColumn };
      }
      return filter;
    });
  }

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

function detectPresetKind(
  message: string,
): "projects_available_city" | "projects_in_progress" | "three_bedroom" | "latest_listings" | null {
  const lower = normalizeForParsing(message);
  const hasProjectWord = /(project|projects|projet|projets)/.test(lower);

  const isAvailableCity =
    hasProjectWord &&
    (/(available|disponible|disponibles)/.test(lower) || /\b(in|a|au|dans)\b/.test(lower)) &&
    Boolean(extractLikelyLocationTerm(message));
  if (isAvailableCity) return "projects_available_city";

  const isInProgress =
    hasProjectWord && /(in progress|en cours|sur plan|ongoing|currently in progress)/.test(lower);
  if (isInProgress) return "projects_in_progress";

  const isThreeBedroom =
    /\b3\b/.test(lower) &&
    /(bedroom|bedrooms|chambre|chambres)/.test(lower) &&
    /(apartment|apartments|appartement|appartements|units|unites|unite)/.test(lower);
  if (isThreeBedroom) return "three_bedroom";

  const isLatestListings =
    /(latest listings|latest projects|newest projects|dernieres annonces|projets recents|latest listing)/.test(lower);
  if (isLatestListings) return "latest_listings";

  return null;
}

async function runPresetQuery(message: string): Promise<PresetExecution | null> {
  const preset = detectPresetKind(message);
  if (!preset) return null;

  if (preset === "projects_available_city") {
    const city = extractLikelyLocationTerm(message);
    if (!city) return null;
    const likeCity = `%${city}%`;

    const baseSql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
      FROM [dbo].[Projects]
      WHERE LOWER(CAST([Address] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000)))`;

    const withAvailability = `${baseSql}
      AND (
        LOWER(CAST([Type] AS NVARCHAR(4000))) LIKE '%livraison%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%available%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%disponible%'
      )
      ORDER BY [Name] ASC;`;

    let result = await query(withAvailability, [likeCity]);
    logSql("preset:projects_available_city:strict", withAvailability, [likeCity], result.rows.length);

    if ((result.rows || []).length === 0) {
      const relaxed = `${baseSql} ORDER BY [Name] ASC;`;
      result = await query(relaxed, [likeCity]);
      logSql("preset:projects_available_city:relaxed", relaxed, [likeCity], result.rows.length);
    }

    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
        filters: [{ field: "address", operator: "contains", value: city }],
        limit: 50,
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (preset === "projects_in_progress") {
    const sql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal], [OverAllProgress]
      FROM [dbo].[Projects]
      WHERE
        LOWER(CAST([Type] AS NVARCHAR(4000))) LIKE '%sur plan%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%progress%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%en cours%'
      ORDER BY [OverAllProgress] DESC, [Name] ASC;`;
    const result = await query(sql, []);
    logSql("preset:projects_in_progress", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal", "overallprogress"] },
        filters: [],
        limit: 50,
        sort: { field: "overallprogress", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (preset === "three_bedroom") {
    const sql = `SELECT TOP (50)
      u.[Id] AS [UnitId],
      u.[NumberOfBedrooms],
      u.[NumberOfBathrooms],
      u.[TotalSurface],
      u.[LatestPrice],
      p.[Name] AS [ProjectName],
      p.[Address] AS [Address],
      i.[Name] AS [ImmeubleName],
      i.[MinPrice],
      i.[MaxPrice]
      FROM [dbo].[Units] u
      LEFT JOIN [dbo].[Projects] p ON p.[Id] = u.[ProjectId]
      LEFT JOIN [dbo].[Immeubles] i ON i.[ProjectId] = u.[ProjectId]
      WHERE u.[NumberOfBedrooms] = @p0
      ORDER BY u.[LatestPrice] ASC;`;
    const result = await query(sql, [3]);
    logSql("preset:three_bedroom", sql, [3], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "units", columns: ["numberofbedrooms", "numberofbathrooms", "totalsurface", "latestprice"] },
        filters: [{ field: "numberofbedrooms", operator: "eq", value: 3 }],
        limit: 50,
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  const latestSql = `SELECT TOP (50)
    [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
    FROM [dbo].[Projects]
    ORDER BY [Id] DESC;`;
  const latestResult = await query(latestSql, []);
  logSql("preset:latest_listings", latestSql, [], latestResult.rows.length);
  return {
    plan: {
      intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
      filters: [],
      limit: 50,
      sort: { field: "id", direction: "desc" },
    },
    rows: latestResult.rows as Record<string, unknown>[],
  };
}

function removeNoisyNameFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const nameCandidates = ["name", "project_name", "title", "nom"];
  const nameColumns = new Set(
    nameCandidates.filter((c) => availableColumns.has(c)).map((v) => v.toLowerCase()),
  );
  const genericValues = new Set(["project", "projects", "projet", "projets", "realestate", "residence", "residences"]);
  const cleaned = plan.filters.filter((filter) => {
    const isNameField = nameColumns.has(filter.field.toLowerCase());
    const isGenericValue = typeof filter.value === "string" && genericValues.has(filter.value.trim().toLowerCase());
    return !(isNameField && isGenericValue);
  });
  return { ...plan, filters: cleaned };
}

function removeAmbiguousAvailabilityFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const statusCandidates = ["status", "state", "etat", "availability", "disponibilite"];
  const statusColumns = new Set(
    statusCandidates.filter((c) => availableColumns.has(c)).map((v) => v.toLowerCase()),
  );
  const ambiguousStatusValues = new Set([
    "available", "availability", "disponible", "disponibilite", "available projects",
  ]);
  const filtered = plan.filters.filter((filter) => {
    const isStatusField = statusColumns.has(filter.field.toLowerCase());
    if (!isStatusField || typeof filter.value !== "string") return true;
    return !ambiguousStatusValues.has(filter.value.trim().toLowerCase());
  });
  return { ...plan, filters: filtered };
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
        return { ...f, field: primaryColumn, operator: useContains ? "contains" : "eq", value: normalizeLocationTerm(f.value) };
      }),
    };
  }

  return {
    ...plan,
    filters: [...plan.filters, { field: primaryColumn, operator: useContains ? "contains" : "eq", value: term }],
  };
}

function applyDirectProjectLocationOverride(plan: QueryPlan, availableColumns: Set<string>, message: string): QueryPlan {
  const lower = message.toLowerCase().trim();
  const looksLikeProjectLocationQuery =
    /(project|projects|projet|projets)/.test(lower) && /\b(in|a|au|à|dans)\b/.test(lower);
  if (!looksLikeProjectLocationQuery) return plan;

  const rawTerm = extractLikelyLocationTerm(message);
  if (!rawTerm) return plan;

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return plan;

  const addressPriority = ["address", "adresse", "full_address", "street_address"];
  const addressColumn = addressPriority.find((col) => availableColumns.has(col));
  const primaryColumn = addressColumn || locationColumns[0];
  const term = normalizeLocationTerm(rawTerm);

  return { ...plan, filters: [{ field: primaryColumn, operator: "contains", value: term }] };
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
    if (KNOWN_CITY_TOKENS.has(word)) return normalizeLocationTerm(word);
  }

  const prepositions = new Set(["in", "a", "au", "dans", "near", "pres", "de"]);
  const stopwords = new Set([
    "what", "about", "location", "city", "ville", "where", "in", "at",
    "de", "des", "du", "la", "le", "les", "au", "aux", "dans",
    "projects", "project", "projets", "projet", "known", "available",
    "show", "list", "have", "you", "your", "the", "all", "are", "is",
  ]);

  for (let i = 0; i < words.length - 1; i++) {
    if (!prepositions.has(words[i])) continue;
    const candidate = words[i + 1];
    if (candidate.length > 1 && !stopwords.has(candidate)) return normalizeLocationTerm(candidate);
  }

  if (words.length === 1 && KNOWN_CITY_TOKENS.has(words[0])) return normalizeLocationTerm(words[0]);
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
    return getRankedLocationColumns(cols).length > 0 && ["name", "project_name", "title", "nom"].some((c) => cols.has(c));
  });

  const scoreTable = (key: string, table: AllowlistTable): number => {
    let s = 0;
    if (key === "projects") s += 100;
    if (key === "project") s += 90;
    if (/project|projet/.test(key)) s += 60;
    if (getRankedLocationColumns(table.columns).length > 0) s += 50;
    if (["name", "project_name", "title", "nom"].some((col) => table.columns.has(col))) s += 30;
    return s;
  };

  const allCandidates = [...projectCandidates, ...structuralCandidates].sort(
    (a, b) => scoreTable(b[0], b[1]) - scoreTable(a[0], a[1]),
  );
  const bestCandidateEntry = allCandidates[0];
  const projects = directProjects || (bestCandidateEntry ? bestCandidateEntry[1] : null);
  if (!projects) return null;

  const tableKey = directProjects ? "projects" : (bestCandidateEntry ? bestCandidateEntry[0] : "projects");
  const primaryLocationColumn = getRankedLocationColumns(projects.columns)[0];
  if (!primaryLocationColumn && cityToken) return null;

  const preferredColumns = [
    "name", "project_name", "title",
    ...(primaryLocationColumn ? [primaryLocationColumn] : []),
    "description", "status", "minprice", "maxprice",
  ].filter((col, idx, arr) => projects.columns.has(col) && arr.indexOf(col) === idx);

  return {
    intent: { type: "lookup", table: tableKey, columns: preferredColumns.length > 0 ? preferredColumns : undefined },
    filters: cityToken && primaryLocationColumn
      ? [{ field: primaryLocationColumn, operator: "contains", value: cityToken }]
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
      return getRankedLocationColumns(table.columns).length > 0 &&
        ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c));
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
        "name", "project_name", "title", locationColumn,
        "description", "status", "minprice", "maxprice",
      ].filter((col, idx, arr) => tableMeta.columns.has(col) && arr.indexOf(col) === idx);

      // Level 2: location column contains city token
      plans.push({
        intent: { type: "lookup" as const, table: tableKey, columns: preferredColumns.length > 0 ? preferredColumns : undefined },
        filters: [{ field: locationColumn, operator: "contains" as const, value: cityToken }],
        limit: 50,
        sort: undefined,
      });
    }

    // Level 3: also search in description column for city token
    const descCol = ["description", "details", "summary"].find((c) => tableMeta.columns.has(c));
    if (descCol) {
      const preferredColumns = [
        "name", "project_name", "title",
        ...getRankedLocationColumns(tableMeta.columns).slice(0, 2),
        descCol, "status", "minprice", "maxprice",
      ].filter((col, idx, arr) => tableMeta.columns.has(col) && arr.indexOf(col) === idx);

      plans.push({
        intent: { type: "lookup" as const, table: tableKey, columns: preferredColumns.length > 0 ? preferredColumns : undefined },
        filters: [{ field: descCol, operator: "contains" as const, value: cityToken }],
        limit: 50,
        sort: undefined,
      });
    }

    // Level 4: search in name column for city token
    const nameCol = ["name", "project_name", "title", "nom"].find((c) => tableMeta.columns.has(c));
    if (nameCol) {
      const preferredColumns = [
        "name", "project_name", "title",
        ...getRankedLocationColumns(tableMeta.columns).slice(0, 2),
        "description", "status", "minprice", "maxprice",
      ].filter((col, idx, arr) => tableMeta.columns.has(col) && arr.indexOf(col) === idx);

      plans.push({
        intent: { type: "lookup" as const, table: tableKey, columns: preferredColumns.length > 0 ? preferredColumns : undefined },
        filters: [{ field: nameCol, operator: "contains" as const, value: cityToken }],
        limit: 50,
        sort: undefined,
      });
    }
  }
  return plans;
}

/**
 * Extract additional filter terms from the message beyond city, e.g. district, property type, status keywords.
 */
function extractAdditionalFilterTerms(message: string): { district: string | null; propertyType: string | null; statusKeyword: string | null } {
  const lower = normalizeForParsing(message);

  // District: words after "in", "a", "au", "dans", "quartier" that are NOT known city tokens
  let district: string | null = null;
  const districtPatterns = [
    /(?:quartier|district|secteur|zone)\s+(?:de\s+|du\s+|d')?(\w+)/,
    /(?:a|au|dans|in)\s+(\w+)\s+(?:a|au|dans|in)\s+(\w+)/,
  ];
  for (const pattern of districtPatterns) {
    const match = lower.match(pattern);
    if (match) {
      // Take the last captured group (for "in X in Y", Y is city, X is district)
      const candidates = match.slice(1).filter(Boolean);
      for (const c of candidates) {
        if (!KNOWN_CITY_TOKENS.has(c) && c.length > 2) {
          district = c;
          break;
        }
      }
    }
  }

  // Property type
  let propertyType: string | null = null;
  const typePatterns: Array<[RegExp, string]> = [
    [/\b(appartement|apartment|appart)\b/, "appartement"],
    [/\b(villa|villas)\b/, "villa"],
    [/\b(studio|studios)\b/, "studio"],
    [/\b(duplex)\b/, "duplex"],
    [/\b(penthouse)\b/, "penthouse"],
    [/\b(bureau|office|bureaux)\b/, "bureau"],
    [/\b(commerce|commercial|shop|magasin)\b/, "commerce"],
    [/\b(terrain|land|lot)\b/, "terrain"],
    [/\b(maison|house)\b/, "maison"],
    [/\b(loft)\b/, "loft"],
    [/\b(riad)\b/, "riad"],
  ];
  for (const [pattern, type] of typePatterns) {
    if (pattern.test(lower)) {
      propertyType = type;
      break;
    }
  }

  // Status keyword
  let statusKeyword: string | null = null;
  const statusPatterns: Array<[RegExp, string]> = [
    [/\b(neuf|new|nouveau|nouvelle)\b/, "neuf"],
    [/\b(ancien|old|resale)\b/, "ancien"],
    [/\b(disponible|available)\b/, "disponible"],
    [/\b(en cours|in progress|sur plan)\b/, "en cours"],
    [/\b(livr[eé]|delivered|pret|ready)\b/, "livre"],
  ];
  for (const [pattern, status] of statusPatterns) {
    if (pattern.test(lower)) {
      statusKeyword = status;
      break;
    }
  }

  return { district, propertyType, statusKeyword };
}

interface ProgressiveFallbackResult {
  plan: QueryPlan;
  rows: Record<string, unknown>[];
  matchLevel: "exact" | "close" | "city" | "semantic" | "alternative";
}

function buildRelaxedLocationPlan(plan: QueryPlan, availableColumns: Set<string>, message: string): QueryPlan | null {
  if (plan.intent.type !== "lookup") return null;
  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return null;

  const relaxed: QueryPlan = { ...plan, filters: [...plan.filters] };
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
      relaxed.filters.push({ field: locationColumns[0], operator: "contains", value: term });
    }
  }
  return changed ? relaxed : null;
}

/**
 * Progressively relax filters to find the best available results.
 * Returns the best match level found with its rows.
 */
async function progressiveFallbackSearch(
  basePlan: QueryPlan,
  tableMeta: AllowlistTable,
  message: string,
  allowlist: SchemaAllowlist,
): Promise<ProgressiveFallbackResult | null> {
  // Level 1: exact (already tried before calling this)
  // Level 2: relax operators to contains
  const relaxedPlan = buildRelaxedLocationPlan(basePlan, tableMeta.columns, message);
  if (relaxedPlan) {
    const retry = compileQueryPlan(relaxedPlan, tableMeta);
    const result = await query(retry.sql, retry.params);
    logSql("progressive:L2-relaxed", retry.sql, retry.params, result.rows.length);
    const rows = dedupeRows(result.rows as Record<string, unknown>[]);
    if (rows.length > 0) {
      return { plan: relaxedPlan, rows, matchLevel: "close" };
    }

    // Level 2b: try alternate location columns
    const firstLocationFilter = relaxedPlan.filters.find((f) => hasLocationLikeColumn(f.field));
    if (firstLocationFilter) {
      const alternatives = getAlternativeLocationColumns(tableMeta.columns, firstLocationFilter.field);
      for (const altCol of alternatives) {
        const altPlan: QueryPlan = {
          ...relaxedPlan,
          filters: relaxedPlan.filters.map((f) =>
            f === firstLocationFilter ? { ...f, field: altCol, operator: "contains" } : f,
          ),
        };
        const altQuery = compileQueryPlan(altPlan, tableMeta);
        const altResult = await query(altQuery.sql, altQuery.params);
        logSql(`progressive:L2b-alt:${altCol}`, altQuery.sql, altQuery.params, altResult.rows.length);
        const altRows = dedupeRows(altResult.rows as Record<string, unknown>[]);
        if (altRows.length > 0) {
          return { plan: altPlan, rows: altRows, matchLevel: "close" };
        }
      }
    }
  }

  // Level 3: drop non-location filters, keep only city
  const cityToken = extractLikelyLocationTerm(message);
  if (cityToken) {
    const locationCols = getRankedLocationColumns(tableMeta.columns);
    for (const locCol of locationCols) {
      const cityOnlyPlan: QueryPlan = {
        ...basePlan,
        filters: [{ field: locCol, operator: "contains", value: cityToken }],
      };
      const cityQuery = compileQueryPlan(cityOnlyPlan, tableMeta);
      const cityResult = await query(cityQuery.sql, cityQuery.params);
      logSql(`progressive:L3-city:${locCol}`, cityQuery.sql, cityQuery.params, cityResult.rows.length);
      const cityRows = dedupeRows(cityResult.rows as Record<string, unknown>[]);
      if (cityRows.length > 0) {
        return { plan: cityOnlyPlan, rows: cityRows, matchLevel: "city" };
      }
    }

    // Level 3b: search description for city token
    const descCol = ["description", "details", "summary"].find((c) => tableMeta.columns.has(c));
    if (descCol) {
      const descPlan: QueryPlan = {
        ...basePlan,
        filters: [{ field: descCol, operator: "contains", value: cityToken }],
      };
      const descQuery = compileQueryPlan(descPlan, tableMeta);
      const descResult = await query(descQuery.sql, descQuery.params);
      logSql(`progressive:L3b-desc`, descQuery.sql, descQuery.params, descResult.rows.length);
      const descRows = dedupeRows(descResult.rows as Record<string, unknown>[]);
      if (descRows.length > 0) {
        return { plan: descPlan, rows: descRows, matchLevel: "city" };
      }
    }
  }

  // Level 4: cross-table city fallback plans
  const cityFallbackPlans = buildCityFallbackPlans(message, allowlist);
  for (const candidatePlan of cityFallbackPlans) {
    const candidateAllowlist = validateAgainstAllowlist(candidatePlan, allowlist);
    if (!candidateAllowlist.ok) continue;
    const candidateQuery = compileQueryPlan(candidatePlan, candidateAllowlist.table);
    const candidateResult = await query(candidateQuery.sql, candidateQuery.params);
    logSql(
      `progressive:L4-cross:${candidatePlan.intent.table}:${candidatePlan.filters[0]?.field || "none"}`,
      candidateQuery.sql, candidateQuery.params, candidateResult.rows.length,
    );
    const candidateRows = dedupeRows(candidateResult.rows as Record<string, unknown>[]);
    if (candidateRows.length > 0) {
      return { plan: candidatePlan, rows: candidateRows, matchLevel: "semantic" };
    }
  }

  // Level 5: rescue lookup
  const rescued = await runProjectCityRescueLookup(message, allowlist);
  if (rescued && rescued.rows.length > 0) {
    return { plan: rescued.plan, rows: rescued.rows, matchLevel: "alternative" };
  }

  // Level 5b: drop ALL filters and return latest from the same table
  if (basePlan.intent.table) {
    const noFilterPlan: QueryPlan = {
      ...basePlan,
      filters: [],
      limit: Math.min(basePlan.limit || 20, 20),
      sort: { field: "id", direction: "desc" },
    };
    try {
      const noFilterQuery = compileQueryPlan(noFilterPlan, tableMeta);
      const noFilterResult = await query(noFilterQuery.sql, noFilterQuery.params);
      logSql("progressive:L5b-nofilter", noFilterQuery.sql, noFilterQuery.params, noFilterResult.rows.length);
      const noFilterRows = dedupeRows(noFilterResult.rows as Record<string, unknown>[]);
      if (noFilterRows.length > 0) {
        return { plan: noFilterPlan, rows: noFilterRows, matchLevel: "alternative" };
      }
    } catch {
      // Sort by id may fail if id is not in columns; ignore
    }
  }

  return null;
}

function getAlternativeLocationColumns(availableColumns: Set<string>, current: string): string[] {
  return rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn)).filter(
    (col) => col.toLowerCase() !== current.toLowerCase(),
  );
}

function applyClientFriendlyProjection(plan: QueryPlan, availableColumns: Set<string>, message: string): QueryPlan {
  if (plan.intent.type !== "lookup") return plan;

  const explicitlyAsksTechnicalFields = includesAny(message, [
    "id", "uuid", "guid", "coord", "coordinate", "latitude", "longitude", "lat", "lng", "location",
  ]);
  if (explicitlyAsksTechnicalFields) return plan;

  const preferred = [
    "name", "project_name", "title", "address", "full_address", "street_address",
    "description", "details", "summary",
  ];
  const blockedPatterns = [/^id$/, /_id$/, /uuid/, /guid/, /lat/, /lng/, /lon/, /coord/, /^location$/];

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
    return { ...plan, intent: { ...plan.intent, columns: Array.from(new Set([...selected, ...keepExtra])).slice(0, 10) } };
  }

  const fallbackColumns = Array.from(availableColumns).filter(
    (column) => !blockedPatterns.some((pattern) => pattern.test(column)),
  );
  if (fallbackColumns.length === 0) return plan;
  return { ...plan, intent: { ...plan.intent, columns: fallbackColumns.slice(0, 6) } };
}

function dedupeRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length <= 1) return rows;

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
    const fingerprint = stableKeys.map((key) => String(row[key] ?? "").trim().toLowerCase()).join("|");
    if (!fingerprint) return true;
    if (seen.has(fingerprint)) return false;
    seen.add(fingerprint);
    return true;
  });
}

function isTechnicalKey(key: string): boolean {
  const lower = key.toLowerCase();
  return lower === "id" || lower.endsWith("_id") || lower.includes("uuid") ||
    lower.includes("guid") || lower.includes("coord") || lower.includes("lat") ||
    lower.includes("lng") || lower.includes("lon");
}

function sanitizeRowsForResponder(rows: Record<string, unknown>[], message: string): Record<string, unknown>[] {
  if (rows.length === 0) return rows;
  const asksTechnical = includesAny(message, [
    "id", "uuid", "guid", "coordinate", "coordinates", "location", "latitude", "longitude", "lat", "lng",
  ]);
  if (asksTechnical) return rows;

  return rows.map((row) => {
    const clean: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (!isTechnicalKey(key)) clean[key] = value;
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

function enrichRowsWithPriceRange(rows: Record<string, unknown>[], message: string): Record<string, unknown>[] {
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
    return { ...row, price_range: `between ${low} and ${high}` };
  });
}

function looksLikeNoResultsAnswer(text: string): boolean {
  const lower = normalizeForParsing(text);
  return lower.includes("no project") || lower.includes("no projects") ||
    lower.includes("couldnt find") || lower.includes("could not find") ||
    lower.includes("aucun projet") || lower.includes("pas de projet") ||
    lower.includes("je nai pas trouve");
}

function firstExistingValue(row: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const actual = Object.keys(row).find((k) => k.toLowerCase() === key.toLowerCase());
    if (!actual) continue;
    const value = row[actual];
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (!text) continue;
    return text;
  }
  return null;
}

function buildDeterministicRowsAnswer(language: ChatLanguage, rows: Record<string, unknown>[], matchLevel?: string): string {
  const prefix = matchLevel && matchLevel !== "exact"
    ? language === "fr"
      ? "Je n'ai pas trouve de correspondance exacte, mais voici les resultats les plus proches:\n"
      : "I couldn't find an exact match, but here are the closest results:\n"
    : language === "fr"
      ? "Voici les projets trouves:\n"
      : "Here are the projects found:\n";

  const picked = rows.slice(0, 5).map((row) => {
    const name = firstExistingValue(row, ["name", "project_name", "title"]) || "Project";
    const city = firstExistingValue(row, ["address", "city", "ville"]);
    const desc = firstExistingValue(row, ["description"]);
    const priceRange = firstExistingValue(row, ["price_range"]);
    const parts: string[] = [name];
    if (city) parts.push(language === "fr" ? `a ${city}` : `in ${city}`);
    if (priceRange) parts.push(language === "fr" ? `prix ${priceRange}` : `price ${priceRange}`);
    if (desc) parts.push(desc);
    return `- ${parts.join(": ")}`;
  });
  return `${prefix}${picked.join("\n")}`;
}

function coerceLanguage(language: unknown): ChatLanguage | null {
  if (language === "en" || language === "fr") return language;
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
- If the user mentions "prix", "price", "budget", "coût", "cost":
    Intent remains LOOKUP
    Include MinPrice and MaxPrice (or PRICE_RANGE concept)
    Do NOT switch to COUNT or AGGREGATE
- status=ok only when plan is executable.
- status=need_clarification when key info is missing or ambiguous; include ask_user in same detected language.
- status=out_of_scope when user asks outside real-estate data; include ask_user in same detected language.
- For intent=count, aggregation must be null.
- For intent=aggregate, aggregation is required.
- Deduplicate select concepts.
- Only ignore values like "test", "dummy", "sample" WHEN they appear ALONE and NOT as part of a longer name.
  If "test" is part of a multi-word entity name (e.g., "lilas test"), treat it as a valid name.
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
    return {
      plannerLanguage: language,
      plannerStatus: "need_clarification",
      askUser: language === "fr"
        ? "Pouvez-vous preciser votre demande (ville, type de projet, ou statut) ?"
        : "Could you clarify your request (city, project type, or status)?",
      executablePlan: { intent: { type: "lookup", table: "projects", columns: ["name"] }, filters: [], limit: 10, sort: undefined },
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
    { select: effectivePlannerPlan.select, notes: effectivePlannerPlan.notes },
    normalizedMessage,
  );
  effectivePlannerPlan.select = hintedSelection.select;
  effectivePlannerPlan.notes = hintedSelection.notes;

  const tableMeta = allowlist.tables.get(effectivePlannerPlan.table.toLowerCase());
  if (!tableMeta) throw new Error(`Planner selected unknown table "${effectivePlannerPlan.table}".`);

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
    if (concept === "PRICE" || concept === "SURFACE") {
      const ranged = candidates.filter((col) => /(min|max)/i.test(col));
      if (ranged.length > 0) return Array.from(new Set([...ranged.slice(0, 2), candidates[0]]));
    }
    return [candidates[0]];
  };

  const resolvedSelect = Array.from(
    new Set(effectivePlannerPlan.select.flatMap((concept) => resolveConceptForSelect(concept))),
  );

  const resolvedFilters = effectivePlannerPlan.filters
    .map((filter) => {
      const column = resolveConcept(filter.concept);
      if (!column) return null;
      const mappedOperator =
        filter.operator === "=" ? "eq" : filter.operator === "!=" ? "neq" : filter.operator === "IN" ? "in" : "contains";
      return { field: column, operator: mappedOperator as QueryPlan["filters"][number]["operator"], value: filter.value };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const resolvedSort = effectivePlannerPlan.sort
    .map((item) => {
      const column = resolveConcept(item.concept);
      if (!column) return null;
      return { field: column, direction: item.direction };
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
          ? { func: effectivePlannerPlan.aggregation.func, column: aggregationColumn }
          : undefined,
    },
    filters: resolvedFilters,
    limit: Math.max(1, Math.min(50, effectivePlannerPlan.limit)),
    sort: resolvedSort.length > 0 ? resolvedSort[0] : undefined,
  };

  const shapeValidation = validateQueryPlan(executablePlan);
  if (!shapeValidation.ok) throw new Error(`Invalid QueryPlan JSON: ${shapeValidation.errors.join(" ")}`);

  const allowlistValidation = validateAgainstAllowlist(shapeValidation.plan, allowlist);
  if (!allowlistValidation.ok) throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);

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
  matchLevel?: "exact" | "close" | "city" | "semantic" | "alternative",
): Promise<string> {
  const presentationRows = enrichRowsWithPriceRange(sanitizeRowsForResponder(rows, message), message);

  const matchQualityHint = matchLevel && matchLevel !== "exact"
    ? `\nIMPORTANT: The results below are NOT exact matches. Match quality level: "${matchLevel}".
- If "close": results are approximate matches (e.g. fuzzy location or operator relaxation).
- If "city": results are from the same city but may not match all criteria.
- If "semantic": results are semantically similar but from a broader search.
- If "alternative": these are the best available alternatives; clearly state they don't exactly match.
You MUST mention this to the user clearly and helpfully. Do NOT present them as exact matches.
Use phrasing like: "I couldn't find an exact match, but here are the closest results..." or the French equivalent.`
    : "";

  const response = await queryAI(
    `You are a client-facing real-estate assistant.
Reply in ${language === "fr" ? "French" : "English"}.
Use only the provided query results and never invent missing facts.
Keep a warm, concise, professional tone.
Do not mention SQL, query plans, allowlists, backend, internal validation, or system errors.
Do not expose technical identifiers (id/uuid/coordinates) unless the user explicitly asked for them.
${matchQualityHint}
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
- Do NOT just say "please rephrase" -- give a specific helpful alternative query.
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
      if (rows.length > 0 && looksLikeNoResultsAnswer(parsed.answer)) {
        return buildDeterministicRowsAnswer(language, presentationRows, matchLevel);
      }
      return parsed.answer;
    }
  } catch {
    // Fall through.
  }

  if (rows.length > 0) return buildDeterministicRowsAnswer(language, presentationRows, matchLevel);

  return language === "fr"
    ? "Je n'ai pas pu formater une reponse claire, mais les resultats sont fournis."
    : "I could not format a clear answer, but the query results are provided.";
}

export async function chat(message: string, requestedLanguage?: unknown): Promise<ChatResponse> {
  const conversationId = randomUUID();
  try {
    const resolvedLanguage = coerceLanguage(requestedLanguage) ?? (await detectLanguage(message));
    const allowlist = await getCachedAllowlist();
    const presetExecution = await runPresetQuery(message);
    if (presetExecution) {
      const rows = dedupeRows(presetExecution.rows);
      const answer = await generateAnswer(resolvedLanguage, message, presetExecution.plan, rows, "exact");
      const suggestions =
        rows.length > 8
          ? await buildChatSuggestions({
              allowlist,
              message,
              language: resolvedLanguage,
              tableKey: presetExecution.plan.intent.table,
            })
          : undefined;

      return {
        conversation_id: conversationId,
        language: resolvedLanguage,
        status: "ok",
        answer,
        suggestions,
        queryPlan: presetExecution.plan,
        results: rows,
      };
    }

    // Handle broad/vague entity requests immediately without clarification
    const broadEntity = detectBroadEntityRequest(message);
    if (broadEntity) {
      const broadExecution = await runBroadEntityQuery(broadEntity);
      if (broadExecution && broadExecution.rows.length > 0) {
        const rows = dedupeRows(broadExecution.rows);
        const answer = await generateAnswer(resolvedLanguage, message, broadExecution.plan, rows, "exact");
        const suggestions =
          rows.length > 8
            ? await buildChatSuggestions({
                allowlist,
                message,
                language: resolvedLanguage,
                tableKey: broadExecution.plan.intent.table,
              })
            : undefined;

        return {
          conversation_id: conversationId,
          language: resolvedLanguage,
          status: "ok",
          answer,
          suggestions,
          queryPlan: broadExecution.plan,
          results: rows,
        };
      }
    }

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

    if (planning.plannerStatus !== "ok") {
      // Before returning a clarification, try progressive fallback to find ANY useful results
      const allowlistCheck = validateAgainstAllowlist(plan, allowlist);
      if (allowlistCheck.ok) {
        const fallbackResult = await progressiveFallbackSearch(plan, allowlistCheck.table, message, allowlist);
        if (fallbackResult && fallbackResult.rows.length > 0) {
          const answer = await generateAnswer(
            planning.plannerLanguage,
            message,
            fallbackResult.plan,
            fallbackResult.rows,
            fallbackResult.matchLevel,
          );
          const suggestions = await buildChatSuggestions({
            allowlist,
            message,
            language: planning.plannerLanguage,
            tableKey: fallbackResult.plan.intent.table,
          });
          return {
            conversation_id: conversationId,
            language: planning.plannerLanguage,
            status: "ok",
            answer,
            suggestions,
            queryPlan: fallbackResult.plan,
            results: fallbackResult.rows,
          };
        }
      }

      const lang = planning.plannerLanguage;
      const fallbackQuestion = lang === "fr" ? "Pouvez-vous reformuler votre demande ?" : "Could you rephrase your request?";
      const followUp = planning.askUser || fallbackQuestion;
      const suggestions = await buildChatSuggestions({
        allowlist,
        message,
        language: lang,
        tableKey: plan.intent.table,
      });
      return {
        conversation_id: conversationId,
        language: lang,
        status: planning.plannerStatus,
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
    let matchLevel: "exact" | "close" | "city" | "semantic" | "alternative" = "exact";
    let { sql, params } = compileQueryPlan(executedPlan, allowlistValidation.table);
    let dbResult = await query(sql, params);
    logSql("main", sql, params, dbResult.rows.length);
    let rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);

    // If exact query returns no results, use progressive fallback
    if (rows.length === 0) {
      const fallbackResult = await progressiveFallbackSearch(
        executedPlan,
        allowlistValidation.table,
        message,
        allowlist,
      );
      if (fallbackResult) {
        executedPlan = fallbackResult.plan;
        rows = fallbackResult.rows;
        matchLevel = fallbackResult.matchLevel;
      }
    }

    const answer = await generateAnswer(resolvedLanguage, message, executedPlan, rows, matchLevel);
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
      conversation_id: conversationId,
      language: resolvedLanguage,
      status: "ok",
      answer,
      suggestions,
      queryPlan: executedPlan,
      results: rows,
    };
  } catch (error) {
    console.error("[chat] Unhandled error:", error);
    const lang = coerceLanguage(requestedLanguage) ?? "en";
    return {
      conversation_id: conversationId,
      language: lang,
      status: "ok",
      answer: lang === "fr"
        ? "Désolé, une erreur est survenue. Veuillez reformuler votre question."
        : "Sorry, an error occurred. Please try rephrasing your question.",
      queryPlan: { intent: { type: "lookup", table: "projects" }, filters: [], limit: 10, sort: undefined },
      results: [],
    };
  }
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
      return ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c)) &&
        getRankedLocationColumns(table.columns).length > 0;
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
      "name", "project_name", "title", ...locationCols,
      "description", "status", "minprice", "maxprice",
    ].filter((col, idx, arr) => table.columns.has(col) && arr.indexOf(col) === idx);

    const whereParts: string[] = [];
    const params: Array<string | number | boolean | Date | null> = [];
    for (const col of locationCols) {
      const ref = `@p${params.length}`;
      params.push(`%${intent.city}%`);
      whereParts.push(`LOWER(CAST(${quoteIdent(col)} AS NVARCHAR(4000))) LIKE LOWER(CAST(${ref} AS NVARCHAR(4000)))`);
    }

    const sql = `SELECT TOP (50) ${selectedColumns.length > 0 ? selectedColumns.map(quoteIdent).join(", ") : "*"}
FROM ${quoteTable(table.schema, table.table)}
WHERE (${whereParts.join(" OR ")});`;

    const result = await query(sql, params);
    logSql(`rescue:${tableKey}`, sql, params, result.rows.length);
    const rows = dedupeRows(result.rows as Record<string, unknown>[]);
    if (rows.length === 0) continue;

    return {
      plan: {
        intent: { type: "lookup", table: tableKey, columns: selectedColumns.length > 0 ? selectedColumns : undefined },
        filters: [{ field: locationCols[0], operator: "contains", value: intent.city }],
        limit: 50,
        sort: undefined,
      },
      rows,
    };
  }
  return null;
}