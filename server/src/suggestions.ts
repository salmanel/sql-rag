import { query } from "./db";
import { ChatLanguage, AllowlistTable, SchemaAllowlist } from "./query-plan";

export interface SuggestionChip {
  label: string;
  payload: string;
  type?: string;
}

interface SuggestionContext {
  allowlist: SchemaAllowlist;
  message: string;
  language: ChatLanguage;
  tableKey?: string;
}

const KNOWN_CITY_MAP: Record<string, string> = {
  casa: "Casablanca",
  casablanca: "Casablanca",
  rabat: "Rabat",
  zenata: "Zenata",
  marrakech: "Marrakech",
  tanger: "Tanger",
  tangier: "Tanger",
  agadir: "Agadir",
  sale: "Sale",
  salé: "Sale",
  temara: "Temara",
  mohammedia: "Mohammedia",
  kenitra: "Kenitra",
  kénitra: "Kenitra",
};

function quoteIdent(identifier: string): string {
  return `[${identifier.replace(/]/g, "]]")}]`;
}

function quoteTable(schema: string, table: string): string {
  return `${quoteIdent(schema)}.${quoteIdent(table)}`;
}

function isJunkValue(value: string): boolean {
  const normalized = value.trim().toLowerCase();
  return ["", "test", "unknown", "n/a", "na", "dummy", "sample", "string"].includes(normalized);
}

function isBadSuggestionValue(value: string): boolean {
  const v = value.trim();
  const lower = v.toLowerCase();
  if (!v) return true;
  if (isJunkValue(v)) return true;
  if (lower.startsWith("http://") || lower.startsWith("https://")) return true;
  if (/^-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?$/.test(v)) return true; // geo coordinate pair
  if (v.length > 48) return true;
  if (/\d{3,}/.test(v)) return true;
  return false;
}

function normalizeToken(value: string): string {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function titleCase(value: string): string {
  return value
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function normalizeCityCandidate(value: string): string | null {
  const raw = value.trim();
  if (isBadSuggestionValue(raw)) return null;

  const normalized = normalizeToken(raw);

  for (const [alias, canonical] of Object.entries(KNOWN_CITY_MAP)) {
    const pattern = new RegExp(`\\b${alias}\\b`, "i");
    if (pattern.test(normalized)) return canonical;
  }

  const tail = raw.split(",").map((part) => part.trim()).filter(Boolean).pop() || raw;
  const tailNormalized = normalizeToken(tail);
  const words = tailNormalized.split(/\s+/).filter(Boolean);
  if (words.length === 0 || words.length > 2) return null;
  if (words.some((word) => /\d/.test(word))) return null;
  if (words.some((word) => ["residence", "residences", "residence", "group", "sam", "by", "beaulieu"].includes(word))) {
    return null;
  }

  return titleCase(tail);
}

function detectMessageContext(message: string): {
  asksPrice: boolean;
  asksType: boolean;
  asksLocation: boolean;
  broadProjects: boolean;
  hasProjectWord: boolean;
} {
  const lower = normalizeToken(message);
  const hasProjectWord = /(project|projects|projet|projets)/.test(lower);
  const asksPrice = /(prix|price|cost|budget|tarif|montant)/.test(lower);
  const asksType = /(type|category|categorie|status|etat|state|disponibilite|availability|available)/.test(lower);
  const asksLocation = /(city|ville|in|a |au |dans|near|location|adresse|address|quartier|zone)/.test(lower);
  const broadProjects =
    /^(all\s+projects|projects|projets|show\s+projects|show\s+me\s+projects|list\s+projects|list\s+all\s+projects)$/i.test(
      lower.trim(),
    ) ||
    (hasProjectWord && !asksPrice && !asksType && !asksLocation);

  return { asksPrice, asksType, asksLocation, broadProjects, hasProjectWord };
}

function dedupeSuggestions(items: SuggestionChip[]): SuggestionChip[] {
  const seen = new Set<string>();
  const output: SuggestionChip[] = [];
  for (const item of items) {
    const key = `${item.label.trim().toLowerCase()}|${item.payload.trim().toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }
  return output;
}

function pickTable(allowlist: SchemaAllowlist, preferredKey?: string): [string, AllowlistTable] | null {
  if (preferredKey) {
    const hit = allowlist.tables.get(preferredKey.toLowerCase());
    if (hit) return [preferredKey.toLowerCase(), hit];
  }
  const projects = allowlist.tables.get("projects");
  if (projects) return ["projects", projects];
  const first = allowlist.tables.entries().next();
  if (first.done) return null;
  return [first.value[0], first.value[1]];
}

function findProjectNameColumn(table: AllowlistTable): string | null {
  return table.conceptMap.PROJECT_NAME[0] || null;
}

function findCityColumn(table: AllowlistTable): string | null {
  const ranked = [...(table.conceptMap.LOCATION_CITY || []), ...(table.conceptMap.LOCATION_TEXT || [])];
  const unique = Array.from(new Set(ranked));
  const best = unique
    .filter((col) => !/(lat|lng|lon|coord|latitude|longitude)/i.test(col))
    .sort((a, b) => {
      const score = (c: string) => {
        const n = c.toLowerCase();
        let s = 0;
        if (/city|ville/.test(n)) s += 100;
        if (/address|adresse/.test(n)) s += 80;
        if (/location/.test(n)) s += 40;
        if (/lat|lng|lon|coord/.test(n)) s -= 100;
        return s;
      };
      return score(b) - score(a);
    });
  return best[0] || null;
}

function findTypeColumn(table: AllowlistTable): string | null {
  const candidates = Array.from(table.columns);
  return (
    candidates.find((c) => /(residencetype|propertytype|type|category|categorie|usage)/i.test(c)) || null
  );
}

function cityPayload(language: ChatLanguage, city: string): string {
  return language === "fr" ? `projets a ${city}` : `projects in ${city}`;
}

function typePayload(language: ChatLanguage, type: string): string {
  return language === "fr" ? `projets de type ${type}` : `${type} projects`;
}

function projectPayload(language: ChatLanguage, name: string): string {
  return language === "fr" ? `prix de ${name}` : `price of ${name}`;
}

function extractPartialProjectName(message: string): string | null {
  const lowered = message.toLowerCase();
  const match = lowered.match(/(?:prix|price|project|projet)\s+(.+)/i);
  if (!match || !match[1]) return null;
  const partial = match[1].trim();
  if (partial.length < 2) return null;
  return partial;
}

function extractPartialCity(message: string): string | null {
  const cleaned = message.toLowerCase().replace(/[^a-z0-9\u00c0-\u017f\s-]/gi, " ").trim();
  if (!cleaned) return null;
  const terms = cleaned.split(/\s+/).filter((t) => t.length >= 3);
  if (terms.length === 0) return null;
  return terms[terms.length - 1];
}

export async function getCitySuggestions(
  table: AllowlistTable,
  language: ChatLanguage,
  partial?: string,
  limit: number = 8,
): Promise<SuggestionChip[]> {
  const cityColumn = findCityColumn(table);
  if (!cityColumn) return [];

  const tableExpr = quoteTable(table.schema, table.table);
  const col = quoteIdent(cityColumn);

  const hasPartial = Boolean(partial && partial.trim().length >= 2);
  const sql = hasPartial
    ? `SELECT TOP (${Math.max(1, Math.min(limit, 8))}) ${col} AS value
       FROM ${tableExpr}
       WHERE ${col} IS NOT NULL AND LTRIM(RTRIM(CAST(${col} AS NVARCHAR(400)))) <> ''
         AND LOWER(CAST(${col} AS NVARCHAR(400))) LIKE @p0
       GROUP BY ${col}
       ORDER BY ${col};`
    : `SELECT TOP (${Math.max(1, Math.min(limit, 8))}) ${col} AS value
       FROM ${tableExpr}
       WHERE ${col} IS NOT NULL AND LTRIM(RTRIM(CAST(${col} AS NVARCHAR(400)))) <> ''
       GROUP BY ${col}
       ORDER BY ${col};`;

  const params = hasPartial ? [`%${normalizeToken(partial!)}%`] : [];
  const result = await query(sql, params);
  const normalizedRows = result.rows
    .map((row) => String(row.value ?? "").trim())
    .map((value) => normalizeCityCandidate(value))
    .filter((value): value is string => Boolean(value))
    .map((value) => ({
      label: value,
      payload: cityPayload(language, value),
      type: "city",
    }));

  return dedupeSuggestions(normalizedRows).slice(0, Math.max(1, Math.min(limit, 8)));
}

export async function getTypeSuggestions(
  table: AllowlistTable,
  language: ChatLanguage,
  limit: number = 8,
): Promise<SuggestionChip[]> {
  const typeColumn = findTypeColumn(table);
  if (!typeColumn) return [];

  const tableExpr = quoteTable(table.schema, table.table);
  const col = quoteIdent(typeColumn);
  const sql = `SELECT TOP (${Math.max(1, Math.min(limit, 8))}) ${col} AS value
               FROM ${tableExpr}
               WHERE ${col} IS NOT NULL AND LTRIM(RTRIM(CAST(${col} AS NVARCHAR(400)))) <> ''
               GROUP BY ${col}
               ORDER BY ${col};`;
  const result = await query(sql);
  return result.rows
    .map((row) => String(row.value ?? "").trim())
    .filter((value) => !isBadSuggestionValue(value))
    .map((value) => ({
      label: value,
      payload: typePayload(language, value),
      type: "type",
    }));
}

export async function getProjectNameSuggestions(
  table: AllowlistTable,
  language: ChatLanguage,
  partial: string,
  limit: number = 8,
): Promise<SuggestionChip[]> {
  const nameColumn = findProjectNameColumn(table);
  if (!nameColumn || partial.trim().length < 2) return [];

  const tableExpr = quoteTable(table.schema, table.table);
  const nameCol = quoteIdent(nameColumn);
  const cityColumn = findCityColumn(table);
  const cityCol = cityColumn ? quoteIdent(cityColumn) : null;
  const sql = cityCol
    ? `SELECT TOP (${Math.max(1, Math.min(limit, 8))}) ${nameCol} AS name, ${cityCol} AS city
       FROM ${tableExpr}
       WHERE ${nameCol} IS NOT NULL
         AND LOWER(CAST(${nameCol} AS NVARCHAR(400))) LIKE @p0
       ORDER BY ${nameCol};`
    : `SELECT TOP (${Math.max(1, Math.min(limit, 8))}) ${nameCol} AS name
       FROM ${tableExpr}
       WHERE ${nameCol} IS NOT NULL
         AND LOWER(CAST(${nameCol} AS NVARCHAR(400))) LIKE @p0
       ORDER BY ${nameCol};`;
  const result = await query(sql, [`%${partial.toLowerCase()}%`]);
  return result.rows
    .map((row) => {
      const name = String(row.name ?? "").trim();
      const city = String(row.city ?? "").trim();
      if (!name || isBadSuggestionValue(name)) return null;
      const label = city && !isJunkValue(city) ? `${name} (${city})` : name;
      return {
        label,
        payload: projectPayload(language, name),
        type: "project",
      } as SuggestionChip;
    })
    .filter((item): item is SuggestionChip => Boolean(item));
}

export async function buildChatSuggestions(context: SuggestionContext): Promise<SuggestionChip[]> {
  const picked = pickTable(context.allowlist, context.tableKey);
  if (!picked) return [];
  const [, table] = picked;
  const lower = normalizeToken(context.message);
  const msgContext = detectMessageContext(context.message);
  const cityPartial = extractPartialCity(context.message);
  const projectPartial = extractPartialProjectName(context.message);

  const suggestions: SuggestionChip[] = [];

  if (msgContext.asksPrice && projectPartial) {
    suggestions.push(...(await getProjectNameSuggestions(table, context.language, projectPartial, 8)));
  }

  const shouldSuggestCities =
    msgContext.broadProjects || msgContext.asksLocation || msgContext.hasProjectWord || suggestions.length === 0;
  if (shouldSuggestCities) {
    suggestions.push(...(await getCitySuggestions(table, context.language, cityPartial || undefined, 8)));
  }

  // Only suggest type chips when the user asks for type/category/status explicitly.
  if (msgContext.asksType && suggestions.length < 8) {
    suggestions.push(...(await getTypeSuggestions(table, context.language, 8)));
  }

  const deduped = dedupeSuggestions(suggestions).slice(0, 8);
  if (deduped.length > 0) return deduped;

  if (msgContext.broadProjects || msgContext.hasProjectWord) {
    return context.language === "fr"
      ? [
          { label: "Casablanca", payload: "projets a casablanca", type: "city" },
          { label: "Rabat", payload: "projets a rabat", type: "city" },
          { label: "Tous les projets", payload: "montre tous les projets", type: "generic" },
        ]
      : [
          { label: "Casablanca", payload: "projects in casablanca", type: "city" },
          { label: "Rabat", payload: "projects in rabat", type: "city" },
          { label: "Show all projects", payload: "show all projects", type: "generic" },
        ];
  }

  return [];
}
