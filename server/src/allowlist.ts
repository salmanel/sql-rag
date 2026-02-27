import { query } from "./db";
import { AllowlistTable, FilterOperator, PlannerConcept, SchemaAllowlist } from "./query-plan";

const DEFAULT_OPERATORS: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "contains",
  "starts_with",
  "in",
];

const CONCEPTS: PlannerConcept[] = [
  "LOCATION_CITY",
  "LOCATION_TEXT",
  "PROJECT_NAME",
  "PROJECT_DESCRIPTION",
  "PROJECT_STATUS",
  "PRICE",
  "SURFACE",
];

function normalizeToken(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function scoreColumn(column: string, include: string[], exclude: string[] = []): number {
  const normalized = normalizeToken(column);
  if (exclude.some((token) => normalized.includes(token))) {
    return 0;
  }

  let score = 0;
  for (const token of include) {
    if (normalized === token) score += 6;
    else if (normalized.startsWith(token)) score += 4;
    else if (normalized.includes(token)) score += 2;
  }
  return score;
}

function applyPreferredOrder(columns: string[], preferred: string[]): string[] {
  const normalized = new Set(columns.map((c) => c.toLowerCase()));
  const orderedPreferred = preferred.filter((c) => normalized.has(c.toLowerCase()));
  const remaining = columns.filter((c) => !orderedPreferred.includes(c));
  return [...orderedPreferred, ...remaining];
}

function fromPreferredColumns(
  columns: Set<string>,
  preferred: Partial<Record<PlannerConcept, string[]>>,
): Record<PlannerConcept, string[]> {
  const base = {} as Record<PlannerConcept, string[]>;
  for (const concept of CONCEPTS) {
    const candidate = preferred[concept] || [];
    base[concept] = candidate.filter((col, idx, arr) => columns.has(col.toLowerCase()) && arr.indexOf(col) === idx);
  }
  return base;
}

function buildConceptMap(tableKey: string, columns: Set<string>): Record<PlannerConcept, string[]> {
  const allColumns = Array.from(columns);
  const conceptTokens: Record<PlannerConcept, { include: string[]; exclude?: string[] }> = {
    LOCATION_CITY: {
      include: ["city", "ville", "commune", "province", "address", "adresse"],
      exclude: ["lat", "lng", "lon", "coord", "latitude", "longitude"],
    },
    LOCATION_TEXT: {
      include: ["address", "adresse", "quartier", "zone", "secteur", "localisation", "location"],
      exclude: ["lat", "lng", "lon", "coord", "latitude", "longitude"],
    },
    PROJECT_NAME: {
      include: ["name", "projectname", "nom", "title", "libelle"],
    },
    PROJECT_DESCRIPTION: {
      include: ["description", "details", "summary", "resume", "about"],
    },
    PROJECT_STATUS: {
      include: ["status", "state", "etat", "availability", "disponibilite"],
    },
    PRICE: {
      include: ["price", "prix", "cost", "amount", "montant", "budget", "tarif", "value", "minprice", "maxprice"],
      exclude: ["surface", "area"],
    },
    SURFACE: {
      include: ["surface", "area", "sqm", "sqft", "m2", "superficie"],
    },
  };

  const map = {} as Record<PlannerConcept, string[]>;
  for (const concept of CONCEPTS) {
    const { include, exclude = [] } = conceptTokens[concept];
    const ranked = allColumns
      .map((column) => ({
        column,
        score: scoreColumn(column, include, exclude),
      }))
      .filter((item) => item.score > 0)
      .sort((a, b) => b.score - a.score)
      .map((item) => item.column);
    map[concept] = ranked;
  }

  // Controlled fallback for city when no explicit city-like column exists.
  if (map.LOCATION_CITY.length === 0) {
    map.LOCATION_CITY = map.LOCATION_TEXT.slice(0, 2);
  }

  // Schema-aware overrides based on your domain tables.
  if (tableKey === "projects") {
    const preferred = fromPreferredColumns(columns, {
      LOCATION_CITY: ["address"],
      LOCATION_TEXT: ["address"],
      PROJECT_NAME: ["name"],
      PROJECT_DESCRIPTION: ["description"],
      PROJECT_STATUS: ["statusglobal", "type", "overallprogress"],
      PRICE: [],
      SURFACE: [],
    });
    for (const concept of CONCEPTS) {
      map[concept] = applyPreferredOrder(map[concept], preferred[concept]);
    }
  }

  if (tableKey === "units") {
    const preferred = fromPreferredColumns(columns, {
      PROJECT_NAME: ["unitnumber", "id"],
      PROJECT_DESCRIPTION: ["view", "orientation"],
      PROJECT_STATUS: ["status"],
      PRICE: ["latestprice", "pricesaleablevalue", "pricesaleablevalue1"],
      SURFACE: [
        "totalsurface",
        "apartmentsurface",
        "balconysurface",
        "terracesurface",
        "gardensurface",
        "saleablevalue",
        "saleablevalue1",
      ],
    });
    for (const concept of CONCEPTS) {
      map[concept] = applyPreferredOrder(map[concept], preferred[concept]);
    }
  }

  if (tableKey === "immeubles") {
    const preferred = fromPreferredColumns(columns, {
      LOCATION_CITY: ["location"],
      LOCATION_TEXT: ["location"],
      PROJECT_NAME: ["name"],
      PROJECT_DESCRIPTION: ["description"],
      PROJECT_STATUS: ["status", "type", "residencytype"],
      PRICE: ["minprice", "maxprice"],
      SURFACE: ["minsellablesurfacerange", "maxsellablesurfacerange"],
    });
    for (const concept of CONCEPTS) {
      map[concept] = applyPreferredOrder(map[concept], preferred[concept]);
    }
  }

  if (tableKey === "quartiers") {
    const preferred = fromPreferredColumns(columns, {
      PROJECT_NAME: ["name"],
      PROJECT_DESCRIPTION: ["description"],
      LOCATION_TEXT: ["name"],
    });
    for (const concept of CONCEPTS) {
      map[concept] = applyPreferredOrder(map[concept], preferred[concept]);
    }
  }

  return map;
}

export async function loadSchemaAllowlist(): Promise<SchemaAllowlist> {
  const schemaResult = await query(`
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
  `);

  const tables = new Map<string, AllowlistTable>();

  for (const row of schemaResult.rows) {
    const schema = String(row.TABLE_SCHEMA);
    const table = String(row.TABLE_NAME);
    const column = String(row.COLUMN_NAME);
    const key = table.toLowerCase();

    if (!tables.has(key)) {
      tables.set(key, { schema, table, columns: new Set<string>(), conceptMap: {} as Record<PlannerConcept, string[]> });
    }

    tables.get(key)!.columns.add(column.toLowerCase());
  }

  for (const table of tables.values()) {
    table.conceptMap = buildConceptMap(table.table.toLowerCase(), table.columns);
  }

  return {
    tables,
    operators: new Set(DEFAULT_OPERATORS),
  };
}

export function renderAllowlistForPrompt(allowlist: SchemaAllowlist): string {
  const lines: string[] = [];

  for (const [key, table] of allowlist.tables.entries()) {
    lines.push(`Table: ${key}`);
    lines.push(`Columns: [${Array.from(table.columns).join(", ")}]`);
    lines.push("ConceptMap:");
    for (const concept of CONCEPTS) {
      lines.push(`  ${concept}: [${table.conceptMap[concept].join(", ")}]`);
    }
    lines.push("");
  }

  return lines.join("\n");
}
