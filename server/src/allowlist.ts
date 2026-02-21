import { query } from "./db";
import { FilterOperator, SchemaAllowlist } from "./query-plan";

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

export async function loadSchemaAllowlist(): Promise<SchemaAllowlist> {
  const schemaResult = await query(`
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
  `);

  const tables = new Map<
    string,
    {
      schema: string;
      table: string;
      columns: Set<string>;
    }
  >();

  for (const row of schemaResult.rows) {
    const schema = String(row.TABLE_SCHEMA);
    const table = String(row.TABLE_NAME);
    const column = String(row.COLUMN_NAME);
    const key = table.toLowerCase();

    if (!tables.has(key)) {
      tables.set(key, { schema, table, columns: new Set<string>() });
    }

    tables.get(key)!.columns.add(column.toLowerCase());
  }

  return {
    tables,
    operators: new Set(DEFAULT_OPERATORS),
  };
}

export function renderAllowlistForPrompt(allowlist: SchemaAllowlist): string {
  const lines: string[] = [];

  for (const [key, table] of allowlist.tables.entries()) {
    lines.push(`${key}: [${Array.from(table.columns).join(", ")}]`);
  }

  return lines.join("\n");
}
