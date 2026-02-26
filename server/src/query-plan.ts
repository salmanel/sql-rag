export type ChatLanguage = "en" | "fr";

export type IntentType = "lookup" | "count" | "aggregate";
export type PlannerStatus = "ok" | "need_clarification" | "out_of_scope";
export type PlannerOperator = "=" | "!=" | "LIKE" | "IN";

export type PlannerConcept =
  | "LOCATION_CITY"
  | "LOCATION_TEXT"
  | "PROJECT_NAME"
  | "PROJECT_DESCRIPTION"
  | "PROJECT_STATUS"
  | "PRICE"
  | "SURFACE";

export type FilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "starts_with"
  | "in";

export interface QueryPlan {
  intent: {
    type: IntentType;
    table: string;
    columns?: string[];
    aggregation?: {
      func: "sum" | "avg" | "min" | "max";
      column: string;
    };
  };
  filters: Array<{
    field: string;
    operator: FilterOperator;
    value: string | number | boolean | Array<string | number | boolean>;
  }>;
  limit: number;
  sort?: {
    field: string;
    direction: "asc" | "desc";
  };
}

export interface AllowlistTable {
  schema: string;
  table: string;
  columns: Set<string>;
  conceptMap: Record<PlannerConcept, string[]>;
}

export interface SchemaAllowlist {
  tables: Map<string, AllowlistTable>;
  operators: Set<FilterOperator>;
}

export interface PlannerQueryPlan {
  language: ChatLanguage;
  status: PlannerStatus;
  intent: IntentType;
  table: string;
  select: PlannerConcept[];
  filters: Array<{
    concept: PlannerConcept;
    operator: PlannerOperator;
    value: string | string[];
  }>;
  sort: Array<{
    concept: PlannerConcept;
    direction: "asc" | "desc";
  }>;
  limit: number;
  aggregation:
    | null
    | {
        func: "sum" | "avg" | "min" | "max";
        concept: Exclude<PlannerConcept, "LOCATION_CITY" | "LOCATION_TEXT">;
        group_by: PlannerConcept[];
      };
  ask_user: string | null;
  notes: string;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const ALLOWED_CONCEPTS: PlannerConcept[] = [
  "LOCATION_CITY",
  "LOCATION_TEXT",
  "PROJECT_NAME",
  "PROJECT_DESCRIPTION",
  "PROJECT_STATUS",
  "PRICE",
  "SURFACE",
];

function isPlannerConcept(value: unknown): value is PlannerConcept {
  return ALLOWED_CONCEPTS.includes(String(value) as PlannerConcept);
}

export function validatePlannerQueryPlan(
  raw: unknown,
): { ok: true; plan: PlannerQueryPlan } | { ok: false; errors: string[] } {
  const errors: string[] = [];

  if (!isPlainObject(raw)) {
    return { ok: false, errors: ["Planner payload must be a JSON object."] };
  }

  const language = raw.language;
  const status = raw.status;
  const intent = raw.intent;
  const table = raw.table;
  const selectRaw = raw.select;
  const filtersRaw = raw.filters;
  const sortRaw = raw.sort;
  const limitRaw = raw.limit;
  const aggregationRaw = raw.aggregation;
  const askUser = raw.ask_user;
  const notes = raw.notes;

  if (!(language === "en" || language === "fr")) errors.push("language must be en or fr.");
  if (!["ok", "need_clarification", "out_of_scope"].includes(String(status))) {
    errors.push("status is invalid.");
  }
  if (!["lookup", "count", "aggregate"].includes(String(intent))) errors.push("intent is invalid.");
  if (typeof table !== "string" || table.trim().length === 0) errors.push("table must be a non-empty string.");
  if (!Array.isArray(selectRaw)) errors.push("select must be an array.");
  if (!Array.isArray(filtersRaw)) errors.push("filters must be an array.");
  if (!Array.isArray(sortRaw)) errors.push("sort must be an array.");

  let limit = 20;
  if (typeof limitRaw === "number" && Number.isFinite(limitRaw)) {
    limit = Math.trunc(limitRaw);
  } else if (typeof limitRaw === "string" && !Number.isNaN(Number(limitRaw))) {
    limit = Math.trunc(Number(limitRaw));
  }
  if (limit < 1 || limit > 50) errors.push("limit must be between 1 and 50.");

  if (askUser !== null && askUser !== undefined && typeof askUser !== "string") {
    errors.push("ask_user must be string or null.");
  }
  if (typeof notes !== "string") errors.push("notes must be a string.");

  const select = (Array.isArray(selectRaw) ? selectRaw : []).filter(isPlannerConcept);
  if (select.length !== (Array.isArray(selectRaw) ? selectRaw.length : 0)) {
    errors.push("select includes unknown concepts.");
  }

  const filters: PlannerQueryPlan["filters"] = [];
  for (const [i, item] of (Array.isArray(filtersRaw) ? filtersRaw : []).entries()) {
    if (!isPlainObject(item)) {
      errors.push(`filters[${i}] must be an object.`);
      continue;
    }
    if (!isPlannerConcept(item.concept)) errors.push(`filters[${i}].concept is invalid.`);
    if (!["=", "!=", "LIKE", "IN"].includes(String(item.operator))) {
      errors.push(`filters[${i}].operator is invalid.`);
    }
    const value = item.value;
    const validValue =
      typeof value === "string" ||
      (Array.isArray(value) && value.length > 0 && value.every((v) => typeof v === "string"));
    if (!validValue) errors.push(`filters[${i}].value must be string or string array.`);
    filters.push({
      concept: item.concept as PlannerConcept,
      operator: item.operator as PlannerOperator,
      value: value as string | string[],
    });
  }

  const sort: PlannerQueryPlan["sort"] = [];
  for (const [i, item] of (Array.isArray(sortRaw) ? sortRaw : []).entries()) {
    if (!isPlainObject(item)) {
      errors.push(`sort[${i}] must be an object.`);
      continue;
    }
    if (!isPlannerConcept(item.concept)) errors.push(`sort[${i}].concept is invalid.`);
    if (!["asc", "desc"].includes(String(item.direction))) errors.push(`sort[${i}].direction is invalid.`);
    sort.push({
      concept: item.concept as PlannerConcept,
      direction: item.direction as "asc" | "desc",
    });
  }

  let aggregation: PlannerQueryPlan["aggregation"] = null;
  if (aggregationRaw !== null && aggregationRaw !== undefined) {
    if (!isPlainObject(aggregationRaw)) {
      errors.push("aggregation must be an object or null.");
    } else {
      if (!["sum", "avg", "min", "max"].includes(String(aggregationRaw.func))) {
        errors.push("aggregation.func is invalid.");
      }
      if (!isPlannerConcept(aggregationRaw.concept)) {
        errors.push("aggregation.concept is invalid.");
      }
      const groupByRaw = aggregationRaw.group_by;
      if (!Array.isArray(groupByRaw) || groupByRaw.some((c) => !isPlannerConcept(c))) {
        errors.push("aggregation.group_by must be an array of concepts.");
      }
      aggregation = {
        func: aggregationRaw.func as "sum" | "avg" | "min" | "max",
        concept: aggregationRaw.concept as Exclude<PlannerConcept, "LOCATION_CITY" | "LOCATION_TEXT">,
        group_by: (Array.isArray(groupByRaw) ? groupByRaw : []) as PlannerConcept[],
      };
    }
  }

  if (intent === "count" && aggregation !== null) {
    errors.push("aggregation must be null for count intent.");
  }
  if (intent === "aggregate" && aggregation === null) {
    errors.push("aggregation is required for aggregate intent.");
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const dedupedSelect = Array.from(new Set(select));

  return {
    ok: true,
    plan: {
      language: language as ChatLanguage,
      status: status as PlannerStatus,
      intent: intent as IntentType,
      table: String(table),
      select: dedupedSelect,
      filters,
      sort,
      limit,
      aggregation,
      ask_user: askUser === undefined ? null : (askUser as string | null),
      notes: String(notes),
    },
  };
}

export function validateQueryPlan(raw: unknown): { ok: true; plan: QueryPlan } | { ok: false; errors: string[] } {
  const errors: string[] = [];

  if (!isPlainObject(raw)) {
    return { ok: false, errors: ["QueryPlan must be a JSON object."] };
  }

  const intent = raw.intent;
  const filtersRaw = raw.filters;
  const limitRaw = raw.limit;
  const sort = raw.sort;

  const filters = Array.isArray(filtersRaw) ? filtersRaw : [];
  let limit: number;
  if (typeof limitRaw === "number" && Number.isFinite(limitRaw)) {
    limit = Math.trunc(limitRaw);
  } else if (typeof limitRaw === "string" && limitRaw.trim().length > 0 && !Number.isNaN(Number(limitRaw))) {
    limit = Math.trunc(Number(limitRaw));
  } else {
    limit = 50;
  }

  if (!isPlainObject(intent)) {
    errors.push("intent must be an object.");
  }

  if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1) {
    errors.push("limit must be an integer greater than 0.");
  }

  if (sort !== undefined && !isPlainObject(sort)) {
    errors.push("sort must be an object when provided.");
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const typedIntent = intent as Record<string, unknown>;
  const type = typedIntent.type;
  const table = typedIntent.table;
  const columns = typedIntent.columns;
  const aggregation = typedIntent.aggregation;

  if (!["lookup", "count", "aggregate"].includes(String(type))) {
    errors.push("intent.type must be one of lookup, count, aggregate.");
  }

  if (typeof table !== "string" || table.trim().length === 0) {
    errors.push("intent.table must be a non-empty string.");
  }

  if (columns !== undefined) {
    if (!Array.isArray(columns) || columns.some((c) => typeof c !== "string" || c.trim().length === 0)) {
      errors.push("intent.columns must be an array of non-empty strings.");
    }
  }

  if (aggregation !== undefined) {
    if (!isPlainObject(aggregation)) {
      errors.push("intent.aggregation must be an object when provided.");
    } else {
      if (!["sum", "avg", "min", "max"].includes(String(aggregation.func))) {
        errors.push("intent.aggregation.func must be one of sum, avg, min, max.");
      }

      if (typeof aggregation.column !== "string" || aggregation.column.trim().length === 0) {
        errors.push("intent.aggregation.column must be a non-empty string.");
      }
    }
  }

  for (const [index, filter] of filters.entries()) {
    if (!isPlainObject(filter)) {
      errors.push(`filters[${index}] must be an object.`);
      continue;
    }

    if (typeof filter.field !== "string" || filter.field.trim().length === 0) {
      errors.push(`filters[${index}].field must be a non-empty string.`);
    }

    if (!["eq", "neq", "gt", "gte", "lt", "lte", "contains", "starts_with", "in"].includes(String(filter.operator))) {
      errors.push(`filters[${index}].operator is invalid.`);
    }

    const filterValue = filter.value;
    if (Array.isArray(filterValue)) {
      if (filterValue.length === 0) {
        errors.push(`filters[${index}].value array cannot be empty.`);
      }

      const invalidItem = filterValue.some((item) => !["string", "number", "boolean"].includes(typeof item));
      if (invalidItem) {
        errors.push(`filters[${index}].value array supports only string, number, boolean.`);
      }
    } else if (!["string", "number", "boolean"].includes(typeof filterValue)) {
      errors.push(`filters[${index}].value must be a scalar or array.`);
    }
  }

  let normalizedSort: { field: string; direction: "asc" | "desc" } | undefined;
  if (sort !== undefined && isPlainObject(sort)) {
    const typedSort = sort as Record<string, unknown>;
    const sortField = typeof typedSort.field === "string" ? typedSort.field.trim() : "";
    const sortDirection = String(typedSort.direction || "").toLowerCase();
    if (sortField && (sortDirection === "asc" || sortDirection === "desc")) {
      normalizedSort = {
        field: sortField,
        direction: sortDirection as "asc" | "desc",
      };
    }
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const normalizedPlan: QueryPlan = {
    intent: {
      type: type as IntentType,
      table: table as string,
      columns: Array.isArray(columns) ? (columns as string[]) : undefined,
      aggregation: isPlainObject(aggregation)
        ? {
            func: aggregation.func as "sum" | "avg" | "min" | "max",
            column: aggregation.column as string,
          }
        : undefined,
    },
    filters: filters as Array<{
      field: string;
      operator: FilterOperator;
      value: string | number | boolean | Array<string | number | boolean>;
    }>,
    limit,
    sort: normalizedSort,
  };

  return { ok: true, plan: normalizedPlan };
}

export function validateAgainstAllowlist(
  plan: QueryPlan,
  allowlist: SchemaAllowlist,
): { ok: true; table: AllowlistTable } | { ok: false; errors: string[] } {
  const errors: string[] = [];
  const tableKey = plan.intent.table.toLowerCase();
  const tableMeta = allowlist.tables.get(tableKey);

  if (!tableMeta) {
    return { ok: false, errors: [`Table "${plan.intent.table}" is not allowlisted.`] };
  }

  const checkColumn = (column: string, fieldLabel: string) => {
    if (!tableMeta.columns.has(column.toLowerCase())) {
      errors.push(`${fieldLabel} "${column}" is not allowlisted for table "${plan.intent.table}".`);
    }
  };

  if (plan.intent.columns) {
    for (const col of plan.intent.columns) {
      checkColumn(col, "Column");
    }
  }

  if (plan.intent.aggregation) {
    checkColumn(plan.intent.aggregation.column, "Aggregation column");
  }

  for (const filter of plan.filters) {
    if (!allowlist.operators.has(filter.operator)) {
      errors.push(`Operator "${filter.operator}" is not allowlisted.`);
    }
    checkColumn(filter.field, "Filter field");
  }

  if (plan.sort) {
    checkColumn(plan.sort.field, "Sort field");
  }

  if (plan.limit < 1 || plan.limit > 500) {
    errors.push("limit must be between 1 and 500.");
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  return { ok: true, table: tableMeta };
}

function quoteIdent(identifier: string): string {
  return `[${identifier.replace(/]/g, "]]")}]`;
}

function quoteTable(schema: string, table: string): string {
  return `${quoteIdent(schema)}.${quoteIdent(table)}`;
}

function normalizeNumber(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  throw new Error("Expected a numeric value for this operator.");
}

export function compileQueryPlan(
  plan: QueryPlan,
  tableMeta: AllowlistTable,
): { sql: string; params: Array<string | number | boolean | Date | null> } {
  const params: Array<string | number | boolean | Date | null> = [];
  const paramRef = (value: string | number | boolean | Date | null): string => {
    const index = params.push(value) - 1;
    return `@p${index}`;
  };

  const tableExpr = quoteTable(tableMeta.schema, tableMeta.table);
  const whereParts: string[] = [];

  for (const filter of plan.filters) {
    const fieldExpr = quoteIdent(filter.field);
    switch (filter.operator) {
      case "eq":
        if (typeof filter.value === "string") {
          whereParts.push(`LOWER(CAST(${fieldExpr} AS NVARCHAR(4000))) = LOWER(CAST(${paramRef(filter.value)} AS NVARCHAR(4000)))`);
        } else {
          whereParts.push(`${fieldExpr} = ${paramRef(filter.value as string | number | boolean)}`);
        }
        break;
      case "neq":
        if (typeof filter.value === "string") {
          whereParts.push(`LOWER(CAST(${fieldExpr} AS NVARCHAR(4000))) <> LOWER(CAST(${paramRef(filter.value)} AS NVARCHAR(4000)))`);
        } else {
          whereParts.push(`${fieldExpr} <> ${paramRef(filter.value as string | number | boolean)}`);
        }
        break;
      case "gt":
        whereParts.push(`${fieldExpr} > ${paramRef(normalizeNumber(filter.value))}`);
        break;
      case "gte":
        whereParts.push(`${fieldExpr} >= ${paramRef(normalizeNumber(filter.value))}`);
        break;
      case "lt":
        whereParts.push(`${fieldExpr} < ${paramRef(normalizeNumber(filter.value))}`);
        break;
      case "lte":
        whereParts.push(`${fieldExpr} <= ${paramRef(normalizeNumber(filter.value))}`);
        break;
      case "contains":
        whereParts.push(
          `LOWER(CAST(${fieldExpr} AS NVARCHAR(4000))) LIKE LOWER(CAST(${paramRef(`%${String(filter.value)}%`)} AS NVARCHAR(4000)))`,
        );
        break;
      case "starts_with":
        whereParts.push(
          `LOWER(CAST(${fieldExpr} AS NVARCHAR(4000))) LIKE LOWER(CAST(${paramRef(`${String(filter.value)}%`)} AS NVARCHAR(4000)))`,
        );
        break;
      case "in": {
        if (!Array.isArray(filter.value) || filter.value.length === 0) {
          throw new Error(`IN filter for "${filter.field}" must provide a non-empty value array.`);
        }
        const inParams = filter.value.map((item) => paramRef(item));
        whereParts.push(`${fieldExpr} IN (${inParams.join(", ")})`);
        break;
      }
      default:
        throw new Error(`Unsupported operator: ${String(filter.operator)}`);
    }
  }

  const whereClause = whereParts.length > 0 ? ` WHERE ${whereParts.join(" AND ")}` : "";
  const orderClause = plan.sort
    ? ` ORDER BY ${quoteIdent(plan.sort.field)} ${plan.sort.direction.toUpperCase()}`
    : "";

  if (plan.intent.type === "count") {
    return {
      sql: `SELECT COUNT(1) AS [total] FROM ${tableExpr}${whereClause};`,
      params,
    };
  }

  if (plan.intent.type === "aggregate") {
    if (!plan.intent.aggregation) {
      throw new Error("aggregation is required for aggregate intent.");
    }
    const func = plan.intent.aggregation.func.toUpperCase();
    const aggCol = quoteIdent(plan.intent.aggregation.column);
    return {
      sql: `SELECT ${func}(${aggCol}) AS [value] FROM ${tableExpr}${whereClause};`,
      params,
    };
  }

  const selectedColumns =
    plan.intent.columns && plan.intent.columns.length > 0
      ? plan.intent.columns.map(quoteIdent).join(", ")
      : "*";

  return {
    sql: `SELECT TOP (${Math.trunc(plan.limit)}) ${selectedColumns} FROM ${tableExpr}${whereClause}${orderClause};`,
    params,
  };
}
