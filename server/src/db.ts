import sql from "mssql";

const config: sql.config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_HOST || "localhost",
  database: process.env.DB_NAME,
  port: parseInt(process.env.DB_PORT || "1433", 10),
  options: {
    encrypt: (process.env.DB_ENCRYPT || "false").toLowerCase() === "true",
    trustServerCertificate: (process.env.DB_TRUST_SERVER_CERT || "true").toLowerCase() === "true",
  },
};

let poolPromise: Promise<sql.ConnectionPool> | null = null;

async function getPool(): Promise<sql.ConnectionPool> {
  if (!poolPromise) {
    poolPromise = sql.connect(config);
  }

  return poolPromise;
}

export async function initializeDb(): Promise<void> {
  await getPool();
}

export interface QueryResultRow {
  [key: string]: unknown;
}

export async function query(
  text: string,
  params: Array<string | number | boolean | Date | null> = [],
): Promise<{ rows: QueryResultRow[] }> {
  const pool = await getPool();
  const request = pool.request();

  params.forEach((param, index) => {
    request.input(`p${index}`, param);
  });

  const result = await request.query(text);
  return { rows: result.recordset ?? [] };
}

export async function closeDb(): Promise<void> {
  if (!poolPromise) {
    return;
  }

  const pool = await poolPromise;
  await pool.close();
  poolPromise = null;
}
