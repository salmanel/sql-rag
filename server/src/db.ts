import sql from "mssql";

const MOCK_MODE = process.env.DB_MOCK === "true";

function getConfig(): sql.config {
  const user = process.env.DB_USER;
  const password = process.env.DB_PASSWORD;
  const database = process.env.DB_NAME;
  const server = process.env.DB_HOST || "localhost";

  if (!user || !password || !database) {
    throw new Error(
      "Missing required database environment variables: DB_USER, DB_PASSWORD, DB_NAME"
    );
  }

  return {
    user,
    password,
    server,
    database,
    port: parseInt(process.env.DB_PORT || "1433", 10),
    options: {
      encrypt: (process.env.DB_ENCRYPT || "false").toLowerCase() === "true",
      trustServerCertificate:
        (process.env.DB_TRUST_SERVER_CERT || "true").toLowerCase() === "true",
    },
  };
}

let poolPromise: Promise<sql.ConnectionPool> | null = null;

async function getPool(): Promise<sql.ConnectionPool> {
  if (!poolPromise) {
    const config = getConfig();
    poolPromise = sql.connect(config).catch((err) => {
      poolPromise = null; // Reset on failure so retry is possible
      throw err;
    });
  }

  return poolPromise;
}

export async function initializeDb(): Promise<void> {
  if (MOCK_MODE) {
    console.log("Running in MOCK mode - no database connection");
    return;
  }
  await getPool();
}

export interface QueryResultRow {
  [key: string]: unknown;
}

export async function query(
  text: string,
  params: Array<string | number | boolean | Date | null> = [],
): Promise<{ rows: QueryResultRow[] }> {
  if (MOCK_MODE) {
    console.log("MOCK query:", text, params);
    return { rows: [] };
  }
  const pool = await getPool();
  const request = pool.request();

  params.forEach((param, index) => {
    request.input(`p${index}`, param);
  });

  const result = await request.query(text);
  return { rows: result.recordset ?? [] };
}

export async function closeDb(): Promise<void> {
  if (MOCK_MODE) return;

  if (!poolPromise) {
    return;
  }

  const pool = await poolPromise;
  await pool.close();
  poolPromise = null;
}
