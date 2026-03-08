import { loadSchemaAllowlist } from "./allowlist";
import type { SchemaAllowlist } from "./query-plan";

let cached: { data: SchemaAllowlist; expiresAt: number } | null = null;
const TTL_MS = 5 * 60 * 1000; // 5 minutes

export async function getCachedAllowlist(): Promise<SchemaAllowlist> {
  const now = Date.now();
  if (cached && now < cached.expiresAt) {
    return cached.data;
  }
  const data = await loadSchemaAllowlist();
  cached = { data, expiresAt: now + TTL_MS };
  return data;
}

export function invalidateAllowlistCache(): void {
  cached = null;
}
