/**
 * Platform DB handle / locator (no org product dependency).
 *
 * Org fills handles (`org` CLI injects `LASTDB_DB`); the SDK forwards them on
 * every data-path request as `X-LastDB-Db`. Mini resolves the locator to a
 * storage_prefix (see design-org-db-handle-platform-gap).
 *
 * Forms:
 * - `lastdb://personal` (default)
 * - `lastdb://org/<org-slug>[/<db-slug>]`
 * - bare `org/db` or `org` shorthand
 * - raw 64-hex `db_hash` / `lastdb://db/<64-hex>`
 */

/** Env var org (and agents) set so apps inherit an explicit DB handle. */
export const LASTDB_DB_ENV = 'LASTDB_DB';

/** HTTP header Mini reads for multi-DB storage scoping. */
export const LASTDB_DB_HEADER = 'X-LastDB-Db';

/** Canonical personal home locator. */
export const PERSONAL_DB_LOCATOR = 'lastdb://personal';

export type PersonalDbHandle = {
  scope: 'personal';
  locator: typeof PERSONAL_DB_LOCATOR;
};

export type OrgDbHandle = {
  scope: 'org';
  orgSlug: string;
  dbSlug?: string;
  locator: string;
};

export type DbHashHandle = {
  scope: 'db_hash';
  dbHash: string;
  locator: string;
};

export type DbHandle = PersonalDbHandle | OrgDbHandle | DbHashHandle;

export class DbLocatorError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = 'DbLocatorError';
    this.code = code;
  }
}

const ORG_LOCATOR =
  /^lastdb:\/\/org\/([a-z0-9][a-z0-9_-]{0,62})(?:\/([a-z0-9][a-z0-9_-]{0,62}))?$/i;
const DB_HASH_LOCATOR = /^lastdb:\/\/db\/([0-9a-fA-F]{64})$/;
const HEX64 = /^[0-9a-fA-F]{64}$/;

export function personalDb(): PersonalDbHandle {
  return { scope: 'personal', locator: PERSONAL_DB_LOCATOR };
}

export function orgDb(orgSlug: string, dbSlug?: string): OrgDbHandle {
  assertSlug(orgSlug, 'org slug');
  if (dbSlug !== undefined) assertSlug(dbSlug, 'db slug');
  const locator =
    dbSlug !== undefined
      ? `lastdb://org/${orgSlug}/${dbSlug}`
      : `lastdb://org/${orgSlug}`;
  return { scope: 'org', orgSlug, dbSlug, locator };
}

export function parseDbLocator(raw: string): DbHandle {
  const s = raw.trim();
  if (
    s.length === 0 ||
    s === 'personal' ||
    s === PERSONAL_DB_LOCATOR ||
    s.toLowerCase() === 'personal'
  ) {
    return personalDb();
  }

  const dbHashForm = s.match(DB_HASH_LOCATOR);
  if (dbHashForm) {
    const dbHash = dbHashForm[1]!.toLowerCase();
    return { scope: 'db_hash', dbHash, locator: `lastdb://db/${dbHash}` };
  }
  if (HEX64.test(s)) {
    const dbHash = s.toLowerCase();
    return { scope: 'db_hash', dbHash, locator: `lastdb://db/${dbHash}` };
  }

  if (!s.startsWith('lastdb://')) {
    const parts = s.split('/').filter(Boolean);
    if (parts.length === 1) return orgDb(parts[0]!);
    if (parts.length === 2) return orgDb(parts[0]!, parts[1]!);
    throw new DbLocatorError(
      'locator_invalid',
      `invalid DB locator: ${JSON.stringify(raw)} (use lastdb://personal or lastdb://org/<slug>[/<db>])`,
    );
  }

  const m = s.match(ORG_LOCATOR);
  if (!m) {
    throw new DbLocatorError(
      'locator_invalid',
      `invalid DB locator: ${JSON.stringify(raw)}`,
    );
  }
  return orgDb(m[1]!, m[2]);
}

/**
 * Resolve the DB locator this client should send on `X-LastDB-Db`.
 *
 * Order: explicit option → `process.env.LASTDB_DB` → personal.
 * Always returns a non-empty canonical locator string.
 */
export function resolveDbLocator(explicit?: string | null): string {
  const fromEnv =
    typeof process !== 'undefined' && process.env
      ? process.env[LASTDB_DB_ENV]
      : undefined;
  const raw = (explicit ?? fromEnv ?? PERSONAL_DB_LOCATOR).trim();
  if (raw.length === 0) return PERSONAL_DB_LOCATOR;
  return parseDbLocator(raw).locator;
}

function assertSlug(slug: string, label: string): void {
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/i.test(slug)) {
    throw new DbLocatorError(
      'slug_invalid',
      `invalid ${label}: ${JSON.stringify(slug)}`,
    );
  }
}
