/**
 * Canonical LastDB runtime API routes, protocol versions, headers, and
 * database-scope path builders (`uds_router.rs` + `database_routes/mod.rs`).
 */

import { RequestRejectedError } from './errors.js';

/** Canonical LastDB runtime API route strings used by the SDK. */
export const LASTDB_API_ROUTES = {
  list: '/api/list',
  query: '/api/query',
  /** Several independent `/api/query` bodies in one request. */
  queryBatch: '/api/queries/batch',
  mutation: '/api/mutation',
  /** Atomic batch of complete conditional record mutations. */
  mutationBatch: '/api/mutations/batch',
  shareSubmissionStatus: '/api/submissions/status',
  /** Create or replace a record's blob, inline, or local-reference file. */
  putBlob: '/api/db/put-blob',
  /** Fetch a record's blob or inline file bytes. */
  fetchBlob: '/api/db/fetch-blob',
  /** Clear a record's blob or inline file via complete replacement. */
  deleteBlob: '/api/db/delete-blob',
  autoIdentity: '/api/system/auto-identity',
  schemas: '/api/schemas',
  /** Single schema lookup prefix (`/api/schema/<encoded-name>`). */
  schemaPrefix: '/api/schema',
  /** Owner socket. The response `definition_id` is the definition the app declared. */
  schemasDeclare: '/api/schemas/declare',
  /** Owner socket database catalog and create endpoint. */
  databases: '/api/databases',
  /** Owner socket owned-database spec binding endpoint. */
  databasesSpec: '/api/databases/spec',
  /** Owner socket signed-log share invitation acceptance endpoint. */
  databasesAccept: '/api/databases/accept',
  /** Owner socket short-lived database session issuance endpoint. */
  databasesSession: '/api/databases/session',
  /** The client↔node compatibility handshake. Admitted on all sessions. */
  version: '/api/version',
} as const;

/** Header carrying an owner-approved short-lived database session token. */
export const DATABASE_SESSION_HEADER = 'X-LastDB-Database-Session';

/** Header carrying the UTF-8 JSON prefix byte length on binary `put-blob` requests. */
export const FILE_BLOB_METADATA_LENGTH_HEADER = 'X-LastDB-File-Blob-Metadata-Length';

/** Lowercase response header carrying the plaintext SHA-256 on binary `fetch-blob` replies. */
export const FILE_SHA256_HEADER = 'x-lastdb-file-sha256';

/** Lowercase response header carrying the plaintext byte length on binary `fetch-blob` replies. */
export const FILE_SIZE_HEADER = 'x-lastdb-file-size';

const DATABASE_SELECTOR_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/** True when `value` matches `lastdbd`'s `valid_selector` grammar (`[A-Za-z0-9_-]{1,64}`). */
export function isValidDatabaseSelector(value: string): boolean {
  return DATABASE_SELECTOR_PATTERN.test(value);
}

/** Validate a database ID or alias against `lastdbd`'s selector grammar. */
export function validateDatabaseSelector(databaseId: string): string {
  if (!isValidDatabaseSelector(databaseId)) {
    throw new RequestRejectedError('database_id', 'Invalid LastDB database ID or alias');
  }
  return databaseId;
}

/** Path prefix (`/api/databases/<selector>`) for a selected database, or `null` for the personal default. */
export function databaseScope(databaseId: string | undefined): string | null {
  if (databaseId === undefined) {
    return null;
  }
  return `${LASTDB_API_ROUTES.databases}/${validateDatabaseSelector(databaseId)}`;
}

/**
 * Resolve an `/api/*` data route against an optional database scope and query string.
 *
 * Mirrors `database_routes::execute_database_request`, which strips
 * `/api/databases/<selector>/` and dispatches `/api/<rest>`.
 */
export function scopedApiPath(
  scope: string | null,
  route: string,
  queryString?: string,
): string {
  const base =
    scope === null
      ? route
      : `${scope}${route.startsWith('/api/') ? route.slice('/api'.length) : route}`;
  return queryString && queryString.length > 0 ? `${base}?${queryString}` : base;
}

/** Build the single-schema metadata route (`/api/schema/<encoded>` or scoped equivalent). */
export function schemaRoute(scope: string | null, schemaName: string): string {
  const trimmed = schemaName.trim();
  if (trimmed.length === 0) {
    throw new RequestRejectedError('invalid_request', 'schema name must not be empty');
  }
  return scopedApiPath(
    scope,
    `${LASTDB_API_ROUTES.schemaPrefix}/${encodeURIComponent(trimmed)}`,
  );
}

/**
 * Validate and build per-client session headers. Never includes the session
 * token in thrown error messages so credentials stay out of logs.
 */
export function resolveSessionHeaders(
  databaseId: string | undefined,
  databaseSession: string | undefined,
): Record<string, string> {
  if (databaseSession === undefined) {
    return {};
  }
  if (databaseId === undefined) {
    throw new RequestRejectedError(
      'database_session',
      'databaseSession requires an explicit databaseId',
    );
  }
  validateDatabaseSelector(databaseId);
  const trimmed = databaseSession.trim();
  if (trimmed.length === 0 || /[\r\n]/.test(trimmed)) {
    throw new RequestRejectedError(
      'database_session',
      'Invalid LastDB database session token',
    );
  }
  return { [DATABASE_SESSION_HEADER]: trimmed };
}
