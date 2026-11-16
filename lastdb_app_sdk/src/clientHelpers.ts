/**
 * Canonical LastDB runtime API routes, protocol versions, database-scope path
 * builders, and shared client error/key/batch helpers.
 */

import {
  AuthenticationRequiredError,
  CasConflictError,
  NodeTooOldError,
  PermissionDeniedError,
  RequestRejectedError,
  UnexpectedResponseError,
  type CasConflictDetail,
} from './errors.js';
import { isObject } from './responseParsers.js';
import type { Transport } from './transport.js';
import type { KeyValue, QueryBatchItem, RecordKeyInput } from './types.js';

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
const DATABASE_SESSION_HEADER = 'X-LastDB-Database-Session';

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

/** Shared runtime context passed to domain operation modules. */
export interface ClientContext {
  readonly appId: string;
  readonly transport: Transport;
  readonly databasePath: string | null;
  readonly definitionIds: Map<string, string>;
}

/** Normalize a point-lookup or blob key input into a non-empty {@link KeyValue}. */
export function normalizeRecordKeyInput(key: RecordKeyInput, verb: string): KeyValue {
  if (typeof key === 'string') {
    if (key.length === 0) {
      throw new RequestRejectedError('invalid_request', `${verb} requires a non-empty hash key`);
    }
    return { hash: key, range: null };
  }
  if (key.hash === null || key.hash.length === 0) {
    throw new RequestRejectedError('invalid_request', `${verb} requires a non-empty hash key`);
  }
  const range = key.range;
  return {
    hash: key.hash,
    range: range !== undefined && range !== null && range.length > 0 ? range : null,
  };
}

function authenticationRequiredReason(body: {
  code?: string;
  error?: string;
  message?: string;
}): 'session_expired' | 'auth_failed' {
  const text = `${body.code ?? ''} ${body.error ?? ''} ${body.message ?? ''}`.toLowerCase();
  if (text.includes('session') && text.includes('expired')) {
    return 'session_expired';
  }
  if (text.includes('token') && text.includes('expired')) {
    return 'session_expired';
  }
  return 'auth_failed';
}

/** Read modeled detail fields off a `409 {error:"cas_conflict"}` body. */
function casConflictDetailOf(body: unknown): CasConflictDetail {
  const detail: CasConflictDetail = {};
  if (!isObject(body)) {
    return detail;
  }
  if (typeof body['schema'] === 'string') {
    detail.schema = body['schema'];
  }
  if (typeof body['key'] === 'string') {
    detail.key = body['key'];
  }
  if (typeof body['expected'] === 'string') {
    detail.expected = body['expected'];
  }
  if (typeof body['actual'] === 'string' || body['actual'] === null) {
    detail.actual = body['actual'] as string | null;
  }
  if (typeof body['message'] === 'string') {
    detail.message = body['message'];
  }
  return detail;
}

/** Extract the best operator-facing message from a JSON or plain-text refusal body. */
function extractErrorMessage(body: unknown, fallback: string): string {
  if (typeof body === 'string' && body.trim().length > 0) {
    return body.trim();
  }
  if (isObject(body)) {
    for (const field of ['message', 'reason', 'error']) {
      const candidate = body[field];
      if (typeof candidate === 'string' && candidate.length > 0) {
        return candidate;
      }
    }
  }
  return fallback;
}

/** Map a non-200 data-plane response into the SDK's typed error hierarchy. */
export function mapDataError(
  verb: string,
  status: number,
  body: unknown,
  appId: string,
): Error {
  const b = (isObject(body) ? body : {}) as {
    kind?: string;
    code?: string;
    error?: string;
    reason?: string;
    message?: string;
  };
  if (status === 401) {
    return new AuthenticationRequiredError(
      authenticationRequiredReason(b),
      extractErrorMessage(body, `${verb} requires authentication`),
      body ?? null,
    );
  }
  if (status === 409 && b.error === 'cas_conflict') {
    return new CasConflictError(casConflictDetailOf(body), body ?? null);
  }
  if (status === 403) {
    return new PermissionDeniedError(
      extractErrorMessage(body, `${verb} permission denied`),
    );
  }
  if (status === 400) {
    if (b.kind === 'unknown_key') {
      return new NodeTooOldError(
        { reason: 'unknown_key', reported: null, build: null, app: appId },
        body ?? null,
      );
    }
    const hasDetail = typeof b.message === 'string' || typeof b.reason === 'string';
    const kind =
      typeof b.kind === 'string'
        ? b.kind
        : hasDetail && typeof b.error === 'string'
          ? b.error
          : 'invalid_request';
    return new RequestRejectedError(
      kind,
      extractErrorMessage(body, `${verb} rejected`),
      body ?? null,
      400,
    );
  }
  if (status === 404) {
    return new RequestRejectedError(
      'not_found',
      extractErrorMessage(body, `${verb} target not found`),
      body ?? null,
      404,
    );
  }
  if (status === 409) {
    return new RequestRejectedError(
      typeof b.error === 'string' ? b.error : 'conflict',
      extractErrorMessage(body, `${verb} conflict`),
      body ?? null,
      409,
    );
  }
  if (status === 413) {
    return new RequestRejectedError(
      typeof b.error === 'string' ? b.error : 'payload_too_large',
      extractErrorMessage(body, `${verb} payload too large`),
      body ?? null,
      413,
    );
  }
  return new UnexpectedResponseError(
    extractErrorMessage(body, `${verb} returned ${status}`),
    status,
    body,
  );
}

export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/** One query of a batch, with its position in the caller's list. */
export interface BatchEntry {
  index: number;
  item: QueryBatchItem;
}

/** Pair each sent query with its `{status, response}` entry from `/api/queries/batch`. */
export function pairBatchReplies(
  chunk: readonly BatchEntry[],
  body: unknown,
): Array<[BatchEntry, { status: number; response: unknown }]> {
  const results = (body as { results?: unknown } | null)?.results;
  const malformed = (): UnexpectedResponseError =>
    new UnexpectedResponseError(
      `query batch returned a malformed reply (expected ${chunk.length} results)`,
      200,
      body,
    );
  if (!Array.isArray(results) || results.length !== chunk.length) {
    throw malformed();
  }
  const pairs: Array<[BatchEntry, { status: number; response: unknown }]> = [];
  for (const [position, entry] of chunk.entries()) {
    const reply = results[position] as { status?: unknown; response?: unknown } | null;
    if (typeof reply !== 'object' || reply === null || typeof reply.status !== 'number') {
      throw malformed();
    }
    pairs.push([entry, { status: reply.status, response: reply.response }]);
  }
  return pairs;
}
