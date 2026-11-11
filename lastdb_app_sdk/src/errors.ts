/**
 * Typed error taxonomy for the LastDB app SDK.
 *
 * Every class maps to a discriminated response the node returns — there is no
 * catch-all "request failed".
 */

/** Base class for every error this SDK raises. */
export class LastDbError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
    // Restore prototype chain for instanceof across the transpile target.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export type TransportErrorKind = 'timeout' | 'protocol' | 'connect';

/** A network/transport failure (socket error, connection refused, timeout, etc.). */
export class TransportError extends LastDbError {
  readonly kind: TransportErrorKind;
  readonly status?: number;

  constructor(
    message: string,
    kind: TransportErrorKind = 'connect',
    options: { status?: number } = {},
  ) {
    super(message);
    this.kind = kind;
    this.status = options.status;
  }
}

/**
 * The node answered with an HTTP status the SDK has no specific class for.
 * Carries the status and the parsed body so the caller can still react.
 */
export class UnexpectedResponseError extends LastDbError {
  constructor(
    message: string,
    readonly status: number,
    readonly body: unknown,
  ) {
    super(message);
  }
}

/** Retry the same body with this UUID when the Share upload outcome is uncertain. */
export class ShareSubmissionUncertainError extends LastDbError {
  constructor(readonly mutationId: string, override readonly cause: unknown) {
    super('Share submission outcome is unavailable. Retry with the same mutation ID and body.');
  }
}

/**
 * The node's paginated `/api/query` response stopped making forward progress
 * or could not produce the unique row count it advertised.
 */
export class QueryPaginationError extends LastDbError {
  constructor(
    readonly reason:
      | 'stalled_page'
      | 'total_count_mismatch',
    readonly detail: {
      totalCount?: number;
      collectedCount: number;
      returnedCount?: number;
      pageSize?: number;
      offset?: number;
    },
  ) {
    super(queryPaginationMessage(reason, detail));
  }
}

function queryPaginationMessage(
  reason: 'stalled_page' | 'total_count_mismatch',
  detail: {
    totalCount?: number;
    collectedCount: number;
    returnedCount?: number;
    pageSize?: number;
    offset?: number;
  },
): string {
  if (reason === 'stalled_page') {
    return (
      'queryAll pagination stalled: the node reported more rows but returned ' +
      'no new record keys on the next page'
    );
  }
  return (
    `queryAll collected ${detail.collectedCount} unique rows` +
    (detail.totalCount === undefined
      ? ''
      : ` but the node reported total_count=${detail.totalCount}`)
  );
}

/**
 * The node refused the read/write under the caller's posture
 * (`{kind: "permission_denied"}`).
 */
export type PermissionCategory =
  | 'namespace_denied'
  | 'unverified_identity'
  | 'write_denied'
  | 'unknown';

export class PermissionDeniedError extends LastDbError {
  readonly category: PermissionCategory;

  constructor(
    readonly reason: string,
    category?: PermissionCategory,
  ) {
    super(reason);
    this.category = category ?? classifyPermissionReason(reason);
  }
}

/**
 * Best-effort classification of the node's discriminated permission-denied
 * reason string into a {@link PermissionCategory}.
 */
export function classifyPermissionReason(reason: string): PermissionCategory {
  const r = reason.toLowerCase();
  if (r.includes('is not granted') && r.includes('isolated namespace')) {
    return 'namespace_denied';
  }
  if (r.includes('code-signature-verified app identity')) {
    return 'unverified_identity';
  }
  if (r.includes('write denied by namespace isolation')) {
    return 'write_denied';
  }
  return 'unknown';
}

export type AuthenticationRequiredReason = 'session_expired' | 'auth_failed';

/**
 * The node rejected a data-path request because the caller's authenticated
 * session/API credential is no longer valid (`401`).
 */
export class AuthenticationRequiredError extends LastDbError {
  constructor(
    readonly reason: AuthenticationRequiredReason,
    message: string,
    /** The raw parsed 401 response body, verbatim (`null` when none). */
    readonly body: unknown = null,
  ) {
    super(message);
  }
}

/**
 * The node rejected the request shape or schema state (`400`). Carries the
 * node's `kind` discriminator, message, and raw parsed response `body`.
 */
export class RequestRejectedError extends LastDbError {
  constructor(
    readonly kind: string,
    message: string,
    /** The raw parsed 400 response body, verbatim (`null` when none). */
    readonly body: unknown = null,
  ) {
    super(message);
  }
}

/**
 * Why the SDK decided the node is too old for this client.
 *
 * - `api_version_below_required`: `GET /api/version` reported a number below
 *   what the app declared.
 * - `unknown_key`: a data route answered `400 {kind:"unknown_key"}`.
 */
export type NodeTooOldReason = 'api_version_below_required' | 'unknown_key';

/** The facts {@link NodeTooOldError} carries for a one-line remedy. */
export interface NodeTooOldDetail {
  reason: NodeTooOldReason;
  required?: number;
  reported: number | null;
  build: string | null;
  app?: string;
}

/** The one line an operator needs. Same shape from every app. */
export function nodeTooOldMessage(detail: NodeTooOldDetail): string {
  const who = detail.app ?? 'this client';
  const build = detail.build ? ` (build ${detail.build})` : '';
  const remedy = 'Run: brew upgrade lastdb && brew services restart lastdb';
  if (detail.reason === 'unknown_key') {
    const need =
      detail.required !== undefined ? ` ${who} needs api_version >= ${detail.required}.` : '';
    return (
      `LastDB node does not know a request key ${who} sent` +
      ` (400 unknown_key)${build}: the client is newer than the node.${need} ${remedy}`
    );
  }
  const reported = detail.reported === null ? 'no api_version' : `api_version ${detail.reported}`;
  return (
    `${who} needs LastDB api_version >= ${detail.required ?? '?'}; ` +
    `this node reports ${reported}${build}. ${remedy}`
  );
}

/**
 * The node is older than this client needs. Subclasses {@link RequestRejectedError}.
 */
export class NodeTooOldError extends RequestRejectedError {
  constructor(
    readonly detail: NodeTooOldDetail,
    body: unknown = null,
  ) {
    super(detail.reason, nodeTooOldMessage(detail), body);
  }
}

/**
 * The detail fields the node carries on a `409 {error:"cas_conflict"}` body.
 */
export interface CasConflictDetail {
  schema?: string;
  key?: string;
  expected?: string;
  actual?: string | null;
  message?: string;
}

/**
 * A compare-and-set (CAS) precondition on a {@link import('./types.js').MutationOp}
 * did not hold (`409 {error:"cas_conflict", ...}`).
 */
export class CasConflictError extends LastDbError {
  readonly schema: string | null;
  readonly key: string | null;
  readonly expected: string | null;
  readonly actual: string | null;

  constructor(
    detail: CasConflictDetail = {},
    readonly body: unknown = null,
  ) {
    super(detail.message ?? casConflictMessage(detail));
    this.schema = detail.schema ?? null;
    this.key = detail.key ?? null;
    this.expected = detail.expected ?? null;
    this.actual = detail.actual ?? null;
  }
}

function casConflictMessage(detail: CasConflictDetail): string {
  const where = detail.schema !== undefined ? ` on schema '${detail.schema}'` : '';
  const values =
    detail.expected !== undefined || detail.actual !== undefined
      ? ` (expected ${JSON.stringify(detail.expected ?? null)}, ` +
        `actual ${JSON.stringify(detail.actual ?? null)})`
      : '';
  return `CAS conflict${where}: the row changed since the expected precondition${values}`;
}
