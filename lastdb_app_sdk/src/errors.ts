/**
 * Typed error taxonomy for the LastDB app SDK.
 *
 * Every class here maps 1:1 to a discriminated response the node actually
 * returns — there is no catch-all "request failed". The mapping is verified
 * against the on-`main` handlers:
 *
 * - Consent flow (`fold_db_node/src/server/routes/apps.rs`):
 *   - `request-consent`  → `404 {error, app_id}` (unknown app),
 *                          `403 {reason: "app_in_sandbox", app_id, error}`,
 *                          `400 {error}` (invalid scope).
 *   - `consent-status`   → `202 {status: "pending"}`,
 *                          `200 {status: "granted", capability}`,
 *                          `403 {status: "denied"}` / `403 {status: "revoked"}`,
 *                          `408 {status: "expired"}`,
 *                          `404 {status: "unknown"}`.
 * - Data path (`fold_db_node::dev_mode` `app_endpoints.rs`, production
 *   `fold_db_node` operation layer): `403 {kind: "permission_denied",
 *   error: "<discriminated reason>"}` where the reason text distinguishes
 *   namespace-denied / unverified-identity / write-denied / revoked, and
 *   `400 {kind, error}` for a request-shape / schema-state rejection.
 * - Capability verifier (app_identity v3.1,
 *   `fold_db/crates/core/src/access/capability_denial.rs`): a failed
 *   per-write capability check returns `403 {status: 403, reason: "<one of
 *   the eight discriminated reasons>", ...detail}` — see
 *   {@link CapabilityDeniedError}.
 */

/** Base class for every error this SDK raises. */
export class FoldDbError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
    // Restore prototype chain for instanceof across the transpile target.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ---------------------------------------------------------------------------
// Transport / protocol
// ---------------------------------------------------------------------------

export type TransportErrorKind = 'timeout' | 'protocol' | 'connect';

/** A network/transport failure (socket error, connection refused, DNS, etc.). */
export class TransportError extends FoldDbError {
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
export class UnexpectedResponseError extends FoldDbError {
  constructor(
    message: string,
    readonly status: number,
    readonly body: unknown,
  ) {
    super(message);
  }
}

/**
 * The node's paginated `/api/query` response stopped making forward progress
 * or could not produce the unique row count it advertised. This is an SDK-side
 * guard around the known-unstable offset pagination path; callers should retry
 * later or narrow the query instead of trusting a partial drain.
 */
export class QueryPaginationError extends FoldDbError {
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
 * `queryAll()` was called with no `filter.filter` (a full unfiltered schema
 * drain — a "scan" in LastDB's DynamoDB-style access model) and without the
 * explicit `{ allowFullScan: true }` opt-in. Scans are deprecated for product
 * apps (`brain design-lastdb-scan-deprecation-path`): they are the dominant
 * cause of node load under `lastdb ops`. Use a point read (`filter: {
 * HashKey: id }`) or a partition read (HashRange) instead, or pass
 * `allowFullScan: true` when a full drain is genuinely required (admin/offline
 * tooling, migrations).
 */
export class FullScanNotAllowedError extends FoldDbError {
  constructor(readonly schemaName: string) {
    super(
      `queryAll('${schemaName}') has no filter and allowFullScan is not set — ` +
        'unfiltered scans are deprecated for product apps. Pass a HashKey/HashRange ' +
        'filter, or opts.allowFullScan: true to run the full drain anyway.',
    );
  }
}

// ---------------------------------------------------------------------------
// Consent flow
// ---------------------------------------------------------------------------

/**
 * `request-consent` → `404`. The app id is not in the canonical app registry
 * the node knows about (publish the app to schema_service first).
 */
export class UnknownAppError extends FoldDbError {
  constructor(readonly appId: string) {
    super(`app '${appId}' is not registered with the node's schema service`);
  }
}

/**
 * `request-consent` → `403 {reason: "app_in_sandbox"}`. The app is registered
 * as `sandbox` tier; only the owner-developer (whose dev pubkey matches the
 * app's `owner_dev_pubkey`) may install it. Promote the app to `live` to lift
 * the gate.
 */
export class AppInSandboxError extends FoldDbError {
  constructor(readonly appId: string) {
    super(
      `app '${appId}' is in sandbox tier; only the owner-developer can install it`,
    );
  }
}

/** `request-consent` → `400`. The requested scope string was malformed. */
export class InvalidScopeError extends FoldDbError {}

/**
 * The owner denied the consent request (`consent-status` → `403 {status:
 * "denied"}`). Terminal: do not auto-re-prompt.
 */
export class ConsentDeniedError extends FoldDbError {
  constructor(readonly requestId: string) {
    super(`consent request '${requestId}' was denied by the node owner`);
  }
}

/**
 * The granted capability was revoked (`consent-status` → `403 {status:
 * "revoked"}`). Per the design's app-side caching table: discard the cached
 * token and surface "access revoked" — do NOT auto-re-prompt.
 */
export class CapabilityRevokedError extends FoldDbError {
  constructor(readonly requestId: string) {
    super(`the capability for request '${requestId}' was revoked`);
  }
}

/**
 * The consent request passed its 5-minute window (`consent-status` → `408
 * {status: "expired"}`). The app must call `requestConsent` again.
 */
export class ConsentExpiredError extends FoldDbError {
  constructor(readonly requestId: string) {
    super(`consent request '${requestId}' expired before it was granted`);
  }
}

/** `consent-status` → `404 {status: "unknown"}`. No request with that id. */
export class ConsentRequestNotFoundError extends FoldDbError {
  constructor(readonly requestId: string) {
    super(`no consent request found for id '${requestId}'`);
  }
}

/**
 * `awaitConsent` gave up before the owner acted on a still-pending request.
 * Distinct from {@link ConsentExpiredError}: the node's record may still be
 * `pending`; this is the SDK's own client-side timeout.
 */
export class ConsentTimeoutError extends FoldDbError {
  constructor(
    readonly requestId: string,
    readonly timeoutMs: number,
  ) {
    super(
      `gave up waiting for consent on '${requestId}' after ${timeoutMs}ms (still pending)`,
    );
  }
}

// ---------------------------------------------------------------------------
// Data path (query / mutation)
// ---------------------------------------------------------------------------

/**
 * The node refused the read/write under the caller's posture
 * (`{kind: "permission_denied"}`). The discriminated `reason` text from the
 * node is preserved verbatim, and `category` classifies it where the reason
 * is recognizable (namespace-denied / unverified-identity / write-denied).
 */
export type PermissionCategory =
  | 'namespace_denied'
  | 'unverified_identity'
  | 'write_denied'
  /** A discriminated capability 403 — see {@link CapabilityDeniedError}. */
  | 'capability_denied'
  | 'unknown';

export class PermissionDeniedError extends FoldDbError {
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
 * reason string into a {@link PermissionCategory}. The reason text is the
 * stable contract (see `uds_isolation_test.rs` assertions); we match on its
 * documented substrings.
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

// ---------------------------------------------------------------------------
// Discriminated capability 403 contract (app_identity v3.1, gap #4)
// ---------------------------------------------------------------------------

/**
 * The eight discriminated `reason` values a node's capability verifier can
 * return on a `403` (`app_identity.md#discriminated-403-reasons`, rendered by
 * `fold_db/crates/core/src/access/capability_denial.rs::CapabilityDenial` as
 * `{status: 403, reason: "<reason>", ...detail}`).
 */
export const CAPABILITY_DENIAL_REASONS = [
  /** `capability_id` is on the node's local revocation list. */
  'capability_revoked',
  /** The capability's `expires_at` is in the past. */
  'capability_expired',
  /** The capability's `app_id` is not in the node's cached app registry. */
  'capability_unknown',
  /** The capability's scope / granted ops do not cover this schema. */
  'capability_out_of_scope',
  /** `X-Capability-Ts` is outside the ±60s replay window (or absent). */
  'capability_replay',
  /** The envelope signature / payload hash did not verify on the node. */
  'capability_bad_sig',
  /** The capability's `node_pubkey` is not this node's key. */
  'capability_for_wrong_node',
  /** No capability was presented for a write that required one. */
  'consent_required',
] as const;

/** One of the eight discriminated capability-403 reasons. */
export type CapabilityDenialReason = (typeof CAPABILITY_DENIAL_REASONS)[number];

/** Type guard: is `s` one of the eight discriminated capability-403 reasons? */
export function isCapabilityDenialReason(
  s: string,
): s is CapabilityDenialReason {
  return (CAPABILITY_DENIAL_REASONS as readonly string[]).includes(s);
}

/**
 * Detail fields a discriminated capability 403 may carry alongside its
 * `reason`, per the `CapabilityDenial` variant payloads.
 */
export interface CapabilityDenialDetail {
  /** `capability_revoked` / `capability_expired` / `capability_unknown`. */
  capabilityId?: string;
  /** `capability_out_of_scope`. */
  schema?: string;
  /** `capability_replay`. */
  timestampSkewSecs?: number;
}

/**
 * A data-path `403` whose body carries a discriminated `reason` field — the
 * capability-verifier contract (`{status: 403, reason: "capability_…", …}`).
 *
 * Subclasses {@link PermissionDeniedError} so existing `instanceof
 * PermissionDeniedError` handling keeps working; its `category` is always
 * `'capability_denied'`. `reason` is the node's verbatim discriminator —
 * one of {@link CAPABILITY_DENIAL_REASONS} for the per-write verifier, or
 * another reason-tagged 403 the node emits on the data path (e.g.
 * `/api/app/search`'s `capability_required` for a header-less call). Use
 * {@link isCapabilityDenialReason} to narrow, and
 * {@link capabilityDenialReaction} for the design's contract reaction.
 */
export class CapabilityDeniedError extends PermissionDeniedError {
  constructor(
    reason: string,
    readonly detail: CapabilityDenialDetail = {},
  ) {
    super(reason, 'capability_denied');
  }

  /** `reason` narrowed to the eight-reason contract, or `null`. */
  get denialReason(): CapabilityDenialReason | null {
    return isCapabilityDenialReason(this.reason) ? this.reason : null;
  }
}

/**
 * What a client should do in response to a discriminated capability-403
 * reason, per the design's "403 handling (contract)" table.
 * `discardToken` → drop the cached capability; `reacquire` → silently run the
 * consent handshake again; `retryOnce` → retry the same request once (a fresh
 * `X-Capability-Ts` is attached automatically); `surface` → propagate to the
 * user/developer instead of silently re-prompting.
 */
export interface CapabilityDenialReaction {
  reason: CapabilityDenialReason;
  discardToken: boolean;
  reacquire: boolean;
  retryOnce: boolean;
  /** A user/developer-facing explanation, when the contract says "surface". */
  surface?: string;
}

/**
 * The design's contract reaction for each discriminated capability-403
 * reason. Pure data — the SDK does not act on it automatically; an app (or a
 * session layer above the client) applies it. `detail` refines the surfaced
 * message where the node provided one.
 */
export function capabilityDenialReaction(
  reason: CapabilityDenialReason,
  detail: CapabilityDenialDetail = {},
): CapabilityDenialReaction {
  switch (reason) {
    case 'capability_revoked':
      // Discard, but DO NOT auto-re-prompt — the owner revoked deliberately.
      return {
        reason,
        discardToken: true,
        reacquire: false,
        retryOnce: false,
        surface:
          'this app’s access to the node was revoked by the owner; ' +
          'ask them to re-grant consent',
      };
    case 'capability_expired':
    case 'capability_unknown':
    case 'consent_required':
      // Expired / stale-client-state / nothing presented: discard + silently
      // re-acquire via the consent handshake.
      return { reason, discardToken: true, reacquire: true, retryOnce: false };
    case 'capability_for_wrong_node':
      // The connection moved nodes — discard + re-acquire against this node.
      return { reason, discardToken: true, reacquire: true, retryOnce: false };
    case 'capability_out_of_scope':
      // Declared scope and attempted operation disagree — a developer bug;
      // re-prompting would not fix it.
      return {
        reason,
        discardToken: false,
        reacquire: false,
        retryOnce: false,
        surface:
          `the granted capability does not cover ${detail.schema ?? 'this schema'} — ` +
          'the declared consent scope and the attempted operation disagree',
      };
    case 'capability_replay':
      // Clock skew — retry once; the retry attaches a fresh X-Capability-Ts.
      return {
        reason,
        discardToken: false,
        reacquire: false,
        retryOnce: true,
        surface:
          'the capability timestamp was rejected as a replay' +
          (detail.timestampSkewSecs !== undefined
            ? ` (skew ${detail.timestampSkewSecs}s)`
            : '') +
          '; check this machine’s clock',
      };
    case 'capability_bad_sig':
      // The stored token is malformed — discard and surface; replaying it
      // can only 403 again.
      return {
        reason,
        discardToken: true,
        reacquire: false,
        retryOnce: false,
        surface:
          'the capability signature failed verification on the node — the ' +
          'stored token is malformed',
      };
  }
}

/**
 * A capability blob failed client-side verification (gap #4) at a point where
 * the SDK must not adopt it: the consent-grant path under
 * `ConnectOptions.verifyCapability`. `problem` discriminates: `malformed`
 * (not base64 JSON in the CapabilityToken shape), `audience_mismatch` (the
 * token is bound to a different `app_id`), or `integrity_mismatch`
 * (`envelope.payload_hash != sha256(JCS(token-minus-envelope))`).
 */
export class CapabilityVerificationError extends FoldDbError {
  constructor(
    readonly problem: 'malformed' | 'audience_mismatch' | 'integrity_mismatch',
    readonly appId: string,
    readonly tokenAppId?: string,
  ) {
    super(
      problem === 'audience_mismatch'
        ? `capability is bound to app '${tokenAppId ?? '?'}', not '${appId}'`
        : problem === 'integrity_mismatch'
          ? `capability for app '${appId}' failed the JCS integrity check ` +
            '(envelope.payload_hash != sha256(JCS(token-minus-envelope)))'
          : `capability for app '${appId}' did not decode as a CapabilityToken`,
    );
  }
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

export type AuthenticationRequiredReason = 'session_expired' | 'auth_failed';

/**
 * The node rejected a data-path request before capability evaluation because
 * the caller's authenticated session/API credential is no longer valid
 * (`401`, commonly `{code:"AUTH_FAILED", error:"Session token expired"}`).
 *
 * Apps should catch this separately from capability-denied errors and start
 * their re-authentication flow instead of treating it as an unexpected crash.
 */
export class AuthenticationRequiredError extends FoldDbError {
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
 * The node rejected the request shape or schema state (`400 {kind:
 * "query_failed" | "mutation_rejected" | "invalid_request" | ...}`). Carries
 * the node's `kind` discriminator and message, plus the raw parsed response
 * `body` — production 400s are not uniform (`{kind, error}` from the dev
 * mirror, `{error}` / `{message}` / richer envelopes from production
 * handlers), and dropping everything but `kind` + one text field lost the
 * node's detail (e.g. its `message` field). The body is surfaced verbatim,
 * the same way the 403 {@link CapabilityDeniedError} surfaces its `detail`.
 */
export class RequestRejectedError extends FoldDbError {
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
 * - `api_version_below_required`: the preflight read `GET /api/version` (or
 *   got a 404 from a node that predates the route, reported as `0`) and the
 *   number is below what the app declared.
 * - `unknown_key`: a data route answered `400 {kind:"unknown_key"}` — the node
 *   does not know a key this client sent. The key is never echoed by the node
 *   (its I4 rule), which is fine: the fix is the same either way.
 */
export type NodeTooOldReason = 'api_version_below_required' | 'unknown_key';

/** The facts {@link NodeTooOldError} carries for a one-line remedy. */
export interface NodeTooOldDetail {
  reason: NodeTooOldReason;
  /** What the app declared it needs (`connect({ requireApiVersion })`). */
  required?: number;
  /** What the node reported (`0` = predates the handshake; `null` = unknown). */
  reported: number | null;
  /** The node's baked build string when it reported one. */
  build: string | null;
  /** Who is asking, for the message (e.g. `brain 0.8.1`). */
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
 * The node is older than this client needs. Subclasses
 * {@link RequestRejectedError} (kind `unknown_key` or
 * `api_version_below_required`) so an app that already catches request
 * rejections keeps working, and can branch on `instanceof NodeTooOldError`
 * to print {@link NodeTooOldError.message} and exit instead of retrying.
 */
export class NodeTooOldError extends RequestRejectedError {
  constructor(
    readonly detail: NodeTooOldDetail,
    body: unknown = null,
  ) {
    super(detail.reason, nodeTooOldMessage(detail), body);
  }
}

// ---------------------------------------------------------------------------
// CAS conflict (compare-and-set precondition failure)
// ---------------------------------------------------------------------------

/**
 * The detail fields the node carries on a `409 {error:"cas_conflict"}` body,
 * surfaced as typed accessors on {@link CasConflictError}. All are optional on
 * the wire — the node fills what it knows about the failed precondition.
 */
export interface CasConflictDetail {
  /** The schema the conflicting write targeted. */
  schema?: string;
  /** The field the CAS precondition was checked against. */
  field?: string;
  /** The row key (rendered) the write targeted. */
  key?: string;
  /** The value the write EXPECTED the field to hold (the precondition). */
  expected?: string;
  /** The field's ACTUAL current value the node observed (`null` when absent). */
  actual?: string | null;
  /** A human-readable message, when the node supplied one. */
  message?: string;
}

/**
 * A compare-and-set (CAS) precondition on a {@link import('./types.js').MutationOp}
 * (its `expected` field) did not hold: the node rejected the write with
 * `409 {error:"cas_conflict", schema?, field?, key?, expected?, actual?, message?}`.
 *
 * The row changed since the caller's expected precondition (or was already
 * present when `{type:"absent"}` was required), so the write was NOT applied.
 * The typed fields (`schema` / `field` / `key` / `expected` / `actual`) let an
 * app re-read the current value and retry without re-parsing free text or
 * re-serializing a generic error body. The verbatim parsed 409 `body` is also
 * carried for anything the node added beyond the modeled fields.
 *
 * A CAS conflict is a normal, expected outcome of contended writes — it is a
 * distinct class (NOT a {@link RequestRejectedError} or an
 * {@link UnexpectedResponseError}) precisely so an app can branch on it and
 * retry rather than treat it as a hard failure.
 */
export class CasConflictError extends FoldDbError {
  /** The schema the conflicting write targeted, or `null`. */
  readonly schema: string | null;
  /** The field the CAS precondition was checked against, or `null`. */
  readonly field: string | null;
  /** The row key (rendered) the write targeted, or `null`. */
  readonly key: string | null;
  /** The value the write EXPECTED the field to hold, or `null`. */
  readonly expected: string | null;
  /** The field's ACTUAL current value the node observed, or `null`. */
  readonly actual: string | null;

  constructor(
    detail: CasConflictDetail = {},
    /** The raw parsed 409 response body, verbatim (`null` when none). */
    readonly body: unknown = null,
  ) {
    super(detail.message ?? casConflictMessage(detail));
    this.schema = detail.schema ?? null;
    this.field = detail.field ?? null;
    this.key = detail.key ?? null;
    this.expected = detail.expected ?? null;
    this.actual = detail.actual ?? null;
  }
}

/** Compose a default message from the modeled CAS-conflict detail. */
function casConflictMessage(detail: CasConflictDetail): string {
  const where =
    detail.field !== undefined
      ? ` on field '${detail.field}'` +
        (detail.schema !== undefined ? ` of schema '${detail.schema}'` : '')
      : detail.schema !== undefined
        ? ` on schema '${detail.schema}'`
        : '';
  const values =
    detail.expected !== undefined || detail.actual !== undefined
      ? ` (expected ${JSON.stringify(detail.expected ?? null)}, ` +
        `actual ${JSON.stringify(detail.actual ?? null)})`
      : '';
  return `CAS conflict${where}: the row changed since the expected precondition${values}`;
}

// ---------------------------------------------------------------------------
// Capability storage
// ---------------------------------------------------------------------------

/** No capability is stored under the requested app id (`loadCapability`). */
export class CapabilityNotFoundError extends FoldDbError {
  constructor(readonly appId: string) {
    super(`no stored capability for app '${appId}'`);
  }
}
