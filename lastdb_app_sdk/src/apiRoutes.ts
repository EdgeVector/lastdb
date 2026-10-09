/** Canonical LastDB runtime API route strings used by the SDK. */
export const LASTDB_API_ROUTES = {
  requestConsent: '/api/apps/request-consent',
  consentStatus: (requestId: string) =>
    `/api/apps/consent-status/${encodeURIComponent(requestId)}`,
  list: '/api/list',
  query: '/api/query',
  /** Several independent `/api/query` bodies in one request (node 2026-10-08+). */
  queryBatch: '/api/queries/batch',
  mutation: '/api/mutation',
  appSearch: '/api/app/search',
  appChanges: '/api/app/changes',
  autoIdentity: '/api/system/auto-identity',
  schemas: '/api/schemas',
  /** The client↔node compatibility handshake. Both sockets, no node state. */
  version: '/api/version',
} as const;

/**
 * The node request-grammar version this SDK was written against — the value
 * `GET /api/version` reports as `api_version` on a node that speaks exactly
 * this grammar. Pinned to the Rust `API_VERSION` constant by
 * `test/apiRoutes.test.ts`, so the two cannot drift apart silently.
 *
 * An app declares the version IT needs (usually this one) and passes it as
 * `connect({ requireApiVersion })`; the SDK refuses to run against an older
 * node with one line that names the fix instead of failing on the first write.
 */
export const LASTDB_SDK_API_VERSION = 1;

/** SDK routes that must stay present in the Rust UDS data router. */
export const LASTDB_UDS_SHARED_ROUTES = [
  ['GET', LASTDB_API_ROUTES.list],
  ['POST', LASTDB_API_ROUTES.query],
  ['POST', LASTDB_API_ROUTES.queryBatch],
  ['POST', LASTDB_API_ROUTES.mutation],
  ['POST', LASTDB_API_ROUTES.appSearch],
  ['POST', LASTDB_API_ROUTES.appChanges],
  ['GET', LASTDB_API_ROUTES.schemas],
  ['GET', LASTDB_API_ROUTES.autoIdentity],
] as const;
