/** Canonical LastDB runtime API route strings used by the SDK. */
export const LASTDB_API_ROUTES = {
  list: '/api/list',
  query: '/api/query',
  /** Several independent `/api/query` bodies in one request. */
  queryBatch: '/api/queries/batch',
  mutation: '/api/mutation',
  appChanges: '/api/app/changes',
  autoIdentity: '/api/system/auto-identity',
  schemas: '/api/schemas',
  /** Owner socket. The response `definition_id` is the definition the app declared. */
  schemasDeclare: '/api/schemas/declare',
  /** The client↔node compatibility handshake. Both sockets, no node state. */
  version: '/api/version',
} as const;

/** Whole-record queries require API 29. The request does not name fields. */
export const LASTDB_SDK_API_VERSION = 29;
