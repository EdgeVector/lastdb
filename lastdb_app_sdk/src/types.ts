/**
 * Wire types for the LastDB runtime `/api/*` surface.
 */

import type { JsonValue, KeyValue, RecordMetadata, RowFields } from './writeTypes.js';

export type {
  BlobThumbnailInput,
  CasExpectation,
  CreateMutationOp,
  DeleteBlobOptions,
  DeleteMutationOp,
  FetchBlobOptions,
  FetchedBlob,
  FileMarker,
  FilePlacement,
  JsonValue,
  KeyValue,
  MutationBatchItem,
  MutationBatchOperations,
  MutationBatchOptions,
  MutationBatchResult,
  MutationConvergence,
  MutationDurability,
  MutationOp,
  MutationOptions,
  MutationResult,
  PutBlobBytesRequest,
  PutBlobLocalPathRequest,
  PutBlobMutationOp,
  PutBlobRequest,
  PutBlobResult,
  RecordKeyInput,
  RecordMetadata,
  ReplaceMutationOp,
  Row,
  RowFields,
  ShareSubmissionResult,
  ShareSubmissionStatus,
} from './writeTypes.js';

/** Pass the complete cursor back unchanged. Values stay strings, including large integers and dates. */
export interface QueryCursor extends KeyValue {
  /** Opaque schema, field-type, key-format, and query compatibility identifier. */
  comparison?: string | null;
  record?: string | null;
  alternate_primary?: KeyValue | null;
}

/** Options for the keys-only `LastDbClient.list` membership page. */
export interface ListOptions {
  /** Select a named alternate key. A hash is required with this option. */
  keyName?: string;
  hash?: string;
  /** Page size sent to Mini (default 100, clamped by Mini to 1000). */
  limit?: number;
  /** Opaque exclusive cursor returned by the previous page. */
  cursor?: string;
}

/** One live record identity from a keys-only schema membership page. */
export interface ListRecordKey {
  hash: string;
  /** Present only when the schema key has a non-empty range component. */
  range?: string;
}

/** One keys-only `GET /api/list` page. */
export interface ListResult {
  schema: string;
  keys: ListRecordKey[];
  /** Primary mutation keys aligned with keys when keyName selects an alternate. */
  primary_keys?: ListRecordKey[];
  key_name?: string;
  next_cursor: string | null;
  has_more: boolean;
  /** Same honesty signal as `has_more`; one page is not a census. */
  truncated: boolean;
}

/** The full per-row envelope the node returns for `/api/query`. */
export interface QueryRow {
  /** Complete-record token, required for replace/delete. */
  revision: string;
  /** Every declared alternate address, including explicitly absent sparse addresses. */
  alternates: Record<string, KeyValue | null>;
  /** Rendered storage key (`"hash:range"`, `"hash"`, or `"range"`). Prefer `keyValue`. */
  key: string;
  /** Structured storage key (`{hash, range}`). Pass verbatim as `MutationOp.key`. */
  keyValue: KeyValue | null;
  /** Coordinates of the named alternate key; `keyValue` remains the primary mutation key. */
  matchedKey?: KeyValue | null;
  /** The row's field values, keyed by field name. */
  fields: RowFields;
  /** The one metadata block of the record, or `null` when absent. */
  metadata: RecordMetadata | null;
}

/**
 * Connection options. Pass `socketPath` to use that Unix socket verbatim, or
 * omit it to discover `LASTDB_SOCKET_PATH` → `FOLDDB_SOCKET_PATH` →
 * `<LASTDB_HOME or ~/.lastdb>/data/folddb.sock`.
 */
export interface ConnectOptions<DatabaseId extends string | undefined = undefined> {
  /** Canonical app id this client acts as. */
  appId: string;
  /** Path to the node's Unix-domain control socket. */
  socketPath?: string;
  /** Database ID or local alias registered on this node. Omit for the default database. */
  databaseId?: DatabaseId;
  /** Short-lived owner-approved session token (`X-LastDB-Database-Session`). Requires `databaseId`. */
  databaseSession?: string;
  /** Headers attached to every request this client sends (per-call headers win). */
  defaultHeaders?: Record<string, string>;
  /** Best-effort client label (`X-LastDB-Client`). Defaults to `appId`. */
  clientId?: string;
  /** Per-request transport timeout in milliseconds (default 30_000). */
  timeoutMs?: number;
  /** Minimum node request-grammar version required (defaults to `LASTDB_SDK_API_VERSION`). */
  requireApiVersion?: number;
  /** Caller label for `NodeTooOldError` messages (defaults to `appId`). */
  appLabel?: string;
}

/** Options when scoping an existing client to a named or shared database via `select`. */
export interface SelectDatabaseOptions {
  /** Short-lived owner-approved session token (`X-LastDB-Database-Session`) for this database. */
  databaseSession?: string;
}

/** Access posture requested for `POST /api/databases/session`. */
export type DatabaseSessionIntent = 'read' | 'write';

/** Short-lived scoped database session issued by `POST /api/databases/session`. */
export interface DatabaseSession {
  databaseId: string;
  intent: DatabaseSessionIntent;
  session: string;
  expiresInSeconds: number;
}

/** The node's answer to `GET /api/version`. */
export interface NodeVersion {
  apiVersion: number;
  build: string | null;
  capabilities: Record<string, boolean>;
  instanceId: string | null;
  minSupportedApiVersion?: number | null;
}

/** Keyed query parameters for `POST /api/query`. */
export interface QueryFilter {
  /** Named alternate key within the schema. Omit for the primary key. */
  keyName?: string;
  /** Key/range filter object passed through verbatim under `filter`. */
  filter?: JsonValue;
  /** Page size (node defaults to 100, clamps to 1000). */
  limit?: number;
  /** Page offset (defaults to 0). */
  offset?: number;
  /** Keyset cursor returned by a prior page's `page.nextCursor`. */
  cursor?: QueryCursor;
}

/** Pagination metadata returned by `/api/query` alongside its `results` page. */
export interface QueryPage {
  totalCount?: number;
  returnedCount: number;
  limit: number;
  offset: number;
  hasMore: boolean;
  nextCursor: QueryCursor | null;
}

/** `POST /api/query` success body. */
export interface QueryResult {
  schema: string;
  rowCount: number;
  rows: QueryRow[];
  page: QueryPage | null;
}

/** Options for `LastDbClient.queryAll` — the auto-paginating keyed query helper. */
export interface QueryAllOptions {
  /** Page size per request (default 100, max 1000). */
  pageSize?: number;
  /** Safety ceiling on total rows fetched across pages (default 100_000). */
  maxRows?: number;
}

/** One schema entry from `GET /api/schemas` or `GET /api/schema/<name>`. */
export interface LoadedSchema {
  name: string;
  identityHash: string | null;
  descriptiveName: string | null;
  ownerAppId: string | null;
  fields: string[];
}

/** Descriptor used to resolve an app-owned loaded schema from `/api/schemas`. */
export interface SchemaDescriptor {
  ownerAppId: string;
  descriptiveName: string;
  fields?: readonly string[];
}

/** `GET /api/system/auto-identity` parsed result. */
export type AutoIdentityResult =
  | { provisioned: true; userHash: string; publicKey: string | null; userId: string | null }
  | { provisioned: false; reason: string; next: string | null };

/** `POST /api/schemas/declare`. */
export interface DeclareSchemaRequest {
  namespace: string;
  schema: Record<string, unknown>;
  intent?: 'catalog_sync' | 'check';
}

/** The definition the declare response named, keyed later by `schemaName`. */
export interface DeclareSchemaResult {
  schemaName: string;
  definitionId: string;
  identityHash: string | null;
  resolution: string | null;
}

/** One item of `LastDbClient.queryBatch`: the arguments of `query`. */
export interface QueryBatchItem {
  schemaName: string;
  filter?: QueryFilter;
}

/** Outcome of one {@link QueryBatchItem}, in request order. */
export type QueryBatchOutcome =
  | { ok: true; result: QueryResult }
  | { ok: false; error: Error };
