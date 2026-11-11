/**
 * Wire types for the LastDB runtime `/api/*` surface.
 */

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/** The field values of a result row, keyed by field name. */
export type RowFields = Record<string, JsonValue>;

/** A result row — fields keyed by name. */
export type Row = RowFields;

/**
 * Explicit record coordinates. Queries return this object as `key`;
 * mutations send it as `key_value`. Coordinates are independent of fields.
 */
export interface KeyValue {
  hash: string | null;
  range: string | null;
}

/** Pass the complete cursor back unchanged. Values stay strings, including large integers and dates. */
export interface QueryCursor extends KeyValue {
  /** Opaque schema, field-type, key-format, and query compatibility identifier. */
  comparison?: string | null;
  record?: string | null;
  alternate_primary?: KeyValue | null;
}

/** Options for the keys-only {@link LastDbClient.list} membership page. */
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

/**
 * One keys-only `GET /api/list` page.
 */
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

/** Mutation convergence mode for `POST /api/mutation`. */
export type MutationConvergence = 'sync' | 'async';

/** Local persistence policy for `POST /api/mutation`. */
export type MutationDurability = 'queued' | 'durable';

/**
 * The marker of a record's file. A record has at most one file; read it with
 * `POST /api/db/fetch-blob` by schema and key.
 */
export interface FileMarker {
  name?: string;
  media_type?: string;
  /** Plaintext size in bytes. */
  size_bytes: number;
  /** Plaintext SHA-256, lowercase hex. */
  sha256: string;
}

/**
 * The one metadata block of a record: the source file name and metadata map of
 * the complete replacement, the marker of the record's file, and its inline file.
 */
export interface RecordMetadata {
  source_file_name: string | null;
  metadata?: Record<string, string>;
  file?: FileMarker;
  inline_file?: FileMarker;
}

/**
 * The full per-row envelope the node returns for `/api/query`.
 */
export interface QueryRow {
  /** Complete-record token, required for replace/delete. */
  revision: string;
  /** Every declared alternate address, including explicitly absent sparse addresses. */
  alternates: Record<string, KeyValue | null>;
  /**
   * The row's storage key rendered to a string (`"hash:range"`, `"hash"`, or
   * `"range"`). Prefer {@link QueryRow.keyValue} for unambiguous row addressing.
   */
  key: string;
  /**
   * The row's structured storage key (`{hash, range}`). Pass it back verbatim
   * as `MutationOp.key` to update or delete this exact row.
   */
  keyValue: KeyValue | null;
  /** Coordinates of the named alternate key; keyValue remains the primary mutation key. */
  matchedKey?: KeyValue | null;
  /** The row's field values, keyed by field name. */
  fields: RowFields;
  /** The one metadata block of the record, or `null` when absent. */
  metadata: RecordMetadata | null;
}

/**
 * Connection options. Pass `socketPath` to use that Unix socket verbatim.
 * Omit it to discover `LASTDB_SOCKET_PATH`, then `FOLDDB_SOCKET_PATH`, then
 * `<LASTDB_HOME or ~/.lastdb>/data/folddb.sock`.
 */
export interface ConnectOptions<DatabaseId extends string | undefined = undefined> {
  /** Canonical app id this client acts as. */
  appId: string;
  /** Path to the node's Unix-domain control socket. */
  socketPath?: string;
  /** Database ID or local alias registered on this node. Omit for the default database. */
  databaseId?: DatabaseId;
  /**
   * Headers attached to every request this client sends, under a per-call
   * header of the same name (which wins).
   */
  defaultHeaders?: Record<string, string>;
  /**
   * Best-effort client label (`X-LastDB-Client`). Defaults to {@link ConnectOptions.appId}.
   */
  clientId?: string;
  /**
   * Per-request transport timeout in milliseconds (default 30_000).
   */
  timeoutMs?: number;
  /**
   * Minimum node request-grammar version this app requires.
   */
  requireApiVersion?: number;
  /**
   * Who is asking, for the `NodeTooOldError` message (defaults to `appId`).
   */
  appLabel?: string;
}

/**
 * The node's answer to `GET /api/version`.
 */
export interface NodeVersion {
  apiVersion: number;
  build: string | null;
  capabilities: Record<string, boolean>;
  instanceId: string | null;
}

/**
 * Keyed query parameters for `POST /api/query`.
 */
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

/**
 * Pagination metadata returned by `/api/query` alongside its `results` page.
 */
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

/**
 * One schema entry from `GET /api/schemas`.
 */
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
  | {
      provisioned: true;
      userHash: string;
      publicKey: string | null;
      userId: string | null;
    }
  | {
      provisioned: false;
      reason: string;
      next: string | null;
    };

/** One condition covers the complete record. */
export type CasExpectation =
  | { type: 'absent' }
  | { type: 'revision'; revision: string };

interface MutationTarget {
  key: KeyValue;
  /** Exactly every declared name; null requires allow_absent in the schema. */
  alternates: Record<string, KeyValue | null>;
  convergence?: MutationConvergence;
  durability?: MutationDurability;
}

interface CompleteRecord {
  record: RowFields;
  /** Absence clears metadata and the file marker. Copy these from a full read when retaining them. */
  metadata?: RecordMetadata | null;
}

/**
 * Complete conditional record mutation. Omitted payload fields disappear.
 */
export type MutationOp = MutationTarget & (
  | (CompleteRecord & { mutationType: 'create'; expected: { type: 'absent' } })
  | (CompleteRecord & { mutationType: 'replace'; expected: { type: 'revision'; revision: string } })
  | { mutationType: 'delete' | 'purge'; expected: { type: 'revision'; revision: string }; record?: never; metadata?: never }
);

export interface MutationResult {
  mutationId: string;
  recordRevision: string;
  backgroundTasksDrained?: boolean;
  convergencePending?: boolean;
}

export interface MutationOptions {
  /** UUID for a database-scoped submission. Reuse it with the same body on retry. */
  mutationId?: string;
}

/** An append receipt from Exemem. The publisher may still reject the mutation. */
export interface ShareSubmissionResult {
  status: 'submitted';
  mutationId: string;
  submissionSequence: number;
}

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

/** One item of {@link LastDbClient.queryBatch}: the arguments of `query`. */
export interface QueryBatchItem {
  schemaName: string;
  filter?: QueryFilter;
}

/** Outcome of one {@link QueryBatchItem}, in request order. */
export type QueryBatchOutcome =
  | { ok: true; result: QueryResult }
  | { ok: false; error: Error };
