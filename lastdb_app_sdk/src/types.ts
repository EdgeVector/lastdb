/**
 * Wire types for the owner-socket operations that are not record calls.
 */

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

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

/** The node's answer to `GET /api/version`. */
export interface NodeVersion {
  apiVersion: number;
  build: string | null;
  capabilities: Record<string, boolean>;
  instanceId: string | null;
  minSupportedApiVersion?: number | null;
}

/** Outcome of one submission this receiving node already sent. */
export interface ShareSubmissionStatus {
  mutationId: string;
  status: 'submitted' | 'published' | 'not_accepted' | 'unknown';
}

/** The field values of a proposal record, keyed by field name. */
export type RowFields = Record<string, JsonValue>;

/**
 * Explicit record coordinates. A proposal sends this object as `key_value`.
 * Coordinates are independent of fields.
 */
export interface KeyValue {
  hash: string | null;
  range: string | null;
}

/** One condition covers the complete record in a proposal. */
export type CasExpectation =
  | { type: 'absent' }
  | { type: 'revision'; revision: string };

/** Convergence field the proposal document can carry. The publisher decides the write. */
export type MutationConvergence = 'sync' | 'async';

/** Durability field the proposal document can carry. It is not a local commit. */
export type MutationDurability = 'queued' | 'durable';

/** File marker inside a proposal. The receiver route refuses a proposal that has one. */
export interface FileMarker {
  name?: string;
  media_type?: string;
  size_bytes: number;
  sha256: string;
}

interface ProposalTarget {
  key: KeyValue;
  /** Exactly every declared name. Null requires allow_absent in the schema. */
  alternates: Record<string, KeyValue | null>;
  convergence?: MutationConvergence;
  durability?: MutationDurability;
}

interface CompleteRecord {
  record: RowFields;
}

export type CreateMutationOp = ProposalTarget &
  CompleteRecord & {
    mutationType: 'create';
    expected: { type: 'absent' };
  };

export type ReplaceMutationOp = ProposalTarget &
  CompleteRecord & {
    mutationType: 'replace';
    expected: { type: 'revision'; revision: string };
  };

export type DeleteMutationOp = ProposalTarget & {
  mutationType: 'delete' | 'purge';
  expected: { type: 'revision'; revision: string };
  record?: never;
};

/**
 * One proposal document for `POST /api/databases/{id}/submissions`.
 * This is the mutation document a received database used to accept.
 * It is not a `/v1/write` item.
 */
export type MutationOp = CreateMutationOp | ReplaceMutationOp | DeleteMutationOp;

/** Options for {@link MutationOp} submission on a received database. */
export interface MutationOptions {
  /** UUID for this proposal. Reuse it with the same body on retry. */
  mutationId?: string;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
  /** Installed definition id. The publisher replaces this with the definition it has. */
  definitionId?: string;
}

/**
 * The answer of one change call. `applied`: the record is in this node's database now.
 * `pending`: the node queued a proposal and the database changes only after the publisher
 * accepts it. The node decides which; the app does not.
 */
export type ChangeResult =
  | { status: 'applied'; mutationId: string }
  | { status: 'pending'; mutationId: string; submissionSequence: number };

/** An append receipt. The publisher may still reject the proposal. */
export interface ShareSubmissionResult {
  status: 'submitted';
  mutationId: string;
  /** Writer sequence within this submission. Correlate by mutationId. */
  submissionSequence: number;
}
