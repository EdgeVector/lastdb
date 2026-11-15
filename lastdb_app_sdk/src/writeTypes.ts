/**
 * Mutation, batch-mutation, and record-file/blob wire types for LastDB `/api/*`.
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

/** Ergonomic key input accepted by `get` and `fetchBlob` (`KeyValue`, `ListRecordKey`, or hash string). */
export type RecordKeyInput =
  | KeyValue
  | { hash: string; range?: string | null }
  | string;

/** Mutation convergence mode for `POST /api/mutation` and `POST /api/mutations/batch`. */
export type MutationConvergence = 'sync' | 'async';

/** Local persistence policy for record mutations. */
export type MutationDurability = 'queued' | 'durable';

/** Storage placement for a record's attached file (`blob` or `inline`). */
export type FilePlacement = 'blob' | 'inline';

/**
 * The marker of a record's file. Read its bytes via `fetchBlob` (`POST /api/db/fetch-blob`).
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
 * The one metadata block of a record: the source file name, metadata map,
 * blob file marker, and inline file marker.
 */
export interface RecordMetadata {
  source_file_name: string | null;
  metadata?: Record<string, string>;
  file?: FileMarker;
  inline_file?: FileMarker;
}

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
  /** Absence clears metadata. Copy from a full read when retaining metadata. */
  metadata?: RecordMetadata | null;
}

export type CreateMutationOp = MutationTarget &
  CompleteRecord & {
    mutationType: 'create';
    expected: { type: 'absent' };
  };

export type ReplaceMutationOp = MutationTarget &
  CompleteRecord & {
    mutationType: 'replace';
    expected: { type: 'revision'; revision: string };
  };

export type DeleteMutationOp = MutationTarget & {
  mutationType: 'delete' | 'purge';
  expected: { type: 'revision'; revision: string };
  record?: never;
  metadata?: never;
};

/** Complete conditional record mutation (`create`, `replace`, `delete`, or `purge`). */
export type MutationOp = CreateMutationOp | ReplaceMutationOp | DeleteMutationOp;

/** Mutations eligible for `putBlob` (`create` or complete `replace`). */
export type PutBlobMutationOp = CreateMutationOp | ReplaceMutationOp;

export interface MutationResult {
  mutationId: string;
  recordRevision: string;
  backgroundTasksDrained?: boolean;
  convergencePending?: boolean;
}

export interface MutationOptions {
  /** UUID for a database-scoped submission. Reuse it with the same body on retry. */
  mutationId?: string;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}

/** An append receipt from Exemem. The publisher may still reject the mutation. */
export interface ShareSubmissionResult {
  status: 'submitted';
  mutationId: string;
  /** Writer sequence within this submission. Correlate by mutationId. */
  submissionSequence: number;
}

export interface ShareSubmissionStatus {
  mutationId: string;
  status: 'submitted' | 'published' | 'discarded' | 'unknown';
}

/** One item in an atomic `POST /api/mutations/batch` request. */
export interface MutationBatchItem {
  schemaName: string;
  op: MutationOp;
}

/** Options for `LastDbClient.mutateBatch`. */
export interface MutationBatchOptions {
  /** Default convergence applied to items that omit per-op `convergence`. */
  convergence?: MutationConvergence;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}

/** Applied operation counts reported on a `POST /api/mutations/batch` receipt. */
export interface MutationBatchOperations {
  created: number;
  updated: number;
  deleted: number;
  total: number;
}

/** Commit receipt returned by `POST /api/mutations/batch`. */
export interface MutationBatchResult {
  mutationIds: string[];
  count: number;
  recordRevisions: Record<string, string>;
  revision: number | null;
  operations: MutationBatchOperations;
  durability: MutationDurability;
  backgroundTasksDrained: boolean;
  convergencePending: boolean;
}

/** Optional inline thumbnail attached alongside a `blob` placement in `putBlob`. */
export interface BlobThumbnailInput {
  bytes: Uint8Array | ArrayBuffer;
  name?: string;
  mediaType?: string;
}

/** Upload raw in-memory bytes for a record's blob or inline file (`POST /api/db/put-blob`). */
export interface PutBlobBytesRequest {
  op: PutBlobMutationOp;
  bytes: Uint8Array | ArrayBuffer;
  localPath?: never;
  name?: string;
  mediaType?: string;
  placement?: FilePlacement;
  thumbnail?: BlobThumbnailInput;
  timeoutMs?: number;
}

/** Reference an existing local file path on an owner node (`POST /api/db/put-blob`). */
export interface PutBlobLocalPathRequest {
  op: PutBlobMutationOp;
  localPath: string;
  bytes?: never;
  name?: string;
  mediaType?: string;
  placement?: 'blob';
  thumbnail?: BlobThumbnailInput;
  timeoutMs?: number;
}

/** Request payload for `LastDbClient.putBlob`. */
export type PutBlobRequest = PutBlobBytesRequest | PutBlobLocalPathRequest;

/** Receipt returned by `POST /api/db/put-blob`. */
export interface PutBlobResult {
  file: FileMarker;
  placement: FilePlacement;
  thumbnail: FileMarker | null;
  mutationId: string;
  recordRevision: string;
  local: boolean;
}

/** Options for `LastDbClient.fetchBlob` (`POST /api/db/fetch-blob`). */
export interface FetchBlobOptions {
  /** Require the record to match this 64-hex revision token when reading. */
  expectedRevision?: string;
  /** Which attached file to read (`'blob'` default, or `'inline'`). */
  placement?: FilePlacement;
  /** Request the JSON envelope so `name` and `mediaType` are populated alongside `bytes`. */
  includeMetadata?: boolean;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}

/** Plaintext bytes and verified digest returned by `LastDbClient.fetchBlob`. */
export interface FetchedBlob {
  bytes: Uint8Array;
  sha256: string;
  sizeBytes: number;
  name?: string;
  mediaType?: string;
}

/** Options for `LastDbClient.deleteBlob` (`POST /api/db/delete-blob`). */
export interface DeleteBlobOptions {
  /** Which attached file to clear (`'blob'` default, or `'inline'`). */
  placement?: FilePlacement;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}
