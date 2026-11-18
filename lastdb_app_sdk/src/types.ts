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

/** Outcome of one submission this receiving node already sent. */
export interface ShareSubmissionStatus {
  mutationId: string;
  status: 'submitted' | 'published' | 'discarded' | 'unknown';
}
