/**
 * The LastDB runtime app client facade.
 *
 * Wraps the node's Unix-socket data plane and delegates handshake, schema,
 * keyed read, mutation, blob, and database-session operations to focused
 * domain modules. `read`, `write`, and `changes` call the app API (`/v1`).
 */

import { existsSync } from 'node:fs';

import { LASTDB_SDK_API_VERSION } from './apiVersion.generated.js';
import { executeAppChanges, executeAppRead, executeAppWrite } from './appApi.js';
import type {
  ChangesReply,
  ChangesRequest,
  ReadItem,
  ReadResult,
  WriteItem,
  WriteResult,
} from './appApi.generated.js';
import {
  executeDeleteBlob,
  executeFetchBlob,
  executePutBlob,
  type DeleteBlobOptions,
  type FetchBlobOptions,
  type FetchedBlob,
  type PutBlobRequest,
  type PutBlobResult,
} from './blobOps.js';
import {
  bindNodeDatabaseSpec,
  createNodeDatabase,
  databaseScope,
  issueNodeDatabaseSession,
  listNodeDatabases,
  validateDatabaseSelector,
  type ClientContext,
  type CreateDatabaseRequest,
  type DatabaseSpec,
  type ListedDatabase,
} from './databases.js';
import { RequestRejectedError, TransportError } from './errors.js';
import { executeMutate, executeMutateBatch, executeShareSubmissionStatus } from './mutationOps.js';
import { executeGet, executeList, executeQuery, executeQueryAll, executeQueryBatch } from './readOps.js';
import {
  executeAutoIdentity,
  executeDeclareSchema,
  executeGetSchema,
  executeListSchemas,
  executeRequireApiVersion,
  executeResolveSchema,
  executeVersion,
  rememberSchemaDefinition,
  type AutoIdentityResult,
  type DeclareSchemaRequest,
  type DeclareSchemaResult,
  type LoadedSchema,
  type SchemaDescriptor,
} from './schemaOps.js';
import { openDiscoveredSocket, udsTransport, withDefaultHeaders, type Transport } from './transport.js';
import type * as Types from './types.js';

const DATABASE_SESSION_HEADER = 'X-LastDB-Database-Session';

function resolveSessionHeaders(
  databaseId: string | undefined,
  databaseSession: string | undefined,
): Record<string, string> {
  if (databaseSession === undefined) return {};
  if (databaseId === undefined) {
    throw new RequestRejectedError(
      'database_session',
      'databaseSession requires an explicit databaseId',
    );
  }
  validateDatabaseSelector(databaseId);
  const trimmed = databaseSession.trim();
  if (trimmed.length === 0 || /[\r\n]/.test(trimmed)) {
    throw new RequestRejectedError('database_session', 'Invalid LastDB database session token');
  }
  return { [DATABASE_SESSION_HEADER]: trimmed };
}

/** Attach `X-LastDB-Client` unless the caller already provided one. */
function buildClientHeaders(
  appId: string,
  clientId?: string,
  defaultHeaders?: Record<string, string>,
): Record<string, string> {
  const headers: Record<string, string> = { ...(defaultHeaders ?? {}) };
  if (!Object.keys(headers).some((k) => k.toLowerCase() === 'x-lastdb-client')) {
    const label = (clientId ?? appId).trim();
    if (label.length > 0) headers['X-LastDB-Client'] = label;
  }
  return headers;
}

/** Connect to a LastDB node over its Unix-domain socket and verify protocol compatibility. */
export async function connect<DatabaseId extends string | undefined = undefined>(
  options: Types.ConnectOptions<DatabaseId>,
): Promise<LastDbClient<DatabaseId>> {
  const defaultHeaders = buildClientHeaders(options.appId, options.clientId, options.defaultHeaders);
  const transport: Transport = options.socketPath
    ? udsTransport(options.socketPath, defaultHeaders, { timeoutMs: options.timeoutMs })
    : openDiscoveredSocket({ defaultHeaders, timeoutMs: options.timeoutMs });

  const client = new LastDbClient<DatabaseId>(options.appId, transport, {
    databaseId: options.databaseId,
    databaseSession: options.databaseSession,
  });
  const required = options.requireApiVersion ?? LASTDB_SDK_API_VERSION;
  if (required > 0) {
    await client.requireApiVersion(required, options.appLabel ?? options.appId);
  }
  return client;
}

/** Behavior switches for {@link LastDbClient} (set via `ConnectOptions` or `ownerClient`). */
export interface LastDbClientOptions<DatabaseId extends string | undefined = undefined> {
  databaseId?: DatabaseId;
  databaseSession?: string;
}

/** A connected LastDB app client. Construct via {@link connect} or `ownerClient`. */
export class LastDbClient<DatabaseId extends string | undefined = undefined> {
  readonly databaseId: DatabaseId;
  private readonly context: ClientContext;

  constructor(
    readonly appId: string,
    private readonly baseTransport: Transport,
    options: LastDbClientOptions<DatabaseId> = {},
  ) {
    const databasePath = databaseScope(options.databaseId);
    const sessionHeaders = resolveSessionHeaders(options.databaseId, options.databaseSession);
    this.databaseId = options.databaseId as DatabaseId;
    this.context = {
      appId,
      transport: withDefaultHeaders(baseTransport, sessionHeaders),
      databasePath,
      definitionIds: new Map<string, string>(),
    };
  }

  /** Where this client is pointed (for diagnostics). */
  get target(): string {
    return this.context.transport.target;
  }

  /** `GET /api/version` — client↔node compatibility handshake. */
  version(): Promise<Types.NodeVersion> {
    return executeVersion(this.context);
  }

  /** Refuse to proceed against a node whose `api_version` is below `required`. */
  requireApiVersion(required: number, appLabel?: string): Promise<Types.NodeVersion> {
    return executeRequireApiVersion(this.context, required, appLabel);
  }

  /** `GET /api/system/auto-identity` — resolve the local owner identity. */
  autoIdentity(): Promise<AutoIdentityResult> {
    return executeAutoIdentity(this.context);
  }

  /** `GET /api/list` — return one page of live primary or alternate keys. */
  list(schemaName: string, opts: Types.ListOptions = {}): Promise<Types.ListResult> {
    return executeList(this.context, schemaName, opts);
  }

  /** Point lookup of a single record by primary `HashKey` or `HashRangeKey`. */
  get(schemaName: string, key: Types.RecordKeyInput): Promise<Types.QueryRow | null> {
    return executeGet(this.context, schemaName, key);
  }

  /** `POST /api/query` — keyed point, multi-get, or range read under one hash. */
  query(schemaName: string, filter: Types.QueryFilter = {}): Promise<Types.QueryResult> {
    return executeQuery(this.context, schemaName, filter);
  }

  /** `POST /api/queries/batch` — read up to 64 keyed queries per request. */
  queryBatch(items: readonly Types.QueryBatchItem[]): Promise<Types.QueryBatchOutcome[]> {
    return executeQueryBatch(this.context, items);
  }

  /** Auto-paginate a keyed query across pages until `hasMore` is false or `maxRows` is reached. */
  queryAll(
    schemaName: string,
    filter: Omit<Types.QueryFilter, 'limit' | 'offset' | 'cursor'> = {},
    opts: Types.QueryAllOptions = {},
  ): Promise<Types.QueryResult> {
    return executeQueryAll(this.context, schemaName, filter, opts);
  }

  /** `POST /api/mutation` — write one complete conditional record into `schemaName`. */
  mutate(
    schemaName: string,
    op: Types.MutationOp,
    options?: DatabaseId extends string ? Types.MutationOptions : never,
  ): Promise<Types.MutationResult | (DatabaseId extends string ? Types.ShareSubmissionResult : never)>;
  mutate(
    schemaName: string,
    op: Types.MutationOp,
    options: Types.MutationOptions = {},
  ): Promise<Types.MutationResult | Types.ShareSubmissionResult> {
    return executeMutate(this.context, schemaName, op, options);
  }

  /** `POST /api/mutations/batch` — commit an atomic batch of conditional record mutations. */
  mutateBatch(
    items: readonly Types.MutationBatchItem[],
    options: Types.MutationBatchOptions = {},
  ): Promise<Types.MutationBatchResult> {
    return executeMutateBatch(this.context, items, options);
  }

  /** Read the outcome of one submission made by this receiving LastDB node. */
  getShareSubmissionStatus(mutationId: string): Promise<Types.ShareSubmissionStatus> {
    return executeShareSubmissionStatus(this.context, mutationId);
  }

  /**
   * `POST /v1/read` — one result per read, in order.
   * A per-item error is returned, not thrown. Does not use this client's `/api` database path.
   */
  read(reads: readonly ReadItem[]): Promise<ReadResult[]> {
    return executeAppRead(this.context, reads);
  }

  /** `POST /v1/write` — commit a list of conditional mutations after the write is on disk. */
  write(mutations: readonly WriteItem[]): Promise<WriteResult> {
    return executeAppWrite(this.context, mutations);
  }

  /** `POST /v1/x/changes` — changed record keys after a saved position. */
  changes(options: ChangesRequest = {}): Promise<ChangesReply> {
    return executeAppChanges(this.context, options);
  }

  /** `POST /api/db/put-blob` — attach or replace a record's blob, inline, or local-path file. */
  putBlob(schemaName: string, request: PutBlobRequest): Promise<PutBlobResult> {
    return executePutBlob(this.context, schemaName, request);
  }

  /** `POST /api/db/fetch-blob` — read a record's blob or inline file bytes. */
  fetchBlob(
    schemaName: string,
    key: Types.RecordKeyInput,
    options: FetchBlobOptions = {},
  ): Promise<FetchedBlob> {
    return executeFetchBlob(this.context, schemaName, key, options);
  }

  /** `POST /api/db/delete-blob` — clear a record's blob or inline file via complete replacement. */
  deleteBlob(
    schemaName: string,
    op: Types.ReplaceMutationOp,
    options: DeleteBlobOptions = {},
  ): Promise<Types.MutationResult> {
    return executeDeleteBlob(this.context, schemaName, op, options);
  }

  /** Remember the 64-hex `definition_id` returned by `declareSchema` for `schemaName`. */
  rememberDefinition(schemaName: string, definitionId: string): void {
    rememberSchemaDefinition(this.context.definitionIds, schemaName, definitionId);
  }

  /** `POST /api/schemas/declare` — declare an app-owned schema. */
  declareSchema(request: DeclareSchemaRequest): Promise<DeclareSchemaResult> {
    return executeDeclareSchema(this.context, request);
  }

  /** `GET /api/schemas` — list schemas loaded on the selected database. */
  listSchemas(): Promise<LoadedSchema[]> {
    return executeListSchemas(this.context);
  }

  /** `GET /api/schema/<name>` — fetch a single loaded schema entry, or `null` on `404`. */
  getSchema(schemaName: string): Promise<LoadedSchema | null> {
    return executeGetSchema(this.context, schemaName);
  }

  /** Resolve an app-owned schema descriptor against `listSchemas()`. */
  resolveSchema(descriptor: SchemaDescriptor): Promise<LoadedSchema | null> {
    return executeResolveSchema(this.context, descriptor);
  }

  /** `GET /api/databases` — list personal, owned, and received shared databases. */
  listDatabases(): Promise<ListedDatabase[]> {
    return listNodeDatabases(this.baseTransport, this.appId);
  }

  /** `POST /api/databases` — create an owned database on this node. */
  createDatabase(request: CreateDatabaseRequest): Promise<ListedDatabase> {
    return createNodeDatabase(this.baseTransport, request, this.appId);
  }

  /** `POST /api/databases/spec` — bind or clear an owned database's spec. */
  bindDatabaseSpec(databaseId: string, spec: DatabaseSpec | null): Promise<ListedDatabase> {
    return bindNodeDatabaseSpec(this.baseTransport, databaseId, spec, this.appId);
  }

  /** `POST /api/databases/session` — issue a short-lived session restricted to `databaseId`. */
  issueDatabaseSession(databaseId: string, intent: Types.DatabaseSessionIntent): Promise<Types.DatabaseSession> {
    return issueNodeDatabaseSession(this.baseTransport, databaseId, intent, this.appId);
  }

  /** Issue a short-lived database session and return a scoped client bound to it. */
  async openDatabaseSession(
    databaseId: string,
    intent: Types.DatabaseSessionIntent,
  ): Promise<{ session: Types.DatabaseSession; client: LastDbClient<string> }> {
    const session = await this.issueDatabaseSession(databaseId, intent);
    return { session, client: this.select(session.databaseId, { databaseSession: session.session }) };
  }

  /** Return a client bound to `databaseId` (and optional `databaseSession`) without mutating `this`. */
  select(databaseId: string, options: Types.SelectDatabaseOptions = {}): LastDbClient<string> {
    return new LastDbClient(this.appId, this.baseTransport, {
      databaseId,
      databaseSession: options.databaseSession,
    });
  }
}

/** Options for {@link ownerClient}. */
export interface OwnerClientOptions<DatabaseId extends string | undefined = undefined> {
  /** Canonical app id this client acts as. */
  appId: string;
  /** Database ID or local alias. Omit for the personal default. */
  databaseId?: DatabaseId;
  /**
   * Short-lived owner-approved database session token (`X-LastDB-Database-Session`).
   * Requires `databaseId`. Keep session tokens out of logs.
   */
  databaseSession?: string;
  /** Best-effort client label (`X-LastDB-Client`). Defaults to `appId`. */
  clientId?: string;
  /** Control-socket path. The file must exist. */
  socketPath: string;
  /** Headers attached to every request this client sends (per-call headers win). */
  defaultHeaders?: Record<string, string>;
  /** Per-request transport timeout in milliseconds (defaults to 30_000). */
  timeoutMs?: number;
}

/** Build a {@link LastDbClient} synchronously against an existing Unix-domain socket. */
export function ownerClient<DatabaseId extends string | undefined = undefined>(
  options: OwnerClientOptions<DatabaseId>,
): LastDbClient<DatabaseId> {
  const { socketPath, timeoutMs } = options;
  if (socketPath.length === 0 || !existsSync(socketPath)) {
    throw new TransportError(
      `ownerClient requires a socketPath whose file exists, got '${socketPath}'`,
      'connect',
    );
  }
  const headers = buildClientHeaders(options.appId, options.clientId, options.defaultHeaders);
  const transport = udsTransport(socketPath, headers, { timeoutMs });
  return new LastDbClient(options.appId, transport, {
    databaseId: options.databaseId,
    databaseSession: options.databaseSession,
  });
}
