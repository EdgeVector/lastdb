/**
 * The LastDB runtime app client facade.
 *
 * Wraps the node's Unix-socket data plane. `read`, `write`, and `changes`
 * call the app API (`/v1`). The other methods are owner-socket operations
 * that are not record calls: version, identity, schemas, databases,
 * share proposals, and share-submission status.
 */

import { existsSync } from 'node:fs';

import { LASTDB_SDK_API_VERSION } from './apiVersion.generated.js';
import { executeAppChanges, executeAppDatabases, executeAppRead, executeAppWrite } from './appApi.js';
import type { DatabasesReplyFor } from './appApi.js';
import type {
  ChangesReply,
  ChangesRequest,
  DatabasesRequest,
  ReadItem,
  ReadResult,
  WriteItem,
  WriteResult,
} from './appApi.generated.js';
import {
  bindNodeDatabaseSpec,
  createNodeDatabase,
  databaseScope,
  listNodeDatabases,
  type ClientContext,
  type CreateDatabaseRequest,
  type DatabaseSpec,
  type ListedDatabase,
} from './databases.js';
import { TransportError } from './errors.js';
import {
  executeAutoIdentity,
  executeGetSchema,
  executeListSchemas,
  executeRequireApiVersion,
  executeResolveSchema,
  executeVersion,
  type AutoIdentityResult,
  type LoadedSchema,
  type SchemaDescriptor,
} from './schemaOps.js';
import { executeSubmitProposal } from './shareProposal.js';
import { executeShareSubmissionStatus } from './shareStatus.js';
import { openDiscoveredSocket, udsTransport, type Transport } from './transport.js';
import type * as Types from './types.js';

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
    this.databaseId = options.databaseId as DatabaseId;
    this.context = {
      appId,
      transport: baseTransport,
      databasePath: databaseScope(options.databaseId),
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

  /**
   * `POST /api/databases/{id}/submissions` — queue one encrypted proposal.
   * The body is the proposal document, not a `/v1/write` item.
   * Select a received whole-database share first. This call does not write a record.
   */
  submitProposal(
    schemaName: string,
    op: Types.MutationOp,
    options?: Types.MutationOptions,
  ): Promise<Types.ShareSubmissionResult> {
    return executeSubmitProposal(this.context, schemaName, op, options);
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

  /**
   * `POST /v1/x/databases` — one databases action.
   * The reply is the owner route's JSON. `submit` answers 202.
   * On the app socket the launcher sets LASTDB_APP, and this call sends that header.
   * `declare_schema` may set `app_id` to the schema namespace. Omit it to use the calling app.
   */
  databases<T extends DatabasesRequest>(
    body: T,
    timeoutMs?: number,
  ): Promise<DatabasesReplyFor<T>> {
    return executeAppDatabases(this.context, body, timeoutMs);
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

  /** `POST /api/databases/spec` — clear an owned database's binding. Attaching a filtered spec is refused. */
  bindDatabaseSpec(databaseId: string, spec: DatabaseSpec | null): Promise<ListedDatabase> {
    return bindNodeDatabaseSpec(this.baseTransport, databaseId, spec, this.appId);
  }

  /**
   * Return a client bound to `databaseId` without mutating `this`.
   * Schema list, schema get, `submitProposal`, and `getShareSubmissionStatus` use it.
   * `read`, `write`, and `changes` do not: name `database` on each item.
   */
  select(databaseId: string): LastDbClient<string> {
    return new LastDbClient(this.appId, this.baseTransport, { databaseId });
  }
}

/** Options for {@link ownerClient}. */
export interface OwnerClientOptions<DatabaseId extends string | undefined = undefined> {
  /** Canonical app id this client acts as. */
  appId: string;
  /** Database ID or local alias. Omit for the personal default. */
  databaseId?: DatabaseId;
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
  });
}
