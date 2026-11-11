/**
 * The LastDB runtime app client.
 *
 * Wraps the node's Unix-socket `/api/*` data plane and maps discriminated
 * responses to the typed errors in `errors.ts`.
 */

import { randomUUID } from 'node:crypto';
import {
  LASTDB_API_ROUTES,
  LASTDB_SDK_API_VERSION,
} from './apiRoutes.js';
import {
  AuthenticationRequiredError,
  CasConflictError,
  NodeTooOldError,
  PermissionDeniedError,
  QueryPaginationError,
  RequestRejectedError,
  ShareSubmissionUncertainError,
  TransportError,
  UnexpectedResponseError,
} from './errors.js';
import {
  openDiscoveredSocket,
  udsTransport,
  type Transport,
} from './transport.js';
import {
  type QueryBatchItem,
  type QueryBatchOutcome,
  type ConnectOptions,
  type AutoIdentityResult,
  type QueryCursor,
  type ListOptions,
  type ListResult,
  type LoadedSchema,
  type NodeVersion,
  type DeclareSchemaRequest,
  type DeclareSchemaResult,
  type MutationOp,
  type MutationOptions,
  type MutationResult,
  type ShareSubmissionResult,
  type QueryAllOptions,
  type QueryFilter,
  type QueryPage,
  type QueryResult,
  type QueryRow,
  type SchemaDescriptor,
} from './types.js';
import {
  parseListResponse,
  parseQueryResponse,
  isObject,
  parseMutationId,
  parseNodeVersion,
  parseAutoIdentityResponse,
  parseSchemaListResponse,
  resolveLoadedSchema,
} from './responseParsers.js';
import { createNodeDatabase, databaseScope, listNodeDatabases } from './databases.js';
import type { CreateDatabaseRequest, ListedDatabase } from './databases.js';
import {
  authenticationRequiredReason,
  casConflictDetailOf,
  toError,
  type BatchEntry,
  pairBatchReplies,
} from './clientHelpers.js';

function queryRowDedupKey(row: QueryRow): string {
  if (row.keyValue !== null) {
    return `kv:${JSON.stringify([row.keyValue.hash, row.keyValue.range])}`;
  }
  return `rendered:${row.key}`;
}

function isMutationId(id: string): boolean {
  return id.length === 36 &&
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id) &&
    id !== '00000000-0000-0000-0000-000000000000';
}

/**
 * Connect to a LastDB node over its Unix-domain socket and verify the required
 * API version before issuing data-plane requests.
 */
export async function connect<DatabaseId extends string | undefined = undefined>(
  options: ConnectOptions<DatabaseId>,
): Promise<LastDbClient<DatabaseId>> {
  const { appId, socketPath } = options;
  const defaultHeaders: Record<string, string> = {
    ...(options.defaultHeaders ?? {}),
  };
  if (
    !Object.keys(defaultHeaders).some(
      (k) => k.toLowerCase() === 'x-lastdb-client',
    )
  ) {
    const clientLabel = (options.clientId ?? appId).trim();
    if (clientLabel.length > 0) {
      defaultHeaders['X-LastDB-Client'] = clientLabel;
    }
  }
  const transport: Transport = socketPath
    ? udsTransport(socketPath, defaultHeaders, {
        timeoutMs: options.timeoutMs,
      })
    : openDiscoveredSocket({
        defaultHeaders,
        timeoutMs: options.timeoutMs,
      });

  const client = new LastDbClient<DatabaseId>(appId, transport, {
    databaseId: options.databaseId,
  });
  const required = Math.max(LASTDB_SDK_API_VERSION, options.requireApiVersion ?? 0);
  if (required > 0) {
    await client.requireApiVersion(required, options.appLabel ?? appId);
  }
  return client;
}

/** Behavior switches for {@link LastDbClient} (set via {@link ConnectOptions}). */
export interface LastDbClientOptions<DatabaseId extends string | undefined = undefined> {
  /** See `ConnectOptions.databaseId`. */
  databaseId?: DatabaseId;
}

/** Most queries the node accepts in one `/api/queries/batch` request. */
const QUERY_BATCH_MAX_ITEMS = 64;

/** A connected LastDB app client. Construct via {@link connect} or `ownerClient`. */
export class LastDbClient<DatabaseId extends string | undefined = undefined> {
  private readonly databasePath: string | null;
  /** Definition ids from declare, keyed by the schema name `mutate` sends. */
  private readonly definitionIds = new Map<string, string>();

  constructor(
    readonly appId: string,
    private readonly transport: Transport,
    options: LastDbClientOptions<DatabaseId> = {},
  ) {
    this.databasePath = databaseScope(options.databaseId);
  }

  /** Where this client is pointed (for diagnostics). */
  get target(): string {
    return this.transport.target;
  }

  /**
   * `GET /api/version` — the client↔node compatibility handshake.
   */
  async version(): Promise<NodeVersion> {
    const res = await this.transport.send('GET', LASTDB_API_ROUTES.version);
    if (res.status !== 200) {
      throw new UnexpectedResponseError(
        `version returned ${res.status}`,
        res.status,
        res.body,
      );
    }
    return parseNodeVersion(res.body);
  }

  /**
   * Refuse to proceed against a node whose `api_version` is below `required`.
   */
  async requireApiVersion(required: number, appLabel?: string): Promise<NodeVersion> {
    const node = await this.version();
    if (node.apiVersion < required) {
      throw new NodeTooOldError({
        reason: 'api_version_below_required',
        required,
        reported: node.apiVersion,
        build: node.build,
        app: appLabel ?? this.appId,
      });
    }
    return node;
  }

  /**
   * `GET /api/list` — return one page of live primary or alternate keys.
   */
  async list(
    schemaName: string,
    opts: ListOptions = {},
  ): Promise<ListResult> {
    const params = new URLSearchParams({ schema: schemaName });
    if (opts.keyName !== undefined) params.set('key_name', opts.keyName);
    if (opts.hash !== undefined) params.set('hash', opts.hash);
    if (opts.limit !== undefined) {
      params.set('limit', String(opts.limit));
    }
    if (opts.cursor !== undefined) {
      params.set('cursor', opts.cursor);
    }
    const res = await this.transport.send(
      'GET',
      `${this.dataPath(LASTDB_API_ROUTES.list)}?${params.toString()}`,
    );
    if (res.status === 200) {
      return {
        ...parseListResponse(res.body, opts.keyName),
        schema: schemaName,
      };
    }
    throw this.mapDataError('list', res.status, res.body);
  }

  /**
   * `POST /api/query` — keyed point, multi-get, or range read under one hash.
   */
  async query(
    schemaName: string,
    filter: QueryFilter = {},
  ): Promise<QueryResult> {
    const body = this.queryBody(schemaName, filter);
    const res = await this.transport.send('POST', this.dataPath(LASTDB_API_ROUTES.query), {
      body,
    });
    if (res.status === 200) {
      return { ...parseQueryResponse(res.body), schema: schemaName };
    }
    throw this.mapDataError('query', res.status, res.body);
  }

  /**
   * `POST /api/queries/batch` — read several independent keyed queries in one request.
   */
  async queryBatch(
    items: readonly QueryBatchItem[],
  ): Promise<QueryBatchOutcome[]> {
    const outcomes: QueryBatchOutcome[] = new Array<QueryBatchOutcome>(
      items.length,
    );
    const ready: BatchEntry[] = items.map((item, index) => ({ index, item }));

    for (let start = 0; start < ready.length; start += QUERY_BATCH_MAX_ITEMS) {
      const chunk = ready.slice(start, start + QUERY_BATCH_MAX_ITEMS);
      const queries = chunk.map(({ item }) =>
        this.queryBody(item.schemaName, item.filter ?? {}),
      );
      const res = await this.transport.send('POST', this.dataPath(LASTDB_API_ROUTES.queryBatch), {
        body: { queries },
      });
      if (res.status !== 200) {
        throw this.mapDataError('query batch', res.status, res.body);
      }
      for (const [entry, reply] of pairBatchReplies(chunk, res.body)) {
        outcomes[entry.index] = this.batchOutcome(entry, reply);
      }
    }
    return outcomes;
  }

  private batchOutcome(
    entry: BatchEntry,
    reply: { status: number; response: unknown },
  ): QueryBatchOutcome {
    if (reply.status !== 200) {
      return {
        ok: false,
        error: this.mapDataError('query', reply.status, reply.response),
      };
    }
    try {
      return {
        ok: true,
        result: { ...parseQueryResponse(reply.response), schema: entry.item.schemaName },
      };
    } catch (error) {
      return { ok: false, error: toError(error) };
    }
  }

  /**
   * Auto-paginate a keyed query across pages until `hasMore` is false or `maxRows` is reached.
   */
  async queryAll(
    schemaName: string,
    filter: Omit<QueryFilter, 'limit' | 'offset' | 'cursor'> = {},
    opts: QueryAllOptions = {},
  ): Promise<QueryResult> {
    const pageSize = opts.pageSize ?? 100;
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) {
      throw new Error(
        `queryAll pageSize must be an integer in [1, 1000], got ${pageSize}`,
      );
    }
    const maxRows = opts.maxRows ?? 100_000;

    const rows: QueryRow[] = [];
    const seenKeys = new Set<string>();
    let schema = '';
    let lastPage: QueryPage | null = null;
    let offset = 0;
    let cursor: QueryCursor | null = null;
    let truncatedByMaxRows = false;

    for (;;) {
      const page = await this.query(schemaName, {
        ...filter,
        limit: pageSize,
        ...(cursor === null ? { offset } : { cursor }),
      });
      schema = page.schema || schema;
      lastPage = page.page;
      let newRows = 0;
      for (const row of page.rows) {
        const key = queryRowDedupKey(row);
        if (seenKeys.has(key)) {
          continue;
        }
        seenKeys.add(key);
        rows.push(row);
        newRows += 1;
      }

      const hasMore =
        page.page !== null
          ? page.page.hasMore
          : page.rows.length >= pageSize;
      if (!hasMore) {
        break;
      }
      if (rows.length >= maxRows) {
        truncatedByMaxRows = true;
        break;
      }
      const nextCursor = page.page?.nextCursor ?? null;
      const moved = nextCursor !== null
        && JSON.stringify(nextCursor) !== JSON.stringify(cursor);
      if (newRows === 0 && !moved) {
        throw new QueryPaginationError('stalled_page', {
          totalCount: page.page?.totalCount,
          collectedCount: rows.length,
          returnedCount: page.rows.length,
          pageSize,
          offset,
        });
      }
      cursor = nextCursor;
      if (cursor === null) {
        offset += page.rows.length;
      }
    }

    if (
      !truncatedByMaxRows &&
      lastPage !== null &&
      lastPage.totalCount !== undefined &&
      lastPage.totalCount !== rows.length
    ) {
      throw new QueryPaginationError('total_count_mismatch', {
        totalCount: lastPage.totalCount,
        collectedCount: rows.length,
        returnedCount: lastPage.returnedCount,
        pageSize,
        offset,
      });
    }

    return {
      schema,
      rowCount: rows.length,
      rows,
      page:
        lastPage === null
          ? null
          : {
              totalCount: lastPage.totalCount,
              returnedCount: rows.length,
              limit: pageSize,
              offset: 0,
              hasMore: truncatedByMaxRows,
              nextCursor: truncatedByMaxRows ? lastPage.nextCursor : null,
            },
    };
  }

  /**
   * `POST /api/mutation`. Writes one complete conditional record into `schemaName`.
   */
  async mutate(
    schemaName: string,
    op: MutationOp,
    options?: DatabaseId extends string ? MutationOptions : never,
  ): Promise<MutationResult | (DatabaseId extends string ? ShareSubmissionResult : never)>;
  async mutate(
    schemaName: string,
    op: MutationOp,
    options: MutationOptions = {},
  ): Promise<MutationResult | ShareSubmissionResult> {
    const body: Record<string, unknown> = {
      type: 'mutation',
      schema: schemaName,
      record: op.record ?? {},
      key_value: op.key,
      alternates: op.alternates,
      mutation_type: op.mutationType,
      expected: op.expected,
      source_file_name: op.metadata?.source_file_name ?? null,
      metadata: op.metadata?.metadata ?? null,
      file: op.metadata?.file ?? null,
    };
    if (op.convergence !== undefined) {
      body.convergence = op.convergence;
    }
    if (op.durability !== undefined) {
      body.durability = op.durability;
    }
    const definitionId = this.definitionIds.get(schemaName);
    if (definitionId !== undefined) {
      body.definition_id = definitionId;
    }
    const mutationId = this.databasePath === null
      ? undefined
      : options.mutationId ?? randomUUID();
    const headers: Record<string, string> = {};
    if (mutationId !== undefined) {
      if (!isMutationId(mutationId)) {
        throw new RequestRejectedError('mutation_id', 'Mutation ID must be a non-nil lowercase UUID');
      }
      headers['Idempotency-Key'] = mutationId;
    }
    const res = await this.transport.send('POST', this.dataPath(LASTDB_API_ROUTES.mutation), {
      headers,
      body,
    }).catch((error: unknown) => {
      const refused = error instanceof TransportError && error.status !== undefined &&
        error.status >= 400 && error.status < 500;
      if (mutationId !== undefined && !refused) {
        throw new ShareSubmissionUncertainError(mutationId, error);
      }
      throw error;
    });
    if (res.status === 202 && mutationId !== undefined) {
      if (isObject(res.body)) {
        const sequence = res.body['submission_sequence'];
        if (
          res.body['ok'] === true && res.body['status'] === 'submitted' &&
          res.body['mutation_id'] === mutationId && typeof sequence === 'number' &&
          Number.isSafeInteger(sequence) && sequence > 0
        ) {
          return { status: 'submitted', mutationId, submissionSequence: sequence };
        }
      }
      throw new ShareSubmissionUncertainError(mutationId, new UnexpectedResponseError(
        'mutation returned an invalid submission receipt', 202, res.body,
      ));
    }
    if (res.status === 200) {
      if (!isObject(res.body)) {
        throw new UnexpectedResponseError(
          'mutation returned a non-object body',
          200,
          res.body,
        );
      }
      const b = res.body;
      const result: MutationResult = {
        mutationId: parseMutationId(b, res.body),
        recordRevision: typeof b['record_revision'] === 'string' ? b['record_revision'] : '',
      };
      if (!/^[0-9a-f]{64}$/.test(result.recordRevision)) {
        throw new UnexpectedResponseError('mutation returned no record revision', 200, res.body);
      }
      if (typeof b['background_tasks_drained'] === 'boolean') {
        result.backgroundTasksDrained = b['background_tasks_drained'];
      }
      if (typeof b['convergence_pending'] === 'boolean') {
        result.convergencePending = b['convergence_pending'];
      }
      return result;
    }
    const error = this.mapDataError('mutation', res.status, res.body);
    if (mutationId !== undefined && res.status >= 500) {
      throw new ShareSubmissionUncertainError(mutationId, error);
    }
    throw error;
  }

  /**
   * Remember the definition id a declare response returned for `schemaName`.
   */
  rememberDefinition(schemaName: string, definitionId: string): void {
    if (!/^[0-9a-f]{64}$/.test(definitionId)) {
      throw new UnexpectedResponseError(
        'definition id must be a lowercase SHA-256',
        0,
        definitionId,
      );
    }
    this.definitionIds.set(schemaName, definitionId);
  }

  /**
   * `POST /api/schemas/declare` on the owner socket.
   */
  async declareSchema(request: DeclareSchemaRequest): Promise<DeclareSchemaResult> {
    const res = await this.transport.send('POST', this.dataPath(LASTDB_API_ROUTES.schemasDeclare), {
      body: request,
    });
    if (res.status !== 200 || !isObject(res.body)) {
      throw new UnexpectedResponseError(
        `declare schema returned ${res.status}`,
        res.status,
        res.body,
      );
    }
    const definitionId = res.body['definition_id'];
    const schemaName = res.body['schema'];
    if (typeof definitionId !== 'string' || typeof schemaName !== 'string' || schemaName.length === 0) {
      throw new UnexpectedResponseError('declare returned no definition id', res.status, res.body);
    }
    this.rememberDefinition(schemaName, definitionId);
    const identity = res.body['identity_hash'];
    const resolution = res.body['resolution'];
    return {
      schemaName,
      definitionId,
      identityHash: typeof identity === 'string' ? identity : null,
      resolution: typeof resolution === 'string' ? resolution : null,
    };
  }

  /**
   * `GET /api/system/auto-identity`. Resolves the local owner identity.
   */
  async autoIdentity(): Promise<AutoIdentityResult> {
    const res = await this.transport.send(
      'GET',
      LASTDB_API_ROUTES.autoIdentity,
    );
    if (res.status === 200) {
      return parseAutoIdentityResponse(res.body);
    }
    if (res.status === 503) {
      const body = isObject(res.body) ? res.body : {};
      return {
        provisioned: false,
        reason:
          typeof body['error'] === 'string'
            ? body['error']
            : 'node_not_provisioned',
        next: typeof body['next'] === 'string' ? body['next'] : null,
      };
    }
    throw new UnexpectedResponseError(
      `auto-identity returned ${res.status}`,
      res.status,
      res.body,
    );
  }

  /** `GET /api/databases`. Personal and received shares stay in the list. */
  listDatabases(): Promise<ListedDatabase[]> {
    return listNodeDatabases(this.transport);
  }

  /** `POST /api/databases`. The new database is shareable and owned by this node. */
  createDatabase(request: CreateDatabaseRequest): Promise<ListedDatabase> {
    return createNodeDatabase(this.transport, request);
  }

  /**
   * A client for one database id or name. This client stays on its current database.
   */
  select(databaseId: string): LastDbClient<string> {
    return new LastDbClient(this.appId, this.transport, { databaseId });
  }

  /**
   * `GET /api/schemas` — list schemas loaded on the node.
   */
  async listSchemas(): Promise<LoadedSchema[]> {
    const res = await this.transport.send('GET', this.dataPath(LASTDB_API_ROUTES.schemas));
    if (res.status === 200) {
      return parseSchemaListResponse(res.body);
    }
    throw new UnexpectedResponseError(
      `schemas list returned ${res.status}`,
      res.status,
      res.body,
    );
  }

  /**
   * Resolve an app-owned schema descriptor to the loaded canonical schema entry.
   */
  async resolveSchema(
    descriptor: SchemaDescriptor,
  ): Promise<LoadedSchema | null> {
    return resolveLoadedSchema(await this.listSchemas(), descriptor);
  }

  private dataPath(route: string): string {
    return this.databasePath === null
      ? route
      : `${this.databasePath}${route.slice('/api'.length)}`;
  }

  private queryBody(
    schemaName: string,
    filter: QueryFilter,
  ): Record<string, unknown> {
    const body: Record<string, unknown> = { schema_name: schemaName };
    if (filter.keyName !== undefined) {
      body.key_name = filter.keyName;
    }
    if (filter.filter !== undefined) {
      body.filter = filter.filter;
    }
    if (filter.limit !== undefined) {
      body.limit = filter.limit;
    }
    if (filter.offset !== undefined) {
      body.offset = filter.offset;
    }
    if (filter.cursor !== undefined) {
      body.cursor = filter.cursor;
    }
    return body;
  }

  /** Map a non-200 data-path response to a typed error. */
  private mapDataError(verb: string, status: number, body: unknown): Error {
    const b = (body ?? {}) as {
      kind?: string;
      code?: string;
      error?: string;
      reason?: string;
      message?: string;
    };
    if (status === 401) {
      return new AuthenticationRequiredError(
        authenticationRequiredReason(b),
        b.error ?? b.message ?? `${verb} requires authentication`,
        body ?? null,
      );
    }
    if (status === 409 && b.error === 'cas_conflict') {
      return new CasConflictError(casConflictDetailOf(body), body ?? null);
    }
    if (status === 403) {
      return new PermissionDeniedError(
        b.error ?? b.reason ?? `${verb} permission denied`,
      );
    }
    if (status === 400) {
      if (b.kind === 'unknown_key') {
        return new NodeTooOldError(
          { reason: 'unknown_key', reported: null, build: null, app: this.appId },
          body ?? null,
        );
      }
      return new RequestRejectedError(
        b.kind ?? 'invalid_request',
        b.error ?? b.message ?? `${verb} rejected`,
        body ?? null,
      );
    }
    return new UnexpectedResponseError(
      `${verb} returned ${status}`,
      status,
      body,
    );
  }
}
