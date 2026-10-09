/**
 * The LastDB runtime app client.
 *
 * Wraps the node's production `/api/*` dialect: connect → request-consent →
 * poll → query/mutation-with-capability. Every method maps the node's
 * discriminated responses to the typed errors in `errors.ts` — no catch-all.
 *
 * Surface verified against `origin/main`:
 * - consent flow handlers: `fold_db_node/src/server/routes/apps.rs`
 * - data path handlers: `fold_db_node::dev_mode` `app_endpoints.rs` (the dev mirror of
 *   production `fold_db_node`'s control-socket route table)
 * - capability header: `fold_db_node/src/handlers/caller.rs`
 *   (`X-App-Capability` = base64 JSON CapabilityToken, `X-Capability-Ts` =
 *   unix epoch seconds)
 */

import {
  capabilityStoreKey,
  defaultCapabilityStore,
  type CapabilityStore,
} from './capabilityStore.js';
import { LASTDB_API_ROUTES } from './apiRoutes.js';
import { verifyCapabilityBlob } from './capabilityToken.js';
import {
  AppInSandboxError,
  AuthenticationRequiredError,
  CapabilityDeniedError,
  CapabilityRevokedError,
  CapabilityVerificationError,
  CasConflictError,
  ConsentDeniedError,
  ConsentExpiredError,
  ConsentRequestNotFoundError,
  ConsentTimeoutError,
  FullScanNotAllowedError,
  InvalidScopeError,
  NodeTooOldError,
  PermissionDeniedError,
  QueryPaginationError,
  RequestRejectedError,
  TransportError,
  UnexpectedResponseError,
  UnknownAppError,
  type CapabilityDenialDetail,
  type CasConflictDetail,
} from './errors.js';
import {
  LASTDB_DB_HEADER,
  resolveDbLocator,
} from './dbHandle.js';
import {
  discoverTransport,
  httpTransport,
  udsTransport,
  type Transport,
  type RawResponse,
} from './transport.js';
import type {
  QueryBatchItem,
  QueryBatchOutcome,
  ConnectOptions,
  ConsentScope,
  AutoIdentityResult,
  ChangesOptions,
  ChangesResult,
  CasExpectation,
  JsonValue,
  KeyValue,
  ListOptions,
  ListResult,
  LoadedSchema,
  NodeVersion,
  MutationCloudCaptureReceipt,
  MutationCloudCaptureState,
  MutationCloudPublicationReceipt,
  MutationCloudPublicationState,
  MutationCloudPublicationTarget,
  MutationOp,
  MutationResult,
  QueryAllOptions,
  QueryFieldPredicate,
  QueryFilter,
  QueryPage,
  QueryResult,
  QueryRow,
  RequestConsentResult,
  ResolveResult,
  SchemaDescriptor,
  RowFields,
  SchemaResolver,
  SchemaResolverResult,
  SearchHit,
  SearchOptions,
  SearchResult,
} from './types.js';

/** The HTTP header carrying `base64(JSON CapabilityToken)`. */
const CAPABILITY_HEADER = 'X-App-Capability';
/** The HTTP header carrying the per-request capability timestamp (epoch secs). */
const CAPABILITY_TS_HEADER = 'X-Capability-Ts';
/** Design-mandated poll cadence for `consent-status` (~2s). */
const DEFAULT_POLL_INTERVAL_MS = 2000;
/**
 * The exact publication route has a 130s server budget. Give that route a
 * larger client deadline without changing the 30s default for ordinary calls.
 */
const EXACT_CLOUD_PUBLICATION_TIMEOUT_MS = 150_000;

function queryRowDedupKey(row: QueryRow): string {
  if (row.keyValue !== null) {
    return `kv:${row.keyValue.hash ?? ''}\u0000${row.keyValue.range ?? ''}`;
  }
  return `rendered:${row.key}`;
}

/** Options for {@link LastDbClient.awaitConsent}. */
export interface AwaitConsentOptions {
  /** Hard client-side ceiling. Throws {@link ConsentTimeoutError} past it. */
  timeoutMs?: number;
  /** Poll interval (default 2000ms, per the design). */
  pollIntervalMs?: number;
}

/** Serialize a {@link ConsentScope} to the node's `scope` string. */
function scopeToString(scope: ConsentScope): string {
  if (scope === 'wildcard') {
    return 'wildcard';
  }
  return `explicit:${scope.explicit.join(',')}`;
}

/** Current unix epoch seconds, as the `X-Capability-Ts` value. */
function nowEpochSecs(): string {
  return String(Math.floor(Date.now() / 1000));
}

/**
 * Connect to a LastDB node. Provide exactly one of `baseUrl` (HTTP) or
 * `socketPath` (Unix-domain socket) — the transport is chosen by which is
 * present. Attempts to auto-load a stored capability for `appId` unless one
 * is supplied inline.
 *
 * **Socket-first discovery.** When you pass `baseUrl` (the local-node case),
 * the SDK PREFERS the node's Unix-domain data-plane socket and falls back to
 * the `baseUrl` TCP listener only when no socket file is present. This follows
 * the discovery order the Rust client uses, with the brand-forward
 * `LASTDB_SOCKET_PATH` preferred ahead of the Rust client's
 * `FOLDDB_SOCKET_PATH` (`LASTDB_SOCKET_PATH` → `FOLDDB_SOCKET_PATH` legacy
 * alias → `FOLDDB_SOCK` legacy alias → `<data_dir>/folddb.sock` → TCP),
 * making the
 * socket the normal app path while keeping TCP working mid-migration. Opt out
 * with `connect({ baseUrl, discoverSocket: false })` to force the TCP listener
 * (e.g. against a remote/non-local node, or a browser-style HTTP path). An
 * explicit `socketPath` always uses that socket verbatim with no discovery.
 */
export async function connect(options: ConnectOptions): Promise<LastDbClient> {
  const { appId, baseUrl, socketPath } = options;
  if ((baseUrl && socketPath) || (!baseUrl && !socketPath)) {
    throw new Error(
      'connect requires exactly one of { baseUrl } or { socketPath }',
    );
  }
  // Self-reported client label for Mini op telemetry. Prefer explicit
  // clientId; otherwise use appId. Never clobber a caller-supplied header.
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
  // Multi-DB handle: explicit option → LASTDB_DB env → personal. Never
  // clobber a caller-supplied X-LastDB-Db.
  const dbLocator = resolveDbLocator(options.db);
  if (
    !Object.keys(defaultHeaders).some(
      (k) => k.toLowerCase() === LASTDB_DB_HEADER.toLowerCase(),
    )
  ) {
    defaultHeaders[LASTDB_DB_HEADER] = dbLocator;
  }
  const discoverSocket = options.discoverSocket ?? true;
  let transport: Transport;
  if (socketPath) {
    // An explicit socket: use it verbatim, no discovery.
    transport = udsTransport(socketPath, defaultHeaders, {
      timeoutMs: options.timeoutMs,
    });
  } else if (discoverSocket) {
    // Local-node case: prefer the data-plane socket, fall back to the
    // supplied TCP base URL when no socket file exists.
    transport = discoverTransport({
      fallbackBaseUrl: baseUrl as string,
      defaultHeaders,
      timeoutMs: options.timeoutMs,
    });
  } else {
    transport = httpTransport(baseUrl as string, defaultHeaders, {
      timeoutMs: options.timeoutMs,
    });
  }

  // The capability store keys by (appId, nodeTarget) so a capability minted
  // by one node is never replayed against another (gap #2). The transport's
  // `target` is the canonical node string (`http://…` for TCP, `unix:…` for
  // a control socket).
  const nodeTarget = transport.target;
  const store =
    options.capabilityStore ??
    defaultCapabilityStore(options.keychainService);
  const storeKey = capabilityStoreKey(appId, nodeTarget);
  const verifyCapability = options.verifyCapability ?? false;

  let capability = options.capability ?? null;
  if (capability !== null) {
    // An inline capability the caller passed explicitly: under verification,
    // a bad one is a hard error (fail fast, don't replay a doomed token).
    if (verifyCapability) {
      const v = verifyCapabilityBlob(capability, appId);
      if (!v.ok) {
        throw new CapabilityVerificationError(v.problem, appId, v.tokenAppId);
      }
    }
  } else {
    // Wrong-node detection: a stored capability bound to a different node is
    // treated as absent (returns null), never blindly sent.
    capability = await store.load(storeKey, { expectedNode: nodeTarget });
    // Under verification, a cached blob that fails decode / audience / JCS
    // integrity is discarded (treated as absent) rather than replayed into a
    // guaranteed 403 — the "or invalid" branch of the client contract.
    if (
      verifyCapability &&
      capability !== null &&
      !verifyCapabilityBlob(capability, appId).ok
    ) {
      await store.remove(storeKey);
      capability = null;
    }
  }
  const client = new LastDbClient(
    appId,
    transport,
    store,
    capability,
    storeKey,
    nodeTarget,
    {
      verifyCapability,
      schemaResolver: options.schemaResolver,
      dbLocator,
    },
  );
  // The compatibility handshake runs before any data request so an app that
  // is newer than the node fails on ONE line that names the fix, not on its
  // first write with a 400 that hides which side moved.
  const required = options.requireApiVersion ?? 0;
  if (required > 0) {
    await client.requireApiVersion(required, options.appLabel ?? appId);
  }
  return client;
}

/** Behavior switches for {@link LastDbClient} (set via {@link ConnectOptions}). */
export interface LastDbClientOptions {
  /** See `ConnectOptions.verifyCapability`. Default `false`. */
  verifyCapability?: boolean;
  /** See `ConnectOptions.schemaResolver`. Default pass-through. */
  schemaResolver?: SchemaResolver;
  /**
   * Canonical DB locator this client targets (also sent as `X-LastDB-Db`).
   * Defaults to personal when omitted.
   */
  dbLocator?: string;
}

/** A connected LastDB app client. Construct via {@link connect}. */
/** Most queries the node accepts in one `/api/queries/batch` request. */
const QUERY_BATCH_MAX_ITEMS = 64;

/** Queries in flight at once when the node lacks the batch route. */
const QUERY_BATCH_FALLBACK_WIDTH = 4;

export class LastDbClient {
  private readonly verifyCapability: boolean;
  private readonly schemaResolver: SchemaResolver | null;
  /** `null` until the node answers; `false` once it lacks `/api/queries/batch`. */
  private queryBatchSupported: boolean | null = null;
  /** Canonical multi-DB locator forwarded on every data-path request. */
  readonly dbLocator: string;

  constructor(
    readonly appId: string,
    private readonly transport: Transport,
    private readonly store: CapabilityStore,
    private capability: string | null,
    /** The node-scoped capability-store key: `capabilityStoreKey(appId, node)`. */
    private readonly storeKey: string,
    /** The canonical node target this client is bound to (transport `target`). */
    private readonly nodeTarget: string,
    options: LastDbClientOptions = {},
  ) {
    this.verifyCapability = options.verifyCapability ?? false;
    this.schemaResolver = options.schemaResolver ?? null;
    this.dbLocator = options.dbLocator ?? resolveDbLocator(undefined);
  }

  /** Where this client is pointed (for diagnostics). */
  get target(): string {
    return this.transport.target;
  }

  /** Whether a capability is currently loaded. */
  get hasCapability(): boolean {
    return this.capability !== null;
  }

  // -------------------------------------------------------------------------
  // Version handshake
  // -------------------------------------------------------------------------

  /**
   * `GET /api/version` — the client↔node compatibility handshake. Reads no
   * node state and needs no capability. A node that predates the route (404)
   * yields `{ apiVersion: 0, handshake: false }` rather than an error, so a
   * caller can still print what it learned.
   */
  async version(): Promise<NodeVersion> {
    let res: RawResponse;
    try {
      res = await this.transport.send('GET', LASTDB_API_ROUTES.version);
    } catch (err) {
      // A node that predates the route answers the content-free bytes
      // `Not Found`, which the stock transports report as a `protocol`
      // TransportError carrying the status. That is the "no handshake"
      // answer, not a transport failure — every other error still throws.
      if (err instanceof TransportError && err.kind === 'protocol' && err.status === 404) {
        return PRE_HANDSHAKE_NODE;
      }
      throw err;
    }
    if (res.status === 404) {
      return PRE_HANDSHAKE_NODE;
    }
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
   * Throws {@link NodeTooOldError}, whose message is the one line an operator
   * needs (`… Run: brew upgrade lastdb …`). Returns the node's version on
   * success so a caller can log it. `connect({ requireApiVersion })` calls
   * this for you.
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

  // -------------------------------------------------------------------------
  // Consent flow
  // -------------------------------------------------------------------------

  /**
   * `POST /api/apps/request-consent`. Returns a `requestId` to poll with
   * {@link awaitConsent}. The owner grants via `folddb consent grant <appId>`.
   */
  async requestConsent(
    scope: ConsentScope = 'wildcard',
  ): Promise<RequestConsentResult> {
    const res = await this.transport.send('POST', LASTDB_API_ROUTES.requestConsent, {
      body: { app_id: this.appId, scope: scopeToString(scope) },
    });
    if (res.status === 202) {
      const body = res.body as { request_id: string; expires_at: string };
      return { requestId: body.request_id, expiresAt: body.expires_at };
    }
    if (res.status === 404) {
      throw new UnknownAppError(this.appId);
    }
    if (res.status === 403) {
      // The node discriminates with `reason: "app_in_sandbox"`.
      throw new AppInSandboxError(this.appId);
    }
    if (res.status === 400) {
      throw new InvalidScopeError(this.errorText(res.body, 'invalid scope'));
    }
    throw new UnexpectedResponseError(
      `request-consent returned ${res.status}`,
      res.status,
      res.body,
    );
  }

  /**
   * Poll `GET /api/apps/consent-status/{requestId}` until the owner grants the
   * request, then store and return the base64 capability. Throws the matching
   * typed error on denied / revoked / expired, and {@link ConsentTimeoutError}
   * if the request is still pending when `timeoutMs` elapses.
   */
  async awaitConsent(
    requestId: string,
    options: AwaitConsentOptions = {},
  ): Promise<string> {
    const timeoutMs = options.timeoutMs ?? 5 * 60 * 1000;
    const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    const deadline = Date.now() + timeoutMs;

    for (;;) {
      const capability = await this.pollConsentOnce(requestId);
      if (capability !== null) {
        // Under `verifyCapability`, gate the granted token BEFORE adopting it:
        // audience binding (the token must be bound to THIS app id) and the
        // JCS integrity binding (envelope.payload_hash ==
        // sha256(JCS(token-minus-envelope))). Without the gate, a mis-minted
        // or substituted token would be stored and replayed once before the
        // node's signature check 403'd it as `capability_bad_sig`.
        if (this.verifyCapability) {
          const v = verifyCapabilityBlob(capability, this.appId);
          if (!v.ok) {
            throw new CapabilityVerificationError(
              v.problem,
              this.appId,
              v.tokenAppId,
            );
          }
        }
        await this.storeCapability(capability);
        return capability;
      }
      if (Date.now() + pollIntervalMs > deadline) {
        throw new ConsentTimeoutError(requestId, timeoutMs);
      }
      await delay(pollIntervalMs);
    }
  }

  /**
   * One `consent-status` poll. Returns the base64 capability on `granted`,
   * `null` while still `pending`, and throws the typed terminal error
   * otherwise. Exposed for callers that want to drive their own poll loop.
   */
  async pollConsentOnce(requestId: string): Promise<string | null> {
    const res = await this.transport.send(
      'GET',
      LASTDB_API_ROUTES.consentStatus(requestId),
    );
    switch (res.status) {
      case 202:
        return null; // pending
      case 200: {
        const body = res.body as { status: string; capability: string };
        return body.capability;
      }
      case 403: {
        const body = res.body as { status?: string };
        if (body?.status === 'revoked') {
          throw new CapabilityRevokedError(requestId);
        }
        throw new ConsentDeniedError(requestId);
      }
      case 408:
        throw new ConsentExpiredError(requestId);
      case 404:
        throw new ConsentRequestNotFoundError(requestId);
      default:
        throw new UnexpectedResponseError(
          `consent-status returned ${res.status}`,
          res.status,
          res.body,
        );
    }
  }

  // -------------------------------------------------------------------------
  // Capability storage
  // -------------------------------------------------------------------------

  /**
   * Persist `capability` for this app **on this node** and use it on
   * subsequent calls. The entry is keyed by (appId, node) and records the
   * bound node, so it is never loaded for a different node.
   */
  async storeCapability(capability: string): Promise<void> {
    this.capability = capability;
    await this.store.store(this.storeKey, capability, this.nodeTarget);
  }

  /**
   * Load this app's stored capability **for this node** into the client.
   * Returns it, or `null` if none is stored (including the wrong-node case:
   * a capability bound to a different node is treated as absent). Called
   * automatically by `connect`.
   */
  async loadCapability(): Promise<string | null> {
    let cap = await this.store.load(this.storeKey, {
      expectedNode: this.nodeTarget,
    });
    // Under `verifyCapability`, a cached blob that fails decode / audience /
    // JCS integrity is discarded (treated as absent) rather than replayed
    // into a guaranteed 403.
    if (
      this.verifyCapability &&
      cap !== null &&
      !verifyCapabilityBlob(cap, this.appId).ok
    ) {
      await this.store.remove(this.storeKey);
      cap = null;
    }
    this.capability = cap;
    return cap;
  }

  // -------------------------------------------------------------------------
  // Data path
  // -------------------------------------------------------------------------

  /**
   * `GET /api/list` — return one page of live record keys without hydrating
   * atom bodies. Use this for membership, then point-read the keys whose
   * fields you need. A page is not a census: follow `next_cursor` while
   * `has_more` is true.
   */
  async list(
    schemaName: string,
    opts: ListOptions = {},
  ): Promise<ListResult> {
    const resolved = await this.resolveDataPathSchema(schemaName);
    const params = new URLSearchParams({ schema: resolved.nodeSchemaName });
    if (opts.limit !== undefined) {
      params.set('limit', String(opts.limit));
    }
    if (opts.cursor !== undefined) {
      params.set('cursor', opts.cursor);
    }
    const res = await this.transport.send(
      'GET',
      `${LASTDB_API_ROUTES.list}?${params.toString()}`,
      { headers: this.capabilityHeaders() },
    );
    if (res.status === 200) {
      return {
        ...parseListResponse(res.body),
        schema: resolved.appSchemaName,
      };
    }
    throw this.mapDataError('list', res.status, res.body);
  }

  /**
   * `POST /api/query`. Reads fields from `schemaName` (a schema or a view).
   * Auto-attaches the capability headers when one is loaded.
   *
   * Pagination: production `fold_db_node` ALWAYS pages — with no `limit` it
   * still caps the response at its default page size (100,
   * `DEFAULT_QUERY_LIMIT`), so a >100-row schema is silently truncated;
   * check `result.page?.hasMore` or use {@link queryAll} to drain it.
   * `filter.limit`/`filter.offset` are forwarded verbatim as the request's
   * top-level pagination fields, and only when set. Both the production node
   * and the dev node (`fold_db_node::dev_mode`) honor them with production-parity
   * semantics (default 100, clamp 1000, `total_count`/`has_more` metadata).
   */
  async query(
    schemaName: string,
    filter: QueryFilter = {},
    opts: { allowFullScan?: boolean } = {},
  ): Promise<QueryResult> {
    const resolved = await this.resolveDataPathSchema(schemaName);
    const body = this.queryBody(resolved, filter);
    const headers: Record<string, string> = { ...this.capabilityHeaders() };
    if (opts.allowFullScan === true) {
      // Mini hard-refuses unfiltered product scans without this admin opt-in.
      headers['X-LastDB-Allow-Full-Scan'] = '1';
    }
    const res = await this.transport.send('POST', LASTDB_API_ROUTES.query, {
      headers,
      body,
    });
    if (res.status === 200) {
      return mapQueryResult(parseQueryResponse(res.body), resolved);
    }
    throw this.mapDataError('query', res.status, res.body);
  }

  /**
   * `POST /api/queries/batch`. Reads several independent queries, possibly
   * from different schemas, in one request, and returns one outcome per item
   * in request order. Items are independent: none reads another's result.
   *
   * A failing item returns `{ ok: false, error }` with the typed error
   * `query` would have thrown; the other items still return rows. The call
   * itself throws only when the whole request is refused (auth, capability,
   * malformed) or the transport fails.
   *
   * More than 64 items go out as several requests, joined in order. The
   * full-scan opt-in is never sent, so an unkeyed item fails with the node's
   * refusal.
   *
   * A node that predates the route answers 404/405. The client then runs the
   * items as plain `query` calls, a few at a time, with the same outcomes,
   * and remembers the answer for its life so later calls do not probe again.
   */
  async queryBatch(
    items: readonly QueryBatchItem[],
  ): Promise<QueryBatchOutcome[]> {
    const outcomes: QueryBatchOutcome[] = new Array<QueryBatchOutcome>(
      items.length,
    );
    const ready: BatchEntry[] = [];
    await Promise.all(
      items.map(async (item, index) => {
        try {
          const resolved = await this.resolveDataPathSchema(item.schemaName);
          ready.push({ index, item, resolved });
        } catch (error) {
          outcomes[index] = { ok: false, error: toError(error) };
        }
      }),
    );
    ready.sort((a, b) => a.index - b.index);

    for (let start = 0; start < ready.length; start += QUERY_BATCH_MAX_ITEMS) {
      const chunk = ready.slice(start, start + QUERY_BATCH_MAX_ITEMS);
      if (this.queryBatchSupported !== false) {
        const queries = chunk.map(({ item, resolved }) =>
          this.queryBody(resolved, item.filter ?? {}),
        );
        let res: RawResponse | null;
        try {
          res = await this.transport.send('POST', LASTDB_API_ROUTES.queryBatch, {
            headers: this.capabilityHeaders(),
            body: { queries },
          });
        } catch (error) {
          // A node without the route answers the content-free bytes `Not Found`.
          // The stock transports report that as a `protocol` TransportError that
          // carries the status (as for `version()`), so it arrives as a throw.
          if (!isMissingRouteError(error)) {
            throw error;
          }
          res = null;
        }
        if (res !== null && res.status === 200) {
          this.queryBatchSupported = true;
          for (const [entry, reply] of pairBatchReplies(chunk, res.body)) {
            outcomes[entry.index] = this.batchOutcome(entry, reply);
          }
          continue;
        }
        if (res !== null && res.status !== 404 && res.status !== 405) {
          throw this.mapDataError('query batch', res.status, res.body);
        }
        this.queryBatchSupported = false;
      }
      await this.queryEach(chunk, outcomes);
    }
    return outcomes;
  }

  /** One batch item's outcome from its `{status, response}` reply entry. */
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
        result: mapQueryResult(parseQueryResponse(reply.response), entry.resolved),
      };
    } catch (error) {
      return { ok: false, error: toError(error) };
    }
  }

  /** Run batch items as plain `query` calls (node without the batch route). */
  private async queryEach(
    chunk: readonly BatchEntry[],
    outcomes: QueryBatchOutcome[],
  ): Promise<void> {
    let next = 0;
    const worker = async (): Promise<void> => {
      for (;;) {
        const entry = chunk[next++];
        if (entry === undefined) {
          return;
        }
        try {
          outcomes[entry.index] = {
            ok: true,
            result: await this.query(entry.item.schemaName, entry.item.filter ?? {}),
          };
        } catch (error) {
          outcomes[entry.index] = { ok: false, error: toError(error) };
        }
      }
    };
    await Promise.all(
      Array.from(
        { length: Math.min(QUERY_BATCH_FALLBACK_WIDTH, chunk.length) },
        worker,
      ),
    );
  }

  /**
   * Drain a query past the node's page cap: issues `query()` repeatedly with
   * `limit`/`offset` until the node reports no more rows, and returns every
   * unique row as one {@link QueryResult}.
   *
   * Termination is two-signal: the node's own `page.hasMore` when it reports
   * pagination metadata (production), else a short page
   * (`rows.length < pageSize`). `opts.maxRows` (default 100k) is the safety
   * ceiling — when hit, the result's `page.hasMore` is `true` so the
   * truncation stays visible.
   *
   * Production offset pagination has historically been unstable, so `queryAll`
   * dedupes by row key across pages and throws {@link QueryPaginationError}
   * when a follow-up page makes no unique progress or when a completed drain
   * cannot match the node's `totalCount`.
   */
  async queryAll(
    schemaName: string,
    filter: Omit<QueryFilter, 'limit' | 'offset' | 'cursor'> = {},
    opts: QueryAllOptions = {},
  ): Promise<QueryResult> {
    if (filter.filter === undefined && opts.allowFullScan !== true) {
      // Unfiltered queryAll is a full schema drain — deprecated for product
      // apps (`brain design-lastdb-scan-deprecation-path`). Point reads
      // (`filter: { HashKey: id }`) and partition reads (HashRange) never hit
      // this gate; only a genuinely unfiltered drain does.
      throw new FullScanNotAllowedError(schemaName);
    }
    const pageSize = opts.pageSize ?? 100;
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) {
      // Production clamps limit to MAX_QUERY_LIMIT (1000); a clamped page
      // would silently desync the offset arithmetic, so reject up front.
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
    let cursor: KeyValue | null = null;
    let truncatedByMaxRows = false;

    for (;;) {
      const page = await this.query(
        schemaName,
        {
          ...filter,
          limit: pageSize,
          ...(cursor === null ? { offset } : { cursor }),
        },
        { allowFullScan: opts.allowFullScan === true },
      );
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
          : page.rows.length >= pageSize; // no metadata: stop on a short page
      if (!hasMore) {
        break;
      }
      if (rows.length >= maxRows) {
        truncatedByMaxRows = true;
        break;
      }
      if (newRows === 0) {
        throw new QueryPaginationError('stalled_page', {
          totalCount: page.page?.totalCount,
          collectedCount: rows.length,
          returnedCount: page.rows.length,
          pageSize,
          offset,
        });
      }
      cursor = page.page?.nextCursor ?? null;
      if (cursor === null) {
        offset += page.rows.length;
      }
    }

    // A drain that the node ended (`has_more: false`) is complete by the
    // node's own signal. The exact count is a cross-check when the node gave
    // one; it is NOT a requirement. Since 2026-08-02 the node skips the count
    // on ascending key-restricted pages ("Skip non-load-bearing query
    // counts"), so requiring it here refused every keyed drain — `situations
    // list` against a live primary answered `queryAll collected 1 unique
    // rows` for a one-row partition. Fail closed only on a REAL mismatch.
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
   * `POST /api/mutation`. Writes one row into `schemaName`. Auto-attaches the
   * capability headers when one is loaded.
   */
  async mutate(schemaName: string, op: MutationOp): Promise<MutationResult> {
    const resolved = await this.resolveDataPathSchema(schemaName);
    const body: Record<string, unknown> = {
      type: 'mutation',
      schema: resolved.nodeSchemaName,
      fields_and_values: mapFieldRecord(op.fields, resolved.appToNodeFields),
      key_value: op.key,
      mutation_type: op.mutationType,
    };
    // CAS precondition: forward it under the node's `expected` key only when
    // the caller set it (an unconditional write omits it). Its `field` names a
    // schema field, so it is mapped app→node the same way `fields_and_values`
    // keys are — a CAS guard on a renamed field must reference the node name.
    if (op.expected !== undefined) {
      body.expected = mapCasExpectation(op.expected, resolved.appToNodeFields);
    }
    if (op.convergence !== undefined) {
      body.convergence = op.convergence;
    }
    if (op.durability !== undefined) {
      body.durability = op.durability;
    }
    if (op.cloudPublication !== undefined) {
      body.cloud_publication = op.cloudPublication;
    }
    if (op.mustExist !== undefined) {
      body.must_exist = op.mustExist;
    }
    const sendOptions: {
      headers: Record<string, string>;
      body: Record<string, unknown>;
      minimumTimeoutMs?: number;
    } = {
      headers: this.capabilityHeaders(),
      body,
    };
    if (op.cloudPublication === 'wait') {
      sendOptions.minimumTimeoutMs = EXACT_CLOUD_PUBLICATION_TIMEOUT_MS;
    }
    const res = await this.transport.send(
      'POST',
      LASTDB_API_ROUTES.mutation,
      sendOptions,
    );
    if (res.status === 200) {
      if (!isObject(res.body)) {
        throw new UnexpectedResponseError(
          'mutation returned a non-object body',
          200,
          res.body,
        );
      }
      const b = res.body;
      const mutationIds = parseMutationIds(b, res.body);
      const written = parseMutationWritten(b, res.body);
      const firingsObserved = parseMutationFiringsObserved(b, res.body);
      const result: MutationResult = {
        written,
        mutationIds,
        firingsObserved,
      };
      if (typeof b['background_tasks_drained'] === 'boolean') {
        result.backgroundTasksDrained = b['background_tasks_drained'];
      }
      if (typeof b['convergence_pending'] === 'boolean') {
        result.convergencePending = b['convergence_pending'];
      }
      if (typeof b['local_committed'] === 'boolean') {
        result.localCommitted = b['local_committed'];
      }
      if (b['cloud_capture'] !== undefined) {
        result.cloudCapture = parseMutationCloudCapture(b['cloud_capture']);
      }
      if (b['cloud_publication'] !== undefined) {
        result.cloudPublication = parseMutationCloudPublication(
          b['cloud_publication'],
        );
      }
      if (op.cloudPublication === 'wait') {
        const exactMutationUuid =
          mutationIds.length === 1 ? mutationIds[0] : undefined;
        if (
          b['durability'] !== 'durable' ||
          result.localCommitted !== true ||
          result.cloudCapture === undefined ||
          result.cloudPublication === undefined ||
          exactMutationUuid === undefined ||
          result.cloudCapture.mutationUuid !== exactMutationUuid ||
          result.cloudPublication.mutationUuid !== exactMutationUuid ||
          result.cloudPublication.state === 'not_requested' ||
          (result.cloudCapture.state === 'failed' &&
            result.cloudPublication.state !== 'failed') ||
          (result.cloudPublication.state !== 'failed' &&
            result.cloudPublication.targets.length === 0)
        ) {
          throw new UnexpectedResponseError(
            'mutation returned an unbound exact cloud publication receipt',
            200,
            res.body,
          );
        }
      }
      return result;
    }
    throw this.mapDataError('mutation', res.status, res.body);
  }

  /**
   * Read durable changed-row metadata since an opaque cursor. The node owns
   * scope: a verified app sees only its authorized schemas, and `target` can
   * only narrow that set. Rows are hints; point-read product state after wake.
   */
  async changes(opts: ChangesOptions = {}): Promise<ChangesResult> {
    const body: Record<string, unknown> = {};
    if (opts.since !== undefined) body.since_cursor = opts.since;
    if (opts.limit !== undefined) body.limit = opts.limit;
    if (opts.target !== undefined) body.target = opts.target;
    const res = await this.transport.send('POST', LASTDB_API_ROUTES.appChanges, {
      headers: this.capabilityHeaders(),
      body,
    });
    if (res.status === 200) return parseChangesResponse(res.body);
    throw this.mapDataError('changes', res.status, res.body);
  }

  /**
   * `POST /api/app/search` — the **node-authoritative scoped search**
   * (`folddb_app_api.md` operation 5). Embeds `query`, ranks it over the
   * node's native index, and returns the top-k hits **only from schemas the
   * app has been granted** (its access-scope set `S(A)`).
   *
   * Scope is decided by the NODE, not the app. The node derives `S(A)` from the
   * capability's verified `app_id` against its own grant ledger; the app never
   * names its scope and **cannot widen it**. The single optional `opts.target`
   * is *intersected* with `S(A)` (never unioned) — a target outside scope just
   * yields no hits from it, with a normal `200`. There is deliberately no way
   * for the app to pass a schema allowlist.
   *
   * Auto-attaches the capability headers (`X-App-Capability` + `X-Capability-Ts`)
   * the same way `query`/`mutate` do. The node REJECTS a header-less call with
   * `403 {reason: "capability_required"}` (mapped to {@link PermissionDeniedError})
   * rather than handing back the owner's whole-index search.
   *
   * Each hit is a full {@link QueryRow} envelope (`key`, `fields`, `metadata`,
   * `authorPubKey`) plus the relevance `score` and the `schemaName` /
   * `schemaDisplayName` the hit came from.
   */
  async search(query: string, opts: SearchOptions = {}): Promise<SearchResult> {
    const body: Record<string, unknown> = { query };
    if (opts.k !== undefined) {
      body.k = opts.k;
    }
    // The single node-intersected `target` — NOT an app-controlled allowlist.
    if (opts.target !== undefined) {
      body.target = opts.target;
    }
    const res = await this.transport.send('POST', LASTDB_API_ROUTES.appSearch, {
      headers: this.capabilityHeaders(),
      body,
    });
    if (res.status === 200) {
      return parseSearchResponse(res.body);
    }
    throw this.mapDataError('search', res.status, res.body);
  }

  // -------------------------------------------------------------------------
  // Owner / host node helpers
  // -------------------------------------------------------------------------

  /**
   * `GET /api/system/auto-identity`. Resolves the local owner identity a
   * host-context app uses for `X-User-Hash`. A not-yet-provisioned node returns
   * `{ provisioned: false }` for the node's canonical 503, so callers can pivot
   * to bootstrap without treating it as a transport failure.
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

  /**
   * `GET /api/schemas`. Lists schemas loaded in the owner node and normalizes
   * the fields host-context apps use to resolve their own canonical schema hash
   * without hand-parsing raw route JSON.
   */
  async listSchemas(): Promise<LoadedSchema[]> {
    const res = await this.transport.send('GET', LASTDB_API_ROUTES.schemas);
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
   * Matching uses `owner_app_id` + `descriptive_name`, and when `fields` is
   * supplied requires the exact same field set regardless of order.
   */
  async resolveSchema(
    descriptor: SchemaDescriptor,
  ): Promise<LoadedSchema | null> {
    return resolveLoadedSchema(await this.listSchemas(), descriptor);
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  /** The `/api/query` request body for one resolved schema and filter. */
  private queryBody(
    resolved: ResolvedSchema,
    filter: QueryFilter,
  ): Record<string, unknown> {
    const body: Record<string, unknown> = { schema_name: resolved.nodeSchemaName };
    if (filter.fields !== undefined) {
      body.fields = filter.fields.map((field) =>
        mapFieldName(field, resolved.appToNodeFields),
      );
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
    if (filter.where !== undefined) {
      body.where = mapPredicateFields(filter.where, resolved.appToNodeFields);
    }
    return body;
  }

  /** Capability headers, present only when a capability is loaded. */
  private capabilityHeaders(): Record<string, string> {
    // Always stamp the DB handle on data-path calls so Mini scopes storage
    // even when the transport was built without defaultHeaders (ownerClient
    // / hand-built transports). Transport defaultHeaders of the same name win
    // only when the caller merges them after this object — here we set it
    // unconditionally on the per-call headers.
    const headers: Record<string, string> = {
      [LASTDB_DB_HEADER]: this.dbLocator,
    };
    if (this.capability !== null) {
      headers[CAPABILITY_HEADER] = this.capability;
      headers[CAPABILITY_TS_HEADER] = nowEpochSecs();
    }
    return headers;
  }

  /**
   * Resolve an app schema name to the node-facing schema + field maps (the
   * data-path schema mapping used by `query`/`mutate`). Distinct from the
   * public {@link resolveSchema} owner-host helper, which resolves a
   * {@link SchemaDescriptor} to a {@link LoadedSchema}; they were merged from
   * two concurrent PRs that both chose the name `resolveSchema`, so the private
   * data-path one is named `resolveDataPathSchema` to avoid the collision.
   */
  private async resolveDataPathSchema(
    appSchemaName: string,
  ): Promise<ResolvedSchema> {
    const result = await this.schemaResolver?.(appSchemaName);
    return normalizeSchemaResolution(appSchemaName, result);
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
      // A CAS precondition (`MutationOp.expected`) did not hold: the node
      // returns `409 {error:"cas_conflict", schema?, field?, key?, expected?,
      // actual?, message?}`. Surface it as the typed CasConflictError so an app
      // can re-read + retry instead of hand-detecting the conflict off a
      // generic error body.
      return new CasConflictError(casConflictDetailOf(body), body ?? null);
    }
    if (status === 403) {
      // Discriminated capability 403 (app_identity v3.1, gap #4): the
      // verifier's body is `{status: 403, reason: "<reason>", ...detail}` —
      // surface the reason VERBATIM plus any detail field, so an app can run
      // the eight-reason contract (`capabilityDenialReaction`). A 403 without
      // a `reason` keeps the legacy `{kind: "permission_denied"}` mapping.
      if (typeof b.reason === 'string') {
        return new CapabilityDeniedError(b.reason, capabilityDetailOf(body));
      }
      return new PermissionDeniedError(b.error ?? `${verb} permission denied`);
    }
    if (status === 400) {
      // `unknown_key` is the node telling us it does not know a key we sent:
      // this client is newer than the node. It is a version-skew fact, not a
      // request-shape bug, so it gets the typed error whose message names
      // the fix. (The node never echoes the key — its I4 rule.)
      if (b.kind === 'unknown_key') {
        return new NodeTooOldError(
          { reason: 'unknown_key', reported: null, build: null, app: this.appId },
          body ?? null,
        );
      }
      // Production 400 bodies are not uniform: the dev mirror sends
      // `{kind, error}`, production handlers send `{error}` or `{message}`
      // (or richer envelopes). Prefer `error`, fall back to the node's
      // `message`, and carry the WHOLE body on the error so nothing the node
      // said is lost (mirrors the 403 CapabilityDeniedError's detail).
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

  /** Extract an `error` string from a node error body, with a fallback. */
  private errorText(body: unknown, fallback: string): string {
    const b = (body ?? {}) as { error?: string };
    return b.error ?? fallback;
  }
}

function authenticationRequiredReason(body: {
  code?: string;
  error?: string;
  message?: string;
}): 'session_expired' | 'auth_failed' {
  const text = `${body.code ?? ''} ${body.error ?? ''} ${body.message ?? ''}`.toLowerCase();
  if (text.includes('session') && text.includes('expired')) {
    return 'session_expired';
  }
  if (text.includes('token') && text.includes('expired')) {
    return 'session_expired';
  }
  return 'auth_failed';
}

interface ResolvedSchema {
  appSchemaName: string;
  identity: string;
  nodeSchemaName: string;
  appToNodeFields: Record<string, string>;
  nodeToAppFields: Record<string, string>;
  outcome: string | null;
}

function normalizeSchemaResolution(
  appSchemaName: string,
  result: SchemaResolverResult,
): ResolvedSchema {
  if (typeof result === 'string') {
    return {
      appSchemaName,
      identity: result,
      nodeSchemaName: result,
      appToNodeFields: {},
      nodeToAppFields: {},
      outcome: null,
    };
  }
  if (isResolveResult(result)) {
    const appToNodeFields = result.adapter?.appToCatalog ?? {};
    return {
      appSchemaName,
      identity: result.identity,
      nodeSchemaName: result.schema ?? result.identity,
      appToNodeFields,
      nodeToAppFields:
        result.adapter?.catalogToApp ?? reverseFieldMap(appToNodeFields),
      outcome: result.outcome ?? null,
    };
  }
  const appToNodeFields = result?.fields ?? {};
  return {
    appSchemaName,
    identity: result?.nodeSchemaName ?? appSchemaName,
    nodeSchemaName: result?.nodeSchemaName ?? appSchemaName,
    appToNodeFields,
    nodeToAppFields: reverseFieldMap(appToNodeFields),
    outcome: null,
  };
}

function isResolveResult(
  result: SchemaResolverResult,
): result is ResolveResult {
  return (
    typeof result === 'object' &&
    result !== null &&
    'identity' in result &&
    typeof result.identity === 'string'
  );
}

function reverseFieldMap(
  fields: Record<string, string>,
): Record<string, string> {
  const reversed: Record<string, string> = {};
  for (const [appField, nodeField] of Object.entries(fields)) {
    if (reversed[nodeField] === undefined) {
      reversed[nodeField] = appField;
    }
  }
  return reversed;
}

function mapFieldName(
  field: string,
  fields: Record<string, string>,
): string {
  return fields[field] ?? field;
}

function mapFieldRecord<T extends JsonValue>(
  row: Record<string, T>,
  fields: Record<string, string>,
): Record<string, T> {
  const mapped: Record<string, T> = {};
  for (const [field, value] of Object.entries(row)) {
    mapped[mapFieldName(field, fields)] = value;
  }
  return mapped;
}

/**
 * Map a CAS {@link CasExpectation}'s `field` app→node (its value, when present,
 * is passed through unchanged — only the field name is a schema field). Used so
 * a `mutate` precondition guards the correct node-side field under a schema map.
 */
function mapCasExpectation(
  expected: CasExpectation,
  fields: Record<string, string>,
): CasExpectation {
  return { ...expected, field: mapFieldName(expected.field, fields) };
}

/**
 * Map each two-pass predicate's `field` app→node, leaving the comparison
 * payload (`value` / `values` / `instant`) untouched — only `field` names a
 * schema field. The variant tag and any unrecognized shape pass through
 * verbatim so the SDK never rewrites a predicate dialect the node may add
 * later; an unknown tag reaches the node and is rejected there, loudly,
 * instead of being silently dropped here.
 */
function mapPredicateFields(
  where: readonly QueryFieldPredicate[],
  fields: Record<string, string>,
): QueryFieldPredicate[] {
  return where.map((predicate) => {
    const [entry] = Object.entries(predicate as Record<string, unknown>);
    if (entry === undefined) {
      return predicate;
    }
    const [tag, body] = entry;
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      return predicate;
    }
    const inner = body as { field?: unknown };
    if (typeof inner.field !== 'string') {
      return predicate;
    }
    return {
      [tag]: { ...inner, field: mapFieldName(inner.field, fields) },
    } as QueryFieldPredicate;
  });
}

function mapQueryResult(
  result: QueryResult,
  resolved: ResolvedSchema,
): QueryResult {
  return {
    ...result,
    schema: resolved.appSchemaName,
    rows: result.rows.map((row) => mapQueryRow(row, resolved.nodeToAppFields)),
  };
}

function mapQueryRow(
  row: QueryRow,
  nodeToAppFields: Record<string, string>,
): QueryRow {
  return {
    ...row,
    fields: mapFieldRecord(row.fields, nodeToAppFields),
    metadata: mapFieldKeyedJson(row.metadata, nodeToAppFields),
  };
}

function mapFieldKeyedJson(
  value: JsonValue,
  fields: Record<string, string>,
): JsonValue {
  if (!isObject(value)) {
    return value;
  }
  return mapFieldRecord(value, fields);
}

/** Parse a successful keys-only `GET /api/list` response. */
export function parseListResponse(body: unknown): ListResult {
  if (!isObject(body) || !isObject(body['list'])) {
    throw new UnexpectedResponseError('list returned an invalid envelope', 200, body);
  }
  const page = body['list'];
  const schema = page['schema'];
  const rawKeys = page['keys'];
  const nextCursor = page['next_cursor'];
  const hasMore = page['has_more'];
  const truncated = page['truncated'];
  if (
    typeof schema !== 'string' ||
    !Array.isArray(rawKeys) ||
    (nextCursor !== undefined && nextCursor !== null && typeof nextCursor !== 'string') ||
    typeof hasMore !== 'boolean' ||
    typeof truncated !== 'boolean'
  ) {
    throw new UnexpectedResponseError('list returned an invalid keys page', 200, body);
  }
  const keys = rawKeys.map((raw): { hash: string; range?: string } => {
    if (!isObject(raw) || typeof raw['hash'] !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid record key', 200, raw);
    }
    const range = raw['range'];
    if (range !== undefined && typeof range !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid record key', 200, raw);
    }
    return range === undefined || range.length === 0
      ? { hash: raw['hash'] }
      : { hash: raw['hash'], range };
  });
  return {
    schema,
    keys,
    next_cursor: typeof nextCursor === 'string' ? nextCursor : null,
    has_more: hasMore,
    truncated,
  };
}

/**
 * Parse a `200` `/api/query` body into a {@link QueryResult}, surfacing the
 * full per-row envelope (gap #3).
 *
 * The node returns its rows under `results` (production `fold_db_node`) or
 * `rows` (the `fold_db_node::dev_mode` mirror); both carry per-key objects shaped
 * `{ key, fields, metadata, author_pub_key }`. Each row is normalized to a
 * {@link QueryRow}. A bare field-map row (a node that pre-dates the envelope)
 * is still accepted: its `key`/`metadata`/`authorPubKey` come back empty/null
 * and the whole object is its `fields` — so the SDK never throws away an
 * envelope the node sends, nor invents one it doesn't.
 */
export function parseQueryResponse(body: unknown): QueryResult {
  const b = (body ?? {}) as {
    schema?: unknown;
    row_count?: unknown;
    results?: unknown;
    rows?: unknown;
  };
  const rawRows: unknown[] = Array.isArray(b.results)
    ? b.results
    : Array.isArray(b.rows)
      ? b.rows
      : [];
  const rows = rawRows.map(parseQueryRow);
  const schema = typeof b.schema === 'string' ? b.schema : '';
  const rowCount =
    typeof b.row_count === 'number' ? b.row_count : rows.length;
  return { schema, rowCount, rows, page: parseQueryPage(body, rows.length) };
}

/**
 * Read production's pagination metadata (`total_count` / `returned_count` /
 * `limit` / `offset` / `has_more`, per `fold_db_node`'s `QueryResponse`) off
 * a 200 `/api/query` body. Returns `null` when the node reported none (the
 * dev-node mirror, pre-pagination nodes) — the SDK never invents pagination
 * the node didn't do. A paginated response may set `total_count: null` when
 * the node intentionally skipped an exact count; keep the page metadata so
 * `queryAll` can fail closed instead of treating the drain as clean.
 */
function parseQueryPage(body: unknown, returnedRows: number): QueryPage | null {
  if (!isObject(body)) {
    return null;
  }
  const totalCount = body['total_count'];
  const limit = body['limit'];
  const offset = body['offset'];
  const hasMore = body['has_more'];
  const nextCursor = parseKeyValue(body['next_cursor']);
  if (
    typeof limit !== 'number' ||
    typeof offset !== 'number' ||
    typeof hasMore !== 'boolean'
  ) {
    return null;
  }
  if (totalCount !== null && totalCount !== undefined && typeof totalCount !== 'number') {
    return null;
  }
  const returnedCount = body['returned_count'];
  return {
    totalCount: typeof totalCount === 'number' ? totalCount : undefined,
    returnedCount:
      typeof returnedCount === 'number' ? returnedCount : returnedRows,
    limit,
    offset,
    hasMore,
    nextCursor,
  };
}

/** Whether a value is a non-array JSON object. */
function isObject(v: unknown): v is Record<string, JsonValue> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

const U64_MAX_DECIMAL = '18446744073709551615';

function isPositiveU64Decimal(value: unknown): value is string {
  if (
    typeof value !== 'string' ||
    !/^[1-9][0-9]*$/.test(value) ||
    value.length > U64_MAX_DECIMAL.length
  ) {
    return false;
  }
  return (
    value.length < U64_MAX_DECIMAL.length || value <= U64_MAX_DECIMAL
  );
}

/** Normalize the legacy ID list and the current single-route mutation ID. */
function parseMutationIds(
  body: Record<string, JsonValue>,
  raw: unknown,
): string[] {
  if ('mutation_ids' in body) {
    const ids = body['mutation_ids'];
    if (
      Array.isArray(ids) &&
      ids.length > 0 &&
      ids.every(isNonEmptyString)
    ) {
      return ids;
    }
    throw new UnexpectedResponseError(
      'mutation returned invalid mutation_ids',
      200,
      raw,
    );
  }
  const id = body['mutation_id'];
  if (isNonEmptyString(id)) {
    return [id];
  }
  throw new UnexpectedResponseError(
    'mutation returned no mutation ID',
    200,
    raw,
  );
}

/** Preserve legacy `written`; otherwise normalize current `success: true`. */
function parseMutationWritten(
  body: Record<string, JsonValue>,
  raw: unknown,
): number {
  if ('written' in body) {
    const written = body['written'];
    if (typeof written === 'number') {
      return written;
    }
    throw new UnexpectedResponseError(
      'mutation returned an invalid written count',
      200,
      raw,
    );
  }
  if (body['success'] === true) {
    return 1;
  }
  throw new UnexpectedResponseError(
    'mutation returned no success marker',
    200,
    raw,
  );
}

/** Preserve the legacy firing count; current single-route responses omit it. */
function parseMutationFiringsObserved(
  body: Record<string, JsonValue>,
  raw: unknown,
): number {
  if (!('firings_observed' in body)) {
    return 0;
  }
  const firingsObserved = body['firings_observed'];
  if (typeof firingsObserved === 'number') {
    return firingsObserved;
  }
  throw new UnexpectedResponseError(
    'mutation returned an invalid firings_observed count',
    200,
    raw,
  );
}

function isMutationCloudCaptureState(
  value: unknown,
): value is MutationCloudCaptureState {
  return value === 'durable' || value === 'failed';
}

function isMutationCloudPublicationState(
  value: unknown,
): value is MutationCloudPublicationState {
  return (
    value === 'not_requested' ||
    value === 'pending' ||
    value === 'published' ||
    value === 'failed'
  );
}

/** Parse one durable cloud-intent receipt without weakening its guarantee. */
function parseMutationCloudCapture(raw: unknown): MutationCloudCaptureReceipt {
  if (!isObject(raw)) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud_capture receipt',
      200,
      raw,
    );
  }
  const state = raw['state'];
  const durable = raw['durable'];
  const mutationUuid = raw['mutation_uuid'];
  const error = raw['error'];
  if (
    !isMutationCloudCaptureState(state) ||
    typeof durable !== 'boolean' ||
    !isNonEmptyString(mutationUuid) ||
    (error !== null && typeof error !== 'string')
  ) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud_capture receipt',
      200,
      raw,
    );
  }
  const coherent =
    state === 'durable'
      ? durable && error === null
      : !durable && isNonEmptyString(error);
  if (!coherent) {
    throw new UnexpectedResponseError(
      'mutation returned an incoherent cloud_capture receipt',
      200,
      raw,
    );
  }
  return { state, durable, mutationUuid, error };
}

/** Parse one exact target coordinate and preserve the frontier as text. */
function parseMutationCloudPublicationTarget(
  raw: unknown,
): MutationCloudPublicationTarget {
  if (!isObject(raw)) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud publication target',
      200,
      raw,
    );
  }
  const targetId = raw['target_id'];
  const targetLabel = raw['target_label'];
  const writerId = raw['writer_id'];
  const frontier = raw['frontier'];
  if (
    !isNonEmptyString(targetId) ||
    typeof targetLabel !== 'string' ||
    !isNonEmptyString(writerId) ||
    !isPositiveU64Decimal(frontier)
  ) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud publication target',
      200,
      raw,
    );
  }
  return { targetId, targetLabel, writerId, frontier };
}

/** Parse the exact off-box receipt and map its wire names to the SDK surface. */
function parseMutationCloudPublication(
  raw: unknown,
): MutationCloudPublicationReceipt {
  if (!isObject(raw)) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud_publication receipt',
      200,
      raw,
    );
  }
  const state = raw['state'];
  const published = raw['published'];
  const mutationUuid = raw['mutation_uuid'];
  const targets = raw['targets'];
  const error = raw['error'];
  if (
    !isMutationCloudPublicationState(state) ||
    typeof published !== 'boolean' ||
    !isNonEmptyString(mutationUuid) ||
    !Array.isArray(targets) ||
    (error !== null && typeof error !== 'string')
  ) {
    throw new UnexpectedResponseError(
      'mutation returned an invalid cloud_publication receipt',
      200,
      raw,
    );
  }
  const coherent =
    (state === 'published' && published && error === null) ||
    (state === 'failed' && !published && isNonEmptyString(error)) ||
    ((state === 'pending' || state === 'not_requested') &&
      !published &&
      error === null);
  if (!coherent) {
    throw new UnexpectedResponseError(
      'mutation returned an incoherent cloud_publication receipt',
      200,
      raw,
    );
  }
  return {
    state,
    published,
    mutationUuid,
    targets: targets.map(parseMutationCloudPublicationTarget),
    error,
  };
}

/**
 * Normalize one raw query row into a {@link QueryRow}. An enveloped row
 * (`{key, fields, metadata, author_pub_key}`) is surfaced faithfully; a bare
 * field-map row is wrapped (its fields are the whole object; key empty,
 * metadata null, author null).
 *
 * The row `key` arrives in one of two node dialects: production
 * `fold_db_node` sends the structured `KeyValue` object (`{hash, range}`),
 * the dev-node mirror sends a pre-rendered string. A structured key is
 * preserved verbatim as `keyValue` (the unambiguous row address) AND rendered
 * to the `key` string the same way fold_db's `KeyValue::Display` does; a
 * string key is kept verbatim with `keyValue: null`. Coercing a structured
 * key to `''` (the pre-fix behavior) lost row addressing and dedup entirely.
 */
function parseQueryRow(raw: unknown): QueryRow {
  if (isObject(raw) && 'fields' in raw && isObject(raw.fields)) {
    const author = raw['author_pub_key'];
    const { key, keyValue } = parseRowKey(raw['key']);
    return {
      key,
      keyValue,
      fields: raw.fields as RowFields,
      metadata: 'metadata' in raw ? (raw['metadata'] as JsonValue) : null,
      authorPubKey: typeof author === 'string' && author.length > 0 ? author : null,
    };
  }
  // Bare field-map row (no envelope): the object *is* the fields.
  return {
    key: '',
    keyValue: null,
    fields: isObject(raw) ? (raw as RowFields) : {},
    metadata: null,
    authorPubKey: null,
  };
}

/**
 * Parse a row's raw `key` into the (rendered string, structured KeyValue)
 * pair. A string is the dev-node dialect — kept verbatim, no structure to
 * recover (the rendered form is ambiguous, so the SDK never reverse-parses
 * it). A `{hash, range}` object is production's `KeyValue` — preserved
 * structurally and rendered like fold_db's `KeyValue::Display`
 * (`"hash:range"` / `"hash"` / `"range"` / `""`). Anything else (absent,
 * null, unrecognized) yields the empty key.
 */
function parseRowKey(rawKey: unknown): { key: string; keyValue: KeyValue | null } {
  const keyValue = parseKeyValue(rawKey);
  if (keyValue !== null) {
    const key =
      keyValue.hash !== null && keyValue.range !== null
        ? `${keyValue.hash}:${keyValue.range}`
        : (keyValue.hash ?? keyValue.range ?? '');
    return { key, keyValue };
  }
  if (typeof rawKey === 'string') {
    return { key: rawKey, keyValue: null };
  }
  return { key: '', keyValue: null };
}

function parseKeyValue(rawKey: unknown): KeyValue | null {
  if (!isObject(rawKey)) {
    return null;
  }
  const hash = typeof rawKey['hash'] === 'string' ? (rawKey['hash'] as string) : null;
  const range = typeof rawKey['range'] === 'string' ? (rawKey['range'] as string) : null;
  return hash !== null || range !== null ? { hash, range } : null;
}

/** Parse a successful `POST /api/app/changes` cursor page. */
export function parseChangesResponse(body: unknown): ChangesResult {
  if (!isObject(body)) {
    throw new UnexpectedResponseError('changes returned a non-object body', 200, body);
  }
  const nextCursor = body['next_cursor'];
  const hasMore = body['has_more'];
  const gap = body['gap'];
  const rawChanges = body['changes'];
  if (typeof nextCursor !== 'string' || typeof hasMore !== 'boolean' || typeof gap !== 'boolean' || !Array.isArray(rawChanges)) {
    throw new UnexpectedResponseError('changes returned an invalid cursor page', 200, body);
  }
  const changes = rawChanges.map((raw) => {
    if (!isObject(raw) || !isObject(raw['key'])) {
      throw new UnexpectedResponseError('changes returned an invalid event', 200, raw);
    }
    const keyRaw = raw['key'];
    const cursor = raw['cursor'];
    const mutationId = raw['mutation_id'];
    const schemaName = raw['schema_name'];
    const operation = raw['operation'];
    const committedAtMs = raw['committed_at_ms'];
    const backgroundTasksDrained = raw['background_tasks_drained'];
    const convergencePending = raw['convergence_pending'];
    if (
      typeof cursor !== 'string' ||
      typeof mutationId !== 'string' ||
      typeof schemaName !== 'string' ||
      typeof operation !== 'string' ||
      typeof committedAtMs !== 'number' ||
      typeof backgroundTasksDrained !== 'boolean' ||
      typeof convergencePending !== 'boolean'
    ) {
      throw new UnexpectedResponseError('changes returned an invalid event', 200, raw);
    }
    return {
      cursor,
      mutationId,
      schemaName,
      key: {
        hash: typeof keyRaw['hash'] === 'string' ? keyRaw['hash'] : null,
        range: typeof keyRaw['range'] === 'string' ? keyRaw['range'] : null,
      },
      operation,
      committedAtMs,
      backgroundTasksDrained,
      convergencePending,
    };
  });
  return { changes, nextCursor, hasMore, gap };
}

/**
 * Parse a `200` `/api/app/search` body into a {@link SearchResult}.
 *
 * The node wraps the hits in its standard envelope: `{ ok, results: [...],
 * user_hash }` (the `ApiResponse<IndexSearchResponse>` shape). Each element of
 * `results` is a full `/api/query` row envelope (`key`, `fields`, `metadata`,
 * `author_pub_key`) with three search-only fields the node adds per hit:
 * `schema_name`, `schema_display_name`, and `score`. Hits arrive in the node's
 * relevance order and are surfaced verbatim — the SDK preserves it.
 *
 * The row part is parsed by the SAME {@link parseQueryRow} used for `query()`,
 * so the envelope handling (enveloped vs bare row, null author) is identical.
 */
export function parseSearchResponse(body: unknown): SearchResult {
  const b = (body ?? {}) as { results?: unknown };
  const rawHits: unknown[] = Array.isArray(b.results) ? b.results : [];
  const hits = rawHits.map(parseSearchHit);
  return { hits };
}

/** Parse `GET /api/system/auto-identity` success JSON. */
/** The answer for a node that predates `GET /api/version`. */
const PRE_HANDSHAKE_NODE: NodeVersion = Object.freeze({
  apiVersion: 0,
  build: null,
  capabilities: {},
  instanceId: null,
  handshake: false,
});

/**
 * Parse a `GET /api/version` 200 body. Tolerant of a node that omits a
 * field: a missing `api_version` reads as `0`, the same as a 404, because a
 * handshake that cannot state its version has not stated compatibility.
 */
export function parseNodeVersion(body: unknown): NodeVersion {
  const b = (body ?? {}) as {
    api_version?: unknown;
    build?: unknown;
    capabilities?: unknown;
    instance_id?: unknown;
  };
  const apiVersion =
    typeof b.api_version === 'number' && Number.isInteger(b.api_version) && b.api_version >= 0
      ? b.api_version
      : 0;
  const capabilities: Record<string, boolean> = {};
  if (b.capabilities && typeof b.capabilities === 'object') {
    for (const [k, v] of Object.entries(b.capabilities as Record<string, unknown>)) {
      if (typeof v === 'boolean') {
        capabilities[k] = v;
      }
    }
  }
  return {
    apiVersion,
    build: typeof b.build === 'string' && b.build.length > 0 ? b.build : null,
    capabilities,
    instanceId: typeof b.instance_id === 'string' && b.instance_id.length > 0 ? b.instance_id : null,
    handshake: true,
  };
}

export function parseAutoIdentityResponse(body: unknown): AutoIdentityResult {
  const b = isObject(body) ? body : {};
  const userHash = b['user_hash'];
  const publicKey = b['public_key'];
  const userId = b['user_id'];
  if (typeof userHash !== 'string' || userHash.length === 0) {
    throw new UnexpectedResponseError(
      'auto-identity returned a provisioned body without user_hash',
      200,
      body,
    );
  }
  return {
    provisioned: true,
    userHash,
    publicKey: typeof publicKey === 'string' ? publicKey : null,
    userId: typeof userId === 'string' ? userId : null,
  };
}

/** Parse `GET /api/schemas` JSON into normalized loaded-schema entries. */
export function parseSchemaListResponse(body: unknown): LoadedSchema[] {
  const b = isObject(body) ? body : {};
  const rawSchemas = Array.isArray(b['schemas']) ? b['schemas'] : [];
  return rawSchemas
    .filter((schema): schema is Record<string, JsonValue> => isObject(schema))
    .map(parseLoadedSchema);
}

/** Resolve a schema descriptor against an already-fetched schema list. */
export function resolveLoadedSchema(
  schemas: readonly LoadedSchema[],
  descriptor: SchemaDescriptor,
): LoadedSchema | null {
  const matches = schemas.filter((schema) => {
    if (schema.ownerAppId !== descriptor.ownerAppId) {
      return false;
    }
    if (schema.descriptiveName !== descriptor.descriptiveName) {
      return false;
    }
    if (descriptor.fields !== undefined) {
      return sameFieldSet(schema.fields, descriptor.fields);
    }
    return true;
  });
  if (matches.length === 0) {
    return null;
  }
  if (matches.length > 1) {
    throw new UnexpectedResponseError(
      `schema descriptor matched ${matches.length} loaded schemas`,
      200,
      matches,
    );
  }
  return matches[0]!;
}

function parseLoadedSchema(schema: Record<string, JsonValue>): LoadedSchema {
  const name = stringOrNull(schema['name']) ?? '';
  const identityHash = stringOrNull(schema['identity_hash']) ?? name;
  return {
    name,
    identityHash,
    descriptiveName: stringOrNull(schema['descriptive_name']),
    ownerAppId: stringOrNull(schema['owner_app_id']),
    fields: stringArray(schema['fields']),
  };
}

function sameFieldSet(
  actual: readonly string[],
  expected: readonly string[],
): boolean {
  if (actual.length !== expected.length) {
    return false;
  }
  const actualSorted = [...actual].sort();
  const expectedSorted = [...expected].sort();
  return actualSorted.every((field, i) => field === expectedSorted[i]);
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((field): field is string => typeof field === 'string')
    : [];
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

/**
 * Normalize one raw search hit. The row envelope (`key`/`fields`/`metadata`/
 * `authorPubKey`) is parsed by {@link parseQueryRow}; the search-only
 * attribution fields (`score`, `schema_name`, `schema_display_name`) are read
 * off the same object. `score` is `null` when the node reported none.
 */
function parseSearchHit(raw: unknown): SearchHit {
  const row = parseQueryRow(raw);
  const obj = isObject(raw) ? raw : {};
  const score = obj['score'];
  const schemaName = obj['schema_name'];
  const schemaDisplayName = obj['schema_display_name'];
  return {
    ...row,
    score: typeof score === 'number' ? score : null,
    schemaName: typeof schemaName === 'string' ? schemaName : '',
    schemaDisplayName:
      typeof schemaDisplayName === 'string' ? schemaDisplayName : null,
  };
}

/**
 * Read the optional detail fields off a discriminated capability-403 body
 * (`capability_id` / `schema` / `timestamp_skew_secs`, per the
 * `CapabilityDenial` variant payloads). Unknown fields are ignored; absent
 * ones stay absent.
 */
function capabilityDetailOf(body: unknown): CapabilityDenialDetail {
  const detail: CapabilityDenialDetail = {};
  if (!isObject(body)) {
    return detail;
  }
  if (typeof body['capability_id'] === 'string') {
    detail.capabilityId = body['capability_id'];
  }
  if (typeof body['schema'] === 'string') {
    detail.schema = body['schema'];
  }
  if (typeof body['timestamp_skew_secs'] === 'number') {
    detail.timestampSkewSecs = body['timestamp_skew_secs'];
  }
  return detail;
}

/**
 * Read the modeled detail fields off a `409 {error:"cas_conflict"}` body
 * (`schema` / `field` / `key` / `expected` / `actual` / `message`, per the
 * node's CAS-conflict contract). Unknown fields are ignored; absent ones stay
 * absent. `actual` is passed through as-is (the node may send an explicit
 * `null` when the field had no current value).
 */
function casConflictDetailOf(body: unknown): CasConflictDetail {
  const detail: CasConflictDetail = {};
  if (!isObject(body)) {
    return detail;
  }
  if (typeof body['schema'] === 'string') {
    detail.schema = body['schema'];
  }
  if (typeof body['field'] === 'string') {
    detail.field = body['field'];
  }
  if (typeof body['key'] === 'string') {
    detail.key = body['key'];
  }
  if (typeof body['expected'] === 'string') {
    detail.expected = body['expected'];
  }
  if (typeof body['actual'] === 'string' || body['actual'] === null) {
    detail.actual = body['actual'] as string | null;
  }
  if (typeof body['message'] === 'string') {
    detail.message = body['message'];
  }
  return detail;
}

/** Promise-based sleep (used between consent polls). */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A 404/405 the transport threw as a `protocol` error: the node lacks the route. */
function isMissingRouteError(error: unknown): boolean {
  return (
    error instanceof TransportError &&
    error.kind === 'protocol' &&
    (error.status === 404 || error.status === 405)
  );
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/** One query of a batch, with its position in the caller's list. */
interface BatchEntry {
  index: number;
  item: QueryBatchItem;
  resolved: ResolvedSchema;
}

/**
 * Pair each sent query with its `{status, response}` entry from the `200` body
 * of `/api/queries/batch`. A reply of the wrong shape or length is a node
 * contract break, not an item failure, so it throws.
 */
function pairBatchReplies(
  chunk: readonly BatchEntry[],
  body: unknown,
): Array<[BatchEntry, { status: number; response: unknown }]> {
  const results = (body as { results?: unknown } | null)?.results;
  const malformed = (): UnexpectedResponseError =>
    new UnexpectedResponseError(
      `query batch returned a malformed reply (expected ${chunk.length} results)`,
      200,
      body,
    );
  if (!Array.isArray(results) || results.length !== chunk.length) {
    throw malformed();
  }
  const pairs: Array<[BatchEntry, { status: number; response: unknown }]> = [];
  for (const [position, entry] of chunk.entries()) {
    const reply = results[position] as { status?: unknown; response?: unknown } | null;
    if (typeof reply !== 'object' || reply === null || typeof reply.status !== 'number') {
      throw malformed();
    }
    pairs.push([entry, { status: reply.status, response: reply.response }]);
  }
  return pairs;
}
