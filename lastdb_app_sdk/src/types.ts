/**
 * Wire types for the LastDB runtime `/api/*` surface.
 *
 * These mirror the JSON the node accepts and returns — confirmed against the
 * handlers on `origin/main`. We do NOT invent fields the node would reject;
 * unknown JSON is carried as `Record<string, JsonValue>` rather than typed.
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

/**
 * A result row — fields keyed by name. Kept as the flattened convenience
 * shape (`QueryRow.fields` is the same data); `query()` also returns the full
 * {@link QueryRow} envelope so the row `key`, `authorPubKey`, and `metadata`
 * are reachable.
 */
export type Row = RowFields;

/**
 * A fold_db row key: a hash component, a range component, or both. This is
 * the node's `KeyValue` wire shape — production `/api/query` returns each
 * row's `key` as this object, and `POST /api/mutation` takes one back as
 * `key_value`. For a Range schema: `{ hash: null, range: "<id>" }`; for a
 * hash-keyed schema: `{ hash: "<id>", range: null }`.
 */
export interface KeyValue {
  hash: string | null;
  range: string | null;
}

/** Options for the keys-only {@link LastDbClient.list} membership page. */
export interface ListOptions {
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
 *
 * The pagination names intentionally match Mini's public list envelope. The
 * cursor is opaque: pass it back unchanged via {@link ListOptions.cursor}.
 */
export interface ListResult {
  schema: string;
  keys: ListRecordKey[];
  next_cursor: string | null;
  has_more: boolean;
  /** Same honesty signal as `has_more`; one page is not a census. */
  truncated: boolean;
}

/** Mutation convergence mode for `POST /api/mutation`. */
export type MutationConvergence = 'sync' | 'async';

/** Local persistence policy for `POST /api/mutation`. */
export type MutationDurability = 'queued' | 'durable';

/** Exact off-box publication policy for one durable delete. */
export type MutationCloudPublication = 'wait';

/**
 * The full per-row envelope the node returns for `/api/query`. The node's
 * `results` array carries one object per result key shaped
 * `{ key, fields, metadata, author_pub_key }` — see
 * `fold_db_node` `execute_query_json_internal` (the production
 * `/api/query` row builder) and `fold_db_node::dev_mode` `app_endpoints::api_query`.
 *
 * A real app needs `key` to update/delete a specific row, and typically
 * `authorPubKey` + `metadata` for provenance — flattening to bare `fields`
 * (the pre-gap-#3 behavior) threw these away.
 */
export interface QueryRow {
  /**
   * The row's storage key rendered to a string. Production `fold_db_node`
   * returns `key` as a structured `{hash, range}` object; the SDK renders it
   * the same way fold_db's `KeyValue::Display` does (`"hash:range"`,
   * `"hash"`, or `"range"`). A dev node (`fold_db_node::dev_mode`) already sends the
   * rendered string, which is kept verbatim. NOTE the rendered form is
   * ambiguous for range values containing `:` — for row addressing prefer
   * the structured {@link QueryRow.keyValue}.
   */
  key: string;
  /**
   * The row's STRUCTURED storage key, when the node sent one (production
   * `/api/query` returns `key` as a `{hash, range}` object). Pass it back
   * verbatim as `MutationOp.key` to update or delete this exact row — unlike
   * the rendered `key` string, it is never ambiguous. `null` when the node
   * sent a pre-rendered string key (the dev-node mirror) or no key at all
   * (bare field-map rows).
   */
  keyValue: KeyValue | null;
  /** The row's field values, keyed by field name. */
  fields: RowFields;
  /**
   * Per-field write metadata as the node stores it (writer pubkey, timestamps,
   * etc.), keyed by field name. Shape is node-defined; left as opaque JSON so
   * the SDK never invents fields. `null`/absent when the node carries none.
   */
  metadata: JsonValue;
  /**
   * The pubkey that authored the row (the first non-empty `writer_pubkey`
   * across the row's field metadata), or `null` when the node reports none.
   */
  authorPubKey: string | null;
}

// ---------------------------------------------------------------------------
// connect()
// ---------------------------------------------------------------------------

/**
 * Connection options. Provide exactly ONE of `baseUrl` (HTTP transport) or
 * `socketPath` (Unix-domain-socket transport) — the transport is chosen by
 * which one is present.
 */
export interface ConnectOptions {
  /** Canonical app id this client acts as (e.g. `"fbrain"`). */
  appId: string;
  /** HTTP base URL of the node, e.g. `http://127.0.0.1:9101`. */
  baseUrl?: string;
  /** Path to the node's Unix-domain control socket. */
  socketPath?: string;
  /**
   * Socket-first discovery for the `baseUrl` (local-node) case. Default
   * `true`: `connect` prefers the node's Unix-domain data-plane socket and
   * falls back to `baseUrl`'s TCP listener only when no socket file is found —
   * the discovery order the Rust client uses, with the brand-forward
   * `LASTDB_SOCKET_PATH` preferred (`LASTDB_SOCKET_PATH` → `FOLDDB_SOCKET_PATH`
   * legacy alias → `FOLDDB_SOCK` legacy alias → `<data_dir>/folddb.sock` →
   * TCP). Set `false`
   * to force the TCP listener (e.g. a remote/non-local node). Ignored when
   * `socketPath` is given (an explicit socket is always used verbatim).
   */
  discoverSocket?: boolean;
  /**
   * Headers attached to EVERY request this client sends (consent flow + data
   * path), under a per-call header of the same name (which wins). The
   * production `fold_db_node` HTTP server is stateless — it resolves the
   * calling user from an `X-User-Hash` header on every request, returning
   * `401 MISSING_USER_CONTEXT` when it is absent — so an app talking to a
   * production node passes `{ 'X-User-Hash': '<hash>' }` here. A dev node
   * (`fold_db_node::dev_mode`) ignores it (TCP callers run as the node owner), so it is
   * safe to set unconditionally.
   */
  defaultHeaders?: Record<string, string>;
  /**
   * Explicit multi-DB handle (`lastdb://personal`, `lastdb://org/…`, or
   * 64-hex). Defaults to `process.env.LASTDB_DB` then personal. Sent on every
   * request as `X-LastDB-Db` so Mini scopes mutate/query (org cohabitation).
   * Prefer letting `org <app>` inject `LASTDB_DB` rather than hard-coding.
   */
  db?: string;
  /**
   * Best-effort ops label for Mini request telemetry (`X-LastDB-Client`).
   * Not a security boundary — any process can claim any string. Defaults to
   * {@link ConnectOptions.appId} when unset. Pass explicitly to override
   * (e.g. CLI vs library package name).
   */
  clientId?: string;
  /**
   * Per-request transport timeout in milliseconds for consent and data-plane
   * HTTP calls. Defaults to 30_000 so a wedged node cannot leave app requests
   * pending forever. An exact cloud-publication mutation raises this to at
   * least 150_000 without shortening a larger value. Set a larger value when
   * the node's handler-timeout override makes its exact route exceed 150s.
   */
  timeoutMs?: number;
  /**
   * Capability store used by `storeCapability` / `loadCapability` and by an
   * auto-load on `connect`. Defaults to an OS-keychain store with a file
   * fallback (see `capabilityStore.ts`). Entries are keyed by (appId, node),
   * so a capability minted by one node is never replayed against another.
   */
  capabilityStore?: import('./capabilityStore.js').CapabilityStore;
  /**
   * macOS Keychain service label the default store keeps capabilities under
   * (default `com.folddb.app-sdk.capability`). Ignored when a custom
   * `capabilityStore` is supplied. Lets two apps / environments keep their
   * keychain namespaces apart without writing a custom store.
   */
  keychainService?: string;
  /**
   * Pre-loaded capability (base64 CapabilityToken). When omitted, `connect`
   * attempts to load one for `appId` from the capability store; if none is
   * found the client starts un-capable (query/mutate will run owner-context
   * on a dev node, or be refused on an enforcing production node until
   * `awaitConsent` provides one).
   */
  capability?: string;
  /**
   * Verify capability blobs client-side (gap #4). When `true`, the SDK runs
   * decode + audience binding (`token.app_id === appId`) + the RFC 8785 JCS
   * integrity binding (`envelope.payload_hash ==
   * sha256(JCS(token-minus-envelope))`, byte-identical to the node's Rust
   * canonicalizer) at every point a capability is adopted:
   *
   * - a granted token (`awaitConsent`) that fails is REJECTED with
   *   `CapabilityVerificationError` (never stored);
   * - a cached token (`connect` auto-load / `loadCapability`) that fails is
   *   DISCARDED and treated as absent (never replayed into a guaranteed 403);
   * - an inline `capability` that fails makes `connect` throw.
   *
   * Default `false` (verbatim token-carrier behavior, e.g. for a dev node's
   * `app trust` override or a test double whose "capability" is an opaque
   * string). Recommended `true` against a production node.
   */
  verifyCapability?: boolean;
  /**
   * Optional app-facing schema resolver for the data path. When present,
   * `query` / `queryAll` / `mutate` call it with the app's schema name before
   * hitting the node. Prefer returning `ResolveResult { identity, schema,
   * adapter, outcome }` from Schema Service resolution: writes are rewritten
   * through `adapter.appToCatalog` before storage, and reads are mapped back
   * through `adapter.catalogToApp` (or the reverse one-to-one map). Omitted
   * schemas and fields pass through unchanged.
   */
  schemaResolver?: SchemaResolver;
  /**
   * The node request-grammar version this app needs (see
   * `LASTDB_SDK_API_VERSION` for the one the SDK was written against). When
   * set, `connect` reads `GET /api/version` first and throws
   * `NodeTooOldError` — one line that names the fix — when the node reports
   * a lower number, or `0` because it predates the route. `0` (or unset)
   * declares no requirement and skips the read. Declare it in the app's
   * manifest and pass it through; raise it only when the app starts sending
   * a key an older node would refuse.
   */
  requireApiVersion?: number;
  /**
   * Who is asking, for the `NodeTooOldError` message (e.g. `brain 0.8.1`).
   * Defaults to `appId`.
   */
  appLabel?: string;
}

/**
 * The node's answer to `GET /api/version`, the client↔node compatibility
 * handshake. `handshake` is `false` when the node predates the route (404):
 * every other field then carries its "unknown" value and `apiVersion` is `0`.
 */
export interface NodeVersion {
  /** The request-grammar version the node speaks (`0` = predates the route). */
  apiVersion: number;
  /** The node's baked build string (`null` when unknown). */
  build: string | null;
  /** Capability flags the node advertises (same map as `/health`). */
  capabilities: Record<string, boolean>;
  /** Process-lifetime instance id (`null` when unknown). */
  instanceId: string | null;
  /** Whether the node answered the route at all. */
  handshake: boolean;
}

// ---------------------------------------------------------------------------
// Consent flow — request / poll bodies
// ---------------------------------------------------------------------------

/**
 * Consent scope. `"wildcard"` requests `{appId}/*` (one prompt covers the
 * app's whole lifecycle); `{ explicit: [...] }` requests named schemas only.
 * Serialized to the node's `scope` string (`"wildcard"` or
 * `"explicit:a,b"`).
 */
export type ConsentScope = 'wildcard' | { explicit: string[] };

/** `POST /api/apps/request-consent` success body (`202`). */
export interface RequestConsentResult {
  requestId: string;
  /** RFC 3339 timestamp when a still-pending request expires. */
  expiresAt: string;
}

// ---------------------------------------------------------------------------
// Query / mutation
// ---------------------------------------------------------------------------

/**
 * Optional query filter. The node's `/api/query` accepts a `fold_db` `Query`
 * — `{schema_name, fields}` plus an optional range filter — alongside
 * top-level `limit`/`offset` pagination fields. Production nodes also accept a
 * top-level `cursor` (`KeyValue`), but ONLY the unfiltered push-down consumes
 * one — see {@link QueryFilter.cursor}. Offset is the paging mechanism for
 * every key-restricted (`HashKey`/`HashRange*`) read, which is every product
 * read; `queryAll` follows `page.nextCursor` when the node returns one and
 * otherwise advances by offset.
 *
 * Pagination: production `fold_db_node` applies `limit` (default 100 —
 * `DEFAULT_QUERY_LIMIT` — clamped to `MAX_QUERY_LIMIT` 1000) and `offset`
 * after fold_db returns, and reports `total_count`/`has_more` so truncation
 * is detectable (surfaced as {@link QueryResult.page}). Even when you pass NO
 * limit the node still caps the page at its default — use
 * `LastDbClient.queryAll` to drain a >100-row schema. The dev node
 * (`fold_db_node::dev_mode`) implements the SAME pagination (default 100, clamp 1000,
 * `total_count`/`has_more` metadata), so `limit`/`offset` and the page
 * metadata behave identically against both.
 */
export interface QueryFilter {
  /**
   * Restrict to these field names. When omitted, the SDK requests every
   * field the schema declares (it cannot know them ahead of time, so a
   * `fields` list is effectively required for a Range schema read — see
   * README). Most callers pass the fields they want back.
   */
  fields?: string[];
  /**
   * A `fold_db` range filter object, passed through verbatim under the
   * query's `filter` key for Range schemas. Shape is node-defined; left
   * opaque so the SDK never invents a filter dialect the node rejects.
   */
  filter?: JsonValue;
  /**
   * Page size. Forwarded verbatim as the request's top-level `limit`; both
   * the production and dev nodes clamp it to `MAX_QUERY_LIMIT` (1000) and
   * default it to `DEFAULT_QUERY_LIMIT` (100) when omitted.
   */
  limit?: number;
  /**
   * Page offset (rows to skip), applied by the node after fold_db returns.
   * Forwarded verbatim as the request's top-level `offset`; defaults to 0 on
   * both node kinds when omitted.
   */
  offset?: number;
  /**
   * Keyset cursor returned by a prior page's `page.nextCursor`. Forwarded
   * verbatim as top-level `cursor`.
   *
   * Only send one the node actually handed you. The node consumes a cursor on
   * a single path — the unfiltered `can_push_down` push-down — and a
   * key-restricted read (`HashKey`/`HashRange*`) carries a filter, so it never
   * reaches that branch. Sending a cursor on a filtered read is silently
   * ignored: the node re-serves the same page, `has_more` never flips, and a
   * client that paged by cursor instead of offset would not terminate. The
   * node therefore returns `next_cursor: null` on every offset-paged shape, so
   * following `page.nextCursor` is safe by construction.
   */
  cursor?: KeyValue;
  /**
   * Two-pass field predicates, forwarded as the request's top-level `where`
   * (`fold_db` `Query.field_predicates`). Multiple predicates are AND'd.
   *
   * The node loads ONLY the predicate fields first, then loads the requested
   * projection for the keys that matched — so a predicate that rejects most of
   * a partition keeps the fat projection off the wire entirely. This is the
   * difference between filtering server-side and draining a partition to
   * `Array.prototype.filter` it in the app.
   *
   * Predicate `field` names are app-facing and are mapped to node-side names
   * exactly like {@link QueryFilter.fields}.
   *
   * A `where` is a filter, NOT an index: the node still walks the candidate
   * keys the `filter` selects. Keep the key filter as narrow as possible and
   * use `where` to trim within it.
   */
  where?: QueryFieldPredicate[];
}

/**
 * One two-pass field predicate (`fold_db` `FieldPredicate`). Externally tagged:
 * exactly one variant key per object, e.g.
 * `{ eq: { field: 'state', value: 'available' } }`.
 */
export type QueryFieldPredicate =
  /** field value == value */
  | { eq: { field: string; value: JsonValue } }
  /** field value is one of values */
  | { in: { field: string; values: JsonValue[] } }
  /** field timestamp >= instant (RFC3339 string, or Unix seconds/millis) */
  | { after: { field: string; instant: JsonValue } }
  /** field timestamp <= instant (RFC3339 string, or Unix seconds/millis) */
  | { before: { field: string; instant: JsonValue } }
  /** field exists and is not null */
  | { present: { field: string } }
  /** field is missing or null */
  | { absent: { field: string } };

// ---------------------------------------------------------------------------
// Schema name / field mapping
// ---------------------------------------------------------------------------

/** Mapping for one app-facing schema name on the query/mutate data path. */
export interface SchemaMapping {
  /**
   * Schema id/name the node expects. Omit to use the app-facing name
   * unchanged (the name==id case).
   */
  nodeSchemaName?: string;
  /**
   * App field name -> node field name. Missing fields pass through unchanged.
   * The map must be one-to-one if the caller wants read rows and metadata to
   * reverse cleanly back to app field names.
   */
  fields?: Record<string, string>;
}

/**
 * Edge adapter for one app-facing schema. `appToCatalog` rewrites caller field
 * names into the catalog/global names stored by the node; `catalogToApp`
 * optionally rewrites rows back for caller convenience. When `catalogToApp` is
 * omitted, the SDK derives it from the one-to-one `appToCatalog` map.
 */
export interface SchemaAdapter {
  appToCatalog?: Record<string, string>;
  catalogToApp?: Record<string, string>;
}

/**
 * Schema Service-style resolver result for the data path. `identity` is the
 * global schema identity the app resolved to; `schema` is the node-facing
 * runtime schema name/id when it differs from `identity`. The SDK never stores
 * app-local dialect fields: query and mutation requests are rewritten through
 * `adapter.appToCatalog` before they reach the node.
 */
export interface ResolveResult {
  identity: string;
  schema?: string;
  adapter?: SchemaAdapter;
  outcome?: string;
}

/**
 * Resolve an app-facing schema name to the node-facing data-path contract.
 * Returning `undefined` / `null` means pass-through; returning a string is a
 * shorthand for `{ nodeSchemaName: string }`. New callers should prefer
 * `ResolveResult { identity, schema, adapter, outcome }`, which makes the
 * global identity and edge adapter explicit.
 */
export type SchemaResolverResult =
  | string
  | SchemaMapping
  | ResolveResult
  | null
  | undefined;

export type SchemaResolver = (
  appSchemaName: string,
) => SchemaResolverResult | Promise<SchemaResolverResult>;

/**
 * Pagination metadata the node's `/api/query` returns alongside its
 * `results`/`rows` page, surfaced verbatim (snake_case → camelCase). Both
 * production `fold_db_node` and the dev node (`fold_db_node::dev_mode`) return it. See
 * `fold_db_node/src/handlers/query.rs::QueryResponse` and
 * `fold_db_node::dev_mode` `app_endpoints::QueryResponse`.
 */
export interface QueryPage {
  /**
   * Total records matching the query before `offset`/`limit` were applied,
   * when the node ran the count. `undefined` means the node intentionally
   * reported pagination state but declined to compute the exact count.
   * Capped at the node's internal fetch cap (10k) for unfiltered queries —
   * when `hasMore` is true and `totalCount` equals that cap there may be
   * more records the node did not load.
   */
  totalCount?: number;
  /** Number of records actually returned in this page (`rows.length`). */
  returnedCount: number;
  /** Page size the node applied (its default when the request sent none). */
  limit: number;
  /** Page offset the node applied (0 when the request sent none). */
  offset: number;
  /** True when more records exist beyond the returned page. */
  hasMore: boolean;
  /** Cursor to pass as `QueryFilter.cursor` for the next page, when available. */
  nextCursor: KeyValue | null;
}

/** `POST /api/query` success body. */
export interface QueryResult {
  schema: string;
  rowCount: number;
  /**
   * The result rows as full envelopes (`key` + `fields` + `metadata` +
   * `authorPubKey`). Use `rows[i].key` to address a specific row for a
   * follow-up update/delete; `rows[i].fields` is the flattened field map.
   */
  rows: QueryRow[];
  /**
   * Pagination metadata, when the node reported it (production `fold_db_node`
   * and the dev node `fold_db_node::dev_mode` both always do; only an older
   * pre-pagination node omits it — then `null`). When present, `page.hasMore`
   * is the truncation signal: a plain `query()` against a >100-row schema
   * returns only the node's default page.
   */
  page: QueryPage | null;
}

/** Options for `LastDbClient.queryAll` — the auto-paginating query helper. */
export interface QueryAllOptions {
  /**
   * Page size per request (the `limit` sent on each page). Defaults to 100,
   * production's `DEFAULT_QUERY_LIMIT`; production clamps anything above
   * `MAX_QUERY_LIMIT` (1000), which would wedge the offset arithmetic, so
   * values above 1000 are rejected client-side.
   */
  pageSize?: number;
  /**
   * Safety ceiling on the total rows fetched across pages (default 100_000).
   * `queryAll` stops fetching once reached and returns what it has — it never
   * loops unbounded against a pathological node.
   */
  maxRows?: number;
  /**
   * Required opt-in to drain an **unfiltered** schema (no `filter.filter`
   * key/range restriction) — a full "scan" in LastDB's DynamoDB-style access
   * model (`brain design-lastdb-scan-deprecation-path`). Unfiltered scans are
   * deprecated for product apps: they are the dominant cause of node load
   * under `lastdb ops`. Point reads (`filter: { HashKey: id }`) and partition
   * reads (HashRange) never require this flag. Defaults to `false`; a
   * `queryAll` call with no `filter.filter` and `allowFullScan` unset throws
   * {@link FullScanNotAllowedError} instead of draining the whole schema.
   */
  allowFullScan?: boolean;
}

// ---------------------------------------------------------------------------
// Scoped search (operation 5)
// ---------------------------------------------------------------------------

/**
 * Options for {@link LastDbClient.search} — the node-authoritative scoped
 * search (`POST /api/app/search`, `folddb_app_api.md` operation 5).
 *
 * Deliberately MINIMAL. The app does NOT pass a schema allowlist and cannot
 * widen its scope: the node derives the access-scope set `S(A)` itself from the
 * capability's verified `app_id` against its own grant ledger. The single
 * optional `target` is **intersected** with `S(A)` (never unioned), so naming a
 * schema the app isn't granted simply yields no hits from it — the SDK exposes
 * no way to imply the app controls scope.
 */
export interface SearchOptions {
  /**
   * Number of top hits to return. The node defaults it (currently 20) and
   * clamps it to its maximum (currently 100) when omitted/over the cap; the SDK
   * forwards it verbatim and does not impose its own default.
   */
  k?: number;
  /**
   * OPTIONAL single schema to narrow the search to. The node **intersects** it
   * with the app's access-scope `S(A)` — it can only ever shrink the result
   * set, never widen it. A `target` outside `S(A)` yields zero hits and a
   * normal `200` (no existence-confirming error). This is NOT an allowlist:
   * the app names at most one schema to narrow to, and scope remains
   * node-authoritative.
   */
  target?: string;
}

/**
 * One ranked hit from {@link LastDbClient.search}. It is a full
 * {@link QueryRow} envelope (`key`, `fields`, `metadata`, `authorPubKey` — the
 * same shape `query()` returns) plus the search-only attribution fields the
 * node adds to each hit:
 *
 * - `score` — the relevance score the node attached (cosine similarity), or
 *   `null` when the node reported none.
 * - `schemaName` — the schema the hit came from (always within the app's
 *   `S(A)`, since forbidden schemas never enter the ranking).
 * - `schemaDisplayName` — the schema's human-readable name, or `null`.
 */
export interface SearchHit extends QueryRow {
  /** Relevance score the node attached (cosine), or `null` when none. */
  score: number | null;
  /** The schema this hit came from (always within the app's access scope). */
  schemaName: string;
  /** The schema's display name, or `null` when the node reported none. */
  schemaDisplayName: string | null;
}

/**
 * `POST /api/app/search` success body, parsed. The hits are returned in the
 * node's relevance order (highest `score` first).
 *
 * **Scope is node-authoritative.** This result contains hits ONLY from schemas
 * the app has been granted (its `S(A)`): the node ranks over that subset by
 * traversal, so the app cannot observe — or infer the existence of — content
 * outside its scope. The app does not (and cannot) widen this.
 */
export interface SearchResult {
  /** Ranked hits, each a full row envelope plus `score` + schema attribution. */
  hits: SearchHit[];
}

// ---------------------------------------------------------------------------
// Owner / host node helpers
// ---------------------------------------------------------------------------

/**
 * One schema entry from `GET /api/schemas`, normalized from the node's
 * flattened `SchemaWithState` JSON. This endpoint is owner/host context: it is
 * intentionally not capability-scoped to an app's access set.
 */
export interface LoadedSchema {
  /** Canonical runtime schema name. On current nodes this is the identity hash. */
  name: string;
  /** Explicit identity hash when the node reports one; falls back to `name`. */
  identityHash: string | null;
  /** Human-readable schema name (`descriptive_name` on the wire). */
  descriptiveName: string | null;
  /** Owning app namespace (`owner_app_id`), when this is an app-owned schema. */
  ownerAppId: string | null;
  /** Declared data field names. */
  fields: string[];
}

/** Descriptor used to resolve an app-owned loaded schema from `/api/schemas`. */
export interface SchemaDescriptor {
  /** Owning app namespace, e.g. `fbrain` or `fsituations`. */
  ownerAppId: string;
  /** The schema's declarative `descriptive_name`, e.g. `Situation`. */
  descriptiveName: string;
  /**
   * Optional exact field-set guard. When supplied, matching ignores order and
   * requires the loaded schema to carry exactly the same declared fields.
   */
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

/**
 * A compare-and-set precondition on a single field of the row being written,
 * mirroring the node's `/api/mutation` `expected` key. The write is applied
 * only if the precondition holds; otherwise the node rejects it with
 * `409 {error:"cas_conflict", ...}` and the SDK raises a
 * {@link import('./errors.js').CasConflictError}.
 *
 * - `{ type: "absent", field }` — the write succeeds only if `field` has no
 *   current value (the CAS form of "create if not present"; pair with
 *   `mutationType: "create"`).
 * - `{ type: "value", field, value }` — the write succeeds only if `field`'s
 *   current value equals `value` (the CAS form of "update from a known
 *   value"; pair with `mutationType: "update"`).
 *
 * Forwarded verbatim under the node's `expected` key; the SDK never invents a
 * dialect the node would reject. Requires a node that implements the
 * `/api/mutation` `expected` primitive (`fold-node-cas-mutation-primitive`);
 * an older node ignores it and applies the write unconditionally.
 */
export type CasExpectation =
  | { type: 'absent'; field: string }
  | { type: 'value'; field: string; value: JsonValue };

/**
 * A mutation operation. Mirrors the node's `Operation::Mutation` envelope
 * (`{type:"mutation", schema, fields_and_values, key_value, mutation_type}`),
 * plus optional CAS and convergence controls.
 * The SDK fills `type` and `schema`; the caller supplies the rest.
 */
export interface MutationOp {
  /** `"create" | "update" | "delete"` — the node's `mutation_type`. */
  mutationType: 'create' | 'update' | 'delete';
  /** Field name → value for the row being written. */
  fields: Record<string, JsonValue>;
  /**
   * The row key. For a Range schema: `{ hash: null, range: "<id>" }`. For a
   * hash-keyed schema: `{ hash: "<id>", range: null }`. Passed through as the
   * node's `key_value`. A {@link QueryRow.keyValue} from a previous query can
   * be passed back verbatim to address that exact row.
   */
  key: KeyValue;
  /**
   * Optional compare-and-set precondition on a single field. When set, it is
   * forwarded verbatim under the node's `expected` key, and the node applies
   * the write only if the precondition holds — otherwise it returns
   * `409 {error:"cas_conflict"}`, which the SDK maps to
   * {@link import('./errors.js').CasConflictError}. Omit it for an
   * unconditional write (the prior SDK behavior). See {@link CasExpectation}.
   */
  expected?: CasExpectation;
  /**
   * Optional post-write convergence mode. Omit for the node default (`async`):
   * the mutation returns after the write commits without waiting on background
   * tasks, and the response may set `convergencePending: true`. Pass `sync`
   * only when the caller needs background work drained before the response.
   */
  convergence?: MutationConvergence;
  /**
   * Optional local persistence policy. Omit for the node default (`queued`).
   * Pass `durable` when the response must follow a local persistence barrier.
   */
  durability?: MutationDurability;
  /**
   * Optional exact cloud publication wait. The node accepts `wait` only with
   * `mutationType: "delete"` and `durability: "durable"`. The server validates
   * this combination and returns an exact target/writer/frontier receipt.
   */
  cloudPublication?: MutationCloudPublication;
  /**
   * Local request policy on `delete`: when `true`, a missing key is a loud
   * miss instead of an idempotent success. Forwarded as the node's
   * `must_exist` field. Omit it (the default) for ordinary delete-means-gone.
   * Not a second eraser — do not send `mutationType: "purge"`.
   */
  mustExist?: boolean;
}

/** Durable cloud-intent state for one locally committed mutation. */
export type MutationCloudCaptureState = 'durable' | 'failed';

/** The crash-safe cloud-intent receipt for one mutation. */
export interface MutationCloudCaptureReceipt {
  state: MutationCloudCaptureState;
  durable: boolean;
  mutationUuid: string;
  error: string | null;
}

/** Exact off-box publication state for one mutation. */
export type MutationCloudPublicationState =
  | 'not_requested'
  | 'pending'
  | 'published'
  | 'failed';

/** One required cloud target coordinate in an exact publication receipt. */
export interface MutationCloudPublicationTarget {
  targetId: string;
  targetLabel: string;
  writerId: string;
  /** Exact decimal frontier. It is a string because it can exceed JS integer precision. */
  frontier: string;
}

/** Off-box publication receipt for one mutation. */
export interface MutationCloudPublicationReceipt {
  state: MutationCloudPublicationState;
  published: boolean;
  mutationUuid: string;
  targets: MutationCloudPublicationTarget[];
  error: string | null;
}

/** `POST /api/mutation` success body. */
export interface MutationResult {
  written: number;
  mutationIds: string[];
  firingsObserved: number;
  backgroundTasksDrained?: boolean;
  convergencePending?: boolean;
  /** Present for a durable delete, including a post-commit cloud failure. */
  localCommitted?: boolean;
  /** Present for a durable delete after the node attempts durable intent capture. */
  cloudCapture?: MutationCloudCaptureReceipt;
  /** Present for a durable delete; inspect `published` for exact off-box success. */
  cloudPublication?: MutationCloudPublicationReceipt;
}

/** Options for the durable, node-scoped app change feed. */
export interface ChangesOptions {
  /** Opaque cursor returned by a prior call. Omit to begin at retained start. */
  since?: string;
  /** Maximum raw feed rows examined by the node (1..500). */
  limit?: number;
  /** Optional single schema target; the node intersects it with caller scope. */
  target?: string;
}

/** Thin changed-row metadata. Consumers point-read the authoritative row. */
export interface ChangeEvent {
  cursor: string;
  mutationId: string;
  schemaName: string;
  key: KeyValue;
  operation: string;
  committedAtMs: number;
  backgroundTasksDrained: boolean;
  convergencePending: boolean;
}

/** One cursor page from `POST /api/app/changes`. */
export interface ChangesResult {
  changes: ChangeEvent[];
  nextCursor: string;
  hasMore: boolean;
  /** Cursor fell behind bounded feed retention; rescan declared product keys. */
  gap: boolean;
}

/** One item of {@link LastDbClient.queryBatch}: the arguments of `query`. */
export interface QueryBatchItem {
  /** App schema name, as passed to `query`. */
  schemaName: string;
  /** Same filter as `query`. A batch never sends the full-scan opt-in. */
  filter?: QueryFilter;
}

/**
 * Outcome of one {@link QueryBatchItem}, in request order. A failing item
 * carries the same typed error `query` would have thrown for it; the other
 * items still return their results.
 */
export type QueryBatchOutcome =
  | { ok: true; result: QueryResult }
  | { ok: false; error: Error };
