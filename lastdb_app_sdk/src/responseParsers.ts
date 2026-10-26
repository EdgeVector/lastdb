/** Parsers that validate node response bodies into typed SDK results. */

import {
  UnexpectedResponseError,
} from './errors.js';
import {
  AutoIdentityResult,
  ChangesResult,
  JsonValue,
  KeyValue,
  QueryCursor,
  ListResult,
  LoadedSchema,
  NodeVersion,
  QueryPage,
  QueryResult,
  QueryRow,
  SchemaDescriptor,
  RowFields,
  SearchHit,
  SearchResult,
} from './types.js';

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
export function parseQueryPage(body: unknown, returnedRows: number): QueryPage | null {
  if (!isObject(body)) {
    return null;
  }
  const totalCount = body['total_count'];
  const limit = body['limit'];
  const offset = body['offset'];
  const hasMore = body['has_more'];
  const nextCursor = parseQueryCursor(body['next_cursor']);
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
export function isObject(v: unknown): v is Record<string, JsonValue> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/** Normalize the legacy ID list and the current single-route mutation ID. */
export function parseMutationIds(
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
export function parseMutationWritten(
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
export function parseMutationFiringsObserved(
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
export function parseQueryRow(raw: unknown): QueryRow {
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
export function parseRowKey(rawKey: unknown): { key: string; keyValue: KeyValue | null } {
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

export function parseKeyValue(rawKey: unknown): KeyValue | null {
  if (!isObject(rawKey)) {
    return null;
  }
  const hash = typeof rawKey['hash'] === 'string' ? (rawKey['hash'] as string) : null;
  const range = typeof rawKey['range'] === 'string' ? (rawKey['range'] as string) : null;
  return hash !== null || range !== null ? { hash, range } : null;
}

function parseQueryCursor(raw: unknown): QueryCursor | null {
  if (raw === null || raw === undefined) return null;
  const key = parseKeyValue(raw);
  if (!key || !isObject(raw)) {
    throw new UnexpectedResponseError('query returned an invalid cursor', 200, raw);
  }
  for (const name of ['hash', 'range']) {
    const value = raw[name];
    if (value !== null && value !== undefined && typeof value !== 'string') {
      throw new UnexpectedResponseError('query returned an invalid cursor coordinate', 200, raw);
    }
  }
  const comparison = raw['comparison'];
  if (comparison === null || comparison === undefined) return key;
  if (typeof comparison !== 'string' || comparison.length === 0) {
    throw new UnexpectedResponseError('query returned an invalid comparison cursor', 200, raw);
  }
  return { ...key, comparison };
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
export const PRE_HANDSHAKE_NODE: NodeVersion = Object.freeze({
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

export function parseLoadedSchema(schema: Record<string, JsonValue>): LoadedSchema {
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

export function sameFieldSet(
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

export function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((field): field is string => typeof field === 'string')
    : [];
}

export function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

/**
 * Normalize one raw search hit. The row envelope (`key`/`fields`/`metadata`/
 * `authorPubKey`) is parsed by {@link parseQueryRow}; the search-only
 * attribution fields (`score`, `schema_name`, `schema_display_name`) are read
 * off the same object. `score` is `null` when the node reported none.
 */
export function parseSearchHit(raw: unknown): SearchHit {
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
