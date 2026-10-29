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

export { parseListResponse } from './listResponse.js';

/**
 * Parse a `200` `/api/query` body into a {@link QueryResult}, surfacing the
 * full per-row envelope.
 *
 * The node returns its rows under `results`, one object per key shaped
 * `{ key, fields, metadata, author_pub_key }` (`lastdb_host`
 * `handlers::query::rows::format_rows`). Each row is normalized to a
 * {@link QueryRow}; a body without a `results` array, or a row without the
 * envelope, is an unexpected response. The node sends no schema name, so
 * `schema` is empty here and the client sets the app schema name.
 */
export function parseQueryResponse(body: unknown): QueryResult {
  const results = isObject(body) ? body['results'] : undefined;
  if (!Array.isArray(results)) {
    throw new UnexpectedResponseError('query returned no results array', 200, body);
  }
  const rows = results.map(parseQueryRow);
  return { schema: '', rowCount: rows.length, rows, page: parseQueryPage(body, rows.length) };
}

/**
 * Read the pagination metadata (`total_count` / `returned_count` / `limit` /
 * `offset` / `has_more`, per `lastdb_host` `handlers::query::paging::page_payload`)
 * off a 200 `/api/query` body. Returns `null` when the body carries none — the
 * SDK never invents pagination the node didn't do. A paginated response may
 * set `total_count: null` when the node intentionally skipped an exact count;
 * keep the page metadata so
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

/**
 * Read the `mutation_id` off a `200` `POST /api/mutation` body. The single
 * route answers `{ mutation_id, success: true, ... }`; a body without a
 * non-empty `mutation_id` and `success: true` is an unexpected response.
 * (`POST /api/mutations/batch` answers `mutation_ids`; this SDK does not call
 * that route.)
 */
export function parseMutationId(
  body: Record<string, JsonValue>,
  raw: unknown,
): string {
  const id = body['mutation_id'];
  if (!isNonEmptyString(id)) {
    throw new UnexpectedResponseError(
      'mutation returned no mutation ID',
      200,
      raw,
    );
  }
  if (body['success'] !== true) {
    throw new UnexpectedResponseError(
      'mutation returned no success marker',
      200,
      raw,
    );
  }
  return id;
}

/**
 * Normalize one raw query row (`{key, fields, metadata, author_pub_key}`)
 * into a {@link QueryRow}. The structured `{hash, range}` key is preserved
 * verbatim as `keyValue` (the unambiguous row address) AND rendered to the
 * `key` string the same way fold_db's `KeyValue::Display` does. A row without
 * an object `key` and an object `fields` is an unexpected response.
 */
export function parseQueryRow(raw: unknown): QueryRow {
  const fields = isObject(raw) ? raw['fields'] : undefined;
  const rawKey = isObject(raw) ? raw['key'] : undefined;
  if (!isObject(raw) || !isObject(fields) || !isObject(rawKey)) {
    throw new UnexpectedResponseError('query returned an invalid row', 200, raw);
  }
  const author = raw['author_pub_key'];
  const { key, keyValue } = parseRowKey(rawKey);
  return {
    key,
    keyValue,
    ...(isObject(raw['matched_key']) ? { matchedKey: parseRowKey(raw['matched_key']).keyValue } : {}),
    fields: fields as RowFields,
    metadata: 'metadata' in raw ? (raw['metadata'] as JsonValue) : null,
    authorPubKey: typeof author === 'string' && author.length > 0 ? author : null,
  };
}

/**
 * Render a row's structured `{hash, range}` key like fold_db's
 * `KeyValue::Display` (`"hash:range"` / `"hash"` / `"range"`). A key with
 * neither coordinate (a single-record schema) renders as `""` with
 * `keyValue: null`.
 */
export function parseRowKey(rawKey: Record<string, JsonValue>): { key: string; keyValue: KeyValue | null } {
  const keyValue = parseKeyValue(rawKey);
  if (keyValue === null) {
    return { key: '', keyValue: null };
  }
  const key =
    keyValue.hash !== null && keyValue.range !== null
      ? `${keyValue.hash}:${keyValue.range}`
      : (keyValue.hash ?? keyValue.range ?? '');
  return { key, keyValue };
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
 * so the envelope handling (structured key, null author) is identical.
 */
export function parseSearchResponse(body: unknown): SearchResult {
  const b = (body ?? {}) as { results?: unknown };
  const rawHits: unknown[] = Array.isArray(b.results) ? b.results : [];
  const hits = rawHits.map(parseSearchHit);
  return { hits };
}

/**
 * Parse a `GET /api/version` 200 body (`lastdb_uds`
 * `uds_router::responses::version_response`). A body without a non-negative
 * integer `api_version` is an unexpected response.
 */
export function parseNodeVersion(body: unknown): NodeVersion {
  const b = (body ?? {}) as {
    api_version?: unknown;
    build?: unknown;
    capabilities?: unknown;
    instance_id?: unknown;
  };
  const apiVersion = b.api_version;
  if (typeof apiVersion !== 'number' || !Number.isInteger(apiVersion) || apiVersion < 0) {
    throw new UnexpectedResponseError('version returned no api_version', 200, body);
  }
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
  };
}

/** Parse `GET /api/system/auto-identity` success JSON. */
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
    independentFields: schema['independent_fields'] === true,
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
