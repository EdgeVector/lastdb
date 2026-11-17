/**
 * Keyed read operations: `list`, `get`, `query`, `queryBatch`, and `queryAll`.
 */

import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  type ClientContext,
} from './databases.js';
import {
  mapDataError,
  QueryPaginationError,
  RequestRejectedError,
  UnexpectedResponseError,
} from './errors.js';
import { parseListResponse, parseQueryResponse, sendJsonObject } from './responseParsers.js';
import type {
  JsonValue,
  KeyValue,
  ListOptions,
  ListResult,
  QueryAllOptions,
  QueryBatchItem,
  QueryBatchOutcome,
  QueryCursor,
  QueryFilter,
  QueryPage,
  QueryResult,
  QueryRow,
  RecordKeyInput,
} from './types.js';

/** Maximum queries `lastdbd` accepts in one `/api/queries/batch` request. */
const QUERY_BATCH_MAX_ITEMS = 64;

interface BatchEntry {
  index: number;
  item: QueryBatchItem;
}

/** Normalize a point-lookup or blob key input into a non-empty {@link KeyValue}. */
export function normalizeRecordKeyInput(key: RecordKeyInput, verb: string): KeyValue {
  if (typeof key === 'string') {
    if (key.length === 0) {
      throw new RequestRejectedError('invalid_request', `${verb} requires a non-empty hash key`);
    }
    return { hash: key, range: null };
  }
  if (key.hash === null || key.hash.length === 0) {
    throw new RequestRejectedError('invalid_request', `${verb} requires a non-empty hash key`);
  }
  const range = key.range;
  return {
    hash: key.hash,
    range: range !== undefined && range !== null && range.length > 0 ? range : null,
  };
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

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

function queryRowDedupKey(row: QueryRow): string {
  if (row.keyValue !== null) {
    return `kv:${JSON.stringify([row.keyValue.hash, row.keyValue.range])}`;
  }
  return `rendered:${row.key}`;
}

function buildQueryBody(
  schemaName: string,
  filter: QueryFilter,
): Record<string, unknown> {
  const body: Record<string, unknown> = { schema_name: schemaName };
  if (filter.keyName !== undefined) body.key_name = filter.keyName;
  if (filter.filter !== undefined) body.filter = filter.filter;
  if (filter.limit !== undefined) body.limit = filter.limit;
  if (filter.offset !== undefined) body.offset = filter.offset;
  if (filter.cursor !== undefined) body.cursor = filter.cursor;
  return body;
}

/** `GET /api/list` — return one page of live primary or alternate keys. */
export async function executeList(
  ctx: ClientContext,
  schemaName: string,
  opts: ListOptions = {},
): Promise<ListResult> {
  const params = new URLSearchParams({ schema: schemaName });
  if (opts.keyName !== undefined) params.set('key_name', opts.keyName);
  if (opts.hash !== undefined) params.set('hash', opts.hash);
  if (opts.limit !== undefined) params.set('limit', String(opts.limit));
  if (opts.cursor !== undefined) params.set('cursor', opts.cursor);
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.list, params.toString());
  const body = await sendJsonObject(ctx.transport, 'GET', path, 'list', ctx.appId);
  return { ...parseListResponse(body, opts.keyName), schema: schemaName };
}

/** Point lookup of a single record by primary `HashKey` or `HashRangeKey`. */
export async function executeGet(
  ctx: ClientContext,
  schemaName: string,
  key: RecordKeyInput,
): Promise<QueryRow | null> {
  const normalized = normalizeRecordKeyInput(key, 'get');
  const filter: JsonValue =
    normalized.range !== null
      ? { HashRangeKey: { hash: normalized.hash, range: normalized.range } }
      : { HashKey: normalized.hash };
  const result = await executeQuery(ctx, schemaName, { filter, limit: 1 });
  return result.rows[0] ?? null;
}

/** `POST /api/query` — keyed point, multi-get, or range read under one hash. */
export async function executeQuery(
  ctx: ClientContext,
  schemaName: string,
  filter: QueryFilter = {},
): Promise<QueryResult> {
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.query);
  const body = await sendJsonObject(ctx.transport, 'POST', path, 'query', ctx.appId, {
    body: buildQueryBody(schemaName, filter),
  });
  return { ...parseQueryResponse(body), schema: schemaName };
}

/** `POST /api/queries/batch` — read several independent keyed queries in one request. */
export async function executeQueryBatch(
  ctx: ClientContext,
  items: readonly QueryBatchItem[],
): Promise<QueryBatchOutcome[]> {
  const outcomes = new Array<QueryBatchOutcome>(items.length);
  const ready: BatchEntry[] = items.map((item, index) => ({ index, item }));
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.queryBatch);

  for (let start = 0; start < ready.length; start += QUERY_BATCH_MAX_ITEMS) {
    const chunk = ready.slice(start, start + QUERY_BATCH_MAX_ITEMS);
    const queries = chunk.map(({ item }) => buildQueryBody(item.schemaName, item.filter ?? {}));
    const body = await sendJsonObject(ctx.transport, 'POST', path, 'query batch', ctx.appId, {
      body: { queries },
    });
    for (const [entry, reply] of pairBatchReplies(chunk, body)) {
      outcomes[entry.index] = parseBatchItemOutcome(ctx, entry, reply);
    }
  }
  return outcomes;
}

function parseBatchItemOutcome(
  ctx: ClientContext,
  entry: BatchEntry,
  reply: { status: number; response: unknown },
): QueryBatchOutcome {
  if (reply.status !== 200) {
    return {
      ok: false,
      error: mapDataError('query', reply.status, reply.response, ctx.appId),
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

/** Auto-paginate a keyed query across pages until `hasMore` is false or `maxRows` is reached. */
export async function executeQueryAll(
  ctx: ClientContext,
  schemaName: string,
  filter: Omit<QueryFilter, 'limit' | 'offset' | 'cursor'> = {},
  opts: QueryAllOptions = {},
): Promise<QueryResult> {
  const pageSize = opts.pageSize ?? 100;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) {
    throw new Error(`queryAll pageSize must be an integer in [1, 1000], got ${pageSize}`);
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
    const page = await executeQuery(ctx, schemaName, {
      ...filter,
      limit: pageSize,
      ...(cursor === null ? { offset } : { cursor }),
    });
    schema = page.schema || schema;
    lastPage = page.page;
    let newRows = 0;
    for (const row of page.rows) {
      const key = queryRowDedupKey(row);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      rows.push(row);
      newRows += 1;
    }

    const hasMore = page.page !== null ? page.page.hasMore : page.rows.length >= pageSize;
    if (!hasMore) break;
    if (rows.length >= maxRows) {
      truncatedByMaxRows = true;
      break;
    }
    const nextCursor = page.page?.nextCursor ?? null;
    const moved = nextCursor !== null && JSON.stringify(nextCursor) !== JSON.stringify(cursor);
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
