/**
 * Keyed read operations: `list`, `get`, `query`, `queryBatch`, and `queryAll`.
 */

import { LASTDB_API_ROUTES, scopedApiPath } from './apiRoutes.js';
import {
  mapDataError,
  normalizeRecordKeyInput,
  pairBatchReplies,
  toError,
  type BatchEntry,
  type ClientContext,
} from './clientHelpers.js';
import { QueryPaginationError } from './errors.js';
import { parseListResponse, parseQueryResponse } from './responseParsers.js';
import type {
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

function queryRowDedupKey(row: QueryRow): string {
  if (row.keyValue !== null) {
    return `kv:${JSON.stringify([row.keyValue.hash, row.keyValue.range])}`;
  }
  return `rendered:${row.key}`;
}

export function buildQueryBody(
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
  const res = await ctx.transport.send('GET', path);
  if (res.status === 200) {
    return { ...parseListResponse(res.body, opts.keyName), schema: schemaName };
  }
  throw mapDataError('list', res.status, res.body, ctx.appId);
}

/** Point lookup of a single record by primary `HashKey` or `HashRangeKey`. */
export async function executeGet(
  ctx: ClientContext,
  schemaName: string,
  key: RecordKeyInput,
): Promise<QueryRow | null> {
  const normalized = normalizeRecordKeyInput(key, 'get');
  const filter =
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
  const res = await ctx.transport.send('POST', path, {
    body: buildQueryBody(schemaName, filter),
  });
  if (res.status === 200) {
    return { ...parseQueryResponse(res.body), schema: schemaName };
  }
  throw mapDataError('query', res.status, res.body, ctx.appId);
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
    const res = await ctx.transport.send('POST', path, { body: { queries } });
    if (res.status !== 200) {
      throw mapDataError('query batch', res.status, res.body, ctx.appId);
    }
    for (const [entry, reply] of pairBatchReplies(chunk, res.body)) {
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
