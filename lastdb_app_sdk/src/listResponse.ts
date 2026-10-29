/** Key-list pages retain alternate coordinates and the keys for future mutations. */
import { UnexpectedResponseError } from './errors.js';
import { ListRecordKey, ListResult } from './types.js';

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function keys(values: unknown, body: unknown): ListRecordKey[] {
  if (!Array.isArray(values)) {
    throw new UnexpectedResponseError('list returned no key array', 200, body);
  }
  return values.map((value): ListRecordKey => {
    if (!record(value) || typeof value['hash'] !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid record key', 200, value);
    }
    const range = value['range'];
    if (range !== undefined && typeof range !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid range', 200, value);
    }
    return range === undefined || range.length === 0
      ? { hash: value['hash'] } : { hash: value['hash'], range };
  });
}

export function parseListResponse(body: unknown): ListResult {
  if (!record(body) || !record(body['list'])) {
    throw new UnexpectedResponseError('list returned an invalid envelope', 200, body);
  }
  const page = body['list'];
  const schema = page['schema'];
  const nextCursor = page['next_cursor'];
  const hasMore = page['has_more'];
  const truncated = page['truncated'];
  if (typeof schema !== 'string' || typeof hasMore !== 'boolean' || typeof truncated !== 'boolean'
    || (nextCursor !== undefined && nextCursor !== null && typeof nextCursor !== 'string')) {
    throw new UnexpectedResponseError('list returned an invalid keys page', 200, body);
  }
  const selected = keys(page['keys'], body);
  const result: ListResult = { schema, keys: selected, has_more: hasMore, truncated,
    next_cursor: typeof nextCursor === 'string' ? nextCursor : null };
  if (page['key_name'] !== undefined) {
    if (typeof page['key_name'] !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid key name', 200, body);
    }
    result.key_name = page['key_name'];
    result.primary_keys = keys(page['primary_keys'], body);
    if (result.primary_keys.length !== selected.length) {
      throw new UnexpectedResponseError('alternate list lost its primary mutation keys', 200, body);
    }
  }
  return result;
}
