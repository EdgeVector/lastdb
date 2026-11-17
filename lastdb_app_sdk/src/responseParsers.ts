/** Parsers that validate node query and record response bodies into typed SDK results. */

import { isObject, mapDataError, UnexpectedResponseError } from './errors.js';
import type { Transport, TransportSendOptions } from './transport.js';
import type {
  FileMarker,
  JsonValue,
  KeyValue,
  ListRecordKey,
  ListResult,
  QueryCursor,
  QueryPage,
  QueryResult,
  QueryRow,
  RecordMetadata,
  RowFields,
} from './types.js';

export { isObject };

const HEX64_PATTERN = /^[0-9a-f]{64}$/;

export function isHex64(value: unknown): value is string {
  return typeof value === 'string' && HEX64_PATTERN.test(value);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/** Send a JSON request, map non-200 errors, and require a JSON object reply. */
export async function sendJsonObject(
  transport: Transport,
  method: 'GET' | 'POST',
  path: string,
  verb: string,
  appId: string,
  options?: TransportSendOptions & { redactBody?: boolean },
): Promise<Record<string, JsonValue>> {
  const { redactBody, ...sendOptions } = options ?? {};
  const hasSendOpts =
    sendOptions.headers !== undefined ||
    sendOptions.body !== undefined ||
    sendOptions.timeoutMs !== undefined;
  const res = hasSendOpts
    ? await transport.send(method, path, sendOptions)
    : await transport.send(method, path);
  if (res.status !== 200) {
    throw mapDataError(verb, res.status, res.body, appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError(
      `${verb} returned a non-object body`,
      200,
      redactBody ? null : res.body,
    );
  }
  return res.body;
}

function parseListKeys(values: unknown, body: unknown): ListRecordKey[] {
  if (!Array.isArray(values)) {
    throw new UnexpectedResponseError('list returned no key array', 200, body);
  }
  return values.map((value): ListRecordKey => {
    if (!isObject(value) || typeof value['hash'] !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid record key', 200, value);
    }
    const range = value['range'];
    if (range !== undefined && typeof range !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid range', 200, value);
    }
    return range === undefined || range.length === 0
      ? { hash: value['hash'] }
      : { hash: value['hash'], range };
  });
}

export function parseListResponse(body: unknown, expectedKeyName?: string): ListResult {
  if (!isObject(body) || !isObject(body['list'])) {
    throw new UnexpectedResponseError('list returned an invalid envelope', 200, body);
  }
  const page = body['list'];
  if (expectedKeyName !== undefined && page['key_name'] !== expectedKeyName) {
    throw new UnexpectedResponseError(
      'node did not honor the requested alternate key; upgrade LastDB',
      200,
      body,
    );
  }
  const schema = page['schema'];
  const nextCursor = page['next_cursor'];
  const hasMore = page['has_more'];
  const truncated = page['truncated'];
  if (
    typeof schema !== 'string' ||
    typeof hasMore !== 'boolean' ||
    typeof truncated !== 'boolean' ||
    (nextCursor !== undefined && nextCursor !== null && typeof nextCursor !== 'string')
  ) {
    throw new UnexpectedResponseError('list returned an invalid keys page', 200, body);
  }
  const selected = parseListKeys(page['keys'], body);
  const result: ListResult = {
    schema,
    keys: selected,
    has_more: hasMore,
    truncated,
    next_cursor: typeof nextCursor === 'string' ? nextCursor : null,
  };
  if (page['key_name'] !== undefined) {
    if (typeof page['key_name'] !== 'string') {
      throw new UnexpectedResponseError('list returned an invalid key name', 200, body);
    }
    result.key_name = page['key_name'];
    result.primary_keys = parseListKeys(page['primary_keys'], body);
    if (result.primary_keys.length !== selected.length) {
      throw new UnexpectedResponseError('alternate list lost its primary mutation keys', 200, body);
    }
  }
  return result;
}

/** Parse a `200` `/api/query` body into a {@link QueryResult}. */
export function parseQueryResponse(body: unknown): QueryResult {
  const results = isObject(body) ? body['results'] : undefined;
  if (!Array.isArray(results)) {
    throw new UnexpectedResponseError('query returned no results array', 200, body);
  }
  const rows = results.map(parseQueryRow);
  return { schema: '', rowCount: rows.length, rows, page: parseQueryPage(body, rows.length) };
}

/** Read the pagination metadata off a `200` `/api/query` body, or `null` when absent. */
function parseQueryPage(body: unknown, returnedRows: number): QueryPage | null {
  if (!isObject(body)) return null;
  const totalCount = body['total_count'];
  const limit = body['limit'];
  const offset = body['offset'];
  const hasMore = body['has_more'];
  const nextCursor = parseQueryCursor(body['next_cursor']);
  if (typeof limit !== 'number' || typeof offset !== 'number' || typeof hasMore !== 'boolean') {
    return null;
  }
  if (totalCount !== null && totalCount !== undefined && typeof totalCount !== 'number') {
    return null;
  }
  const returnedCount = body['returned_count'];
  return {
    totalCount: typeof totalCount === 'number' ? totalCount : undefined,
    returnedCount: typeof returnedCount === 'number' ? returnedCount : returnedRows,
    limit,
    offset,
    hasMore,
    nextCursor,
  };
}

/** Normalize one raw query row (`{key, fields, metadata}`) into a {@link QueryRow}. */
function parseQueryRow(raw: unknown): QueryRow {
  const fields = isObject(raw) ? raw['fields'] : undefined;
  const rawKey = isObject(raw) ? raw['key'] : undefined;
  if (!isObject(raw) || !isObject(fields) || !isObject(rawKey)) {
    throw new UnexpectedResponseError('query returned an invalid row', 200, raw);
  }
  const { key, keyValue } = parseRowKey(rawKey);
  return {
    key,
    keyValue,
    revision: parseRevision(raw['revision'], raw),
    alternates: parseAlternates(raw['alternates'], raw),
    ...(isObject(raw['matched_key'])
      ? { matchedKey: parseRowKey(raw['matched_key']).keyValue }
      : {}),
    fields: fields as RowFields,
    metadata: parseRecordMetadata(raw['metadata']),
  };
}

function parseRevision(value: unknown, raw: unknown): string {
  if (!isHex64(value)) {
    throw new UnexpectedResponseError('query returned no record revision', 200, raw);
  }
  return value;
}

function parseAlternates(value: unknown, raw: unknown): Record<string, KeyValue | null> {
  if (!isObject(value)) {
    throw new UnexpectedResponseError('query returned no alternate addresses', 200, raw);
  }
  return Object.fromEntries(
    Object.entries(value).map(([name, key]) => {
      if (key === null) return [name, null];
      const parsed = parseKeyValue(key);
      if (!parsed || !parsed.hash) {
        throw new UnexpectedResponseError('query returned invalid alternate address', 200, raw);
      }
      return [name, parsed];
    }),
  );
}

/** Read the metadata block of a row, or `null` when absent. */
function parseRecordMetadata(raw: unknown): RecordMetadata | null {
  if (!isObject(raw)) return null;
  const sourceFileName = raw['source_file_name'];
  const map = raw['metadata'];
  const metadata: RecordMetadata = {
    source_file_name: typeof sourceFileName === 'string' ? sourceFileName : null,
  };
  if (isObject(map)) {
    metadata.metadata = Object.fromEntries(
      Object.entries(map).filter((entry): entry is [string, string] => typeof entry[1] === 'string'),
    );
  }
  if (isObject(raw['file'])) {
    metadata.file = parseFileMarker(raw['file'], 'file marker', raw);
  }
  if (isObject(raw['inline_file'])) {
    metadata.inline_file = parseFileMarker(raw['inline_file'], 'inline file marker', raw);
  }
  return metadata;
}

export function parseFileMarker(
  file: Record<string, unknown>,
  what: string,
  raw: unknown,
): FileMarker {
  if (
    !isHex64(file['sha256']) ||
    typeof file['size_bytes'] !== 'number' ||
    !Number.isSafeInteger(file['size_bytes']) ||
    file['size_bytes'] < 0
  ) {
    throw new UnexpectedResponseError(`response returned invalid ${what}`, 200, raw);
  }
  return {
    sha256: file['sha256'],
    size_bytes: file['size_bytes'],
    ...(typeof file['name'] === 'string' ? { name: file['name'] } : {}),
    ...(typeof file['media_type'] === 'string' ? { media_type: file['media_type'] } : {}),
  };
}

/** Render a row's structured `{hash, range}` key (`"hash:range"` / `"hash"` / `"range"`). */
function parseRowKey(rawKey: Record<string, JsonValue>): {
  key: string;
  keyValue: KeyValue | null;
} {
  const keyValue = parseKeyValue(rawKey);
  if (keyValue === null) return { key: '', keyValue: null };
  const key =
    keyValue.hash !== null && keyValue.range !== null
      ? `${keyValue.hash}:${keyValue.range}`
      : (keyValue.hash ?? keyValue.range ?? '');
  return { key, keyValue };
}

function parseKeyValue(rawKey: unknown): KeyValue | null {
  if (!isObject(rawKey)) return null;
  const hash = typeof rawKey['hash'] === 'string' ? rawKey['hash'] : null;
  const range = typeof rawKey['range'] === 'string' ? rawKey['range'] : null;
  return hash !== null || range !== null ? { hash, range } : null;
}

function parseQueryCursor(raw: unknown): QueryCursor | null {
  if (raw === null || raw === undefined) return null;
  if (!isObject(raw)) {
    throw new UnexpectedResponseError('query returned an invalid cursor', 200, raw);
  }
  const record = raw['record'];
  if (record !== null && record !== undefined && (typeof record !== 'string' || record.length === 0)) {
    throw new UnexpectedResponseError('query returned an invalid record cursor', 200, raw);
  }
  const key = typeof record === 'string' ? { hash: null, range: null } : parseKeyValue(raw);
  if (!key) {
    throw new UnexpectedResponseError('query returned an invalid cursor', 200, raw);
  }
  for (const name of ['hash', 'range']) {
    const value = raw[name];
    if (value !== null && value !== undefined && typeof value !== 'string') {
      throw new UnexpectedResponseError('query returned an invalid cursor coordinate', 200, raw);
    }
  }
  const comparison = raw['comparison'];
  if (
    comparison !== null &&
    comparison !== undefined &&
    (typeof comparison !== 'string' || comparison.length === 0)
  ) {
    throw new UnexpectedResponseError('query returned an invalid comparison cursor', 200, raw);
  }
  return { ...raw, ...key };
}
