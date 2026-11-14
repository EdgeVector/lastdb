/** Parsers that validate node query and record response bodies into typed SDK results. */

import { UnexpectedResponseError } from './errors.js';
import type {
  FileMarker,
  JsonValue,
  KeyValue,
  QueryCursor,
  QueryPage,
  QueryResult,
  QueryRow,
  RecordMetadata,
  RowFields,
} from './types.js';

export { parseListResponse } from './listResponse.js';
export {
  parseAutoIdentityResponse,
  parseGetSchemaResponse,
  parseLoadedSchema,
  parseNodeVersion,
  parseSchemaListResponse,
  resolveLoadedSchema,
  sameFieldSet,
} from './schemaParsers.js';

/** Whether a value is a non-array JSON object. */
export function isObject(v: unknown): v is Record<string, JsonValue> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

export function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((field): field is string => typeof field === 'string')
    : [];
}

export function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
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
export function parseQueryPage(body: unknown, returnedRows: number): QueryPage | null {
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

/** Read the `mutation_id` off a `200` `POST /api/mutation` body. */
export function parseMutationId(body: Record<string, JsonValue>, raw: unknown): string {
  const id = body['mutation_id'];
  if (!isNonEmptyString(id)) {
    throw new UnexpectedResponseError('mutation returned no mutation ID', 200, raw);
  }
  if (body['success'] !== true) {
    throw new UnexpectedResponseError('mutation returned no success marker', 200, raw);
  }
  return id;
}

/** Normalize one raw query row (`{key, fields, metadata}`) into a {@link QueryRow}. */
export function parseQueryRow(raw: unknown): QueryRow {
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
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) {
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
export function parseRecordMetadata(raw: unknown): RecordMetadata | null {
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
    typeof file['sha256'] !== 'string' ||
    !/^[0-9a-f]{64}$/.test(file['sha256']) ||
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
export function parseRowKey(rawKey: Record<string, JsonValue>): {
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

export function parseKeyValue(rawKey: unknown): KeyValue | null {
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
