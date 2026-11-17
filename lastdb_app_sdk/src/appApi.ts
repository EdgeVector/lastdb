/**
 * App API: `POST /v1/read`, `POST /v1/write`, and `POST /v1/x/changes`.
 * A non-200 reply throws {@link AppApiError}. A failed read in a list is a result.
 */

import type { ClientContext } from './databases.js';
import { LastDbError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type { JsonValue, KeyValue, RowFields } from './types.js';

const READ_PATH = '/v1/read';
const WRITE_PATH = '/v1/write';
const CHANGES_PATH = '/v1/x/changes';

/** A non-200 app API reply. `code` is the body's `error.code`. `message` is `Error.message`. */
export class AppApiError extends LastDbError {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/** A range under one hash: the whole hash, a prefix, or `start` up to `end`. */
export interface AppApiRange {
  hash: string;
  prefix?: string;
  start?: string;
  end?: string;
}

interface AppApiReadBase {
  schema: string;
  database?: string;
  via?: string;
  limit?: number;
  cursor?: string;
  keys_only?: boolean;
  file?: boolean;
}

/** One item of `POST /v1/read`. Names `schema` and exactly one of `key` or `range`. */
export type AppApiRead =
  | (AppApiReadBase & { key: KeyValue; range?: never })
  | (AppApiReadBase & { range: AppApiRange; key?: never });

/** File facts on a read record. `path` is set only when the read set `file: true`. */
export interface AppApiFile {
  name: string | null;
  media_type: string | null;
  size: number;
  path?: string | null;
}

export interface AppApiRecord {
  key: KeyValue;
  revision: string;
  record: RowFields;
  file: AppApiFile | null;
}

/** One read that failed. Returned in order. Not thrown. */
export interface AppApiReadError {
  error: { code: string; message: string };
}

/** One entry of the `results` array from `POST /v1/read`. */
export type AppApiReadResult =
  | { records: AppApiRecord[]; next_cursor: string | null }
  | { keys: KeyValue[]; next_cursor: string | null }
  | AppApiReadError;

/** A path the caller can read. No file bytes go through the app API. */
export interface AppApiFileLink {
  path: string;
  name?: string;
  media_type?: string;
}

interface AppApiMutationBase {
  schema: string;
  database?: string;
  key: KeyValue;
}

export type AppApiCreate = AppApiMutationBase & {
  op: 'create';
  record: RowFields;
  alternates?: Record<string, KeyValue | null>;
  file?: AppApiFileLink;
};

export type AppApiReplace = AppApiMutationBase & {
  op: 'replace';
  revision: string;
  record: RowFields;
  alternates?: Record<string, KeyValue | null>;
  file?: AppApiFileLink;
};

export type AppApiDelete = AppApiMutationBase & {
  op: 'delete';
  revision: string;
};

/** One item of `POST /v1/write`. */
export type AppApiMutation = AppApiCreate | AppApiReplace | AppApiDelete;

/** Receipt from `POST /v1/write`, after the write is on disk. `revisions` follows request order. */
export interface AppApiWriteResult {
  written: number;
  revisions: string[];
}

/** Body of `POST /v1/x/changes`. `after` defaults to the oldest kept entry. `limit` 0 reads `head` only. */
export interface AppApiChangesOptions {
  after?: number;
  limit?: number;
  schemas?: readonly string[];
  database?: string;
}

export interface AppApiChange {
  seq: number;
  schema: string;
  key: KeyValue;
  deleted: boolean;
}

/** Page from `POST /v1/x/changes`. Save `through`. Rebuild when `id` changes or `gap` is true. */
export interface AppApiChangesResult {
  id: string;
  head: number;
  through: number;
  gap: boolean;
  changes: AppApiChange[];
}

function invalid(message: string, body: unknown): UnexpectedResponseError {
  return new UnexpectedResponseError(message, 200, body);
}

function appApiFailure(status: number, body: unknown): AppApiError {
  const envelope = isObject(body) ? body['error'] : undefined;
  const code = isObject(envelope) && typeof envelope['code'] === 'string' ? envelope['code'] : '';
  const message =
    isObject(envelope) && typeof envelope['message'] === 'string'
      ? envelope['message']
      : `app API request returned status ${status}`;
  return new AppApiError(status, code, message);
}

async function postAppApi(ctx: ClientContext, path: string, body: unknown): Promise<unknown> {
  const res = await ctx.transport.send('POST', path, { body });
  if (res.status !== 200) throw appApiFailure(res.status, res.body);
  return res.body;
}

function keyBody(key: KeyValue): KeyValue {
  return { hash: key.hash, range: key.range };
}

function rangeBody(range: AppApiRange): Record<string, unknown> {
  const body: Record<string, unknown> = { hash: range.hash };
  if (range.prefix !== undefined) body.prefix = range.prefix;
  if (range.start !== undefined) body.start = range.start;
  if (range.end !== undefined) body.end = range.end;
  return body;
}

function readItemBody(item: AppApiRead): Record<string, unknown> {
  const body: Record<string, unknown> = { schema: item.schema };
  if (item.database !== undefined) body.database = item.database;
  if (item.via !== undefined) body.via = item.via;
  if (item.limit !== undefined) body.limit = item.limit;
  if (item.cursor !== undefined) body.cursor = item.cursor;
  if (item.keys_only !== undefined) body.keys_only = item.keys_only;
  if (item.file !== undefined) body.file = item.file;
  if (item.key !== undefined) body.key = keyBody(item.key);
  if (item.range !== undefined) body.range = rangeBody(item.range);
  return body;
}

function fileLinkBody(link: AppApiFileLink): Record<string, unknown> {
  const body: Record<string, unknown> = { path: link.path };
  if (link.name !== undefined) body.name = link.name;
  if (link.media_type !== undefined) body.media_type = link.media_type;
  return body;
}

function alternatesBody(
  alternates: Record<string, KeyValue | null>,
): Record<string, KeyValue | null> {
  const body: Record<string, KeyValue | null> = {};
  for (const [name, key] of Object.entries(alternates)) {
    body[name] = key === null ? null : keyBody(key);
  }
  return body;
}

function mutationBody(mutation: AppApiMutation): Record<string, unknown> {
  const body: Record<string, unknown> = {
    schema: mutation.schema,
    op: mutation.op,
    key: keyBody(mutation.key),
  };
  if (mutation.database !== undefined) body.database = mutation.database;
  if (mutation.op === 'delete') {
    body.revision = mutation.revision;
    return body;
  }
  body.record = mutation.record;
  if (mutation.alternates !== undefined) body.alternates = alternatesBody(mutation.alternates);
  if (mutation.file !== undefined) body.file = fileLinkBody(mutation.file);
  if (mutation.op === 'replace') body.revision = mutation.revision;
  return body;
}

function changesBody(options: AppApiChangesOptions): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  if (options.after !== undefined) body.after = options.after;
  if (options.limit !== undefined) body.limit = options.limit;
  if (options.schemas !== undefined) body.schemas = options.schemas;
  if (options.database !== undefined) body.database = options.database;
  return body;
}

function parseKey(value: JsonValue | undefined, body: unknown): KeyValue {
  if (!isObject(value)) throw invalid('app API returned an invalid key', body);
  const hash = value['hash'];
  const range = value['range'];
  if (
    (hash !== undefined && hash !== null && typeof hash !== 'string') ||
    (range !== undefined && range !== null && typeof range !== 'string')
  ) {
    throw invalid('app API returned an invalid key', body);
  }
  return {
    hash: typeof hash === 'string' ? hash : null,
    range: typeof range === 'string' ? range : null,
  };
}

function parseCursor(value: JsonValue | undefined, body: unknown): string | null {
  if (value === null) return null;
  if (typeof value === 'string') return value;
  throw invalid('app API returned an invalid cursor', body);
}

function optionalText(value: JsonValue | undefined, body: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value;
  throw invalid('app API returned an invalid file', body);
}

function parseFile(value: JsonValue | undefined, body: unknown): AppApiFile | null {
  if (value === undefined) throw invalid('app API returned a record with no file', body);
  if (value === null) return null;
  if (!isObject(value) || typeof value['size'] !== 'number' || !Number.isFinite(value['size'])) {
    throw invalid('app API returned an invalid file', body);
  }
  const path = value['path'];
  if (path !== undefined && path !== null && typeof path !== 'string') {
    throw invalid('app API returned an invalid file', body);
  }
  const file: AppApiFile = {
    name: optionalText(value['name'], body),
    media_type: optionalText(value['media_type'], body),
    size: value['size'],
  };
  if (path !== undefined) file.path = typeof path === 'string' ? path : null;
  return file;
}

function parseRecord(value: JsonValue, body: unknown): AppApiRecord {
  if (!isObject(value) || typeof value['revision'] !== 'string' || value['revision'].length === 0) {
    throw invalid('app API returned an invalid record', body);
  }
  if (!isObject(value['record'])) throw invalid('app API returned an invalid record', body);
  return {
    key: parseKey(value['key'], body),
    revision: value['revision'],
    record: value['record'],
    file: parseFile(value['file'], body),
  };
}

function parseReadResult(value: JsonValue, body: unknown): AppApiReadResult {
  if (!isObject(value)) throw invalid('app API returned an invalid read result', body);
  if ('error' in value) {
    const error = value['error'];
    if (!isObject(error) || typeof error['code'] !== 'string' || typeof error['message'] !== 'string') {
      throw invalid('app API returned an invalid read error', body);
    }
    return { error: { code: error['code'], message: error['message'] } };
  }
  if (!('next_cursor' in value)) throw invalid('app API returned a read result with no cursor', body);
  const nextCursor = parseCursor(value['next_cursor'], body);
  if ('records' in value && !('keys' in value)) {
    if (!Array.isArray(value['records'])) throw invalid('app API returned invalid records', body);
    return { records: value['records'].map((row) => parseRecord(row, body)), next_cursor: nextCursor };
  }
  if ('keys' in value && !('records' in value)) {
    if (!Array.isArray(value['keys'])) throw invalid('app API returned invalid keys', body);
    return { keys: value['keys'].map((key) => parseKey(key, body)), next_cursor: nextCursor };
  }
  throw invalid('app API returned a read result with no records or keys', body);
}

function parseReadResults(body: unknown, expected: number): AppApiReadResult[] {
  if (!isObject(body) || !Array.isArray(body['results']) || body['results'].length !== expected) {
    throw invalid(`app read returned a malformed reply (expected ${expected} results)`, body);
  }
  return body['results'].map((item) => parseReadResult(item, body));
}

function parseWriteResult(body: unknown, expected: number): AppApiWriteResult {
  if (!isObject(body) || !Array.isArray(body['revisions'])) {
    throw invalid('app API write returned an invalid receipt', body);
  }
  const revisions: string[] = [];
  for (const revision of body['revisions']) {
    if (typeof revision !== 'string' || revision.length === 0) {
      throw invalid('app API write returned an invalid revision', body);
    }
    revisions.push(revision);
  }
  if (body['written'] !== expected || revisions.length !== expected) {
    throw invalid('app API write returned an invalid written count', body);
  }
  return { written: expected, revisions };
}

function parseCount(value: JsonValue | undefined, label: string, body: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw invalid(`app API changes returned an invalid ${label}`, body);
  }
  return value;
}

function parseChange(value: JsonValue, body: unknown): AppApiChange {
  if (!isObject(value) || typeof value['schema'] !== 'string' || typeof value['deleted'] !== 'boolean') {
    throw invalid('app API changes returned an invalid entry', body);
  }
  return {
    seq: parseCount(value['seq'], 'seq', body),
    schema: value['schema'],
    key: parseKey(value['key'], body),
    deleted: value['deleted'],
  };
}

function parseChanges(body: unknown): AppApiChangesResult {
  if (
    !isObject(body) ||
    typeof body['id'] !== 'string' ||
    body['id'].length === 0 ||
    typeof body['gap'] !== 'boolean' ||
    !Array.isArray(body['changes'])
  ) {
    throw invalid('app API changes returned an invalid page', body);
  }
  return {
    id: body['id'],
    head: parseCount(body['head'], 'head', body),
    through: parseCount(body['through'], 'through', body),
    gap: body['gap'],
    changes: body['changes'].map((entry) => parseChange(entry, body)),
  };
}

export async function executeAppRead(
  ctx: ClientContext,
  reads: readonly AppApiRead[],
): Promise<AppApiReadResult[]> {
  const body = await postAppApi(ctx, READ_PATH, { reads: reads.map(readItemBody) });
  return parseReadResults(body, reads.length);
}

export async function executeAppWrite(
  ctx: ClientContext,
  mutations: readonly AppApiMutation[],
): Promise<AppApiWriteResult> {
  const body = await postAppApi(ctx, WRITE_PATH, { mutations: mutations.map(mutationBody) });
  return parseWriteResult(body, mutations.length);
}

export async function executeAppChanges(
  ctx: ClientContext,
  options: AppApiChangesOptions,
): Promise<AppApiChangesResult> {
  const body = await postAppApi(ctx, CHANGES_PATH, changesBody(options));
  return parseChanges(body);
}
