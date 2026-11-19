/**
 * App API: `POST /v1/read`, `POST /v1/write`, `POST /v1/x/changes`, and `POST /v1/x/databases`.
 * A refused reply throws {@link AppApiError}. A failed read in a list is a result.
 * Request and reply types live in `appApi.generated.ts`.
 */

import { appApiFailure } from './appApiError.js';
import {
  APP_API_ERROR_CODES,
  APP_API_ERROR_REASONS,
} from './appApi.generated.js';
import type * as Api from './appApi.generated.js';
import type { ClientContext } from './databases.js';
import { UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type { JsonValue } from './types.js';

export { AppApiError } from './appApiError.js';

/** Reply from `POST /api/apps/register`. `app` is the app id. There is no expiry. */
export interface AppRegistration {
  app: string;
}

const READ_PATH = '/v1/read';
const WRITE_PATH = '/v1/write';
const CHANGES_PATH = '/v1/x/changes';
const DATABASES_PATH = '/v1/x/databases';

/** Owner JSON for one `databases` action. `submit` is the 202 reply. */
export type DatabasesReplyFor<T extends Api.DatabasesRequest> =
  T extends { action: 'list' } ? Api.DatabasesListReply
  : T extends { action: 'create' | 'accept' } ? Api.DatabasesDatabaseReply
  : T extends { action: 'schemas' } ? Api.DatabasesSchemasReply
  : T extends { action: 'declare_schema' } ? Api.DatabasesDeclareReply
  : T extends { action: 'share_config' } ? Api.DatabasesShareConfigReply
  : T extends { action: 'set_share_config' } ? Api.DatabasesShareUpdateReply
  : T extends { action: 'invite' } ? Api.DatabasesInviteReply
  : T extends { action: 'share_link' } ? Api.DatabasesShareLinkReply
  : T extends { action: 'submit' } ? Api.DatabasesSubmitReply
  : T extends { action: 'submission_status' } ? Api.DatabasesStatusReply
  : never;

function invalid(message: string, body: unknown): UnexpectedResponseError {
  return new UnexpectedResponseError(message, 200, body);
}

function listed(value: string, options: readonly string[]): boolean {
  return options.includes(value);
}

async function postAppApi(ctx: ClientContext, path: string, body: unknown): Promise<unknown> {
  const app = process.env.LASTDB_APP; // the app id, set by the launcher
  const opts = app ? { body, headers: { 'X-LastDB-App': app } } : { body };
  const res = await ctx.transport.send('POST', path, opts);
  if (res.status !== 200) throw appApiFailure(res.status, res.body);
  return res.body;
}

function keyBody(key: Api.KeyInput): Record<string, string | null> {
  const body: Record<string, string | null> = {};
  if (key.hash !== undefined) body.hash = key.hash;
  if (key.range !== undefined) body.range = key.range;
  return body;
}

function rangeBody(range: Api.Range): Record<string, string> {
  const body: Record<string, string> = { hash: range.hash };
  if (typeof range.prefix === 'string') body.prefix = range.prefix;
  if (typeof range.start === 'string') body.start = range.start;
  if (typeof range.end === 'string') body.end = range.end;
  return body;
}

function readItemBody(item: Api.ReadItem): Record<string, unknown> {
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

function fileLinkBody(link: Api.FileLink): Record<string, unknown> {
  const body: Record<string, unknown> = { path: link.path };
  if (link.name !== undefined) body.name = link.name;
  if (link.media_type !== undefined) body.media_type = link.media_type;
  return body;
}

function alternatesBody(alternates: Api.Alternates): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(alternates)) {
    body[name] = value === null ? null : keyBody(value);
  }
  return body;
}

function mutationBody(mutation: Api.WriteItem): Record<string, unknown> {
  const body: Record<string, unknown> = {
    schema: mutation.schema,
    op: mutation.op,
    key: keyBody(mutation.key),
  };
  if (mutation.database !== undefined) body.database = mutation.database;
  if (mutation.op === 'delete') {
    body.revision = mutation.revision;
    if (mutation.alternates !== undefined) body.alternates = mutation.alternates;
    return body;
  }
  body.record = mutation.record;
  if (mutation.alternates !== undefined) body.alternates = alternatesBody(mutation.alternates);
  if (mutation.file !== undefined) {
    body.file = mutation.file === null ? null : fileLinkBody(mutation.file);
  }
  if (mutation.op === 'replace') body.revision = mutation.revision;
  return body;
}

function changesBody(options: Api.ChangesRequest): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  if (options.after !== undefined) body.after = options.after;
  if (options.limit !== undefined) body.limit = options.limit;
  if (options.schemas !== undefined) body.schemas = options.schemas;
  if (options.database !== undefined) body.database = options.database;
  return body;
}

function parseKey(value: JsonValue | undefined, body: unknown): Api.Key {
  if (!isObject(value) || !('hash' in value) || !('range' in value)) {
    throw invalid('app API returned an invalid key', body);
  }
  const hash = value['hash'];
  const range = value['range'];
  if (
    (hash !== null && typeof hash !== 'string') ||
    (range !== null && typeof range !== 'string')
  ) {
    throw invalid('app API returned an invalid key', body);
  }
  return { hash, range };
}

function parseCursor(value: JsonValue | undefined, body: unknown): string | null {
  if (value === null) return null;
  if (typeof value === 'string') return value;
  throw invalid('app API returned an invalid cursor', body);
}

function textOrNull(value: JsonValue | undefined, body: unknown): string | null {
  if (value === null) return null;
  if (typeof value === 'string') return value;
  throw invalid('app API returned an invalid file', body);
}

function parseFile(value: JsonValue | undefined, body: unknown): Api.FileInfo | null {
  if (value === undefined) throw invalid('app API returned a record with no file', body);
  if (value === null) return null;
  if (
    !isObject(value) ||
    !('name' in value) ||
    !('media_type' in value) ||
    !('size' in value) ||
    !('local' in value)
  ) {
    throw invalid('app API returned an invalid file', body);
  }
  const size = value['size'];
  const local = value['local'];
  if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0) {
    throw invalid('app API returned an invalid file', body);
  }
  if (local !== null && typeof local !== 'string') {
    throw invalid('app API returned an invalid file', body);
  }
  const file: Api.FileInfo = {
    name: textOrNull(value['name'], body),
    media_type: textOrNull(value['media_type'], body),
    size,
    local: typeof local === 'string' ? local : null,
  };
  if ('path' in value) {
    const path = value['path'];
    if (path !== null && typeof path !== 'string') throw invalid('app API returned an invalid file', body);
    file.path = path === null ? null : path;
  }
  return file;
}

/** A missing key is absent. Null is not the contract: the node omits the key. */
function parseAuthor(value: JsonValue | undefined, body: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value === 'string' && value.length > 0) return value;
  throw invalid('app API returned an invalid record', body);
}

function parseRecord(value: JsonValue, body: unknown): Api.Record {
  if (!isObject(value) || typeof value['revision'] !== 'string' || value['revision'].length === 0) {
    throw invalid('app API returned an invalid record', body);
  }
  if (!isObject(value['record'])) throw invalid('app API returned an invalid record', body);
  const record: Api.Record = {
    key: parseKey(value['key'], body),
    revision: value['revision'],
    record: value['record'],
    file: parseFile(value['file'], body),
  };
  const createdBy = parseAuthor(value['created_by'], body);
  const updatedBy = parseAuthor(value['updated_by'], body);
  if (createdBy !== undefined) record.created_by = createdBy;
  if (updatedBy !== undefined) record.updated_by = updatedBy;
  return record;
}

function parseItemError(value: Record<string, JsonValue>, body: unknown): Api.Error {
  const code = value['code'];
  const message = value['message'];
  if (typeof code !== 'string' || !listed(code, APP_API_ERROR_CODES) || typeof message !== 'string') {
    throw invalid('app API returned an invalid read error', body);
  }
  const error: Api.Error = { code: code as Api.ErrorCode, message };
  if ('index' in value) {
    const index = value['index'];
    if (typeof index !== 'number' || !Number.isSafeInteger(index) || index < 0) {
      throw invalid('app API returned an invalid read error', body);
    }
    error.index = index;
  }
  if ('reason' in value) {
    const reason = value['reason'];
    if (typeof reason !== 'string' || !listed(reason, APP_API_ERROR_REASONS)) {
      throw invalid('app API returned an invalid read error', body);
    }
    error.reason = reason as Api.ErrorReason;
  }
  return error;
}

function parseReadResult(value: JsonValue, body: unknown): Api.ReadResult {
  if (!isObject(value)) throw invalid('app API returned an invalid read result', body);
  if ('error' in value) {
    const error = value['error'];
    if (!isObject(error)) throw invalid('app API returned an invalid read error', body);
    return { error: parseItemError(error, body) };
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

function parseReadResults(body: unknown, expected: number): Api.ReadResult[] {
  if (!isObject(body) || !Array.isArray(body['results']) || body['results'].length !== expected) {
    throw invalid(`app read returned a malformed reply (expected ${expected} results)`, body);
  }
  return body['results'].map((item) => parseReadResult(item, body));
}

function parseWriteResult(body: unknown, expected: number): Api.WriteResult {
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

function parseChange(value: JsonValue, body: unknown): Api.ChangeEntry {
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

function parseChanges(body: unknown): Api.ChangesReply {
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
  reads: readonly Api.ReadItem[],
): Promise<Api.ReadResult[]> {
  const body = await postAppApi(ctx, READ_PATH, { reads: reads.map(readItemBody) });
  return parseReadResults(body, reads.length);
}

export async function executeAppWrite(
  ctx: ClientContext,
  mutations: readonly Api.WriteItem[],
): Promise<Api.WriteResult> {
  const body = await postAppApi(ctx, WRITE_PATH, { mutations: mutations.map(mutationBody) });
  return parseWriteResult(body, mutations.length);
}

export async function executeAppChanges(
  ctx: ClientContext,
  options: Api.ChangesRequest,
): Promise<Api.ChangesReply> {
  const body = await postAppApi(ctx, CHANGES_PATH, changesBody(options));
  return parseChanges(body);
}

/** `POST /v1/x/databases`. Accepts 200 and 202. The body is the owner JSON. */
export async function executeAppDatabases<T extends Api.DatabasesRequest>(
  ctx: ClientContext,
  body: T,
  timeoutMs?: number,
): Promise<DatabasesReplyFor<T>> {
  const app = process.env.LASTDB_APP; // the app id, set by the launcher
  const headers = app ? { 'X-LastDB-App': app } : undefined;
  const res = await ctx.transport.send('POST', DATABASES_PATH, { body, headers, timeoutMs });
  if (res.status !== 200 && res.status !== 202) throw appApiFailure(res.status, res.body);
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError(
      'app API databases returned a reply that is not an object',
      res.status,
      res.body,
    );
  }
  return res.body as DatabasesReplyFor<T>;
}
