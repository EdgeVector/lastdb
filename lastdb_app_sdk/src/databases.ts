/**
 * Owner-socket database create, list, and select.
 *
 * Select does not open a session. {@link databaseScope} is the same
 * `/api/databases/<selector>` prefix `connect({ databaseId })` already uses.
 * The unscoped client remains the personal default.
 */

import { RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type { Transport } from './transport.js';

const DATABASES_PATH = '/api/databases';
const SELECTOR = /^[A-Za-z0-9_-]{1,64}$/;
const UUID_HYPHENATED =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const UUID_SIMPLE = /^[0-9a-fA-F]{32}$/;
/** Versioned spec binding stored on an owned database. */
export interface DatabaseSpec {
  name: string;
  version: number;
}

/** One row from `GET /api/databases`, including personal and received shares. */
export interface ListedDatabase {
  id: string;
  name: string | null;
  kind: string | null;
  spec: DatabaseSpec | null;
  access: string;
  state: string;
  publisher?: string;
  snapshot?: string | null;
  expires_ms?: number;
}

/** Body of `POST /api/databases`. Omit kind for a shareable database. */
export interface CreateDatabaseRequest {
  name: string;
  kind?: 'personal' | 'shareable';
  spec?: DatabaseSpec | null;
}

/** Path prefix for one selected database, or null for the personal default. */
export function databaseScope(databaseId: string | undefined): string | null {
  if (databaseId === undefined) {
    return null;
  }
  if (!SELECTOR.test(databaseId)) {
    throw new RequestRejectedError('database_id', 'Invalid LastDB database ID or alias');
  }
  return `${DATABASES_PATH}/${databaseId}`;
}

/** `GET /api/databases` on the owner socket. */
export async function listNodeDatabases(transport: Transport): Promise<ListedDatabase[]> {
  const res = await transport.send('GET', DATABASES_PATH);
  if (res.status !== 200 || !isObject(res.body) || !Array.isArray(res.body['databases'])) {
    throw refusal('database list', res.status, res.body);
  }
  return res.body['databases'].map(parseListedDatabase);
}

/** `POST /api/databases` on the owner socket. */
export async function createNodeDatabase(
  transport: Transport,
  request: CreateDatabaseRequest,
): Promise<ListedDatabase> {
  const body = createBody(request);
  const res = await transport.send('POST', DATABASES_PATH, { body });
  if (res.status !== 200 || !isObject(res.body)) {
    throw refusal('database create', res.status, res.body);
  }
  return parseListedDatabase(res.body['database']);
}

function createBody(request: CreateDatabaseRequest): CreateDatabaseRequest {
  const name = request.name;
  if (!SELECTOR.test(name) || name.toLowerCase() === 'personal' || isUuidName(name)) {
    throw new RequestRejectedError('invalid_request', 'invalid database name');
  }
  const spec = request.spec ?? null;
  if (spec !== null && !validSpec(spec)) {
    throw new RequestRejectedError('invalid_request', 'invalid spec binding');
  }
  const kind = request.kind;
  if (kind !== undefined && kind !== 'personal' && kind !== 'shareable') {
    throw new RequestRejectedError('invalid_request', 'invalid database access policy');
  }
  if (kind === 'personal' && spec !== null) {
    throw new RequestRejectedError('invalid_request', 'private databases cannot bind a share spec');
  }
  const body = kind === undefined ? { name } : { name, kind };
  return spec === null ? body : { ...body, spec };
}

function validSpec(spec: DatabaseSpec): boolean {
  return SELECTOR.test(spec.name)
    && Number.isInteger(spec.version)
    && spec.version >= 1
    && spec.version <= 4_294_967_295;
}

function isUuidName(value: string): boolean {
  return UUID_HYPHENATED.test(value) || UUID_SIMPLE.test(value);
}

export function parseListedDatabase(value: unknown): ListedDatabase {
  if (!isObject(value) || typeof value['id'] !== 'string' || value['id'].length === 0) {
    throw new UnexpectedResponseError('database list returned a row with no id', 200, value);
  }
  const name = value['name'];
  if (!(name === null || typeof name === 'string')) {
    throw new UnexpectedResponseError('database list returned a row with no name', 200, value);
  }
  if (typeof value['access'] !== 'string' || typeof value['state'] !== 'string') {
    throw new UnexpectedResponseError('database list returned a row with no access', 200, value);
  }
  const listed: ListedDatabase = {
    id: value['id'],
    name,
    kind: typeof value['kind'] === 'string' ? value['kind'] : null,
    spec: parseSpec(value['spec']),
    access: value['access'],
    state: value['state'],
  };
  if (typeof value['publisher'] === 'string') {
    listed.publisher = value['publisher'];
  }
  if (value['snapshot'] === null || typeof value['snapshot'] === 'string') {
    listed.snapshot = value['snapshot'];
  }
  if (typeof value['expires_ms'] === 'number') {
    listed.expires_ms = value['expires_ms'];
  }
  return listed;
}

function parseSpec(value: unknown): DatabaseSpec | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (!isObject(value) || typeof value['name'] !== 'string' || typeof value['version'] !== 'number') {
    throw new UnexpectedResponseError('database list returned an invalid spec binding', 200, value);
  }
  return { name: value['name'], version: value['version'] };
}

function refusal(verb: string, status: number, body: unknown): Error {
  const message = messageFrom(body, `${verb} returned ${status}`);
  if (status === 400 || status === 409 || status === 404) {
    return new RequestRejectedError(status === 409 ? 'conflict' : 'invalid_request', message, body);
  }
  return new UnexpectedResponseError(message, status, body);
}

function messageFrom(body: unknown, fallback: string): string {
  if (typeof body === 'string' && body.trim().length > 0) {
    return body.trim();
  }
  if (isObject(body)) {
    const error = body['error'] ?? body['message'];
    if (typeof error === 'string' && error.length > 0) {
      return error;
    }
  }
  return fallback;
}
