/**
 * Owner-socket database catalog, creation, spec binding, and session issuance.
 */

import {
  isValidDatabaseSelector,
  LASTDB_API_ROUTES,
  validateDatabaseSelector,
} from './apiRoutes.js';
import { mapDataError } from './clientHelpers.js';
import { RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type { Transport } from './transport.js';
import type { DatabaseSession, DatabaseSessionIntent } from './types.js';

const UUID_HYPHENATED =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const UUID_SIMPLE = /^[0-9a-fA-F]{32}$/;

/** Canonical names, legacy definition IDs, or all schemas. An empty list grants reads only. */
export type ShareWriteScope = { names: string[] } | string[] | 'all';

/** The last observed grant for this receiving node. Use it to display available actions. */
export interface SharedDatabaseAccess {
  state: 'unknown' | 'active' | 'revoked' | 'absent';
  role: 'read' | 'submit' | null;
  writeSchemas?: ShareWriteScope;
  revision: string | null;
  confirmedAtMs: string | null;
  stale: boolean;
}

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
  sharingMode?: 'whole_database';
  selfAccess?: SharedDatabaseAccess;
  publisher?: string;
  snapshot?: string | null;
  expires_ms?: number;
}

/** Body of `POST /api/databases`. Omit `kind` for a shareable database. */
export interface CreateDatabaseRequest {
  name: string;
  kind?: 'personal' | 'shareable';
  spec?: DatabaseSpec | null;
}

/** `GET /api/databases` on the owner socket. */
export async function listNodeDatabases(
  transport: Transport,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase[]> {
  const res = await transport.send('GET', LASTDB_API_ROUTES.databases);
  if (res.status !== 200) {
    throw mapDataError('database list', res.status, res.body, appId);
  }
  if (!isObject(res.body) || !Array.isArray(res.body['databases'])) {
    throw new UnexpectedResponseError('database list returned no databases array', 200, res.body);
  }
  return res.body['databases'].map(parseListedDatabase);
}

/** `POST /api/databases` on the owner socket. */
export async function createNodeDatabase(
  transport: Transport,
  request: CreateDatabaseRequest,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase> {
  const body = buildCreateBody(request);
  const res = await transport.send('POST', LASTDB_API_ROUTES.databases, { body });
  if (res.status !== 200) {
    throw mapDataError('database create', res.status, res.body, appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError('database create returned a non-object body', 200, res.body);
  }
  return parseListedDatabase(res.body['database']);
}

/** `POST /api/databases/spec` — bind or clear the versioned spec of an owned database. */
export async function bindNodeDatabaseSpec(
  transport: Transport,
  databaseId: string,
  spec: DatabaseSpec | null,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase> {
  validateDatabaseSelector(databaseId);
  if (spec !== null && !validSpec(spec)) {
    throw new RequestRejectedError('invalid_request', 'invalid spec binding');
  }
  const res = await transport.send('POST', LASTDB_API_ROUTES.databasesSpec, {
    body: { id: databaseId, spec },
  });
  if (res.status !== 200) {
    throw mapDataError('database bind spec', res.status, res.body, appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError(
      'database bind spec returned a non-object body',
      200,
      res.body,
    );
  }
  return parseListedDatabase(res.body['database']);
}

/**
 * `POST /api/databases/session` — issue an owner-approved short-lived session
 * restricted to `databaseId` and `intent` (`'read'` or `'write'`).
 */
export async function issueNodeDatabaseSession(
  transport: Transport,
  databaseId: string,
  intent: DatabaseSessionIntent,
  appId = 'lastdb-sdk',
): Promise<DatabaseSession> {
  validateDatabaseSelector(databaseId);
  if (intent !== 'read' && intent !== 'write') {
    throw new RequestRejectedError(
      'invalid_request',
      'database session intent must be read or write',
    );
  }
  const res = await transport.send('POST', LASTDB_API_ROUTES.databasesSession, {
    body: { databaseId, intent },
  });
  if (res.status !== 200) {
    throw mapDataError('database session', res.status, res.body, appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError('database session returned a non-object body', 200, null);
  }
  const session = res.body['session'];
  const expiresInSeconds = res.body['expiresInSeconds'];
  if (
    typeof session !== 'string' ||
    session.length === 0 ||
    typeof expiresInSeconds !== 'number' ||
    !Number.isSafeInteger(expiresInSeconds) ||
    expiresInSeconds <= 0
  ) {
    throw new UnexpectedResponseError('database session returned an invalid token envelope', 200, null);
  }
  return { databaseId, intent, session, expiresInSeconds };
}

function buildCreateBody(request: CreateDatabaseRequest): CreateDatabaseRequest {
  const name = request.name;
  if (!isValidDatabaseSelector(name) || name.toLowerCase() === 'personal' || isUuidName(name)) {
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
    throw new RequestRejectedError(
      'invalid_request',
      'private databases cannot bind a share spec',
    );
  }
  const body = kind === undefined ? { name } : { name, kind };
  return spec === null ? body : { ...body, spec };
}

function validSpec(spec: DatabaseSpec): boolean {
  return (
    isValidDatabaseSelector(spec.name) &&
    Number.isInteger(spec.version) &&
    spec.version >= 1 &&
    spec.version <= 4_294_967_295
  );
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
  if (value['sharingMode'] === 'whole_database') {
    listed.sharingMode = value['sharingMode'];
  }
  if (value['selfAccess'] !== undefined) {
    listed.selfAccess = parseSharedDatabaseAccess(value['selfAccess']);
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
  if (
    !isObject(value) ||
    typeof value['name'] !== 'string' ||
    typeof value['version'] !== 'number'
  ) {
    throw new UnexpectedResponseError('database list returned an invalid spec binding', 200, value);
  }
  return { name: value['name'], version: value['version'] };
}

function decimalOrNull(value: unknown): value is string | null {
  return value === null || (typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value));
}

function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item: unknown) => typeof item === 'string');
}

function parseScope(value: unknown): ShareWriteScope | undefined {
  if (value === undefined || value === 'all' || strings(value)) return value;
  if (isObject(value) && strings(value['names'])) return { names: value['names'] };
  throw new UnexpectedResponseError('database returned an invalid write scope', 200, null);
}

export function parseSharedDatabaseAccess(value: unknown): SharedDatabaseAccess {
  if (isObject(value)) {
    const { state, role, revision, confirmedAtMs, stale, writeSchemas } = value;
    if (
      (state === 'active' || state === 'revoked' || state === 'absent' || state === 'unknown') &&
      (role === 'read' || role === 'submit' || role === null) &&
      decimalOrNull(revision) &&
      decimalOrNull(confirmedAtMs) &&
      typeof stale === 'boolean'
    ) {
      const access: SharedDatabaseAccess = { state, role, revision, confirmedAtMs, stale };
      const scope = parseScope(writeSchemas);
      if (scope !== undefined) access.writeSchemas = scope;
      return access;
    }
  }
  throw new UnexpectedResponseError('database returned an invalid recipient grant', 200, null);
}
