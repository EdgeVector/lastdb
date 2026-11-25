/**
 * Canonical API routes, database-scope path builders, and owner-socket database
 * catalog, creation, and spec binding.
 */

import { RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { isObject, sendJsonObject } from './responseParsers.js';
import type { Transport } from './transport.js';

/** Canonical LastDB runtime API route strings used by the SDK. */
export const LASTDB_API_ROUTES = {
  /** Owner socket. Scoped to `/api/databases/{id}/submissions`. A proposal, not a record write. */
  shareSubmission: '/api/submissions',
  shareSubmissionStatus: '/api/submissions/status',
  /** Owner socket. Scoped to `/api/databases/{id}/change`. One change that applies or queues. */
  change: '/api/change',
  autoIdentity: '/api/system/auto-identity',
  schemas: '/api/schemas',
  /** Single schema lookup prefix (`/api/schema/<encoded-name>`). */
  schemaPrefix: '/api/schema',
  /** Owner socket database catalog and create endpoint. */
  databases: '/api/databases',
  /** Owner socket owned-database spec binding endpoint. */
  databasesSpec: '/api/databases/spec',
  /** Owner socket open-material acceptance endpoint. */
  databasesAccept: '/api/databases/accept',
  /** The client↔node compatibility handshake. */
  version: '/api/version',
} as const;

/** Shared runtime context passed to domain operation modules. */
export interface ClientContext {
  readonly appId: string;
  readonly transport: Transport;
  readonly databasePath: string | null;
}

const DATABASE_SELECTOR_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/** True when `value` matches `lastdbd`'s `valid_selector` grammar (`[A-Za-z0-9_-]{1,64}`). */
function isValidDatabaseSelector(value: string): boolean {
  return DATABASE_SELECTOR_PATTERN.test(value);
}

/** Validate a database ID or alias against `lastdbd`'s selector grammar. */
export function validateDatabaseSelector(databaseId: string): string {
  if (!isValidDatabaseSelector(databaseId)) {
    throw new RequestRejectedError('database_id', 'Invalid LastDB database ID or alias');
  }
  return databaseId;
}

/** Path prefix (`/api/databases/<selector>`) for a selected database, or `null` for the personal default. */
export function databaseScope(databaseId: string | undefined): string | null {
  return databaseId === undefined
    ? null
    : `${LASTDB_API_ROUTES.databases}/${validateDatabaseSelector(databaseId)}`;
}

/** Resolve an `/api/*` data route against an optional database scope and query string. */
export function scopedApiPath(
  scope: string | null,
  route: string,
  queryString?: string,
): string {
  const base =
    scope === null
      ? route
      : `${scope}${route.startsWith('/api/') ? route.slice('/api'.length) : route}`;
  return queryString && queryString.length > 0 ? `${base}?${queryString}` : base;
}

/** Build the single-schema metadata route (`/api/schema/<encoded>` or scoped equivalent). */
export function schemaRoute(scope: string | null, schemaName: string): string {
  const trimmed = schemaName.trim();
  if (trimmed.length === 0) {
    throw new RequestRejectedError('invalid_request', 'schema name must not be empty');
  }
  return scopedApiPath(
    scope,
    `${LASTDB_API_ROUTES.schemaPrefix}/${encodeURIComponent(trimmed)}`,
  );
}

const UUID_HYPHENATED =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const UUID_SIMPLE = /^[0-9a-fA-F]{32}$/;

/** The newest write lease held in a received database's signed history. */
export interface SharedMemberLease {
  entryHash: string;
  authoredAtMs: number;
  expiresAtMs: number;
  closesAtMs: number;
  covers: boolean;
  directWrites: boolean;
  directWritesUntilMs: number | null;
}

/** This node's role in a received database, from the folded signed grants. */
export interface SharedDatabaseAccess {
  role: 'read' | 'submit' | 'write';
  lease?: SharedMemberLease | null;
}

/** Versioned spec binding stored on an owned database. */
export interface DatabaseSpec {
  name: string;
  version: number;
}

export type PublishOutcome = 'confirmed' | 'refused' | 'unconfirmed';

export interface PublishAttempt {
  at: string;
  outcome: PublishOutcome;
  status: number | string | null;
}

export interface ReceivedPublish {
  pending: number | null;
  lastAttempt: PublishAttempt | null;
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
  leaseLengthMs?: number;
  selfAccess?: SharedDatabaseAccess;
  publish?: ReceivedPublish;
  publisher?: string;
  snapshot?: string | null;
  expires_ms?: number;
}

/** Body of `POST /api/databases`. Omit `kind` for a shareable database. */
export interface CreateDatabaseRequest {
  name: string;
  kind?: 'personal' | 'shareable';
  /** Whole-database sharing only. Filtered or app catalog specs are refused. */
  sharingMode?: 'whole_database';
}

/** `GET /api/databases` on the owner socket. */
export async function listNodeDatabases(
  transport: Transport,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase[]> {
  const body = await sendJsonObject(transport, 'GET', LASTDB_API_ROUTES.databases, 'database list', appId);
  if (!Array.isArray(body['databases'])) {
    throw new UnexpectedResponseError('database list returned no databases array', 200, body);
  }
  return body['databases'].map(parseListedDatabase);
}

/** `POST /api/databases` on the owner socket. */
export async function createNodeDatabase(
  transport: Transport,
  request: CreateDatabaseRequest,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase> {
  const body = await sendJsonObject(
    transport,
    'POST',
    LASTDB_API_ROUTES.databases,
    'database create',
    appId,
    { body: buildCreateBody(request) },
  );
  return parseListedDatabase(body['database']);
}

/** `POST /api/databases/spec` — clear an owned database's binding. Attaching a filtered spec is refused. */
export async function bindNodeDatabaseSpec(
  transport: Transport,
  databaseId: string,
  spec: DatabaseSpec | null,
  appId = 'lastdb-sdk',
): Promise<ListedDatabase> {
  validateDatabaseSelector(databaseId);
  if (spec !== null) {
    throw new RequestRejectedError(
      'invalid_request',
      'filtered share specs are not supported',
    );
  }
  const body = await sendJsonObject(
    transport,
    'POST',
    LASTDB_API_ROUTES.databasesSpec,
    'database bind spec',
    appId,
    { body: { id: databaseId, spec: null } },
  );
  return parseListedDatabase(body['database']);
}

function buildCreateBody(request: CreateDatabaseRequest): CreateDatabaseRequest {
  const name = request.name;
  if (!isValidDatabaseSelector(name) || name.toLowerCase() === 'personal' || isUuidName(name)) {
    throw new RequestRejectedError('invalid_request', 'invalid database name');
  }
  const kind = request.kind;
  if (kind !== undefined && kind !== 'personal' && kind !== 'shareable') {
    throw new RequestRejectedError('invalid_request', 'invalid database access policy');
  }
  const sharingMode = request.sharingMode;
  if (sharingMode !== undefined && sharingMode !== 'whole_database') {
    throw new RequestRejectedError('invalid_request', 'invalid database sharing mode');
  }
  if (kind === 'personal' && sharingMode !== undefined) {
    throw new RequestRejectedError(
      'invalid_request',
      'private databases cannot use whole-database sharing',
    );
  }
  const body: CreateDatabaseRequest = kind === undefined ? { name } : { name, kind };
  if (sharingMode !== undefined) {
    body.sharingMode = sharingMode;
  }
  return body;
}

function isUuidName(value: string): boolean {
  return UUID_HYPHENATED.test(value) || UUID_SIMPLE.test(value);
}

function parseListedDatabase(value: unknown): ListedDatabase {
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
  if (value['leaseLengthMs'] !== undefined) {
    const length = value['leaseLengthMs'];
    if (typeof length !== 'number' || !Number.isSafeInteger(length) || length <= 0) {
      throw new UnexpectedResponseError('database list returned an invalid lease length', 200, value);
    }
    listed.leaseLengthMs = length;
  }
  if (value['selfAccess'] !== undefined) {
    listed.selfAccess = parseSharedDatabaseAccess(value['selfAccess']);
  }
  if (value['publish'] !== undefined) {
    listed.publish = parseReceivedPublish(value['publish']);
  }
  if (value['snapshot'] === null || typeof value['snapshot'] === 'string') {
    listed.snapshot = value['snapshot'];
  }
  if (typeof value['expires_ms'] === 'number') {
    listed.expires_ms = value['expires_ms'];
  }
  return listed;
}

function parseReceivedPublish(value: unknown): ReceivedPublish {
  if (!isObject(value)) {
    throw new UnexpectedResponseError(
      'database list returned an invalid publish state',
      200,
      value,
    );
  }
  const pending = value['pending'];
  if (
    pending !== null &&
    (typeof pending !== 'number' || !Number.isInteger(pending) || pending < 0)
  ) {
    throw new UnexpectedResponseError(
      'database list returned an invalid publish state',
      200,
      value,
    );
  }
  const last = value['lastAttempt'];
  if (last === null) {
    return { pending, lastAttempt: null };
  }
  if (!isObject(last)) {
    throw new UnexpectedResponseError(
      'database list returned an invalid publish attempt',
      200,
      value,
    );
  }
  const at = last['at'];
  const outcome = last['outcome'];
  const status = last['status'];
  if (typeof at !== 'string' || !publishOutcome(outcome) || !publishStatus(status)) {
    throw new UnexpectedResponseError(
      'database list returned an invalid publish attempt',
      200,
      value,
    );
  }
  return { pending, lastAttempt: { at, outcome, status } };
}

function publishStatus(value: unknown): value is number | string | null {
  return value === null || typeof value === 'string' || (typeof value === 'number' && Number.isInteger(value));
}

function publishOutcome(value: unknown): value is PublishOutcome {
  return value === 'confirmed' || value === 'refused' || value === 'unconfirmed';
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

function nonNegativeInt(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function parseSharedMemberLease(value: unknown): SharedMemberLease {
  if (isObject(value)) {
    const { entryHash, authoredAtMs, expiresAtMs, closesAtMs, covers, directWrites, directWritesUntilMs } = value;
    if (
      typeof entryHash === 'string' &&
      entryHash.length > 0 &&
      nonNegativeInt(authoredAtMs) &&
      nonNegativeInt(expiresAtMs) &&
      nonNegativeInt(closesAtMs) &&
      typeof covers === 'boolean' &&
      typeof directWrites === 'boolean' &&
      (directWritesUntilMs === null || nonNegativeInt(directWritesUntilMs))
    ) {
      return { entryHash, authoredAtMs, expiresAtMs, closesAtMs, covers, directWrites, directWritesUntilMs };
    }
  }
  throw new UnexpectedResponseError('database returned an invalid member lease', 200, value);
}

function parseSharedDatabaseAccess(value: unknown): SharedDatabaseAccess {
  if (isObject(value)) {
    const { role } = value;
    if (role === 'read' || role === 'submit' || role === 'write') {
      const access: SharedDatabaseAccess = { role };
      if (value['lease'] !== undefined) {
        access.lease = value['lease'] === null ? null : parseSharedMemberLease(value['lease']);
      }
      return access;
    }
  }
  throw new UnexpectedResponseError('database returned an invalid recipient grant', 200, null);
}
