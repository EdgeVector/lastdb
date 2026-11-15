/**
 * Node compatibility handshake, owner identity, and schema catalog operations.
 */

import {
  LASTDB_API_ROUTES,
  schemaRoute,
  scopedApiPath,
} from './apiRoutes.js';
import { mapDataError, type ClientContext } from './clientHelpers.js';
import { NodeTooOldError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type {
  AutoIdentityResult,
  DeclareSchemaRequest,
  DeclareSchemaResult,
  JsonValue,
  LoadedSchema,
  NodeVersion,
  SchemaDescriptor,
} from './types.js';

const DEFINITION_ID_PATTERN = /^[0-9a-f]{64}$/;

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((field): field is string => typeof field === 'string')
    : [];
}

/** Parse a `GET /api/version` 200 body. */
function parseNodeVersion(body: unknown): NodeVersion {
  const b = isObject(body) ? body : {};
  const apiVersion = b['api_version'];
  if (typeof apiVersion !== 'number' || !Number.isInteger(apiVersion) || apiVersion < 0) {
    throw new UnexpectedResponseError('version returned no api_version', 200, body);
  }
  const capabilities: Record<string, boolean> = {};
  if (isObject(b['capabilities'])) {
    for (const [k, v] of Object.entries(b['capabilities'])) {
      if (typeof v === 'boolean') capabilities[k] = v;
    }
  }
  const minApi = b['min_supported_api_version'];
  const build = b['build'];
  const instanceId = b['instance_id'];
  return {
    apiVersion,
    build: typeof build === 'string' && build.length > 0 ? build : null,
    capabilities,
    instanceId: typeof instanceId === 'string' && instanceId.length > 0 ? instanceId : null,
    minSupportedApiVersion:
      typeof minApi === 'number' && Number.isInteger(minApi) && minApi >= 0 ? minApi : null,
  };
}

/** Parse `GET /api/system/auto-identity` success JSON. */
function parseAutoIdentityResponse(body: unknown): AutoIdentityResult {
  const b = isObject(body) ? body : {};
  const userHash = b['user_hash'];
  if (typeof userHash !== 'string' || userHash.length === 0) {
    throw new UnexpectedResponseError(
      'auto-identity returned a provisioned body without user_hash',
      200,
      body,
    );
  }
  return {
    provisioned: true,
    userHash,
    publicKey: stringOrNull(b['public_key']),
    userId: stringOrNull(b['user_id']),
  };
}

function parseLoadedSchema(schema: Record<string, JsonValue>): LoadedSchema {
  const name = stringOrNull(schema['name']) ?? '';
  const identityHash = stringOrNull(schema['identity_hash']) ?? name;
  return {
    name,
    identityHash,
    descriptiveName: stringOrNull(schema['descriptive_name']),
    ownerAppId: stringOrNull(schema['owner_app_id']),
    fields: stringArray(schema['fields']),
  };
}

function parseSchemaListResponse(body: unknown): LoadedSchema[] {
  const b = isObject(body) ? body : {};
  const rawSchemas = Array.isArray(b['schemas']) ? b['schemas'] : [];
  return rawSchemas.filter(isObject).map(parseLoadedSchema);
}

function parseGetSchemaResponse(body: unknown): LoadedSchema {
  if (!isObject(body) || !isObject(body['schema'])) {
    throw new UnexpectedResponseError('schema lookup returned no schema object', 200, body);
  }
  return parseLoadedSchema(body['schema']);
}

function sameFieldSet(actual: readonly string[], expected: readonly string[]): boolean {
  if (actual.length !== expected.length) return false;
  const actualSorted = [...actual].sort();
  const expectedSorted = [...expected].sort();
  return actualSorted.every((field, i) => field === expectedSorted[i]);
}

function resolveLoadedSchema(
  schemas: readonly LoadedSchema[],
  descriptor: SchemaDescriptor,
): LoadedSchema | null {
  const matches = schemas.filter((schema) => {
    if (schema.ownerAppId !== descriptor.ownerAppId) return false;
    if (schema.descriptiveName !== descriptor.descriptiveName) return false;
    return descriptor.fields === undefined || sameFieldSet(schema.fields, descriptor.fields);
  });
  if (matches.length === 0) return null;
  if (matches.length > 1) {
    throw new UnexpectedResponseError(
      `schema descriptor matched ${matches.length} loaded schemas`,
      200,
      matches,
    );
  }
  return matches[0] ?? null;
}

/** Record a declared schema's 64-hex `definition_id` for subsequent mutations. */
export function rememberSchemaDefinition(
  definitionIds: Map<string, string>,
  schemaName: string,
  definitionId: string,
): void {
  if (!DEFINITION_ID_PATTERN.test(definitionId)) {
    throw new UnexpectedResponseError(
      'definition id must be a lowercase SHA-256',
      0,
      definitionId,
    );
  }
  definitionIds.set(schemaName, definitionId);
}

/** `GET /api/version` — client↔node compatibility handshake. */
export async function executeVersion(ctx: ClientContext): Promise<NodeVersion> {
  const res = await ctx.transport.send('GET', LASTDB_API_ROUTES.version);
  if (res.status !== 200) {
    throw mapDataError('version', res.status, res.body, ctx.appId);
  }
  return parseNodeVersion(res.body);
}

/** Verify that the connected node reports `api_version >= required`. */
export async function executeRequireApiVersion(
  ctx: ClientContext,
  required: number,
  appLabel?: string,
): Promise<NodeVersion> {
  const node = await executeVersion(ctx);
  if (node.apiVersion < required) {
    throw new NodeTooOldError({
      reason: 'api_version_below_required',
      required,
      reported: node.apiVersion,
      build: node.build,
      app: appLabel ?? ctx.appId,
    });
  }
  return node;
}

/** `GET /api/system/auto-identity` — resolve the local owner identity. */
export async function executeAutoIdentity(ctx: ClientContext): Promise<AutoIdentityResult> {
  const res = await ctx.transport.send('GET', LASTDB_API_ROUTES.autoIdentity);
  if (res.status === 200) {
    return parseAutoIdentityResponse(res.body);
  }
  if (res.status === 503) {
    const body = isObject(res.body) ? res.body : {};
    return {
      provisioned: false,
      reason: typeof body['error'] === 'string' ? body['error'] : 'node_not_provisioned',
      next: typeof body['next'] === 'string' ? body['next'] : null,
    };
  }
  throw mapDataError('auto-identity', res.status, res.body, ctx.appId);
}

/** `POST /api/schemas/declare` (or database-scoped equivalent). */
export async function executeDeclareSchema(
  ctx: ClientContext,
  request: DeclareSchemaRequest,
): Promise<DeclareSchemaResult> {
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.schemasDeclare);
  const res = await ctx.transport.send('POST', path, { body: request });
  if (res.status !== 200) {
    throw mapDataError('declare schema', res.status, res.body, ctx.appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError('declare schema returned a non-object body', 200, res.body);
  }
  const definitionId = res.body['definition_id'];
  const schemaName = res.body['schema'];
  if (
    typeof definitionId !== 'string' ||
    typeof schemaName !== 'string' ||
    schemaName.length === 0
  ) {
    throw new UnexpectedResponseError('declare returned no definition id', 200, res.body);
  }
  rememberSchemaDefinition(ctx.definitionIds, schemaName, definitionId);
  const identity = res.body['identity_hash'];
  const resolution = res.body['resolution'];
  return {
    schemaName,
    definitionId,
    identityHash: typeof identity === 'string' ? identity : null,
    resolution: typeof resolution === 'string' ? resolution : null,
  };
}

/** `GET /api/schemas` (or database-scoped equivalent). */
export async function executeListSchemas(ctx: ClientContext): Promise<LoadedSchema[]> {
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.schemas);
  const res = await ctx.transport.send('GET', path);
  if (res.status === 200) {
    return parseSchemaListResponse(res.body);
  }
  throw mapDataError('schemas list', res.status, res.body, ctx.appId);
}

/** `GET /api/schema/<encoded>` (or database-scoped equivalent). Returns `null` on `404`. */
export async function executeGetSchema(
  ctx: ClientContext,
  schemaName: string,
): Promise<LoadedSchema | null> {
  const path = schemaRoute(ctx.databasePath, schemaName);
  const res = await ctx.transport.send('GET', path);
  if (res.status === 200) {
    return parseGetSchemaResponse(res.body);
  }
  if (res.status === 404) {
    return null;
  }
  throw mapDataError('get schema', res.status, res.body, ctx.appId);
}

/** Resolve an app-owned schema descriptor against `listSchemas()`. */
export async function executeResolveSchema(
  ctx: ClientContext,
  descriptor: SchemaDescriptor,
): Promise<LoadedSchema | null> {
  return resolveLoadedSchema(await executeListSchemas(ctx), descriptor);
}
