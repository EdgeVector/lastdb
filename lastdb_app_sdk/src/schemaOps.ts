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
import {
  isObject,
  parseAutoIdentityResponse,
  parseGetSchemaResponse,
  parseNodeVersion,
  parseSchemaListResponse,
  resolveLoadedSchema,
} from './responseParsers.js';
import type {
  AutoIdentityResult,
  DeclareSchemaRequest,
  DeclareSchemaResult,
  LoadedSchema,
  NodeVersion,
  SchemaDescriptor,
} from './types.js';

const DEFINITION_ID_PATTERN = /^[0-9a-f]{64}$/;

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
