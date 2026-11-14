/**
 * Parsers for node version, owner auto-identity, and schema catalog responses.
 */

import { UnexpectedResponseError } from './errors.js';
import type {
  AutoIdentityResult,
  JsonValue,
  LoadedSchema,
  NodeVersion,
  SchemaDescriptor,
} from './types.js';

function isPlainObject(v: unknown): v is Record<string, JsonValue> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((field): field is string => typeof field === 'string')
    : [];
}

/** Parse a `GET /api/version` 200 body. */
export function parseNodeVersion(body: unknown): NodeVersion {
  const b = isPlainObject(body) ? body : {};
  const apiVersion = b['api_version'];
  if (typeof apiVersion !== 'number' || !Number.isInteger(apiVersion) || apiVersion < 0) {
    throw new UnexpectedResponseError('version returned no api_version', 200, body);
  }
  const capabilities: Record<string, boolean> = {};
  if (isPlainObject(b['capabilities'])) {
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
export function parseAutoIdentityResponse(body: unknown): AutoIdentityResult {
  const b = isPlainObject(body) ? body : {};
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

/** Parse `GET /api/schemas` JSON into normalized loaded-schema entries. */
export function parseSchemaListResponse(body: unknown): LoadedSchema[] {
  const b = isPlainObject(body) ? body : {};
  const rawSchemas = Array.isArray(b['schemas']) ? b['schemas'] : [];
  return rawSchemas
    .filter((schema): schema is Record<string, JsonValue> => isPlainObject(schema))
    .map(parseLoadedSchema);
}

/** Parse `GET /api/schema/<name>` JSON into a normalized {@link LoadedSchema}. */
export function parseGetSchemaResponse(body: unknown): LoadedSchema {
  if (!isPlainObject(body) || !isPlainObject(body['schema'])) {
    throw new UnexpectedResponseError('schema lookup returned no schema object', 200, body);
  }
  return parseLoadedSchema(body['schema']);
}

/** Resolve a schema descriptor against an already-fetched schema list. */
export function resolveLoadedSchema(
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

export function parseLoadedSchema(schema: Record<string, JsonValue>): LoadedSchema {
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

export function sameFieldSet(actual: readonly string[], expected: readonly string[]): boolean {
  if (actual.length !== expected.length) return false;
  const actualSorted = [...actual].sort();
  const expectedSorted = [...expected].sort();
  return actualSorted.every((field, i) => field === expectedSorted[i]);
}
