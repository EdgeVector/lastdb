import { UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';

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
    if ((state === 'active' || state === 'revoked' || state === 'absent' || state === 'unknown')
      && (role === 'read' || role === 'submit' || role === null)
      && decimalOrNull(revision) && decimalOrNull(confirmedAtMs) && typeof stale === 'boolean') {
      const access: SharedDatabaseAccess = { state, role, revision, confirmedAtMs, stale };
      const scope = parseScope(writeSchemas);
      if (scope !== undefined) access.writeSchemas = scope;
      return access;
    }
  }
  throw new UnexpectedResponseError('database returned an invalid recipient grant', 200, null);
}
