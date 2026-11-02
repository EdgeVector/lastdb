/** Schema-resolution result and app<->node field-name mapping helpers. */

import {
  CasExpectation,
  JsonValue,
  QueryFieldPredicate,
  QueryResult,
  QueryRow,
  ResolveResult,
  SchemaResolverResult,
} from './types.js';

export interface ResolvedSchema {
  appSchemaName: string;
  identity: string;
  nodeSchemaName: string;
  appToNodeFields: Record<string, string>;
  nodeToAppFields: Record<string, string>;
  outcome: string | null;
}

export function normalizeSchemaResolution(
  appSchemaName: string,
  result: SchemaResolverResult,
): ResolvedSchema {
  if (typeof result === 'string') {
    return {
      appSchemaName,
      identity: result,
      nodeSchemaName: result,
      appToNodeFields: {},
      nodeToAppFields: {},
      outcome: null,
    };
  }
  if (isResolveResult(result)) {
    const appToNodeFields = result.adapter?.appToCatalog ?? {};
    return {
      appSchemaName,
      identity: result.identity,
      nodeSchemaName: result.schema ?? result.identity,
      appToNodeFields,
      nodeToAppFields:
        result.adapter?.catalogToApp ?? reverseFieldMap(appToNodeFields),
      outcome: result.outcome ?? null,
    };
  }
  const appToNodeFields = result?.fields ?? {};
  return {
    appSchemaName,
    identity: result?.nodeSchemaName ?? appSchemaName,
    nodeSchemaName: result?.nodeSchemaName ?? appSchemaName,
    appToNodeFields,
    nodeToAppFields: reverseFieldMap(appToNodeFields),
    outcome: null,
  };
}

export function isResolveResult(
  result: SchemaResolverResult,
): result is ResolveResult {
  return (
    typeof result === 'object' &&
    result !== null &&
    'identity' in result &&
    typeof result.identity === 'string'
  );
}

export function reverseFieldMap(
  fields: Record<string, string>,
): Record<string, string> {
  const reversed: Record<string, string> = {};
  for (const [appField, nodeField] of Object.entries(fields)) {
    if (reversed[nodeField] === undefined) {
      reversed[nodeField] = appField;
    }
  }
  return reversed;
}

export function mapFieldName(
  field: string,
  fields: Record<string, string>,
): string {
  return fields[field] ?? field;
}

export function mapFieldRecord<T extends JsonValue>(
  row: Record<string, T>,
  fields: Record<string, string>,
): Record<string, T> {
  const mapped: Record<string, T> = {};
  for (const [field, value] of Object.entries(row)) {
    mapped[mapFieldName(field, fields)] = value;
  }
  return mapped;
}

/**
 * Map a CAS {@link CasExpectation}'s `field` app→node (its value, when present,
 * is passed through unchanged — only the field name is a schema field). Used so
 * a `mutate` precondition guards the correct node-side field under a schema map.
 */
export function mapCasExpectation(
  expected: CasExpectation,
  fields: Record<string, string>,
): CasExpectation {
  return { ...expected, field: mapFieldName(expected.field, fields) };
}

/**
 * Map each two-pass predicate's `field` app→node, leaving the comparison
 * payload (`value` / `values` / `instant`) untouched — only `field` names a
 * schema field. The variant tag and any unrecognized shape pass through
 * verbatim so the SDK never rewrites a predicate dialect the node may add
 * later; an unknown tag reaches the node and is rejected there, loudly,
 * instead of being silently dropped here.
 */
export function mapPredicateFields(
  where: readonly QueryFieldPredicate[],
  fields: Record<string, string>,
): QueryFieldPredicate[] {
  return where.map((predicate) => {
    const [entry] = Object.entries(predicate as Record<string, unknown>);
    if (entry === undefined) {
      return predicate;
    }
    const [tag, body] = entry;
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      return predicate;
    }
    const inner = body as { field?: unknown };
    if (typeof inner.field !== 'string') {
      return predicate;
    }
    return {
      [tag]: { ...inner, field: mapFieldName(inner.field, fields) },
    } as QueryFieldPredicate;
  });
}

export function mapQueryResult(
  result: QueryResult,
  resolved: ResolvedSchema,
): QueryResult {
  return {
    ...result,
    schema: resolved.appSchemaName,
    rows: result.rows.map((row) => mapQueryRow(row, resolved.nodeToAppFields)),
  };
}

export function mapQueryRow(
  row: QueryRow,
  nodeToAppFields: Record<string, string>,
): QueryRow {
  return {
    ...row,
    fields: mapFieldRecord(row.fields, nodeToAppFields),
  };
}
