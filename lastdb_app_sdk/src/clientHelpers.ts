/** Small shared helpers for the runtime client: error details, delay, batch pairing. */

import {
  UnexpectedResponseError,
  type CapabilityDenialDetail,
  type CasConflictDetail,
} from './errors.js';
import {
  QueryBatchItem,
} from './types.js';
import {
  ResolvedSchema,
} from './schemaMapping.js';
import {
  isObject,
} from './responseParsers.js';

export function authenticationRequiredReason(body: {
  code?: string;
  error?: string;
  message?: string;
}): 'session_expired' | 'auth_failed' {
  const text = `${body.code ?? ''} ${body.error ?? ''} ${body.message ?? ''}`.toLowerCase();
  if (text.includes('session') && text.includes('expired')) {
    return 'session_expired';
  }
  if (text.includes('token') && text.includes('expired')) {
    return 'session_expired';
  }
  return 'auth_failed';
}

export function capabilityDetailOf(body: unknown): CapabilityDenialDetail {
  const detail: CapabilityDenialDetail = {};
  if (!isObject(body)) {
    return detail;
  }
  if (typeof body['capability_id'] === 'string') {
    detail.capabilityId = body['capability_id'];
  }
  if (typeof body['schema'] === 'string') {
    detail.schema = body['schema'];
  }
  if (typeof body['timestamp_skew_secs'] === 'number') {
    detail.timestampSkewSecs = body['timestamp_skew_secs'];
  }
  return detail;
}

/**
 * Read the modeled detail fields off a `409 {error:"cas_conflict"}` body
 * (`schema` / `field` / `key` / `expected` / `actual` / `message`, per the
 * node's CAS-conflict contract). Unknown fields are ignored; absent ones stay
 * absent. `actual` is passed through as-is (the node may send an explicit
 * `null` when the field had no current value).
 */
export function casConflictDetailOf(body: unknown): CasConflictDetail {
  const detail: CasConflictDetail = {};
  if (!isObject(body)) {
    return detail;
  }
  if (typeof body['schema'] === 'string') {
    detail.schema = body['schema'];
  }
  if (typeof body['field'] === 'string') {
    detail.field = body['field'];
  }
  if (typeof body['key'] === 'string') {
    detail.key = body['key'];
  }
  if (typeof body['expected'] === 'string') {
    detail.expected = body['expected'];
  }
  if (typeof body['actual'] === 'string' || body['actual'] === null) {
    detail.actual = body['actual'] as string | null;
  }
  if (typeof body['message'] === 'string') {
    detail.message = body['message'];
  }
  return detail;
}

/** Promise-based sleep (used between consent polls). */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/** One query of a batch, with its position in the caller's list. */
export interface BatchEntry {
  index: number;
  item: QueryBatchItem;
  resolved: ResolvedSchema;
}

/**
 * Pair each sent query with its `{status, response}` entry from the `200` body
 * of `/api/queries/batch`. A reply of the wrong shape or length is a node
 * contract break, not an item failure, so it throws.
 */
export function pairBatchReplies(
  chunk: readonly BatchEntry[],
  body: unknown,
): Array<[BatchEntry, { status: number; response: unknown }]> {
  const results = (body as { results?: unknown } | null)?.results;
  const malformed = (): UnexpectedResponseError =>
    new UnexpectedResponseError(
      `query batch returned a malformed reply (expected ${chunk.length} results)`,
      200,
      body,
    );
  if (!Array.isArray(results) || results.length !== chunk.length) {
    throw malformed();
  }
  const pairs: Array<[BatchEntry, { status: number; response: unknown }]> = [];
  for (const [position, entry] of chunk.entries()) {
    const reply = results[position] as { status?: unknown; response?: unknown } | null;
    if (typeof reply !== 'object' || reply === null || typeof reply.status !== 'number') {
      throw malformed();
    }
    pairs.push([entry, { status: reply.status, response: reply.response }]);
  }
  return pairs;
}
