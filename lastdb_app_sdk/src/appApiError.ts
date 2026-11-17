/**
 * Typed error for a non-200 app API reply.
 * Callers that read only `code` and `message` stay valid.
 */

import { LastDbError } from './errors.js';
import { isObject } from './responseParsers.js';

export class AppApiError extends LastDbError {
  readonly status: number;
  readonly code: string;
  /** Zero-based position when the refusal concerns one item in the list. */
  readonly index?: number;
  /** `exists`, `absent`, or `revision` when a write condition failed. */
  readonly reason?: string;

  constructor(status: number, code: string, message: string, index?: number, reason?: string) {
    super(message);
    this.status = status;
    this.code = code;
    this.index = index;
    this.reason = reason;
  }
}

/** Build {@link AppApiError} from a non-200 app API body. */
export function appApiFailure(status: number, body: unknown): AppApiError {
  const error = isObject(body) ? body['error'] : undefined;
  const text = (name: string): string | undefined =>
    isObject(error) && typeof error[name] === 'string' ? error[name] : undefined;
  const rawIndex = isObject(error) ? error['index'] : undefined;
  const index =
    typeof rawIndex === 'number' && Number.isInteger(rawIndex) && rawIndex >= 0
      ? rawIndex
      : undefined;
  const message = text('message') ?? `app API request returned status ${status}`;
  return new AppApiError(status, text('code') ?? '', message, index, text('reason'));
}
