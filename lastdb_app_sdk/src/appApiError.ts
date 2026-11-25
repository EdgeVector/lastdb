/**
 * Typed error for a refused app API reply.
 * Callers that read only `code` and `message` stay valid.
 */

import { APP_API_ERROR_REASONS } from './appApi.generated.js';
import type { ErrorReason } from './appApi.generated.js';
import { LastDbError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';

function knownReason(value: string): value is ErrorReason {
  return (APP_API_ERROR_REASONS as readonly string[]).includes(value);
}

export class AppApiError extends LastDbError {
  readonly status: number;
  readonly code: string;
  /** Zero-based position when the refusal concerns one item in the list. */
  readonly index?: number;
  /** `exists`, `absent`, or `revision` when a write condition failed. */
  readonly reason?: ErrorReason;

  constructor(status: number, code: string, message: string, index?: number, reason?: ErrorReason) {
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
  const reason = text('reason');
  if (reason !== undefined && !knownReason(reason)) {
    throw new UnexpectedResponseError('app API returned an unknown error reason', status, body);
  }
  const rawCode = text('code');
  const code = rawCode && rawCode.length > 0 ? rawCode : status === 503 ? 'busy' : '';
  return new AppApiError(status, code, message, index, reason);
}
