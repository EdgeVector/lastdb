/** Change one record of a database: the node applies it, or queues it as a proposal. */

import { randomUUID } from 'node:crypto';

import { executeAppDatabases } from './appApi.js';
import { AppApiError } from './appApiError.js';
import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  validateDatabaseSelector,
  type ClientContext,
} from './databases.js';
import {
  mapDataError,
  RequestRejectedError,
  ShareSubmissionUncertainError,
  TransportError,
  UnexpectedResponseError,
} from './errors.js';
import { isObject } from './responseParsers.js';
import { buildProposalDocument, isMutationId } from './shareProposal.js';
import type { ChangeResult, MutationOp, MutationOptions } from './types.js';

/**
 * `POST /api/databases/{id}/change` on the owner socket, or the `change` action of the
 * `databases` extension when `LASTDB_APP` is set. The body is the proposal document.
 * The node decides: 200 `applied` when it may sign the change now, 202 `pending`
 * when it queued a proposal. The app never asks which.
 */
export async function executeChangeRecord(
  ctx: ClientContext,
  schemaName: string,
  op: MutationOp,
  options: MutationOptions = {},
): Promise<ChangeResult> {
  const mutationId = options.mutationId ?? randomUUID();
  if (!isMutationId(mutationId)) {
    throw new RequestRejectedError('mutation_id', 'Mutation ID must be a non-nil lowercase UUID');
  }
  const body = buildProposalDocument(schemaName, op, options.definitionId);
  if (process.env.LASTDB_APP) {
    return changeThroughExtension(ctx, body, mutationId, options.timeoutMs);
  }
  if (ctx.databasePath === null) {
    throw new RequestRejectedError('invalid_request', 'a change needs a selected database');
  }
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.change);
  const res = await ctx.transport
    .send('POST', path, {
      headers: { 'Idempotency-Key': mutationId },
      body,
      timeoutMs: options.timeoutMs,
    })
    .catch((error: unknown) => {
      throw refusedOrUncertain(error, mutationId);
    });
  if (res.status === 200 || res.status === 202) {
    return parseChange(res.body, res.status, mutationId);
  }
  const error = mapDataError('change', res.status, res.body, ctx.appId);
  if (res.status >= 500) throw new ShareSubmissionUncertainError(mutationId, error);
  throw error;
}

async function changeThroughExtension(
  ctx: ClientContext,
  document: Record<string, unknown>,
  mutationId: string,
  timeoutMs: number | undefined,
): Promise<ChangeResult> {
  const prefix = `${LASTDB_API_ROUTES.databases}/`;
  const scope = ctx.databasePath;
  if (scope !== null && !scope.startsWith(prefix)) {
    throw new RequestRejectedError('invalid_request', 'a change needs a database selector');
  }
  // No selector names the personal database, as on every `databases` action.
  const database = scope === null ? undefined : validateDatabaseSelector(scope.slice(prefix.length));
  try {
    const reply = await executeAppDatabases(ctx, {
      action: 'change',
      ...(database === undefined ? {} : { database }),
      mutation: document,
      idempotency_key: mutationId,
    }, timeoutMs);
    return parseChange(reply, undefined, mutationId);
  } catch (error: unknown) {
    throw refusedOrUncertain(error, mutationId);
  }
}

/** A refusal stays itself. An outcome the app cannot see is uncertain: retry with the same id. */
function refusedOrUncertain(error: unknown, mutationId: string): unknown {
  if (error instanceof ShareSubmissionUncertainError) return error;
  if (error instanceof AppApiError && error.status < 500) return error;
  const refused =
    error instanceof TransportError &&
    error.status !== undefined &&
    error.status >= 400 &&
    error.status < 500;
  return refused ? error : new ShareSubmissionUncertainError(mutationId, error);
}

function parseChange(raw: unknown, httpStatus: number | undefined, mutationId: string): ChangeResult {
  if (isObject(raw) && raw['ok'] === true && raw['mutation_id'] === mutationId) {
    if (raw['status'] === 'applied' && httpStatus !== 202) {
      return { status: 'applied', mutationId };
    }
    const sequence = raw['submission_sequence'];
    if (
      raw['status'] === 'pending' &&
      httpStatus !== 200 &&
      typeof sequence === 'number' &&
      Number.isSafeInteger(sequence) &&
      sequence > 0
    ) {
      return { status: 'pending', mutationId, submissionSequence: sequence };
    }
  }
  throw new ShareSubmissionUncertainError(
    mutationId,
    new UnexpectedResponseError('change returned an invalid receipt', httpStatus ?? 200, raw ?? null),
  );
}
