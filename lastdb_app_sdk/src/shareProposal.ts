/** Submit one encrypted proposal from a receiver of a whole-database share. */

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
import type { MutationOp, MutationOptions, ShareSubmissionResult } from './types.js';

const UUID_LOWERCASE = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const NIL_UUID = '00000000-0000-0000-0000-000000000000';

export function isMutationId(id: string): boolean {
  return id.length === 36 && UUID_LOWERCASE.test(id) && id !== NIL_UUID;
}

/** Serialize one {@link MutationOp} into the proposal document the node parses. */
export function buildProposalDocument(
  schemaName: string,
  op: MutationOp,
  definitionId: string | undefined,
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    type: 'mutation',
    schema: schemaName,
    record: op.record ?? {},
    key_value: op.key,
    mutation_type: op.mutationType,
    expected: op.expected,
    file: null,
  };
  if (op.mutationType !== 'delete') body.alternates = op.alternates;
  if (op.convergence !== undefined) body.convergence = op.convergence;
  if (op.durability !== undefined) body.durability = op.durability;
  if (definitionId !== undefined) body.definition_id = definitionId;
  return body;
}

/**
 * `POST /api/databases/{id}/submissions` — queue one proposal.
 * The body is the proposal document, not a `/v1/write` item.
 */
export async function executeSubmitProposal(
  ctx: ClientContext,
  schemaName: string,
  op: MutationOp,
  options: MutationOptions = {},
): Promise<ShareSubmissionResult> {
  if (ctx.databasePath === null) {
    throw new RequestRejectedError(
      'invalid_request',
      'a proposal needs a received database',
    );
  }
  const mutationId = options.mutationId ?? randomUUID();
  if (!isMutationId(mutationId)) {
    throw new RequestRejectedError('mutation_id', 'Mutation ID must be a non-nil lowercase UUID');
  }
  const body = buildProposalDocument(schemaName, op, options.definitionId);
  if (process.env.LASTDB_APP) {
    return submitThroughExtension(ctx, body, mutationId, options.timeoutMs);
  }
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.shareSubmission);
  const res = await ctx.transport
    .send('POST', path, {
      headers: { 'Idempotency-Key': mutationId },
      body,
      timeoutMs: options.timeoutMs,
    })
    .catch((error: unknown) => {
      const refused =
        error instanceof TransportError &&
        error.status !== undefined &&
        error.status >= 400 &&
        error.status < 500;
      if (!refused) throw new ShareSubmissionUncertainError(mutationId, error);
      throw error;
    });

  if (res.status === 202) {
    return parseSubmitted(res.body, mutationId);
  }
  const error = mapDataError('proposal', res.status, res.body, ctx.appId);
  if (res.status >= 500) throw new ShareSubmissionUncertainError(mutationId, error);
  throw error;
}

/**
 * App-socket submit. The owner socket keeps the `/api/databases/{id}/submissions` path.
 * A client on the app socket without LASTDB_APP stays on that path, and the app socket answers 404.
 */
async function submitThroughExtension(
  ctx: ClientContext,
  document: Record<string, unknown>,
  mutationId: string,
  timeoutMs: number | undefined,
): Promise<ShareSubmissionResult> {
  const prefix = `${LASTDB_API_ROUTES.databases}/`;
  const scope = ctx.databasePath;
  if (scope === null || !scope.startsWith(prefix)) {
    throw new RequestRejectedError('invalid_request', 'a proposal needs a received database');
  }
  const database = validateDatabaseSelector(scope.slice(prefix.length));
  try {
    const reply = await executeAppDatabases(ctx, {
      action: 'submit',
      database,
      mutation: document,
      idempotency_key: mutationId,
    }, timeoutMs);
    return parseSubmitted(reply, mutationId);
  } catch (error: unknown) {
    if (error instanceof ShareSubmissionUncertainError) throw error;
    if (error instanceof AppApiError && error.status < 500) throw error;
    const refused =
      error instanceof TransportError &&
      error.status !== undefined &&
      error.status >= 400 &&
      error.status < 500;
    if (refused) throw error;
    throw new ShareSubmissionUncertainError(mutationId, error);
  }
}

function parseSubmitted(raw: unknown, mutationId: string): ShareSubmissionResult {
  if (isObject(raw)) {
    const sequence = raw['submission_sequence'];
    if (
      raw['ok'] === true &&
      raw['status'] === 'submitted' &&
      raw['mutation_id'] === mutationId &&
      typeof sequence === 'number' &&
      Number.isSafeInteger(sequence) &&
      sequence > 0
    ) {
      return { status: 'submitted', mutationId, submissionSequence: sequence };
    }
  }
  throw new ShareSubmissionUncertainError(
    mutationId,
    new UnexpectedResponseError('proposal returned an invalid submission receipt', 202, null),
  );
}
