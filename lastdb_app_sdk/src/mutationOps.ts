/**
 * Conditional single-record and batch mutation operations (`POST /api/mutation`
 * and `POST /api/mutations/batch`).
 */

import { randomUUID } from 'node:crypto';

import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  type ClientContext,
} from './databases.js';
import {
  mapDataError,
  RequestRejectedError,
  ShareSubmissionUncertainError,
  TransportError,
  UnexpectedResponseError,
} from './errors.js';
import { isNonEmptyString, isObject } from './responseParsers.js';
import type {
  JsonValue,
  MutationBatchItem,
  MutationBatchOperations,
  MutationBatchOptions,
  MutationBatchResult,
  MutationDurability,
  MutationOp,
  MutationOptions,
  MutationResult,
  ShareSubmissionResult,
  ShareSubmissionStatus,
} from './types.js';

const REVISION_HEX = /^[0-9a-f]{64}$/;
const UUID_LOWERCASE = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const NIL_UUID = '00000000-0000-0000-0000-000000000000';

function isMutationId(id: string): boolean {
  return id.length === 36 && UUID_LOWERCASE.test(id) && id !== NIL_UUID;
}

function parseMutationId(body: Record<string, JsonValue>, raw: unknown): string {
  const id = body['mutation_id'];
  if (!isNonEmptyString(id)) {
    throw new UnexpectedResponseError('mutation returned no mutation ID', 200, raw);
  }
  if (body['success'] !== true) {
    throw new UnexpectedResponseError('mutation returned no success marker', 200, raw);
  }
  return id;
}

/** Look up this receiver's proposal by the UUID used for submission. */
export async function executeShareSubmissionStatus(
  ctx: ClientContext,
  mutationId: string,
): Promise<ShareSubmissionStatus> {
  if (ctx.databasePath === null || !isMutationId(mutationId)) {
    throw new RequestRejectedError('invalid_request', 'submission status needs a received database and mutation UUID');
  }
  const reply = await ctx.transport.send(
    'POST', scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.shareSubmissionStatus),
    { body: { mutationId } },
  );
  if (reply.status !== 200) {
    throw mapDataError('submission status', reply.status, reply.body, ctx.appId);
  }
  if (isObject(reply.body) && reply.body['mutation_id'] === mutationId) {
    const status = reply.body['status'];
    if (status === 'submitted' || status === 'published' || status === 'discarded' || status === 'unknown') {
      return { mutationId, status };
    }
  }
  throw new UnexpectedResponseError('received an invalid share submission status', 200, null);
}

/** Serialize one {@link MutationOp} into the daemon's `Operation::Mutation` wire object. */
export function buildMutationOperation(
  schemaName: string,
  op: MutationOp,
  definitionIds: ReadonlyMap<string, string>,
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    type: 'mutation',
    schema: schemaName,
    record: op.record ?? {},
    key_value: op.key,
    alternates: op.alternates,
    mutation_type: op.mutationType,
    expected: op.expected,
    source_file_name: op.metadata?.source_file_name ?? null,
    metadata: op.metadata?.metadata ?? null,
    file: op.metadata?.file ?? null,
  };
  if (op.convergence !== undefined) body.convergence = op.convergence;
  if (op.durability !== undefined) body.durability = op.durability;
  const definitionId = definitionIds.get(schemaName);
  if (definitionId !== undefined) body.definition_id = definitionId;
  return body;
}

/** Parse a `200` commit receipt from `POST /api/mutation` or `POST /api/db/delete-blob`. */
export function parseCommittedMutation(rawBody: unknown): MutationResult {
  if (!isObject(rawBody)) {
    throw new UnexpectedResponseError('mutation returned a non-object body', 200, rawBody);
  }
  const recordRevision =
    typeof rawBody['record_revision'] === 'string' ? rawBody['record_revision'] : '';
  if (!REVISION_HEX.test(recordRevision)) {
    throw new UnexpectedResponseError('mutation returned no record revision', 200, rawBody);
  }
  const result: MutationResult = {
    mutationId: parseMutationId(rawBody, rawBody),
    recordRevision,
  };
  if (typeof rawBody['background_tasks_drained'] === 'boolean') {
    result.backgroundTasksDrained = rawBody['background_tasks_drained'];
  }
  if (typeof rawBody['convergence_pending'] === 'boolean') {
    result.convergencePending = rawBody['convergence_pending'];
  }
  return result;
}

/** `POST /api/mutation` — write one complete conditional record into `schemaName`. */
export async function executeMutate(
  ctx: ClientContext,
  schemaName: string,
  op: MutationOp,
  options: MutationOptions = {},
): Promise<MutationResult | ShareSubmissionResult> {
  const body = buildMutationOperation(schemaName, op, ctx.definitionIds);
  const mutationId =
    ctx.databasePath === null ? undefined : (options.mutationId ?? randomUUID());
  const headers: Record<string, string> = {};
  if (mutationId !== undefined) {
    if (!isMutationId(mutationId)) {
      throw new RequestRejectedError('mutation_id', 'Mutation ID must be a non-nil lowercase UUID');
    }
    headers['Idempotency-Key'] = mutationId;
  }

  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.mutation);
  const res = await ctx.transport
    .send('POST', path, { headers, body, timeoutMs: options.timeoutMs })
    .catch((error: unknown) => {
      const refused =
        error instanceof TransportError &&
        error.status !== undefined &&
        error.status >= 400 &&
        error.status < 500;
      if (mutationId !== undefined && !refused) {
        throw new ShareSubmissionUncertainError(mutationId, error);
      }
      throw error;
    });

  if (res.status === 202 && mutationId !== undefined) {
    if (isObject(res.body)) {
      const sequence = res.body['submission_sequence'];
      if (
        res.body['ok'] === true &&
        res.body['status'] === 'submitted' &&
        res.body['mutation_id'] === mutationId &&
        typeof sequence === 'number' &&
        Number.isSafeInteger(sequence) &&
        sequence > 0
      ) {
        return { status: 'submitted', mutationId, submissionSequence: sequence };
      }
    }
    throw new ShareSubmissionUncertainError(
      mutationId,
      new UnexpectedResponseError('mutation returned an invalid submission receipt', 202, res.body),
    );
  }
  if (res.status === 200) return parseCommittedMutation(res.body);

  const error = mapDataError('mutation', res.status, res.body, ctx.appId);
  if (mutationId !== undefined && res.status >= 500) {
    throw new ShareSubmissionUncertainError(mutationId, error);
  }
  throw error;
}

/** `POST /api/mutations/batch` — commit an atomic batch of conditional record mutations. */
export async function executeMutateBatch(
  ctx: ClientContext,
  items: readonly MutationBatchItem[],
  options: MutationBatchOptions = {},
): Promise<MutationBatchResult> {
  if (items.length === 0) {
    throw new RequestRejectedError(
      'invalid_request',
      'Mutation batch must contain at least one mutation',
    );
  }
  const body: Record<string, unknown> = {
    mutations: items.map((item) =>
      buildMutationOperation(item.schemaName, item.op, ctx.definitionIds),
    ),
  };
  if (options.convergence !== undefined) body.convergence = options.convergence;
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.mutationBatch);
  const res = await ctx.transport.send('POST', path, { body, timeoutMs: options.timeoutMs });
  if (res.status === 200) return parseMutationBatchResult(res.body, items.length);
  throw mapDataError('mutation batch', res.status, res.body, ctx.appId);
}

function parseNonNegInt(value: unknown, label: string, raw: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new UnexpectedResponseError(`mutation batch returned invalid ${label}`, 200, raw);
  }
  return value;
}

function parseBatchOperations(value: unknown, raw: unknown): MutationBatchOperations {
  if (!isObject(value)) {
    throw new UnexpectedResponseError('mutation batch returned no operations summary', 200, raw);
  }
  return {
    created: parseNonNegInt(value['created'], 'operations.created', raw),
    updated: parseNonNegInt(value['updated'], 'operations.updated', raw),
    deleted: parseNonNegInt(value['deleted'], 'operations.deleted', raw),
    total: parseNonNegInt(value['total'], 'operations.total', raw),
  };
}

function parseMutationBatchResult(
  rawBody: unknown,
  expectedCount: number,
): MutationBatchResult {
  if (!isObject(rawBody) || rawBody['ok'] !== true) {
    throw new UnexpectedResponseError('mutation batch returned an invalid envelope', 200, rawBody);
  }
  const rawIds = rawBody['mutation_ids'];
  if (
    !Array.isArray(rawIds) ||
    rawIds.length !== expectedCount ||
    !rawIds.every((id): id is string => typeof id === 'string' && id.length > 0)
  ) {
    throw new UnexpectedResponseError(
      `mutation batch returned invalid mutation_ids (expected ${expectedCount})`,
      200,
      rawBody,
    );
  }
  const count = parseNonNegInt(rawBody['count'], 'count', rawBody);
  const rawRevs = rawBody['record_revisions'];
  if (!isObject(rawRevs)) {
    throw new UnexpectedResponseError('mutation batch returned no record_revisions', 200, rawBody);
  }
  const recordRevisions: Record<string, string> = {};
  for (const [id, rev] of Object.entries(rawRevs)) {
    if (typeof rev !== 'string' || !REVISION_HEX.test(rev)) {
      throw new UnexpectedResponseError('mutation batch returned an invalid record revision', 200, rawBody);
    }
    recordRevisions[id] = rev;
  }
  const durabilityRaw = rawBody['durability'];
  if (durabilityRaw !== 'queued' && durabilityRaw !== 'durable') {
    throw new UnexpectedResponseError('mutation batch returned invalid durability', 200, rawBody);
  }
  const durability: MutationDurability = durabilityRaw;
  const revNum = rawBody['revision'];
  const revision =
    revNum === null || revNum === undefined ? null : parseNonNegInt(revNum, 'revision', rawBody);
  const drained = rawBody['background_tasks_drained'];
  const pending = rawBody['convergence_pending'];
  if (typeof drained !== 'boolean' || typeof pending !== 'boolean') {
    throw new UnexpectedResponseError('mutation batch returned invalid convergence flags', 200, rawBody);
  }
  return {
    mutationIds: rawIds,
    count,
    recordRevisions,
    revision,
    operations: parseBatchOperations(rawBody['operations'], rawBody),
    durability,
    backgroundTasksDrained: drained,
    convergencePending: pending,
  };
}
