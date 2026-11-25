/** Read the outcome of one submission this receiving node already sent. */

import { executeAppDatabases } from './appApi.js';
import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  validateDatabaseSelector,
  type ClientContext,
} from './databases.js';
import { RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { sendJsonObject } from './responseParsers.js';
import { isMutationId } from './shareProposal.js';
import type { ShareSubmissionStatus } from './types.js';

/** Look up this receiver's proposal by the UUID used for submission. */
export async function executeShareSubmissionStatus(
  ctx: ClientContext,
  mutationId: string,
): Promise<ShareSubmissionStatus> {
  if (ctx.databasePath === null || !isMutationId(mutationId)) {
    throw new RequestRejectedError(
      'invalid_request',
      'submission status needs a received database and mutation UUID',
    );
  }
  const body = process.env.LASTDB_APP
    ? await statusThroughExtension(ctx, ctx.databasePath, mutationId)
    : await sendJsonObject(
        ctx.transport,
        'POST',
        scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.shareSubmissionStatus),
        'submission status',
        ctx.appId,
        { body: { mutationId }, redactBody: true },
      );
  if (body['mutation_id'] === mutationId) {
    const status = body['status'];
    if (status === 'submitted' || status === 'published' || status === 'not_accepted' || status === 'unknown') {
      return { mutationId, status };
    }
  }
  throw new UnexpectedResponseError('received an invalid share submission status', 200, null);
}

async function statusThroughExtension(
  ctx: ClientContext,
  scope: string,
  mutationId: string,
): Promise<Record<string, unknown>> {
  const prefix = `${LASTDB_API_ROUTES.databases}/`;
  if (!scope.startsWith(prefix)) {
    throw new RequestRejectedError(
      'invalid_request',
      'submission status needs a received database and mutation UUID',
    );
  }
  const database = validateDatabaseSelector(scope.slice(prefix.length));
  return executeAppDatabases(ctx, {
    action: 'submission_status',
    database,
    mutationId,
  });
}
