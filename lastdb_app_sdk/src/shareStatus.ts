/** Read the outcome of one submission this receiving node already sent. */

import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  type ClientContext,
} from './databases.js';
import { RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { sendJsonObject } from './responseParsers.js';
import type { ShareSubmissionStatus } from './types.js';

const UUID_LOWERCASE = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const NIL_UUID = '00000000-0000-0000-0000-000000000000';

function isMutationId(id: string): boolean {
  return id.length === 36 && UUID_LOWERCASE.test(id) && id !== NIL_UUID;
}

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
  const body = await sendJsonObject(
    ctx.transport,
    'POST',
    scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.shareSubmissionStatus),
    'submission status',
    ctx.appId,
    { body: { mutationId }, redactBody: true },
  );
  if (body['mutation_id'] === mutationId) {
    const status = body['status'];
    if (status === 'submitted' || status === 'published' || status === 'discarded' || status === 'unknown') {
      return { mutationId, status };
    }
  }
  throw new UnexpectedResponseError('received an invalid share submission status', 200, null);
}
