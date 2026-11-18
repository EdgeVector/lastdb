/** JSON request helper for the owner-socket routes that remain. */

import { isObject, mapDataError, UnexpectedResponseError } from './errors.js';
import type { Transport, TransportSendOptions } from './transport.js';
import type { JsonValue } from './types.js';

export { isObject };

/** Send a JSON request, map non-200 errors, and require a JSON object reply. */
export async function sendJsonObject(
  transport: Transport,
  method: 'GET' | 'POST',
  path: string,
  verb: string,
  appId: string,
  options?: TransportSendOptions & { redactBody?: boolean },
): Promise<Record<string, JsonValue>> {
  const { redactBody, ...sendOptions } = options ?? {};
  const hasSendOpts =
    sendOptions.headers !== undefined ||
    sendOptions.body !== undefined ||
    sendOptions.timeoutMs !== undefined;
  const res = hasSendOpts
    ? await transport.send(method, path, sendOptions)
    : await transport.send(method, path);
  if (res.status !== 200) {
    throw mapDataError(verb, res.status, res.body, appId);
  }
  if (!isObject(res.body)) {
    throw new UnexpectedResponseError(
      `${verb} returned a non-object body`,
      200,
      redactBody ? null : res.body,
    );
  }
  return res.body;
}
