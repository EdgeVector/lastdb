/**
 * `ownerClient` — the synchronous entry point for an owner-context app.
 *
 * Pass an explicit `socketPath` whose file exists. {@link connect} is the async
 * path that also supports socket discovery and the API version handshake.
 */

import { existsSync } from 'node:fs';

import { LastDbClient } from './client.js';
import { TransportError } from './errors.js';
import { udsTransport, type Transport } from './transport.js';

/** Options for {@link ownerClient}. */
export interface OwnerClientOptions {
  /** Canonical app id this client acts as. */
  appId: string;
  /**
   * Control-socket path. The file must exist. An owner-context app passes the
   * path it already resolved (`LASTDB_SOCKET_PATH`, `FOLDDB_SOCKET_PATH`, or
   * `<LASTDB_HOME or ~/.lastdb>/data/folddb.sock`).
   */
  socketPath: string;
  /**
   * Headers attached to every request this client sends. A per-call header of
   * the same name wins.
   */
  defaultHeaders?: Record<string, string>;
  /**
   * Per-request transport timeout in milliseconds. Defaults to 30_000 so a
   * wedged node cannot leave app requests pending forever.
   */
  timeoutMs?: number;
}

function resolveOwnerTransport(options: OwnerClientOptions): Transport {
  const { socketPath, defaultHeaders, timeoutMs } = options;
  if (socketPath.length > 0 && existsSync(socketPath)) {
    return udsTransport(socketPath, defaultHeaders ?? {}, { timeoutMs });
  }
  throw new TransportError(
    `ownerClient requires a socketPath whose file exists, got '${socketPath}'`,
    'connect',
  );
}

/**
 * Build a {@link LastDbClient} synchronously for an owner-context app.
 *
 * The socket file must exist.
 */
export function ownerClient(options: OwnerClientOptions): LastDbClient {
  const transport = resolveOwnerTransport(options);
  return new LastDbClient(options.appId, transport);
}
