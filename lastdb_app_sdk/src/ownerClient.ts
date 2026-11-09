/**
 * `ownerClient` — the synchronous entry point for an owner-context app.
 *
 * It does not load a capability and it does not touch the keychain. Pass a
 * `socketPath` whose file exists. {@link connect} remains the async path for
 * consent apps.
 */

import { existsSync } from 'node:fs';

import { capabilityStoreKey, type CapabilityStore } from './capabilityStore.js';
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

/**
 * The capability store an owner-context client is given: a no-op. Owner-context
 * apps never mint, store, or replay a capability, so `store`/`load`/`remove`
 * are all no-ops and the
 * SDK never touches a real keychain or the filesystem.
 */
const NOOP_CAPABILITY_STORE: CapabilityStore = {
  async store() {},
  async load() {
    return null;
  },
  async remove() {},
};

/**
 * Resolve the transport for an owner-context client.
 *
 * The `socketPath` file must exist. Otherwise this throws {@link TransportError}.
 */
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
 * Build a capability-less {@link LastDbClient} for an owner-context app.
 *
 * Synchronous and keychain-free. The socket file must exist. There is no
 * async consent load.
 *
 * Use {@link connect} instead for consent apps.
 *
 * @example
 * ```ts
 * const client = ownerClient({
 *   appId: 'my-app',
 *   socketPath: '/Users/me/.lastdb/data/folddb.sock',
 * });
 * const rows = await client.queryAll('MySchema');
 * ```
 */
export function ownerClient(options: OwnerClientOptions): LastDbClient {
  const transport = resolveOwnerTransport(options);
  return new LastDbClient(
    options.appId,
    transport,
    NOOP_CAPABILITY_STORE,
    null,
    capabilityStoreKey(options.appId, transport.target),
    transport.target,
  );
}
