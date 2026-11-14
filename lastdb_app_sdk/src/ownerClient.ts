/**
 * `ownerClient` — the synchronous entry point for an owner-context or
 * session-approved app when `socketPath` has already been resolved.
 */

import { existsSync } from 'node:fs';

import { buildClientHeaders, LastDbClient } from './client.js';
import { TransportError } from './errors.js';
import { udsTransport, type Transport } from './transport.js';

/** Options for {@link ownerClient}. */
export interface OwnerClientOptions<DatabaseId extends string | undefined = undefined> {
  /** Canonical app id this client acts as. */
  appId: string;
  /** Database ID or local alias. Omit for the personal default. */
  databaseId?: DatabaseId;
  /**
   * Short-lived owner-approved database session token (`X-LastDB-Database-Session`).
   * Requires `databaseId`. Keep session tokens out of logs.
   */
  databaseSession?: string;
  /** Best-effort client label (`X-LastDB-Client`). Defaults to `appId`. */
  clientId?: string;
  /** Control-socket path. The file must exist. */
  socketPath: string;
  /** Headers attached to every request this client sends (per-call headers win). */
  defaultHeaders?: Record<string, string>;
  /** Per-request transport timeout in milliseconds (defaults to 30_000). */
  timeoutMs?: number;
}

function resolveOwnerTransport(options: OwnerClientOptions<string | undefined>): Transport {
  const { socketPath, timeoutMs } = options;
  if (socketPath.length > 0 && existsSync(socketPath)) {
    const headers = buildClientHeaders(
      options.appId,
      options.clientId,
      options.defaultHeaders,
    );
    return udsTransport(socketPath, headers, { timeoutMs });
  }
  throw new TransportError(
    `ownerClient requires a socketPath whose file exists, got '${socketPath}'`,
    'connect',
  );
}

/** Build a {@link LastDbClient} synchronously against an existing Unix-domain socket. */
export function ownerClient<DatabaseId extends string | undefined = undefined>(
  options: OwnerClientOptions<DatabaseId>,
): LastDbClient<DatabaseId> {
  const transport = resolveOwnerTransport(options);
  return new LastDbClient(options.appId, transport, {
    databaseId: options.databaseId,
    databaseSession: options.databaseSession,
  });
}
