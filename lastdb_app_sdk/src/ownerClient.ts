/**
 * `ownerClient` — the ergonomic entry point for OWNER / HOST-context apps.
 *
 * Some apps (fbrain, fsituations, and other first-party host tools) run in the
 * node OWNER's own context: they authenticate with an `X-User-Hash` identity
 * header only, never go through the consent/capability flow, and resolve their
 * OWN socket path from their config. For them the async {@link connect} does not
 * fit:
 *
 * - `connect` is async and auto-loads a stored capability from the default OS
 *   keychain — an owner-context app wants neither (no keychain touch, no async
 *   consent load);
 * - `ConnectOptions` accepts EITHER `baseUrl` OR `socketPath`, never a
 *   caller-resolved explicit socket path ALONGSIDE a loopback-gated `baseUrl`
 *   TCP fallback;
 * - `connect`'s `discoverSocket` reads only env + the default data-dir; it
 *   ignores an app-supplied config socket path.
 *
 * So today every owner-context app hand-builds the transport (`udsTransport` /
 * `httpTransport`) and calls the 6-arg `new LastDbClient(...)` positional
 * constructor with a no-op capability store. `ownerClient` collapses that
 * boilerplate into one synchronous, keychain-free factory.
 */

import { existsSync } from 'node:fs';

import { capabilityStoreKey, type CapabilityStore } from './capabilityStore.js';
import { LastDbClient } from './client.js';
import {
  LASTDB_DB_HEADER,
  resolveDbLocator,
} from './dbHandle.js';
import { TransportError } from './errors.js';
import {
  discoverTransport,
  httpTransport,
  udsTransport,
  type Transport,
} from './transport.js';

/** Options for {@link ownerClient}. */
export interface OwnerClientOptions {
  /** Canonical app id this client acts as (e.g. `"fbrain"`). */
  appId: string;
  /**
   * Explicit multi-DB handle. Defaults to `process.env.LASTDB_DB` then
   * `lastdb://personal`. Forwarded as `X-LastDB-Db` on every request.
   */
  db?: string;
  /**
   * Loopback TCP base URL of the node (e.g. `http://127.0.0.1:9101`), used as
   * the fallback when no socket is available. Optional only when a
   * `socketPath` that exists is supplied — otherwise there is nothing to talk
   * to and the factory throws.
   */
  baseUrl?: string;
  /**
   * A caller-resolved explicit control-socket path (e.g. from the app's
   * `nodeSocketPath` config or a `FOLDDB_SOCKET_PATH` env read). Preferred over
   * `baseUrl` when the file exists AND `baseUrl` is loopback (or absent) — the
   * loopback gate: a socket is only substituted for a LOCAL TCP target, never
   * a remote one. A non-existent path transparently falls back to TCP.
   */
  socketPath?: string;
  /**
   * When `true`, and no explicit `socketPath` resolved, discover the node's
   * data-plane socket from the environment + default data dir (the same
   * `LASTDB_SOCKET_PATH` → `FOLDDB_SOCKET_PATH` → `FOLDDB_SOCK` →
   * `<data_dir>/folddb.sock` order {@link connect} uses), falling back to
   * `baseUrl`'s TCP listener when none exists. Still loopback-gated: discovery
   * only runs when `baseUrl` is loopback (or absent). Default `false` — an
   * owner-context app that resolves its own path leaves this off. Ignored when
   * an explicit `socketPath` is used.
   */
  discoverSocket?: boolean;
  /**
   * Headers attached to EVERY request this client sends. The production node's
   * HTTP server is stateless and resolves the calling user from an
   * `X-User-Hash` header on every request, so an owner-context app passes
   * `{ 'X-User-Hash': '<hash>' }` here. A UDS caller does not strictly need it
   * (the socket carries kernel peer credentials), but passing it is harmless.
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
 * apps never mint, store, or replay a capability (they authenticate as the node
 * owner via `X-User-Hash`), so `store`/`load`/`remove` are all no-ops and the
 * SDK never touches a real keychain or the filesystem.
 */
const NOOP_CAPABILITY_STORE: CapabilityStore = {
  async store() {},
  async load() {
    return null;
  },
  async remove() {},
};

/** Loopback hostnames a socket may be substituted for (the loopback gate). */
const LOOPBACK_HOSTS = new Set(['127.0.0.1', 'localhost', '::1', '[::1]']);

/**
 * Whether `url` points at the loopback interface. A non-URL string is treated
 * as non-loopback (the safe default: never redirect an unparseable target to a
 * local socket).
 */
function isLoopbackNodeUrl(url: string): boolean {
  try {
    return LOOPBACK_HOSTS.has(new URL(url).hostname);
  } catch {
    return false;
  }
}

/**
 * Resolve the transport for an owner-context client, honoring an explicit
 * socket path with a loopback-gated TCP fallback:
 *
 * 1. an explicit `socketPath` that EXISTS, when `baseUrl` is loopback or absent;
 * 2. env/data-dir socket discovery when `discoverSocket` is set and `baseUrl`
 *    is loopback or absent;
 * 3. the `baseUrl` TCP listener.
 *
 * Throws {@link TransportError} when none of these yields a target.
 */
function resolveOwnerTransport(options: OwnerClientOptions): Transport {
  const { baseUrl, socketPath, discoverSocket, defaultHeaders, timeoutMs } =
    options;
  const headers = defaultHeaders ?? {};
  // The loopback gate: a socket is only ever substituted for a LOCAL TCP
  // target. No baseUrl at all means the caller is committed to the socket, so
  // that is loopback-permissible too.
  const socketPermitted = baseUrl === undefined || isLoopbackNodeUrl(baseUrl);

  // 1. Caller-resolved explicit socket path (existing file, loopback-gated).
  if (
    socketPath !== undefined &&
    socketPath.length > 0 &&
    socketPermitted &&
    existsSync(socketPath)
  ) {
    return udsTransport(socketPath, headers, { timeoutMs });
  }

  // 2. Optional env/data-dir discovery (still loopback-gated).
  if (discoverSocket && baseUrl !== undefined && socketPermitted) {
    return discoverTransport({
      fallbackBaseUrl: baseUrl,
      defaultHeaders: headers,
      timeoutMs,
    });
  }

  // 3. TCP fallback.
  if (baseUrl !== undefined && baseUrl.length > 0) {
    return httpTransport(baseUrl, headers, { timeoutMs });
  }

  throw new TransportError(
    'ownerClient requires a reachable node: pass a loopback `baseUrl`, or a `socketPath` whose file exists',
    'connect',
  );
}

/**
 * Build a capability-less {@link LastDbClient} for an OWNER / HOST-context app.
 *
 * Synchronous and keychain-free: it resolves the transport (an explicit socket
 * path with a loopback-gated TCP fallback — see {@link resolveOwnerTransport}),
 * wires a no-op capability store, and returns a ready client. There is no async
 * consent load and no OS-keychain access — an owner-context app authenticates
 * via its `defaultHeaders` (`X-User-Hash`) and never carries a capability.
 *
 * Use {@link connect} instead for consent/capability apps (async, auto-loads a
 * stored capability, drives the request-consent flow).
 *
 * @example
 * ```ts
 * const client = ownerClient({
 *   appId: 'fsituations',
 *   baseUrl: 'http://127.0.0.1:9101',
 *   socketPath: '/Users/me/.folddb/data/folddb.sock',
 *   defaultHeaders: { 'X-User-Hash': userHash },
 * });
 * const rows = await client.queryAll('MySchema', { fields: ['a', 'b'] });
 * ```
 */
export function ownerClient(options: OwnerClientOptions): LastDbClient {
  const dbLocator = resolveDbLocator(options.db);
  // Inject X-LastDB-Db into transport default headers when not already set.
  const defaultHeaders: Record<string, string> = {
    ...(options.defaultHeaders ?? {}),
  };
  if (
    !Object.keys(defaultHeaders).some(
      (k) => k.toLowerCase() === LASTDB_DB_HEADER.toLowerCase(),
    )
  ) {
    defaultHeaders[LASTDB_DB_HEADER] = dbLocator;
  }
  const transport = resolveOwnerTransport({ ...options, defaultHeaders });
  return new LastDbClient(
    options.appId,
    transport,
    NOOP_CAPABILITY_STORE,
    null,
    capabilityStoreKey(options.appId, transport.target),
    transport.target,
    { dbLocator },
  );
}
