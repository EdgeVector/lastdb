/**
 * Unix-domain socket path discovery and transport header composition helpers.
 */

import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

import { TransportError } from './errors.js';
import { udsTransport, type Transport } from './transport.js';

const SOCKET_FILE_NAME = 'folddb.sock';
const LASTDB_SOCKET_PATH_ENV = 'LASTDB_SOCKET_PATH';
const FOLDDB_SOCKET_PATH_ENV = 'FOLDDB_SOCKET_PATH';

function expandTilde(p: string): string {
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return join(homedir(), p.slice(2));
  return p;
}

function resolveDefaultHome(): string | null {
  const home = homedir();
  return home ? join(home, '.lastdb') : null;
}

function resolveSocketPath(env: NodeJS.ProcessEnv): string | null {
  const lastdbExplicit = env[LASTDB_SOCKET_PATH_ENV];
  if (lastdbExplicit && lastdbExplicit.length > 0) {
    return expandTilde(lastdbExplicit);
  }
  const explicit = env[FOLDDB_SOCKET_PATH_ENV];
  if (explicit && explicit.length > 0) {
    return expandTilde(explicit);
  }
  const lastdbHome = env.LASTDB_HOME;
  const home =
    lastdbHome && lastdbHome.length > 0 ? expandTilde(lastdbHome) : resolveDefaultHome();
  return home === null ? null : join(home, 'data', SOCKET_FILE_NAME);
}

/**
 * Open the discovered data-plane socket (`LASTDB_SOCKET_PATH`, then
 * `FOLDDB_SOCKET_PATH`, then `<LASTDB_HOME or ~/.lastdb>/data/folddb.sock`).
 */
export function openDiscoveredSocket(options: {
  defaultHeaders?: Record<string, string>;
  timeoutMs?: number;
  env?: NodeJS.ProcessEnv;
}): Transport {
  const { defaultHeaders = {}, timeoutMs } = options;
  const resolved = resolveSocketPath(options.env ?? process.env);
  if (resolved === null || !existsSync(resolved)) {
    throw new TransportError(
      `no LastDB socket at ${resolved ?? '(unresolved path)'}; set LASTDB_SOCKET_PATH or pass socketPath`,
      'connect',
    );
  }
  return udsTransport(resolved, defaultHeaders, { timeoutMs });
}

/** Case-insensitively merge request header layers (later layers override earlier ones). */
export function mergeHeaders(
  ...layers: Array<Record<string, string> | undefined>
): Record<string, string> {
  const merged: Record<string, string> = {};
  const keyByLower = new Map<string, string>();
  for (const layer of layers) {
    if (!layer) continue;
    for (const [key, value] of Object.entries(layer)) {
      const lower = key.toLowerCase();
      const prior = keyByLower.get(lower);
      if (prior !== undefined && prior !== key) {
        delete merged[prior];
      }
      keyByLower.set(lower, key);
      merged[key] = value;
    }
  }
  return merged;
}

/** Wrap an existing {@link Transport} with additional default headers (per-call headers still win). */
export function withDefaultHeaders(
  transport: Transport,
  extraHeaders: Record<string, string>,
): Transport {
  if (Object.keys(extraHeaders).length === 0) {
    return transport;
  }
  const wrapped: Transport = {
    target: transport.target,
    supportsBinaryBody: transport.supportsBinaryBody,
    send(method, path, options = {}) {
      return transport.send(method, path, {
        ...options,
        headers: mergeHeaders(extraHeaders, options.headers),
      });
    },
  };
  if (transport.sendBinary !== undefined) {
    const sendBinary = transport.sendBinary.bind(transport);
    wrapped.sendBinary = (method, path, options = {}) =>
      sendBinary(method, path, {
        ...options,
        headers: mergeHeaders(extraHeaders, options.headers),
      });
  }
  return wrapped;
}
