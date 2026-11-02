/**
 * HTTP transport over either a TCP base URL or a Unix-domain socket.
 *
 * Both transports speak the same HTTP/1.1 dialect. `lastdbd` serves its route
 * table (`lastdb_uds` `uds_router::route_table`) over its Unix-domain sockets.
 * Node's built-in `http` client handles UDS natively via the `socketPath`
 * request option, so one implementation covers both — the only difference is
 * whether we pass `host`/`port` or `socketPath`.
 */

import { randomUUID } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import type { IncomingMessage } from 'node:http';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

import { TransportError } from './errors.js';

/**
 * The per-request correlation-ID header the node's request-ops telemetry
 * reads (sanitized to ≤96 chars server-side and rendered as `req=<id>` in
 * `lastdb ops` "Slowest recent"). The transport mints a fresh UUID per
 * request when neither the per-call headers nor the transport's
 * `defaultHeaders` already carry one — per-request by design: a static
 * default header would stamp every call with the same ID and defeat
 * correlation. A caller-supplied value (either seam) is never clobbered.
 */
export const REQUEST_ID_HEADER = 'x-lastdb-request-id';

/** A parsed HTTP response: status + the parsed JSON body (or `null`). */
export interface RawResponse {
  status: number;
  body: unknown;
}

/**
 * The transport contract the client depends on. Tests inject a mock
 * implementation to exercise the error taxonomy without a live node.
 */
export interface Transport {
  /** A short description of where this transport points (for diagnostics). */
  readonly target: string;
  /** Perform one request and return the parsed response. */
  send(
    method: 'GET' | 'POST',
    path: string,
    options?: {
      headers?: Record<string, string>;
      body?: unknown;
      /** Override this transport's timeout for one request. */
      timeoutMs?: number;
    },
  ): Promise<RawResponse>;
}

interface HttpTargetTcp {
  kind: 'tcp';
  host: string;
  port: number;
  protocol: 'http:' | 'https:';
}

interface HttpTargetUds {
  kind: 'uds';
  socketPath: string;
}

type HttpTarget = HttpTargetTcp | HttpTargetUds;

/** Per-transport behavior shared by TCP and Unix-domain-socket HTTP. */
export interface TransportOptions {
  /**
   * Per-request timeout in milliseconds. Defaults to 30s so a wedged node
   * cannot leave app data-plane calls pending forever.
   */
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/**
 * Build a {@link Transport} for a TCP base URL (e.g.
 * `http://127.0.0.1:9101`). Throws on a non-http(s) URL. `defaultHeaders` are
 * attached to every request this transport sends (under a per-call header of
 * the same name, which wins) — used to carry a node-required identity header
 * such as `X-User-Hash` that the production `fold_db_node` reads to resolve the
 * caller (its HTTP server is stateless: identity comes from the header).
 */
export function httpTransport(
  baseUrl: string,
  defaultHeaders: Record<string, string> = {},
  options: TransportOptions = {},
): Transport {
  const url = new URL(baseUrl);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new TransportError(
      `baseUrl must be http:// or https://, got '${url.protocol}'`,
      'protocol',
    );
  }
  const target: HttpTarget = {
    kind: 'tcp',
    host: url.hostname,
    port: url.port ? Number(url.port) : url.protocol === 'https:' ? 443 : 80,
    protocol: url.protocol,
  };
  return new NodeHttpTransport(target, baseUrl, defaultHeaders, options);
}

/**
 * Build a {@link Transport} that speaks HTTP over a Unix-domain socket. See
 * {@link httpTransport} for the `defaultHeaders` contract.
 */
export function udsTransport(
  socketPath: string,
  defaultHeaders: Record<string, string> = {},
  options: TransportOptions = {},
): Transport {
  return new NodeHttpTransport(
    { kind: 'uds', socketPath },
    `unix:${socketPath}`,
    defaultHeaders,
    options,
  );
}

/** The fixed file name the node binds its data-plane socket under. */
const SOCKET_FILE_NAME = 'folddb.sock';
/** Canonical explicit node-socket override (the name the `lastdb` CLI reads). */
const LASTDB_SOCKET_PATH_ENV = 'LASTDB_SOCKET_PATH';
/** Second explicit override, read when {@link LASTDB_SOCKET_PATH_ENV} is unset. */
const FOLDDB_SOCKET_PATH_ENV = 'FOLDDB_SOCKET_PATH';

/**
 * Expand a leading `~` (or `~/...`) against the current user's home directory,
 * mirroring the Rust `expand_tilde` the node uses to resolve env-supplied
 * paths. A non-tilde path is returned unchanged.
 */
function expandTilde(p: string): string {
  if (p === '~') {
    return homedir();
  }
  if (p.startsWith('~/')) {
    return join(homedir(), p.slice(2));
  }
  return p;
}

/**
 * Resolve the node's home directory the same way the Rust `folddb_home()`
 * does: `LASTDB_HOME` (tilde-expanded) when it is set, else `~/.lastdb`.
 */
function resolveLastdbHome(env: NodeJS.ProcessEnv): string | null {
  const lastdbHome = env.LASTDB_HOME;
  if (lastdbHome && lastdbHome.length > 0) {
    return expandTilde(lastdbHome);
  }
  const home = homedir();
  if (!home) {
    return null;
  }
  return join(home, '.lastdb');
}

/**
 * Resolve the node's default data-plane socket path: an explicit
 * `LASTDB_SOCKET_PATH` override (tilde-expanded) wins, then
 * `FOLDDB_SOCKET_PATH`, otherwise `<lastdb_home>/data/folddb.sock`.
 * Returns `null` only when neither an env override is set nor a home dir can
 * be resolved.
 */
function resolveSocketPath(env: NodeJS.ProcessEnv): string | null {
  const lastdbExplicit = env[LASTDB_SOCKET_PATH_ENV];
  if (lastdbExplicit && lastdbExplicit.length > 0) {
    return expandTilde(lastdbExplicit);
  }
  const explicit = env[FOLDDB_SOCKET_PATH_ENV];
  if (explicit && explicit.length > 0) {
    return expandTilde(explicit);
  }
  const home = resolveLastdbHome(env);
  if (home === null) {
    return null;
  }
  return join(home, 'data', SOCKET_FILE_NAME);
}

/**
 * Discover which transport an app should use against a local node. The
 * `lastdb` CLI reads the same `LASTDB_SOCKET_PATH` override, so a TypeScript
 * app and the CLI agree on where the socket lives.
 *
 * Order (highest priority first):
 * 1. `LASTDB_SOCKET_PATH` — canonical explicit socket-path override.
 * 2. `FOLDDB_SOCKET_PATH` — second explicit override, read when
 *    `LASTDB_SOCKET_PATH` is unset.
 * 3. `<data_dir>/folddb.sock` — the default the node binds, resolved via
 *    {@link resolveSocketPath} (`LASTDB_HOME`, else `~/.lastdb`).
 * 4. Loopback TCP at `fallbackBaseUrl` — the fallback when no socket exists.
 *
 * A socket path is only chosen when the file actually EXISTS, so a node that
 * binds no socket (a pre-data-plane node, or one whose bind failed)
 * transparently falls back to TCP. `defaultHeaders` are attached to whichever
 * transport is built (see {@link httpTransport}); a UDS caller does not need
 * the `X-User-Hash` identity header — the socket carries kernel peer
 * credentials — but passing it is harmless.
 *
 * On a non-Unix platform there is no UDS transport, so this always returns the
 * TCP transport (matching the Rust `#[cfg(not(unix))]` discovery).
 */
export function discoverTransport(options: {
  fallbackBaseUrl: string;
  defaultHeaders?: Record<string, string>;
  timeoutMs?: number;
  /** Override the environment read for discovery (defaults to `process.env`). */
  env?: NodeJS.ProcessEnv;
}): Transport {
  const { fallbackBaseUrl, defaultHeaders = {}, timeoutMs } = options;
  const env = options.env ?? process.env;

  if (process.platform !== 'win32') {
    const resolved = resolveSocketPath(env);
    if (resolved !== null && existsSync(resolved)) {
      return udsTransport(resolved, defaultHeaders, { timeoutMs });
    }
  }

  return httpTransport(fallbackBaseUrl, defaultHeaders, { timeoutMs });
}

/** Node `http`-backed transport shared by the TCP and UDS variants. */
class NodeHttpTransport implements Transport {
  private readonly timeoutMs: number;

  constructor(
    private readonly t: HttpTarget,
    readonly target: string,
    private readonly defaultHeaders: Record<string, string> = {},
    options: TransportOptions = {},
  ) {
    this.timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    if (!Number.isFinite(this.timeoutMs) || this.timeoutMs <= 0) {
      throw new TransportError(
        `transport timeoutMs must be a positive finite number, got ${String(options.timeoutMs)}`,
        'protocol',
      );
    }
  }

  send(
    method: 'GET' | 'POST',
    path: string,
    options: {
      headers?: Record<string, string>;
      body?: unknown;
      timeoutMs?: number;
    } = {},
  ): Promise<RawResponse> {
    const timeoutMs = options.timeoutMs ?? this.timeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      return Promise.reject(
        new TransportError(
          `request timeoutMs must be a positive finite number, got ${String(timeoutMs)}`,
          'protocol',
        ),
      );
    }
    const payload =
      options.body === undefined ? undefined : JSON.stringify(options.body);
    const headers: Record<string, string> = {
      accept: 'application/json',
      ...this.defaultHeaders,
      ...(options.headers ?? {}),
    };
    // Correlation ID: one fresh UUID per request unless the caller supplied
    // one (per-call or via defaultHeaders — checked case-insensitively so a
    // caller's `X-LastDB-Request-Id` spelling is honored, not duplicated).
    if (
      !Object.keys(headers).some(
        (k) => k.toLowerCase() === REQUEST_ID_HEADER,
      )
    ) {
      headers[REQUEST_ID_HEADER] = randomUUID();
    }
    if (payload !== undefined) {
      headers['content-type'] = 'application/json';
      headers['content-length'] = String(Buffer.byteLength(payload));
    }

    const requestOptions =
      this.t.kind === 'tcp'
        ? {
            host: this.t.host,
            port: this.t.port,
            protocol: this.t.protocol,
            method,
            path,
            headers,
            timeout: timeoutMs,
          }
        : {
            socketPath: this.t.socketPath,
            method,
            path,
            headers,
            timeout: timeoutMs,
          };

    return new Promise<RawResponse>((resolve, reject) => {
      const req = httpRequest(requestOptions, (res: IncomingMessage) => {
        const chunks: Buffer[] = [];
        res.on('data', (chunk: Buffer) => chunks.push(chunk));
        res.on('end', () => {
          const status = res.statusCode ?? 0;
          const text = Buffer.concat(chunks).toString('utf8');
          let body: unknown = null;
          if (text.length > 0) {
            try {
              body = JSON.parse(text);
            } catch {
              // The node always answers JSON on these routes; a non-JSON body
              // is a transport-level surprise, not a typed protocol error.
              reject(
                new TransportError(
                  `non-JSON response (${status}) from ${this.target}${path}: ${text.slice(0, 200)}`,
                  'protocol',
                  { status },
                ),
              );
              return;
            }
          }
          resolve({ status, body });
        });
      });
      req.on('timeout', () => {
        req.destroy(
          new TransportError(
            `request to ${this.target}${path} timed out after ${timeoutMs}ms`,
            'timeout',
          ),
        );
      });
      req.on('error', (err: Error) => {
        if (err instanceof TransportError) {
          reject(err);
          return;
        }
        reject(
          new TransportError(
            `request to ${this.target}${path} failed: ${err.message}`,
            'connect',
          ),
        );
      });
      if (payload !== undefined) {
        req.write(payload);
      }
      req.end();
    });
  }
}
