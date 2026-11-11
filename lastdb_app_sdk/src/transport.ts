/**
 * HTTP/1.1 transport over the node's Unix-domain socket.
 *
 * `lastdbd` serves its route table (`lastdb_uds` `uds_router::route_table`)
 * on `folddb.sock`. Node's built-in `http` client reaches that socket through
 * the `socketPath` request option.
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

/** The transport contract the client depends on. */
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

/** Per-request options for the Unix-domain socket transport. */
export interface TransportOptions {
  /**
   * Per-request timeout in milliseconds. Defaults to 30s so a wedged node
   * cannot leave app data-plane calls pending forever.
   */
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/**
 * Build a {@link Transport} that speaks HTTP over a Unix-domain socket.
 *
 * `defaultHeaders` are attached to every request (a per-call header of the
 * same name wins). Callers use them for `X-LastDB-Client` and the request id.
 */
export function udsTransport(
  socketPath: string,
  defaultHeaders: Record<string, string> = {},
  options: TransportOptions = {},
): Transport {
  return new NodeHttpTransport(
    socketPath,
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
 * Open the discovered data-plane socket.
 *
 * Order: `LASTDB_SOCKET_PATH`, then `FOLDDB_SOCKET_PATH`, then
 * `<LASTDB_HOME or ~/.lastdb>/data/folddb.sock`. The file must exist.
 */
export function openDiscoveredSocket(options: {
  defaultHeaders?: Record<string, string>;
  timeoutMs?: number;
  env?: NodeJS.ProcessEnv;
}): Transport {
  const { defaultHeaders = {}, timeoutMs } = options;
  const env = options.env ?? process.env;
  const resolved = resolveSocketPath(env);
  if (resolved === null || !existsSync(resolved)) {
    throw new TransportError(
      `no LastDB socket at ${resolved ?? '(unresolved path)'}; set LASTDB_SOCKET_PATH or pass socketPath`,
      'connect',
    );
  }
  return udsTransport(resolved, defaultHeaders, { timeoutMs });
}

/** Node `http` transport for one Unix-domain socket. */
class NodeHttpTransport implements Transport {
  private readonly timeoutMs: number;

  constructor(
    private readonly socketPath: string,
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

    const requestOptions = {
      socketPath: this.socketPath,
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
              // Success bodies are JSON. A refusal may be the plain status text.
              if (status >= 200 && status < 300) {
                reject(
                  new TransportError(
                    `non-JSON response (${status}) from ${this.target}${path}: ${text.slice(0, 200)}`,
                    'protocol',
                    { status },
                  ),
                );
                return;
              }
              body = text;
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
