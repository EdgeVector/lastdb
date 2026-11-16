/**
 * HTTP/1.1 JSON and binary transport over the node's Unix-domain socket.
 *
 * `lastdbd` serves its route table (`lastdb_uds::uds_router::route`) on
 * `folddb.sock`. Node's built-in `http` client reaches that socket through
 * the `socketPath` request option.
 */

import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { request as httpRequest, type IncomingMessage } from 'node:http';
import { homedir } from 'node:os';
import { join } from 'node:path';

import { TransportError } from './errors.js';

const SOCKET_FILE_NAME = 'folddb.sock';
const LASTDB_SOCKET_PATH_ENV = 'LASTDB_SOCKET_PATH';
const FOLDDB_SOCKET_PATH_ENV = 'FOLDDB_SOCKET_PATH';

/**
 * Per-request correlation-ID header read by the node's request-ops telemetry.
 * Minted fresh per request unless supplied per-call or via `defaultHeaders`.
 */
export const REQUEST_ID_HEADER = 'x-lastdb-request-id';

/** A parsed HTTP response: status + the parsed JSON body (or `null`). */
export interface RawResponse {
  status: number;
  body: unknown;
}

/** A binary HTTP response from routes that stream `application/octet-stream`. */
export interface BinaryResponse {
  status: number;
  /** Lowercase response headers. */
  headers: Record<string, string>;
  /** Raw response bytes on 2xx replies; empty on non-2xx replies. */
  bytes: Uint8Array;
  /** Parsed JSON or plain-text error body on non-2xx replies; `null` on 2xx. */
  errorBody: unknown;
}

export interface TransportSendOptions {
  headers?: Record<string, string>;
  body?: unknown;
  /** Override this transport's timeout for one request. */
  timeoutMs?: number;
}

/** The transport contract the client depends on. */
export interface Transport {
  /** A short description of where this transport points (for diagnostics). */
  readonly target: string;
  /** True when `send` accepts raw `Uint8Array` / `ArrayBuffer` bodies directly. */
  readonly supportsBinaryBody?: boolean;
  /** Perform one request and return the parsed JSON response. */
  send(method: 'GET' | 'POST', path: string, options?: TransportSendOptions): Promise<RawResponse>;
  /** Perform one request expecting a binary `application/octet-stream` 2xx body. */
  sendBinary?(
    method: 'GET' | 'POST',
    path: string,
    options?: TransportSendOptions,
  ): Promise<BinaryResponse>;
}

/** Per-request options for the Unix-domain socket transport. */
export interface TransportOptions {
  /** Per-request timeout in milliseconds (defaults to 30_000). */
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** Build a {@link Transport} that speaks HTTP over a Unix-domain socket. */
export function udsTransport(
  socketPath: string,
  defaultHeaders: Record<string, string> = {},
  options: TransportOptions = {},
): Transport {
  return new NodeHttpTransport(socketPath, `unix:${socketPath}`, defaultHeaders, options);
}

function expandTilde(p: string): string {
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return join(homedir(), p.slice(2));
  return p;
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
    lastdbHome && lastdbHome.length > 0
      ? expandTilde(lastdbHome)
      : homedir()
        ? join(homedir(), '.lastdb')
        : null;
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
function mergeHeaders(
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

function hasHeader(headers: Record<string, string>, lowerName: string): boolean {
  return Object.keys(headers).some((k) => k.toLowerCase() === lowerName);
}

function normalizeResponseHeaders(raw: IncomingMessage['headers']): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (typeof value === 'string') {
      out[key.toLowerCase()] = value;
    } else if (Array.isArray(value)) {
      out[key.toLowerCase()] = value.join(', ');
    }
  }
  return out;
}

function parseLooseErrorBody(buf: Buffer): unknown {
  if (buf.byteLength === 0) return null;
  const text = buf.toString('utf8');
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

class NodeHttpTransport implements Transport {
  readonly supportsBinaryBody = true;
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

  async send(
    method: 'GET' | 'POST',
    path: string,
    options: TransportSendOptions = {},
  ): Promise<RawResponse> {
    const { status, buf } = await this.execute(method, path, 'application/json', options);
    if (buf.byteLength === 0) return { status, body: null };
    const text = buf.toString('utf8');
    try {
      return { status, body: JSON.parse(text) };
    } catch {
      if (status >= 200 && status < 300) {
        throw new TransportError(
          `non-JSON response (${status}) from ${this.target}${path}: ${text.slice(0, 200)}`,
          'protocol',
          { status },
        );
      }
      return { status, body: text };
    }
  }

  async sendBinary(
    method: 'GET' | 'POST',
    path: string,
    options: TransportSendOptions = {},
  ): Promise<BinaryResponse> {
    const { status, headers, buf } = await this.execute(
      method,
      path,
      'application/octet-stream',
      options,
    );
    if (status >= 200 && status < 300) {
      return {
        status,
        headers,
        bytes: new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength),
        errorBody: null,
      };
    }
    return { status, headers, bytes: new Uint8Array(0), errorBody: parseLooseErrorBody(buf) };
  }

  private execute(
    method: 'GET' | 'POST',
    path: string,
    defaultAccept: string,
    options: TransportSendOptions,
  ): Promise<{ status: number; headers: Record<string, string>; buf: Buffer }> {
    const timeoutMs = options.timeoutMs ?? this.timeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      return Promise.reject(
        new TransportError(
          `request timeoutMs must be a positive finite number, got ${String(timeoutMs)}`,
          'protocol',
        ),
      );
    }
    const headers = mergeHeaders({ accept: defaultAccept }, this.defaultHeaders, options.headers);
    if (!hasHeader(headers, REQUEST_ID_HEADER)) {
      headers[REQUEST_ID_HEADER] = randomUUID();
    }
    const payload = encodeRequestBody(options.body, headers);

    return new Promise((resolve, reject) => {
      const req = httpRequest(
        { socketPath: this.socketPath, method, path, headers, timeout: timeoutMs },
        (res: IncomingMessage) => {
          const chunks: Buffer[] = [];
          res.on('data', (chunk: Buffer) => chunks.push(chunk));
          res.on('end', () => {
            resolve({
              status: res.statusCode ?? 0,
              headers: normalizeResponseHeaders(res.headers),
              buf: Buffer.concat(chunks),
            });
          });
        },
      );
      req.on('timeout', () => {
        req.destroy(
          new TransportError(
            `request to ${this.target}${path} timed out after ${timeoutMs}ms`,
            'timeout',
          ),
        );
      });
      req.on('error', (err: Error) => {
        reject(
          err instanceof TransportError
            ? err
            : new TransportError(`request to ${this.target}${path} failed: ${err.message}`, 'connect'),
        );
      });
      if (payload !== undefined) req.write(payload);
      req.end();
    });
  }
}

function encodeRequestBody(body: unknown, headers: Record<string, string>): Buffer | undefined {
  if (body === undefined) return undefined;
  if (body instanceof Uint8Array || body instanceof ArrayBuffer) {
    const buf =
      body instanceof Uint8Array
        ? Buffer.isBuffer(body)
          ? body
          : Buffer.from(body.buffer, body.byteOffset, body.byteLength)
        : Buffer.from(body);
    if (!hasHeader(headers, 'content-type')) headers['content-type'] = 'application/octet-stream';
    headers['content-length'] = String(buf.byteLength);
    return buf;
  }
  const buf = Buffer.from(JSON.stringify(body), 'utf8');
  if (!hasHeader(headers, 'content-type')) headers['content-type'] = 'application/json';
  headers['content-length'] = String(buf.byteLength);
  return buf;
}
