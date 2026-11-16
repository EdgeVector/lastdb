/**
 * Record file and blob operations (`POST /api/db/put-blob`,
 * `POST /api/db/fetch-blob`, and `POST /api/db/delete-blob`).
 */

import {
  LASTDB_API_ROUTES,
  scopedApiPath,
  type ClientContext,
} from './databases.js';
import { mapDataError, RequestRejectedError, UnexpectedResponseError } from './errors.js';
import { buildMutationOperation, parseCommittedMutation } from './mutationOps.js';
import { normalizeRecordKeyInput } from './readOps.js';
import { isNonEmptyString, isObject, parseFileMarker } from './responseParsers.js';
import type {
  CreateMutationOp,
  FileMarker,
  FilePlacement,
  MutationResult,
  RecordKeyInput,
  ReplaceMutationOp,
} from './types.js';

const FILE_BLOB_METADATA_LENGTH_HEADER = 'X-LastDB-File-Blob-Metadata-Length';
const FILE_SHA256_HEADER = 'x-lastdb-file-sha256';
const FILE_SIZE_HEADER = 'x-lastdb-file-size';
const SHA256_HEX = /^[0-9a-f]{64}$/;

/** Mutations eligible for `putBlob` (`create` or complete `replace`). */
export type PutBlobMutationOp = CreateMutationOp | ReplaceMutationOp;

/** Optional inline thumbnail attached alongside a `blob` placement in `putBlob`. */
export interface BlobThumbnailInput {
  bytes: Uint8Array | ArrayBuffer;
  name?: string;
  mediaType?: string;
}

/** Upload raw in-memory bytes for a record's blob or inline file (`POST /api/db/put-blob`). */
export interface PutBlobBytesRequest {
  op: PutBlobMutationOp;
  bytes: Uint8Array | ArrayBuffer;
  localPath?: never;
  name?: string;
  mediaType?: string;
  placement?: FilePlacement;
  thumbnail?: BlobThumbnailInput;
  timeoutMs?: number;
}

/** Reference an existing local file path on an owner node (`POST /api/db/put-blob`). */
export interface PutBlobLocalPathRequest {
  op: PutBlobMutationOp;
  localPath: string;
  bytes?: never;
  name?: string;
  mediaType?: string;
  placement?: 'blob';
  thumbnail?: BlobThumbnailInput;
  timeoutMs?: number;
}

/** Request payload for `LastDbClient.putBlob`. */
export type PutBlobRequest = PutBlobBytesRequest | PutBlobLocalPathRequest;

/** Receipt returned by `POST /api/db/put-blob`. */
export interface PutBlobResult {
  file: FileMarker;
  placement: FilePlacement;
  thumbnail: FileMarker | null;
  mutationId: string;
  recordRevision: string;
  local: boolean;
}

/** Options for `LastDbClient.fetchBlob` (`POST /api/db/fetch-blob`). */
export interface FetchBlobOptions {
  /** Require the record to match this 64-hex revision token when reading. */
  expectedRevision?: string;
  /** Which attached file to read (`'blob'` default, or `'inline'`). */
  placement?: FilePlacement;
  /** Request the JSON envelope so `name` and `mediaType` are populated alongside `bytes`. */
  includeMetadata?: boolean;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}

/** Plaintext bytes and verified digest returned by `LastDbClient.fetchBlob`. */
export interface FetchedBlob {
  bytes: Uint8Array;
  sha256: string;
  sizeBytes: number;
  name?: string;
  mediaType?: string;
}

/** Options for `LastDbClient.deleteBlob` (`POST /api/db/delete-blob`). */
export interface DeleteBlobOptions {
  /** Which attached file to clear (`'blob'` default, or `'inline'`). */
  placement?: FilePlacement;
  /** Optional per-request timeout override in milliseconds. */
  timeoutMs?: number;
}

function toBuffer(input: Uint8Array | ArrayBuffer): Buffer {
  if (Buffer.isBuffer(input)) return input;
  if (input instanceof Uint8Array) {
    return Buffer.from(input.buffer, input.byteOffset, input.byteLength);
  }
  return Buffer.from(input);
}

function encodeThumbnail(thumb: BlobThumbnailInput): Record<string, unknown> {
  const out: Record<string, unknown> = { bytes_b64: toBuffer(thumb.bytes).toString('base64') };
  if (thumb.name !== undefined) out.name = thumb.name;
  if (thumb.mediaType !== undefined) out.media_type = thumb.mediaType;
  return out;
}

function isLocalPathRequest(
  req: PutBlobRequest,
): req is Extract<PutBlobRequest, { localPath: string }> {
  return 'localPath' in req && typeof req.localPath === 'string';
}

/** `POST /api/db/put-blob` — create or replace a record with a blob, inline, or local-path file. */
export async function executePutBlob(
  ctx: ClientContext,
  schemaName: string,
  request: PutBlobRequest,
): Promise<PutBlobResult> {
  if (request.op.mutationType !== 'create' && request.op.mutationType !== 'replace') {
    throw new RequestRejectedError(
      'invalid_request',
      'put-blob requires create or complete replacement',
    );
  }
  const placement: FilePlacement = request.placement ?? 'blob';
  const meta: Record<string, unknown> = {
    mutation: buildMutationOperation(schemaName, request.op, ctx.definitionIds),
  };
  if (request.name !== undefined) meta.name = request.name;
  if (request.mediaType !== undefined) meta.media_type = request.mediaType;
  if (request.placement !== undefined) meta.placement = request.placement;
  if (request.thumbnail !== undefined) {
    if (placement === 'inline') {
      throw new RequestRejectedError(
        'invalid_request',
        'an inline put takes no thumbnail; the bytes are the inline file',
      );
    }
    meta.thumbnail = encodeThumbnail(request.thumbnail);
  }

  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.putBlob);
  if (isLocalPathRequest(request)) {
    if (placement === 'inline') {
      throw new RequestRejectedError(
        'invalid_request',
        'an inline file stores its bytes; it has no local path',
      );
    }
    if (request.localPath.trim().length === 0) {
      throw new RequestRejectedError('invalid_request', 'put-blob localPath must not be empty');
    }
    const res = await ctx.transport.send('POST', path, {
      body: { ...meta, local_path: request.localPath },
      timeoutMs: request.timeoutMs,
    });
    if (res.status === 200) return parsePutBlobResult(res.body);
    throw mapDataError('put blob', res.status, res.body, ctx.appId);
  }

  const fileBytes = toBuffer(request.bytes);
  const res =
    ctx.transport.supportsBinaryBody === true
      ? await sendRawPutBlob(ctx, path, meta, fileBytes, request.timeoutMs)
      : await ctx.transport.send('POST', path, {
          body: { ...meta, bytes_b64: fileBytes.toString('base64') },
          timeoutMs: request.timeoutMs,
        });
  if (res.status === 200) return parsePutBlobResult(res.body);
  throw mapDataError('put blob', res.status, res.body, ctx.appId);
}

function sendRawPutBlob(
  ctx: ClientContext,
  path: string,
  meta: Record<string, unknown>,
  fileBytes: Buffer,
  timeoutMs: number | undefined,
) {
  const metaJson = Buffer.from(JSON.stringify(meta), 'utf8');
  return ctx.transport.send('POST', path, {
    headers: {
      'content-type': 'application/octet-stream',
      [FILE_BLOB_METADATA_LENGTH_HEADER]: String(metaJson.byteLength),
    },
    body: Buffer.concat([metaJson, fileBytes]),
    timeoutMs,
  });
}

function parsePutBlobResult(rawBody: unknown): PutBlobResult {
  if (!isObject(rawBody) || rawBody['ok'] !== true || !isObject(rawBody['file'])) {
    throw new UnexpectedResponseError('put-blob returned an invalid receipt', 200, rawBody);
  }
  const placement = rawBody['placement'];
  if (placement !== 'blob' && placement !== 'inline') {
    throw new UnexpectedResponseError('put-blob returned an invalid placement', 200, rawBody);
  }
  const mutationId = rawBody['mutation_id'];
  const recordRevision = rawBody['record_revision'];
  if (!isNonEmptyString(mutationId) || typeof recordRevision !== 'string' || !SHA256_HEX.test(recordRevision)) {
    throw new UnexpectedResponseError('put-blob returned invalid mutation coordinates', 200, rawBody);
  }
  const rawThumb = rawBody['thumbnail'];
  const thumbnail = isObject(rawThumb)
    ? parseFileMarker(rawThumb, 'put-blob thumbnail marker', rawBody)
    : null;
  return {
    file: parseFileMarker(rawBody['file'], 'put-blob file marker', rawBody),
    placement,
    thumbnail,
    mutationId,
    recordRevision,
    local: rawBody['local'] === true,
  };
}

/** `POST /api/db/fetch-blob` — fetch a record's blob or inline file bytes. */
export async function executeFetchBlob(
  ctx: ClientContext,
  schemaName: string,
  key: RecordKeyInput,
  options: FetchBlobOptions = {},
): Promise<FetchedBlob> {
  const body: Record<string, unknown> = {
    schema: schemaName,
    key: normalizeRecordKeyInput(key, 'fetchBlob'),
  };
  if (options.expectedRevision !== undefined) body.expected_revision = options.expectedRevision;
  if (options.placement !== undefined) body.placement = options.placement;
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.fetchBlob);

  if (!options.includeMetadata && ctx.transport.sendBinary !== undefined) {
    const res = await ctx.transport.sendBinary('POST', path, { body, timeoutMs: options.timeoutMs });
    if (res.status !== 200) throw mapDataError('fetch blob', res.status, res.errorBody, ctx.appId);
    const sha256 = res.headers[FILE_SHA256_HEADER];
    const sizeHeader = res.headers[FILE_SIZE_HEADER];
    const sizeBytes = sizeHeader !== undefined ? Number(sizeHeader) : NaN;
    if (
      typeof sha256 !== 'string' ||
      !SHA256_HEX.test(sha256) ||
      !Number.isSafeInteger(sizeBytes) ||
      sizeBytes !== res.bytes.byteLength
    ) {
      throw new UnexpectedResponseError(
        'fetch-blob binary reply had missing or mismatched integrity headers',
        200,
        res.headers,
      );
    }
    return { bytes: res.bytes, sha256, sizeBytes };
  }

  const res = await ctx.transport.send('POST', path, { body, timeoutMs: options.timeoutMs });
  if (res.status !== 200) throw mapDataError('fetch blob', res.status, res.body, ctx.appId);
  return parseJsonFetchedBlob(res.body);
}

function parseJsonFetchedBlob(rawBody: unknown): FetchedBlob {
  if (!isObject(rawBody) || !isObject(rawBody['file'])) {
    throw new UnexpectedResponseError('fetch-blob returned no file object', 200, rawBody);
  }
  const fileObj = rawBody['file'];
  const marker = parseFileMarker(fileObj, 'fetch-blob file marker', rawBody);
  const b64 = fileObj['bytes_b64'];
  if (typeof b64 !== 'string') {
    throw new UnexpectedResponseError('fetch-blob returned no bytes_b64', 200, rawBody);
  }
  const decoded = Buffer.from(b64, 'base64');
  if (decoded.byteLength !== marker.size_bytes) {
    throw new UnexpectedResponseError(
      `fetch-blob decoded ${decoded.byteLength} bytes, expected ${marker.size_bytes}`,
      200,
      rawBody,
    );
  }
  return {
    bytes: new Uint8Array(decoded.buffer, decoded.byteOffset, decoded.byteLength),
    sha256: marker.sha256,
    sizeBytes: marker.size_bytes,
    ...(marker.name !== undefined ? { name: marker.name } : {}),
    ...(marker.media_type !== undefined ? { mediaType: marker.media_type } : {}),
  };
}

/** `POST /api/db/delete-blob` — clear a record's blob or inline file via complete replacement. */
export async function executeDeleteBlob(
  ctx: ClientContext,
  schemaName: string,
  op: ReplaceMutationOp,
  options: DeleteBlobOptions = {},
): Promise<MutationResult> {
  if (op.mutationType !== 'replace') {
    throw new RequestRejectedError('invalid_request', 'delete-blob requires a complete replacement');
  }
  const placement: FilePlacement = options.placement ?? 'blob';
  if (placement === 'blob' && op.metadata?.file !== undefined) {
    throw new RequestRejectedError(
      'invalid_request',
      'delete-blob requires a complete replacement without a file marker',
    );
  }
  const body: Record<string, unknown> = {
    mutation: buildMutationOperation(schemaName, op, ctx.definitionIds),
  };
  if (options.placement !== undefined) body.placement = options.placement;
  const path = scopedApiPath(ctx.databasePath, LASTDB_API_ROUTES.deleteBlob);
  const res = await ctx.transport.send('POST', path, { body, timeoutMs: options.timeoutMs });
  if (res.status === 200) return parseCommittedMutation(res.body);
  throw mapDataError('delete blob', res.status, res.body, ctx.appId);
}
