/**
 * File and inline_file request/reply helpers for the app API.
 */

import type * as Api from './appApi.generated.js';
import { UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';
import type { JsonValue } from './types.js';

function invalid(message: string, body: unknown): UnexpectedResponseError {
  return new UnexpectedResponseError(message, 200, body);
}

export function fileLinkBody(link: Api.FileLink): Record<string, unknown> {
  const body: Record<string, unknown> = { path: link.path };
  if (link.name !== undefined) body.name = link.name;
  if (link.media_type !== undefined) body.media_type = link.media_type;
  return body;
}

function textOrNull(value: JsonValue | undefined, body: unknown): string | null {
  if (value === null) return null;
  if (typeof value === 'string') return value;
  throw invalid('app API returned an invalid file', body);
}

export function parseFile(value: JsonValue | undefined, body: unknown): Api.FileInfo | null {
  if (value === undefined) throw invalid('app API returned a record with no file', body);
  if (value === null) return null;
  if (
    !isObject(value) ||
    !('name' in value) ||
    !('media_type' in value) ||
    !('size' in value) ||
    !('local' in value)
  ) {
    throw invalid('app API returned an invalid file', body);
  }
  const size = value['size'];
  const local = value['local'];
  if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0) {
    throw invalid('app API returned an invalid file', body);
  }
  if (local !== null && typeof local !== 'string') {
    throw invalid('app API returned an invalid file', body);
  }
  const file: Api.FileInfo = {
    name: textOrNull(value['name'], body),
    media_type: textOrNull(value['media_type'], body),
    size,
    local: typeof local === 'string' ? local : null,
  };
  if ('path' in value) {
    const path = value['path'];
    if (path !== null && typeof path !== 'string') {
      throw invalid('app API returned an invalid file', body);
    }
    file.path = path === null ? null : path;
  }
  return file;
}

export function parseInlineFile(
  value: JsonValue | undefined,
  body: unknown,
): Api.InlineFileInfo | null {
  if (value === undefined) {
    throw invalid('app API returned a record with no inline_file', body);
  }
  if (value === null) return null;
  if (
    !isObject(value) ||
    !('name' in value) ||
    !('media_type' in value) ||
    !('size' in value) ||
    !('sha256' in value)
  ) {
    throw invalid('app API returned an invalid inline_file', body);
  }
  const size = value['size'];
  const sha256 = value['sha256'];
  if (
    typeof size !== 'number' ||
    !Number.isSafeInteger(size) ||
    size < 0 ||
    size > 65536
  ) {
    throw invalid('app API returned an invalid inline_file', body);
  }
  if (typeof sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(sha256)) {
    throw invalid('app API returned an invalid inline_file', body);
  }
  return {
    name: textOrNull(value['name'], body),
    media_type: textOrNull(value['media_type'], body),
    size,
    sha256,
  };
}
