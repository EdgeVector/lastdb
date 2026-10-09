/**
 * RFC 8785 JSON Canonicalization Scheme (JCS).
 *
 * This is the TypeScript port of the Rust `app_identity_crypto::canonicalize`
 * (which wraps the `json_canon` crate). The two MUST produce byte-identical
 * output: a capability token minted + signed by `fold_db_node` is verified
 * against bytes the node canonicalized with the Rust implementation, and the
 * envelope's `payload_hash` is `sha256(JCS(payload))`. Any drift here breaks
 * every client-side integrity check.
 *
 * The 12 golden vectors that pin cross-implementation agreement live at
 * `fold/app_identity_crypto/tests/golden_vectors.rs`; `test/jcs.test.ts`
 * reproduces them byte-for-byte against this module.
 *
 * RFC 8785 rules implemented:
 *   - Object keys sorted lexicographically by UTF-16 code unit.
 *   - No insignificant whitespace.
 *   - Strings serialized as UTF-8 with the minimal escape set (RFC 8785
 *     §3.2.2.2 / ECMAScript QuoteJSONString): `"`, `\`, the C0 control
 *     chars, and any UTF-16 code unit in the surrogate range that is NOT
 *     part of a well-formed pair are escaped. Valid surrogate pairs and
 *     all other non-ASCII chars pass through as their literal UTF-8.
 *   - Numbers in ECMAScript `Number.prototype.toString()` (shortest
 *     round-trip) form — which is exactly what `JSON.stringify` emits for a
 *     finite JS number, including normalizing `1.0e2` → `100`.
 */

import type { JsonValue } from './types.js';

/** Raised when a value cannot be canonicalized (e.g. a non-finite number). */
export class JcsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'JcsError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Canonicalize a JSON value per RFC 8785. Returns the canonical UTF-8 string.
 * Use {@link canonicalizeBytes} when you need the raw bytes for hashing.
 *
 * Throws {@link JcsError} on non-finite numbers (NaN / Infinity), which JSON
 * — and therefore JCS — cannot represent.
 */
export function canonicalize(value: JsonValue): string {
  return serialize(value);
}

/** Canonicalize and return the UTF-8 byte encoding (input to SHA-256 / Ed25519). */
export function canonicalizeBytes(value: JsonValue): Uint8Array {
  return new TextEncoder().encode(canonicalize(value));
}

function serialize(value: JsonValue): string {
  if (value === null) return 'null';
  switch (typeof value) {
    case 'boolean':
      return value ? 'true' : 'false';
    case 'number':
      return serializeNumber(value);
    case 'string':
      return serializeString(value);
    case 'object':
      if (Array.isArray(value)) return serializeArray(value);
      return serializeObject(value as { [key: string]: JsonValue });
    default:
      throw new JcsError(`unsupported JSON value of type ${typeof value}`);
  }
}

function serializeNumber(n: number): string {
  if (!Number.isFinite(n)) {
    throw new JcsError(
      `non-finite number cannot be canonicalized: ${String(n)}`,
    );
  }
  // RFC 8785 §3.2.2.3 prescribes the ECMAScript Number-to-String algorithm,
  // which is precisely what V8's (and JavaScriptCore's) JSON.stringify uses
  // for a finite number: shortest round-trip, integers without a trailing
  // `.0`, `1e2` rendered as `100`. We defer to it rather than re-implement
  // Ryū. `-0` collapses to `0` (JSON.stringify(-0) === "0"), matching the
  // Rust serde_json/json_canon behavior.
  return JSON.stringify(n);
}

// Escape exactly the characters RFC 8785 requires (same set as ECMAScript
// QuoteJSONString): the two structural chars `"` and `\`, the C0 control
// block U+0000..U+001F, and any UTF-16 code unit in the surrogate range
// that is NOT part of a well-formed pair. The five "short escapes" (\b \t
// \n \f \r) use their two-char form; any other escaped code unit uses the
// six-char \u00XX form. Non-control, non-surrogate characters (café, em
// dash, 漢字, emoji) pass through as their literal UTF-8 bytes — NEVER
// \u-escaped.
function serializeString(s: string): string {
  let out = '"';
  for (let i = 0; i < s.length; i++) {
    const ch = s.charCodeAt(i);
    switch (ch) {
      case 0x22: // "
        out += '\\"';
        break;
      case 0x5c: // \
        out += '\\\\';
        break;
      case 0x08: // \b
        out += '\\b';
        break;
      case 0x09: // \t
        out += '\\t';
        break;
      case 0x0a: // \n
        out += '\\n';
        break;
      case 0x0c: // \f
        out += '\\f';
        break;
      case 0x0d: // \r
        out += '\\r';
        break;
      default:
        if (ch < 0x20) {
          out += '\\u' + ch.toString(16).padStart(4, '0');
        } else if (ch >= 0xd800 && ch <= 0xdfff) {
          // Surrogate range (U+D800..U+DFFF). ECMA-262 QuoteJSONString /
          // RFC 8785 §3.2.2.2 emit a well-formed pair as the raw UTF-8 of
          // its codepoint, but escape any unpaired surrogate as \uXXXX —
          // otherwise TextEncoder substitutes U+FFFD on the byte path, so
          // sha256(canonical) would not round-trip through a conformant
          // verifier.
          const next =
            ch <= 0xdbff && i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
          if (next >= 0xdc00 && next <= 0xdfff) {
            out += (s[i] as string) + (s[i + 1] as string);
            i++;
          } else {
            out += '\\u' + ch.toString(16).padStart(4, '0');
          }
        } else {
          out += s[i];
        }
    }
  }
  return out + '"';
}

function serializeArray(arr: JsonValue[]): string {
  if (arr.length === 0) return '[]';
  const parts = new Array<string>(arr.length);
  for (let i = 0; i < arr.length; i++) {
    // RFC 8785: array order is preserved; `undefined` is not a JSON value.
    parts[i] = serialize(arr[i] as JsonValue);
  }
  return '[' + parts.join(',') + ']';
}

function serializeObject(obj: { [key: string]: JsonValue }): string {
  const keys = Object.keys(obj);
  if (keys.length === 0) return '{}';
  // RFC 8785 §3.2.3: sort member names by UTF-16 code unit. JavaScript string
  // `<` compares by UTF-16 code unit, which is exactly the required order
  // (matching the Rust `json_canon` crate, which sorts by the same key bytes).
  keys.sort(compareCodeUnits);
  const parts = new Array<string>(keys.length);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i] as string;
    parts[i] = serializeString(key) + ':' + serialize(obj[key] as JsonValue);
  }
  return '{' + parts.join(',') + '}';
}

// Compare two strings by UTF-16 code unit. `String.prototype.localeCompare`
// is locale-sensitive and wrong here; the default `<` operator on strings is
// already code-unit ordered, so this is a thin, explicit wrapper to make the
// ordering contract obvious at the call site.
function compareCodeUnits(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}
