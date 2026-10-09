/**
 * Client-side capability-token decode + integrity verification (gap #4).
 *
 * The node mints a `CapabilityToken` (base64 JSON) whose `envelope.payload_hash`
 * is `sha256(JCS(token-minus-envelope))` — see
 * `fold/fold_db/crates/core/src/access/caller_context.rs::CapabilityToken` and
 * `fold/app_identity_crypto` (`compute_payload_hash`). An SDK app is a TOKEN
 * CARRIER: the node mints + signs; the app stores the opaque base64 blob and
 * replays it verbatim. It never re-signs. What the app CAN do client-side is:
 *
 *   - **integrity binding** — recompute `sha256(JCS(token-minus-envelope))`
 *     and compare it to `envelope.payload_hash`, with the same RFC 8785 JCS
 *     the Rust node uses. A tampered / truncated / bit-rotted cached blob
 *     fails here and is discarded instead of being replayed into a
 *     guaranteed `403 {reason: "capability_bad_sig"}`.
 *   - **audience binding** — check the token's `app_id` is the app the client
 *     acts as, so a token granted to another app is never stored or replayed.
 *
 * Neither check substitutes for the node's Ed25519 signature verification
 * (which needs the node's key); they make the client fail fast and keep its
 * cache clean. Enable them on the client via
 * `ConnectOptions.verifyCapability: true`, or call these helpers directly.
 */

import { createHash } from 'node:crypto';

import { canonicalize } from './jcs.js';
import type { JsonValue } from './types.js';

/**
 * SignatureEnvelope as it crosses the wire — mirrors
 * `app_identity_crypto::SignatureEnvelope`. The client only reads `purpose`
 * and `payload_hash` (for the integrity check); the rest is opaque.
 */
export interface SignatureEnvelope {
  version: number;
  /** `"capability_grant"` for a CapabilityToken. */
  purpose: string;
  /** `"Ed25519"`. */
  alg: string;
  key_id: string;
  issued_at: string;
  expires_at?: string;
  /** `"dev"` | `"prod"`. */
  env: string;
  /** Lowercase-hex sha256 of `JCS(token-minus-envelope)`. */
  payload_hash: string;
  sig?: string;
}

/**
 * CapabilityToken as it crosses the wire — mirrors
 * `fold_db/crates/core/src/access/caller_context.rs::CapabilityToken`. The
 * client decodes it only to read `app_id` (audience binding) + `node_pubkey`
 * (diagnostics) and to run the payload-hash integrity check. Everything else
 * is replayed verbatim via the original base64 blob.
 */
export interface CapabilityToken {
  envelope: SignatureEnvelope;
  capability_id: string;
  app_id: string;
  /** CapabilityScope — opaque to the client. */
  scope: unknown;
  granted_ops: unknown[];
  granted_at: string;
  expires_at?: string;
  /** Base64 Ed25519 public key of the node the grant is bound to. */
  node_pubkey: string;
}

/** The envelope `purpose` a CapabilityToken must carry. */
export const CAPABILITY_GRANT_PURPOSE = 'capability_grant';

/**
 * Decode a base64 CapabilityToken blob to its parsed JSON, or `null` when it
 * is not base64 JSON in the CapabilityToken shape. Never throws.
 */
export function decodeCapabilityBlob(blob: string): CapabilityToken | null {
  let json: unknown;
  try {
    const text = Buffer.from(blob, 'base64').toString('utf8');
    json = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isCapabilityToken(json)) return null;
  return json;
}

function isCapabilityToken(v: unknown): v is CapabilityToken {
  if (v === null || typeof v !== 'object' || Array.isArray(v)) return false;
  const o = v as Record<string, unknown>;
  if (typeof o.capability_id !== 'string') return false;
  if (typeof o.app_id !== 'string') return false;
  if (typeof o.node_pubkey !== 'string') return false;
  const env = o.envelope;
  if (env === null || typeof env !== 'object' || Array.isArray(env)) {
    return false;
  }
  const e = env as Record<string, unknown>;
  return typeof e.payload_hash === 'string' && typeof e.purpose === 'string';
}

/**
 * Verify the *integrity binding* of a decoded token: the envelope's
 * `payload_hash` must equal `sha256(JCS(token-minus-envelope))`, computed with
 * the same JCS the Rust node uses (this is exactly the node's
 * `signature_is_valid` minus the Ed25519 step, which needs the node's key).
 *
 * This is the JCS-load-bearing path: a tampered or truncated cached blob fails
 * here and is discarded, so the app never replays a token guaranteed to 403
 * as `capability_bad_sig`. It is NOT a substitute for the node's signature
 * check — the node still verifies the Ed25519 signature on every write.
 */
export function tokenIntegrityValid(token: CapabilityToken): boolean {
  if (token.envelope.purpose !== CAPABILITY_GRANT_PURPOSE) return false;
  // Reconstruct signing_payload = the token JSON minus its `envelope` key,
  // exactly as the Rust `CapabilityToken::signing_payload` does.
  const payload = { ...(token as unknown as Record<string, JsonValue>) };
  delete payload.envelope;
  let recomputed: string;
  try {
    recomputed = sha256Hex(canonicalize(payload as JsonValue));
  } catch {
    return false;
  }
  return recomputed === token.envelope.payload_hash;
}

/** Why {@link verifyCapabilityBlob} rejected a blob. */
export type CapabilityBlobProblem =
  /** Not base64 JSON in the CapabilityToken shape. */
  | 'malformed'
  /** The token's `app_id` is not the app this client acts as. */
  | 'audience_mismatch'
  /** `envelope.payload_hash != sha256(JCS(token-minus-envelope))`. */
  | 'integrity_mismatch';

/** Result of {@link verifyCapabilityBlob}. */
export type CapabilityBlobVerification =
  | { ok: true; token: CapabilityToken }
  | { ok: false; problem: CapabilityBlobProblem; tokenAppId?: string };

/**
 * Run the full client-side validation an app should apply to a capability
 * blob before storing or replaying it: decode, audience binding
 * (`token.app_id === expectedAppId`), and JCS integrity binding. Returns a
 * discriminated result rather than throwing, so call sites choose their own
 * severity (a grant-path mismatch is an error; a cache-load mismatch just
 * treats the entry as absent).
 */
export function verifyCapabilityBlob(
  blob: string,
  expectedAppId: string,
): CapabilityBlobVerification {
  const token = decodeCapabilityBlob(blob);
  if (token === null) {
    return { ok: false, problem: 'malformed' };
  }
  if (token.app_id !== expectedAppId) {
    return { ok: false, problem: 'audience_mismatch', tokenAppId: token.app_id };
  }
  if (!tokenIntegrityValid(token)) {
    return { ok: false, problem: 'integrity_mismatch', tokenAppId: token.app_id };
  }
  return { ok: true, token };
}

/**
 * Lowercase-hex SHA-256 of a UTF-8 string — matches the Rust
 * `app_identity_crypto::compute_payload_hash` (lowercase hex of the sha256 of
 * the JCS bytes).
 */
export function sha256Hex(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex');
}
