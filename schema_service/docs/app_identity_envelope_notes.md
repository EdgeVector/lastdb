# App Identity — envelope & cert interop notes (Lane B2b)

Implementation notes for the publish-side trust gate shipped in Lane B2b
of [app_identity v3.1](../../exemem-workspace/docs/designs/app_identity.md):
`POST /v1/apps`, the `owner_app_id` schema-claim gate on
`POST /v1/schemas`, and `apps[]` in `GET /v1/snapshot`.

Written for the downstream lanes (B2c cross-env mirror, C fold_db_node,
C2 `folddb dev`) and for the exemem-infra operator who wires the config.

## The cert is ES256, not Ed25519 — and is not a SignatureEnvelope

The design (§ "Typed signature envelopes") models every signed artifact
as an Ed25519 `SignatureEnvelope`. Lane B1 (the exemem `auth_service`
`POST /v1/dev-cert` endpoint) shipped a **deviation**: the exemem root
key lives in AWS KMS, and KMS has **no Ed25519 SIGN_VERIFY**. So the
DevCert is signed with **`ES256`** (`ECDSA_SHA_256` over P-256), and it
is a flat `DevCert` struct — *not* a `SignatureEnvelope`:

```
DevCert { version, purpose, alg="ES256", key_id, dev_pubkey,
          user_hash, issued_at, expires_at, env, sig }
```

Consequences for this lane:

- `app_identity_crypto::verify_envelope` (Ed25519-only) **cannot** verify
  the cert. A dedicated ES256 path was added to the shared crate:
  `app_identity_crypto::{DevCert, verify_dev_cert, root_key_id}`
  (`app_identity_crypto/src/dev_cert.rs`). It lives in the shared crate
  because fold_db_node (Lane C) must verify the same cert.
- The **two algorithms split cleanly by signer**:
  - **Cert** (`X-Exemem-Dev-Cert`) — ES256, signed by the exemem **root**
    (KMS). Verified against the configured root SPKI DER.
  - **`X-Signature`** envelope (`app_register` / `schema_claim`) —
    Ed25519, signed by the **developer's** own key. Verified with
    `verify_envelope` against the `dev_pubkey` the cert vouches for.
- `key_id` = `sha256(SubjectPublicKeyInfo DER of root pubkey)`, hex —
  matches what auth_service stamps (it hashes the KMS `GetPublicKey` DER).

### Canonicalization parity

auth_service signs `SHA-256(serde_jcs(cert_without_sig))`. The verifier
recomputes the bytes with this crate's `json_canon`-backed `canonicalize`.
A crate test (`json_canon_matches_serde_jcs_on_cert`) asserts the two JCS
implementations are byte-identical on a representative cert, so the two
services cannot silently diverge. The ECDSA `Verifier` SHA-256-prehashes
the message itself, matching KMS `MessageType::Digest` over the same
canonical bytes.

## Signed payloads (the `payload_hash` contract)

`SignatureEnvelope.payload_hash` = `sha256(JCS(payload))`. The **payload**
is defined per endpoint — the client (Lane C2 `folddb dev`) must hash the
identical object:

| Endpoint | envelope purpose | signed payload |
|---|---|---|
| `POST /v1/apps` | `app_register` | the whole request body `{ app_id, metadata }` |
| `POST /v1/schemas` | `schema_claim` | the request body's `schema` sub-object (the full `DeclarativeSchemaDefinition` as sent, including any client-set `identity_hash`) |

JCS normalizes key order and whitespace, so byte formatting is
irrelevant — only the field set and values must match.

## Config (exemem-infra must wire this)

`schema_service_core::app_identity::AppIdentityConfig::from_env()` reads:

- `APP_IDENTITY_ROOT_PUBKEYS` — comma-separated base64 P-256
  SubjectPublicKeyInfo DER blobs. This is a **set** (versioning
  lookahead: a key rotation stages the new key alongside the old, and the
  cert's `key_id` selects which to verify against). exemem-infra sources
  these from the KMS `GetPublicKey` output of the
  `exemem-app-identity-root` key. **There is no committed default** — the
  real pubkey is a deploy-time value.
- `APP_IDENTITY_REVOKED_DEV_PUBKEYS` — comma-separated base64 Ed25519 dev
  pubkeys to refuse. See "Offline revocation" below.
- `ENVIRONMENT` — `prod`/`production` → `Env::Prod`, else `Env::Dev`. The
  cert's `env` and the envelope's `env` must match the deployment.

### Enforcement is gated on configuration

When **no** trusted roots are configured, the service is
pre-app-identity: the `POST /v1/schemas` gate is a **passthrough**
(legacy registration keeps working; seeds and un-namespaced User schemas
are accepted), and `POST /v1/apps` rejects every cert with `401`
(nothing to verify against). Enforcement activates the moment an operator
bakes in the root pubkey — which, per the design's destructive-reset
migration, is exactly when the deployment goes app-aware. This keeps the
pre-migration `main` build and its test suite working unchanged.

## Verification order (`POST /v1/apps`)

1. Parse body → `{ app_id, metadata }`.
2. `app_id` regex `^[a-z][a-z0-9-]{0,39}$` → `400 invalid_app_id`.
3. Metadata bounds (`display_name<=80`, `description<=500`,
   `homepage_url<=200`, `icon_url<=200?`, total `< 2KB` after JCS) →
   `400 invalid_metadata` (+ `detail`).
4. Verify cert (alg, trusted `key_id`, expiry, ES256 sig) →
   `401 cert_invalid` / `cert_expired`.
5. Verify `X-Signature` (purpose, env, `key_id == sha256(dev_pubkey)`,
   sig, `payload_hash`) → `401 envelope_invalid`.
6. First-write-wins → `201 Created` / `200` idempotent /
   `409 app_id_taken`.

Input validation precedes crypto so the `400` cases need no valid
signature, and leaks nothing (format checks only).

### Idempotency & conflict semantics

- Same dev + byte-identical metadata re-post → `200` (idempotent replay).
- Any other second write (different dev, or same dev with changed
  metadata) → `409 app_id_taken` with `current_owner_dev_pubkey`. App
  registrations are immutable (first-write-wins).

The in-memory registry enforces first-write-wins under a single write
lock; the S3 backend's `save_app` is additionally insert-if-absent so a
cross-instance race can't clobber the winner.

## Offline revocation (`403 dev_revoked`)

schema_service verifies certs **offline** — it cannot consult exemem's
`developers` table in real time, and unexpired DevCerts stay valid for
their 24h TTL. The design's remedy for key compromise is "redeploy
schema_service". To make that actionable, `AppIdentityConfig` carries a
`revoked_dev_pubkeys` denylist (`APP_IDENTITY_REVOKED_DEV_PUBKEYS`),
checked during verification. A cert whose `dev_pubkey` is on the denylist
yields `403 dev_revoked` on `schema_claim` (and is refused — collapsed to
`401 cert_invalid` — on `app_register`, since the design's `/v1/apps`
response set has no `403`).

## Out of scope here (downstream lanes)

- **Cross-env mirror + reconciler** (B2c). `POST /v1/apps` returns
  `mirrored_envs: [<this_env>]` — it honestly reports only the env that
  committed locally; it does not yet fan out to other envs.
- **DevCert mint endpoint** (B1, already shipped in `auth_service`).
- **Client publishing flow** (C2, `folddb dev`) — the producer of the
  `app_register` / `schema_claim` envelopes. The payload definitions
  above are the contract it must implement.
