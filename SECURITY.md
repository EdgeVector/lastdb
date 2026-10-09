# Security

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub's
[private vulnerability reporting](https://github.com/EdgeVector/lastdb/security/advisories/new)
on this repository. Do not open public issues for security reports.

## Data-at-rest posture

fold_db is being brought to a **password-manager-grade** at-rest posture:
all user data encrypted at rest, no plaintext fallback in shipping
configurations, a documented key hierarchy (OS keychain and/or
Argon2id-derived passphrase root), zeroized key material, and defined
lock/unlock semantics.

The authoritative threat model, verified current-state analysis, gap
list, and target architecture live in
[`docs/security/at-rest-threat-model.md`](docs/security/at-rest-threat-model.md).

Current state, abbreviated (2026-06-10):

- Atom content (`main`), derived metadata, and uploaded file blobs are
  AES-256-GCM encrypted at rest.
- Node identity, credentials, and API keys are encrypted under an
  OS-keychain master key in desktop (`os-keychain`) builds; headless
  builds rely on the `FOLDDB_MASTER_KEY` env var and otherwise fall back
  to plaintext with `0600` permissions — closing that fallback is
  tracked in the threat-model doc's gap list.
- Schemas, permissions, lineage, the consent ledger, and the embedding
  index are not yet encrypted; each has a tracked implementation card.

## Key invariants

- **No silent key minting.** If sealed data exists but no master key can
  be resolved, fold_db errors instead of minting a fresh key that would
  orphan existing ciphertext (`fold_db_node/src/secure_store.rs`).
- **No silent key rotation.** Existing keys are never overwritten by
  fallback paths; migrations only ever copy keys into more secure homes.
- **E2E before egress.** Cloud sync and blob upload payloads are
  encrypted client-side; the cloud stores ciphertext only.
