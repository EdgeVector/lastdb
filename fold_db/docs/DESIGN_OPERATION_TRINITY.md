# Operation Trinity — strict at-rest encryption

Canonical North Star: brain `project-operation-trinity`.

Sibling design: `DESIGN_HASHKEY_BLIND_V1.md`.

Status (2026-10-26): the Son atom content seal is retired. A disk format v2
atom row is plain JSON with no content seal. The block encryption of each run
file protects it at rest (see `docs/designs/native-element-encryption-v1.md`).
This program has no atom content seal, no `ATB:` binary atom row, and no atom
content open or row format switch. No range uses OPE: a format 4 home stores
the typed range payload inside the encrypted run-file blocks.

## Goal

Ship LastDB with **no plaintext fallbacks**: HashKey always blinded, RangeKey
and atom rows (and the file KDK inside them) always inside encrypted
run-file blocks, and file blobs always encrypted under that file KDK (cloud
**and** durable local CAS).

## Mapping

| Person | Surface | Strict seal |
|--------|---------|-------------|
| **Father** | Molecules (`mk:` / `mhr:`) | HashKey `blind_v1` · RangeKey `ope_v1` |
| **Son** | Atoms | Plain JSON row inside an encrypted run-file block; the file KDK lives inside the atom row |
| **Holy Ghost** | Files | Cloud CAS under per-blob DEK; local `cas_blobs` sealed under same DEK (DEK not stored in CAS) |

API/SDK always use **plaintext** HashKey/RangeKey (Option I). Storage tokens
never returned as API keys.

## Product defaults

| Knob | Binary default (unset env) | Trinity product primary | Escape (tests / migrate only) |
|------|----------------------------|-------------------------|--------------------------------|
| Hash key encoding | `blind_v1` (no setting) | same | none |
| Range key encoding | typed profile payload as plain bytes, format 4 (no setting) | same | none |
| Local CAS write | sealed dedup put | same | none |

## Code anchors

- Codec: `fold_db/crates/core/src/atom/molecule_key_codec.rs`
- Atom row codec: `fold_db/crates/core/src/db_operations/atom_store/codec.rs`
  (`encode_plain_atom`, `decode_plain_atom`)
- File KDK seal: `fold_db/crates/core/src/sharing/file_blob_seal.rs`
  (`seal_file_blob`, `seal_file_blob_for_access`)
- Local CAS: `fold_db/crates/core/src/sharing/blob_cas.rs`
- Resolve with DEK: `fold_db/crates/core/src/sharing/query_slice/file_bytes.rs` (`resolve_file_bytes`)
- Factory: `fold_db/crates/core/src/fold_db_core/factory/local.rs`

## Accepted residuals (v1)

- `mord:` / update_order may still store API-form hash/range strings
- Structural atom fields (uuid, schema name, timestamps) stay plain
- Frame AEAD packaging is optional defense-in-depth, not required for Trinity

## Primary promote

Never flip Tom’s primary home first. Use `lastdb-safe-upgrade` when the binary
changes. Situations notice required.
