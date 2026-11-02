# Operation Trinity — strict at-rest encryption

Canonical North Star: brain `project-operation-trinity`.

Sibling design: `DESIGN_HASHKEY_BLIND_V1.md`.

Status: a format 8 record is plain JSON with no field value seal. The block
encryption of each run file protects it at rest (see
`docs/designs/native-element-encryption-v1.md`). No range uses OPE: a format 8
home stores the typed range payload inside the encrypted run-file blocks.

## Goal

Ship LastDB with **no plaintext fallbacks**: HashKey always blinded, RangeKey
and records (and the file KDK inside them) always inside encrypted run-file
blocks, and file blobs always encrypted under that file KDK (cloud **and**
durable local CAS).

## Mapping

| Person | Surface | Strict seal |
|--------|---------|-------------|
| **Father** | Field keys (the HashKey and RangeKey of each record key) | HashKey `blind_v1` · RangeKey typed payload inside encrypted run blocks |
| **Son** | Records | Plain JSON record inside an encrypted run-file block; the file KDK lives inside the record |
| **Holy Ghost** | Files | Cloud CAS under per-blob DEK; local `cas_blobs` sealed under same DEK (DEK not stored in CAS) |

API/SDK always use **plaintext** HashKey/RangeKey (Option I). Storage tokens
never returned as API keys.

## Product defaults

| Knob | Default | Setting |
|------|---------|---------|
| Hash key encoding | `blind_v1` | none |
| Range key encoding | typed profile payload as plain bytes inside encrypted run blocks | none |
| Local CAS write | sealed dedup put | none |

## Code anchors

- Codec: `fold_db/crates/model/src/field_key_codec.rs`
- Record value: `fold_db/crates/model/src/record_value.rs`
- File KDK seal: `fold_db/crates/core/src/sharing/file_blob_seal.rs`
  (`seal_file_blob`, `seal_file_blob_for_access`)
- Local CAS: `fold_db/crates/core/src/sharing/blob_cas.rs`
- Resolve with DEK: `fold_db/crates/core/src/sharing/query_slice/file_bytes.rs` (`resolve_file_bytes`)
- Factory: `fold_db/crates/core/src/fold_db_core/factory/local.rs`

## Accepted residuals

- Structural record fields (schema name, stamps, timestamps) are plain JSON
  inside the encrypted run-file blocks, the same as field values.

## Primary promote

Never flip Tom’s primary home first. Use `lastdb-safe-upgrade` when the binary
changes. Situations notice required.
