# Operation Trinity — strict at-rest encryption

Canonical North Star: brain `project-operation-trinity`.

Sibling designs: `DESIGN_HASHKEY_BLIND_V1.md`, `DESIGN_RANGEKEY_OPE_V1.md`.

## Goal

Ship LastDB with **no plaintext fallbacks**: HashKey always blinded, RangeKey
always OPE, atom content and atom file-KDK always sealed, and file blobs always
encrypted under that file KDK (cloud **and** durable local CAS).

## Mapping

| Person | Surface | Strict seal |
|--------|---------|-------------|
| **Father** | Molecules (`mk:` / `mhr:`) | HashKey `blind_v1` · RangeKey `ope_v1` |
| **Son** | Atoms | `content` sealed under account E2E key; file KDK lives inside sealed content |
| **Holy Ghost** | Files | Cloud CAS under per-blob DEK; local `cas_blobs` sealed under same DEK (DEK not stored in CAS) |

API/SDK always use **plaintext** HashKey/RangeKey (Option I). Storage tokens
never returned as API keys.

## Product defaults

| Knob | Binary default (unset env) | Trinity product primary (resealed home) | Escape (tests / migrate only) |
|------|----------------------------|------------------------------------------|--------------------------------|
| `LASTDB_HASH_KEY_ENCODING` | unset → `blind_v1` | same | `plain` |
| `LASTDB_RANGE_KEY_ENCODING` | unset → `ope_v1` | same | `plain` |
| Atom content open | **dual-read on** (legacy plain opens) | **`LASTDB_ATOM_CONTENT_STRICT=1` forever** on LaunchAgent after `lastdb_reseal_atom_content` | dual-read during migrate only |
| Atom row format | legacy JSON row | `LASTDB_ATOM_CONTENT_BINARY=1` after all readers support `ATB:` | unset during reader rollout |
| Local CAS plain write | **refused** | same | `LASTDB_ALLOW_PLAIN_CAS=1` |

> **Won't-undo:** after a home is resealed, STRICT is not optional on that
> product primary. Dual-read remains the binary default so *unresealed* homes
> and migrate tooling are not bricked — it is not the long-term posture for a
> completed Trinity cutover.

## Code anchors

- Codec: `fold_db/crates/core/src/atom/molecule_key_codec.rs`
- Content seal: `fold_db/crates/core/src/atom/content_at_rest.rs`
- File KDK seal/open: `fold_db/crates/core/src/sync/engine/file_blob.rs`
- Local CAS: `fold_db/crates/core/src/sharing/blob_cas.rs`
- Resolve with DEK: `fold_db/crates/core/src/sharing/query_slice.rs` (`resolve_file_bytes`)
- Factory: `fold_db/crates/core/src/fold_db_core/factory/local.rs`

## Binary atom rows

The `ATB:` container removes the JSON-string base64 layer from sealed atom
content. It keeps one atom row and the existing storage key. The row contains:

- The four-byte `ATB:` marker.
- A four-byte big-endian JSON header length.
- A JSON header without the `content` field.
- Raw `ENB:` content bytes.

Readers accept legacy JSON rows and `ATB:` rows. Writers use legacy JSON unless
`LASTDB_ATOM_CONTENT_BINARY=1`. Enable the writer only after every reader uses a
binary that accepts `ATB:` rows.

Use `lastdb_recompress_atom_content --binary` for an offline, bounded rewrite.
Run it on an encrypted CoW copy first. The report separates the nested content
byte reduction from the logical row byte reduction. It reports zero outer KV
seam reduction because `LASTDB_KV_AT_REST_RAW` controls that layer.

## Accepted residuals (v1)

- `mord:` / update_order may still store API-form hash/range strings
- Structural atom fields (uuid, schema name, timestamps) stay plain
- OPE is order-preserving (order leakage accepted; not full confidentiality)
- Frame AEAD packaging is optional defense-in-depth, not required for Trinity

## Primary promote

Never flip Tom’s primary home first. CoW Trinity bar GREEN, then promote
(candidate via `lastdb-safe-upgrade` when binary changes; STRICT env via
LaunchAgent when Son cutover). Situations notice required.

### Terminal verification (M0 proof checklist)

Run from fold checkout (CoW never primary first):

```bash
LASTDBD=…/lastdbd RESEAL=…/lastdb_reseal_atom_content \
  SCRATCH=… ./scripts/operation-trinity-cow-bar.sh <run-id>
```

| Gate | Evidence |
|------|----------|
| Father encodings | Boot log `BlindV1` + `OpeV1` |
| Father storage keys | Bar scans `tips` `mk:` windows; leftover `field_tips` is residue. API slug `default` is **not** the storage hash segment |
| Father Option I API | Board query `filter.HashKey=default` returns title |
| Son reseal | `reseal-*.json` ok, errors=0, sealed_or_resealed > 0 |
| Son at-rest | Atom `.seg` files contain `ENC:` samples under CoW home |
| Son plain-fail | `cargo test` strict open + `trinity_bar` under `LASTDB_ATOM_CONTENT_STRICT=1` |
| Ghost | `cargo test -p fold_db --lib sharing::blob_cas` |
| Primary | LaunchAgent has `LASTDB_ATOM_CONTENT_STRICT=1`; Board + `kanban list` GREEN |

Evidence paths: `$SCRATCH/father-bar.log`, `son-bar.log`, `ghost-bar.log`,
`cow-trinity-bar-product-*.log`, `reseal-*.json`.
