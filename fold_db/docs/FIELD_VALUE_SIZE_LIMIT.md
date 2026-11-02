# Record and field value size limits

A format 8 home stores one record for each key. A local write has one size limit:
the record cap on the whole record. Read [the canonical model](../../docs/lastdb-canonical-model.md)
for the record model.

## Record cap (64 KiB)

The encoded record holds every field of the key, its stamps and its metadata.
Its cap is **64 KiB** (`MAX_RECORD_BYTES` in `fold_db/crates/model/src/record_value.rs`,
decision D3). No setting changes it.

| | |
|--|--|
| **What is limited** | The encoded record value at one key |
| **Error** | `SchemaError::RecordTooLarge` → HTTP **413** `{ "error": "field_value_too_large", "scope": "record", size, limit, hint, … }` |
| **Status** | `GET /api/status` → `limits.max_record_bytes`; the `Limits:` line of `lastdb status` prints `max_record` |
| **Code** | `fold_db/crates/core/src/db_operations/record_store/record_cap.rs` |

A request that writes one key in two groups stages the earlier record without the cap.
Only the final record of the request must fit. The store checks it after the last group
(`require_records_fit`).

## Peer field value limit

`GET /api/status` reports `limits.max_field_value_bytes`. The `Limits:` line of
`lastdb status` prints it as `max_peer_field_value`. No write path reads this
limit. A local write uses the record cap above.

| | |
|--|--|
| **What the status shows** | The configured peer field value limit |
| **Default** | **256 KiB** (`262_144` bytes) |
| **Setting** | `LASTDB_MAX_FIELD_VALUE_BYTES` |
| **Range** | 1 KiB to 1 MiB. A value outside the range clamps to the nearest end |

Units: plain integer bytes, or a `k`/`kb`/`kib` or `m`/`mb`/`mib` suffix (any case).
An invalid value selects the 256 KiB default. The node reads the setting once at
start, so restart `lastdbd` after a change.

## Which shape?

Pick the shape from the kind of data. The 413 `hint` gives the same advice.

1. **Many small fields that change together: put them in one record**, up to the cap.
   One read returns all of them. Keep the record bounded.
2. **A list or log that grows: write one record per entry** in a HashRange schema.
   The hash is the partition and the range is the entry key. Do not rewrite one
   growing record on every append.
3. **Large opaque bytes (files, packs, images, PDFs, base64 of any of those): store a
   file blob (CAS)** and keep its pointer (hash, size, media type) in the record.

A record is not a blob store. Base64 bytes in a record make the record and its
field history large.

## Client guidance

| Symptom | Action |
|---------|--------|
| `error: "field_value_too_large"` with `scope: "record"` | Pick the shape (see "Which shape?"): fewer fields in one record, one record per entry, or a file blob for opaque bytes |
| A record that grows on each write | Bound the record by bytes, not by element count, or move to one record per entry |
| Multi-MB binary data | Use a file blob. The record cap does not change |

A client that reads, changes and writes back a list record must keep its own byte
budget, well under the 64 KiB cap.

## Code pointers

- `fold_db/crates/model/src/record_value.rs` (`MAX_RECORD_BYTES`)
- `fold_db/crates/core/src/db_operations/record_store/record_cap.rs`
- `fold_db/crates/model/src/content_limit.rs` (`RECORD_TOO_LARGE_HINT`, `FIELD_VALUE_TOO_LARGE_HINT`)
- `fold_db/crates/core/src/content_limit.rs` (`max_field_value_bytes`)
