# Field value size limit

> **Model note (2026-11-01).** Since home format 7 (the record model) a local write has one
> limit: the 64 KiB record cap (`MAX_RECORD_BYTES`) on the whole record. The field value
> limit below (`LASTDB_MAX_FIELD_VALUE_BYTES`) now applies only to one field value of a
> peer record. Read [the canonical model](../../docs/lastdb-canonical-model.md) for the record cap.

**Product rule (2026-10-12):** one global ceiling of **256 KiB** for the content of a new record write. It was 64 KiB before. A record may pack many small fields up to that size. LastDB is still not a blob store: see "Which shape?" below.

| | |
|--|--|
| **What is limited** | One record's `content` field — the JSON value of a single schema field |
| **How measured** | `serde_json::Value::to_string().len()` (**before** encryption / ENC / frame AEAD) |
| **Default** | **256 KiB** (`262_144` bytes) |
| **Env override** | `LASTDB_MAX_FIELD_VALUE_BYTES` |
| **Absolute max** | **1 MiB** (env cannot raise higher) |
| **Minimum** | **1 KiB** (env cannot set lower) |
| **Error** | `SchemaError::FieldValueTooLarge` → HTTP **413** `{ "error": "field_value_too_large", size, limit, hint, … }` |
| **Status** | `GET /api/status` → `limits.max_field_value_bytes` (the effective value); `lastdb status` prints a `Limits:` line |

## Record cap of a format 7 home (64 KiB)

A format 7 home stores one record for each key. The encoded record holds every field of the key, its stamps and its metadata. Its cap is **64 KiB** (`MAX_RECORD_BYTES` in `fold_db/crates/model/src/record_value.rs`, decision D3). No env changes it.

| | |
|--|--|
| **What is limited** | The encoded record value at one key |
| **Error** | `SchemaError::RecordTooLarge` → HTTP **413** `{ "error": "field_value_too_large", "scope": "record", size, limit, hint, … }` |
| **Status** | `GET /api/status` → `limits.max_record_bytes`; the `Limits:` line of `lastdb status` prints `max_record` |

- A request that writes one key twice checks the cap on the final record of the request only.
- A peer import keeps a merged record above the cap out of the page. The stored record stays, and the import receipt names the record in `skipped_records` with the reason. The rest of the page lands.

## Why 256 KiB

- **Memory.** At the 10,000-record key budget of the warm set, the worst case is 10,000 × 256 KiB = 2.5 GiB of resident content.
- **Cloud item size.** A sealed 256 KiB record is about 341 KiB, under the 400 KiB DynamoDB item limit.

## Which shape?

Pick the shape from the kind of data. The 413 `hint` gives the same advice.

1. **Many small fields that change together: pack them into one record**, up to the ceiling. One read returns all of them. Keep the packed document bounded.
2. **A list or log that grows: write one record per entry** in a HashRange schema. The hash is the partition and the range is the entry key. Do not rewrite one growing document on every append.
3. **Large opaque bytes (files, packs, images, PDFs, base64 of any of those): store a file blob (CAS)** and keep a pointer (hash, size, media type) in the record.

## What records are (and are not)

- **Records** = immutable, content-addressed **field values**: strings, numbers, JSON objects, or a packed document up to the ceiling.
- **Not** pack files, images, PDFs, git objects, multi-MB logs, or base64 of any of those.

Embedding binary as base64 in records blows up storage and history. That is the lesson from lastgit pack storage.

## Env examples

```bash
# default (omit env): 256 KiB
unset LASTDB_MAX_FIELD_VALUE_BYTES

# raise to 512 KiB for a special case (still under the 1 MiB absolute max)
export LASTDB_MAX_FIELD_VALUE_BYTES=512k

# lower to 64 KiB (plain bytes also work: 65536)
export LASTDB_MAX_FIELD_VALUE_BYTES=64k
```

Units accepted: plain integer bytes, or trailing `k`/`kb`/`kib`, `m`/`mb`/`mib` (case-insensitive). Invalid values fall back to the 256 KiB default. Values above 1 MiB clamp to 1 MiB. Values below 1 KiB clamp to 1 KiB.

Restart `lastdbd` after changing the env (limit is resolved once per process).

## Enforcement points (fold_db core)

- `RecordStore::create_record`
- `RecordStore::batch_store_records`
- `RecordStore::create_and_store_record_for_mutation_deferred`
- Mutation prepare + CAS (via `create_record`)

Reads of **legacy** records already larger than the limit still work; only **new writes** are rejected.

The `lastdb restore` command uses the 1 MiB absolute limit for authenticated replay. This preserves records that a source accepted with a raised limit. The restored daemon keeps its own configured limit for new writes.

The check reads **only the incoming value** — never the stored one — so an
over-limit document is *not* frozen: it can always be written smaller. What can
get stuck is a **client shape**: a read-modify-write caller that regrows the
value past the limit on every write and has no size awareness of its own.

## Observability — `target: lastdb::record_size`

Every write goes through `enforce_field_value_limit(schema_name, content)`,
which enforces the limit *and* names what happened. Grep the node log
(`lastdbd.err.log`) for `lastdb::record_size`:

| Level | When | Fields |
|---|---|---|
| `ERROR` | write **rejected** | `schema`, `size_bytes`, `limit_bytes`, `over_by_bytes` |
| `ERROR` | accepted, but over `HEADROOM_ALARM_FRACTION` (80%) of the effective limit (209,715 bytes at the 256 KiB default) | `schema`, `size_bytes`, `limit_bytes`, `remaining_bytes` |
| `WARN` | accepted, but over the 256 KiB **default** (only while the env raise is in effect) | `schema`, `size_bytes`, `default_limit_bytes`, `effective_limit_bytes` |

Why each exists:

- **Rejection logging.** The typed `413` reaches the owner, but `render` only
  logs `5xx`, so a rejection used to leave the node log empty and the failing
  client unidentifiable. `schema` is the join key back to `lastdb ops`, which
  records `client` per schema.
- **Headroom alarm.** A single-row list index rewritten in full on every
  mutation grows monotonically, so it does not fail until the write that crosses
  the ceiling — and that write half-commits (record in, index out). Reporting at
  80% with `remaining_bytes` makes the wedge date derivable in advance. A packed
  document that does not grow also logs here once it passes 80%: bound it, or
  split it.
- **Over-default warning.** While the limit is raised above the default, these
  writes succeed silently, so nothing says which clients block a return to the
  default.

Ordinary writes log nothing.

### The anti-pattern these catch

Measured on the primary over one ~3 h daemon session (2026-07-28), when the
default was 64 KiB, the env raise was load-bearing for exactly three schemas —
all the same shape, a **single-row list index** rewritten in full on every
mutation:

| Schema | Size | vs the 64 KiB default then |
|---|---|---|
| `fbrain/RecordListIndex` | 446 KB peak | 6.8× |
| `fkanban/CardListIndex` | 272 KB, `+5.9 KB` over the session | 4.1× |
| blob index (key `all_blobs`) | 259 KB, `+0.8 KB` over the session | 4.0× |

Two of the three grow monotonically, so the limit is a deadline, not a ceiling.
A higher default buys time; it does not fix a growing document. Bounding the
document, or writing one record per entry, is the fix (`situations`
`recent_notices` bounded its document: 103,659 B → 9,258 B). Note the cost is
not only the limit — each of those writes re-encrypts and re-appends the whole
document for a few bytes of delta.

## Client guidance

| Symptom | Action |
|---------|--------|
| `error: "field_value_too_large"` | Pick the shape (see "Which shape?"): pack fewer fields, write one record per entry, or move opaque bytes to a file blob (CAS) |
| `remaining_bytes` shrinking in the log | Bound the document **now** — cap it by bytes, not by element count — or move to one record per entry |
| Need a bit more headroom ops-side | Set `LASTDB_MAX_FIELD_VALUE_BYTES` ≤ 1 MiB and restart — buys time, does not fix a growing document |
| Need multi-MB binaries | **Do not** raise the record limit — use the blob plane |

A read-modify-write client that maintains a list document should enforce its own
**byte** budget, well under the 256 KiB default so it stays correct on a node
that runs a lower env override.

## Code pointers

- `fold_db/crates/core/src/content_limit.rs`
- `crate::content_limit::max_field_value_bytes()`
- `crate::content_limit::FIELD_VALUE_TOO_LARGE_HINT` (the 413 `hint` text)
