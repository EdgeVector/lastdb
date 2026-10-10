# Atom content size limit

**Won't-undo product rule (Tom, 2026-07-24):** atom field payloads are size-capped so LastDB is not used as a blob store.

| | |
|--|--|
| **What is limited** | One atom's `content` field — the JSON value of a single schema field |
| **How measured** | `serde_json::Value::to_string().len()` (**before** encryption / ENC / frame AEAD) |
| **Default** | **64 KiB** (`65_536` bytes) |
| **Env override** | `LASTDB_MAX_ATOM_CONTENT_BYTES` |
| **Absolute max** | **1 MiB** (env cannot raise higher) |
| **Minimum** | **1 KiB** (env cannot set lower) |
| **Error** | `SchemaError::AtomContentTooLarge` → HTTP **413** `{ "error": "atom_content_too_large", size, limit, … }` |
| **Status** | `GET /api/status` → `limits.max_atom_content_bytes`; `lastdb status` prints a `Limits:` line |

## What atoms are (and are not)

- **Atoms** = immutable, content-addressed **structured field values** (strings, numbers, small JSON objects).
- **Not** pack files, images, PDFs, git objects, multi‑MB logs, or base64 of any of those.
- Large / opaque bytes → **file-blob / CAS**; store a pointer (hash, size, mime) in the atom.

This is the same lesson as lastgit pack storage: embedding binary as base64 in atoms blows storage and history.

## Env examples

```bash
# default (omit env): 64 KiB
unset LASTDB_MAX_ATOM_CONTENT_BYTES

# raise to 256 KiB for a special case (still under 1 MiB absolute max)
export LASTDB_MAX_ATOM_CONTENT_BYTES=256k

# plain bytes also fine
export LASTDB_MAX_ATOM_CONTENT_BYTES=131072
```

Units accepted: plain integer bytes, or trailing `k`/`kb`/`kib`, `m`/`mb`/`mib` (case-insensitive). Invalid values fall back to the 64 KiB default. Values above 1 MiB clamp to 1 MiB.

Restart `lastdbd` after changing the env (limit is resolved once per process).

## Enforcement points (fold_db core)

- `AtomStore::create_atom`
- `AtomStore::batch_store_atoms`
- `AtomStore::create_and_store_atom_for_mutation_deferred`
- Mutation prepare + CAS (via `create_atom`)

Reads of **legacy** atoms already larger than the limit still work; only **new writes** are rejected.

The `lastdb restore` command uses the 1 MiB absolute limit for authenticated replay. This preserves atoms that a source accepted with a raised limit. The restored daemon keeps its own configured limit for new writes.

The check reads **only the incoming value** — never the stored one — so an
over-limit document is *not* frozen: it can always be written smaller. What can
get stuck is a **client shape**: a read-modify-write caller that regrows the
value past the limit on every write and has no size awareness of its own.

## Observability — `target: lastdb::atom_size`

Every write goes through `enforce_atom_content_limit(schema_name, content)`,
which enforces the limit *and* names what happened. Grep the node log
(`lastdbd.err.log`) for `lastdb::atom_size`:

| Level | When | Fields |
|---|---|---|
| `ERROR` | write **rejected** | `schema`, `size_bytes`, `limit_bytes`, `over_by_bytes` |
| `ERROR` | accepted, but over `HEADROOM_ALARM_FRACTION` (80%) of the effective limit | `schema`, `size_bytes`, `limit_bytes`, `remaining_bytes` |
| `WARN` | accepted, but over the 64 KiB **default** (only while the env raise is in effect) | `schema`, `size_bytes`, `default_limit_bytes`, `effective_limit_bytes` |

Why each exists:

- **Rejection logging.** The typed `413` reaches the owner, but `render` only
  logs `5xx`, so a rejection used to leave the node log empty and the failing
  client unidentifiable. `schema` is the join key back to `lastdb ops`, which
  records `client` per schema.
- **Headroom alarm.** A single-row list index rewritten in full on every
  mutation grows monotonically, so it does not fail until the write that crosses
  the ceiling — and that write half-commits (record in, index out). Reporting at
  80% with `remaining_bytes` makes the wedge date derivable in advance.
- **Over-default warning.** While the limit is raised, these writes succeed
  silently, so nothing says which clients block a return to the default.

Ordinary writes log nothing.

### The anti-pattern these catch

Measured on the primary over one ~3 h daemon session (2026-07-28), the raise was
load-bearing for exactly three schemas — all the same shape, a **single-row list
index** rewritten in full on every mutation:

| Schema | Size | vs 64 KiB default |
|---|---|---|
| `fbrain/RecordListIndex` | 446 KB peak | 6.8× |
| `fkanban/CardListIndex` | 272 KB, `+5.9 KB` over the session | 4.1× |
| blob index (key `all_blobs`) | 259 KB, `+0.8 KB` over the session | 4.0× |

Two of the three grow monotonically, so the limit is a deadline, not a ceiling.
Raising the env buys days; bounding the document is the fix (`situations`
`recent_notices` did exactly that: 103,659 B → 9,258 B, budgeted against the
64 KiB default). Note the cost is not only the limit — each of those writes
re-encrypts and re-appends the whole document for a few bytes of delta.

## Client guidance

| Symptom | Action |
|---------|--------|
| `error: "atom_content_too_large"` | Split the field, truncate, or put bytes in file-blob/CAS |
| `remaining_bytes` shrinking in the log | Bound the document **now** — cap it by bytes, not by element count |
| Need a bit more headroom ops-side | Set `LASTDB_MAX_ATOM_CONTENT_BYTES` ≤ 1 MiB and restart — buys time, does not fix a growing document |
| Need multi‑MB binaries | **Do not** raise the atom limit — use the blob plane |

A read-modify-write client that maintains a list document should enforce its own
**byte** budget against the 64 KiB default, so it stays correct on a node that is
not running an env raise.

## Code pointers

- `fold_db/crates/core/src/atom/size_limit.rs`
- `crate::atom::max_atom_content_bytes()`
- Preference / brain: `preference-lastdb-atom-size-hard-limit-64kib`
