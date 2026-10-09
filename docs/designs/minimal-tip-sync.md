# Design: Minimal tips + sync (LWW clocks)

| Field | Value |
|-------|--------|
| Status | Accepted direction (2026-07-16) — implementation phased |
| Product owner | Tom |
| Repo | `EdgeVector/fold` |
| Related | Local-first outbox bandage; `preference-cloud-sync-never-blocks-local-rw`; native-index posture below |

## Summary

Make **local latest** cheap and **multi-device sync** still possible by replacing fat per-slot `AtomEntry` tips with thin tips `{ content_id, written_at, device_id }`. History, HashRange page/order indexes, and per-tip signatures become **policy**, not the default cost of every write.

## Goals

1. Tip storage scales with **#slots × small pointer**, not **#slots × crypto JSON**.
2. Sync remains **per-slot LWW** on `(written_at, device_id, content_id)`.
3. External APIs (`/api/mutation`, `/api/query`) stay stable.
4. Migration can slim existing `mk:` values in place (prefer **same keys**, thinner values).

## Non-goals (this track)

- Removing native search from Mini (see `native-index-posture.md`).
- Full CRDT / OT.
- Mandatory as-of for every schema.
- Reintroducing `schemaidx` full atom copies.

## Physical model (target)

| Key | Value | Default |
|-----|--------|---------|
| `atom:{C}` / `a:{C}` | Immutable value | Always |
| `mk:{M}:{key}` (or future `t:`) | **Thin tip head** `{ c, t, d, prev_tip_id? }` | Always |
| `tv:{version_id}` | Archived tip version (same shape as head) | On overwrite |
| `history:…` | Legacy mutation event log | **Off** (not written); purge with `clear-history --keep-last 0` |
| `mord` / `mhr` / … | HashRange helpers | **Off** unless field needs them |
| `cas_blobs` / blob ids | File bytes | Files only |
| Presence `k:{S}:{K}` | Record exists | Optional list helper |

**Thin tip fields:** `atom_uuid` (c), `written_at` (t), `device_id` (d), optional
`prev_tip_id` (link to previous **tip version** node).

**Dropped from default path:** per-tip `writer_pubkey`, `signature`, `provenance`;
always-on `history:` event stream; `schemaidx` (already retired).

### Tip version chain (real per-slot history)

Instead of a fat `history:` MutationEvent log, each overwrite **archives** the
prior tip head under `tv:{id}` and points the new head at it:

```text
mk:  →  { c: atom-3, t3, d, prev_tip_id: v2 }
tv:v2 → { c: atom-2, t2, d, prev_tip_id: v1 }
tv:v1 → { c: atom-1, t1, d, prev_tip_id: "" }
```

- **Molecule** stays the field container; chain is **per tip slot** (per record key)
- `as_of T`: walk `prev_tip_id` until `written_at <= T`
- Atoms stay immutable values; tips/versions only store ids
- Legacy `history:` rows may still exist until purged; new mutations do not append them

## LWW / sync

```
wins(a, b):
  a.t > b.t  OR  (a.t == b.t AND a.d > b.d)  OR  (tie → a.c > b.c)
```

- Trust: account + encrypted channel; optional **frame** signature on sync batches.
- Do **not** require per-tip signature verify on local read or default apply.

## History policy (examples)

| Schema class | History |
|--------------|---------|
| Telemetry / metrics | none |
| Kanban cards | none or keep_1 |
| Brain Reference / SOP | keep_1 or keep_n |

## Implementation phases

See build plan in session notes / follow-up cards. Ordered:

0. Design freeze (this doc)  
1. `ThinTip` type + dual deserialize (fat legacy + thin)  
2. Write thin; history off by default  
3. Read dual  
4. Sync apply LWW on thin fields + device id  
5. Schema policy hooks for history / HashRange indexes  
6. Backfill `migrate-thin-tips` + GC on **CoW copy** of real data  
7. Delete fat-tip write path  

**Primary cutover only after CoW real-data smoke is green.**

## Relationship to local-first outbox

Drop-oldest / local-first outbox is a **bandage** so full durable outbox never blocks local R/W. Longer-term store-level log capture is proposed separately (`store-level-log-based-cloud-sync.md`). Minimal tips reduce **what** lands in any log; local-first reduces **when** cloud couples into the write.

## Acceptance (track done)

- New tips have no signature/pubkey/provenance fields.  
- Two-device LWW by `(t, d, c)`.  
- After backfill on CoW data: tip class ≪ ~135 MiB historical fat tips.  
- Apps unchanged at the mutation/query wire.
