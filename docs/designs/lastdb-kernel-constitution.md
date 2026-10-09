# LastDB kernel constitution

**Status:** active (2026-07-13)  
**North star:** `north-star-lastdb-minimal-node`  
**Card:** `lastdb-kernel-constitution-and-capability-tiers`  
**Pinned test:** `fold_db` → `kernel_constitution_test`

## Why this exists

LastDB Mini (`lastdbd`) is a **minimal product shell** (Unix-socket daemon, no
desktop UI, no ingestion). It is **not** a minimal codebase: it still depends
on the full `fold_db` core (~50k LOC) plus host/uds/identity packaging.

Before any peel, feature-gate, crate split, or greenfield “nano” experiment,
we need a shared definition of **what still counts as LastDB**. This document
is that definition. The automated test is the constitution: code is legal to
drop from the kernel only if the constitution stays green (or a deliberately
lower bar is re-ratified here).

## Success bars (two tracks)

| Track | Bar | Question it answers |
|-------|-----|---------------------|
| **Peel** (in-tree) | **A — API parity** | Can fbrain/fkanban-shaped clients still speak Mini routes? |
| **Nano** (clean-room repo) | **B — model parity** | Do we still have schema-named field atoms + durable mutate/query? |
| **LOC contest only** | **C** | Smallest durable schema+put/get — *not* the production rewrite target |

This constitution targets **Bar B for the kernel test** (in-process `FoldDB`
API) and documents **Bar A** as K2 (wire), without requiring a UDS test in
the first land.

## Capability tiers

### K0 — Stone (must always pass)

The smallest honest LastDB. Without these, it is a generic KV or file store.

| # | Capability | Notes |
|---|------------|--------|
| K0.1 | Open durable store at a path | `FoldDB::new(path)` |
| K0.2 | Declare a multi-field schema | ≥3 fields; hash key |
| K0.3 | Approve / make schema Available | write path requires Available |
| K0.4 | Create N records | `MutationType::Create` batch |
| K0.5 | Point query by primary key | `HashRangeFilter::HashKey` |
| K0.6 | List / multi-row read | unfiltered or multi-key query returns N |
| K0.7 | Hard delete | `MutationType::Delete`; default reads hide leftover tombstones |
| K0.8 | Shutdown + reopen same path | data + schema survive process boundary |

**Rough module home today:** `storage`, `atom`, `schema` (declare),  
`fold_db_core` (mutate/query), `db_operations` (write path).

### K1 — Brain (local app shape)

What fbrain/fkanban need from the **core** (not the socket).

| # | Capability | Notes |
|---|------------|--------|
| K1.1 | Multi-field co-key query | one row → multiple fields aligned |
| K1.2 | Update existing row | `MutationType::Update` |
| K1.3 | Tombstone visibility opt-in | `include_tombstones` / history tools |
| K1.4 | Schema identity hash | deterministic hash after declare/load |
| K1.5 | Native text / embedding search | owner search; optional semantic feature |
| K1.6 | Molecule / field history | heads + prior atoms for a key |
| K1.7 | CAS mutation (optional for brain, required for lastgit-shaped) | `CasExpectation` |

**Pinned in `kernel_constitution_test` today:** K1.1–K1.4.  
K1.5–K1.7 remain documented; covered by existing specialized tests until
explicitly folded into the constitution suite.

### K2 — Mini product (Bar A / wire)

What ships as `lastdbd` over the owner Unix socket. Same JSON shapes as
`lastdb_host` / `lastdb_uds::DataRoute`.

| Route / surface | Tier map |
|-----------------|----------|
| `POST /api/schemas/declare` | K0.2–K0.3 + wire |
| `GET /api/schemas`, `GET /api/schema/{name}` | catalog |
| `POST /api/mutation`, `POST /api/mutations/batch` | K0.4, K1.2 |
| `POST /api/query` | K0.5–K0.6, K1.1 |
| `GET /api/native-index/search` | K1.5 |
| molecule history / atom content routes | K1.6 |
| `GET /api/status`, `GET /api/system/auto-identity` | host, not kernel |
| owner peer-cred + app consent floor | host security (keep) |
| schema load from schema service | setup; mandatory for every schema identity; cached registered identities remain usable offline |

**Not pinned by `kernel_constitution_test` (in-process only).** A future
card may add a Mini UDS constitution smoke without expanding K0.

### K3 — Fleet / product accretion (first peel candidates)

Present in the monorepo; **not** required for local LastDB constitution.

| Area | Rough LOC (core) | Peel notes |
|------|------------------|------------|
| Cloud sync (`sync/`) | ~14k | default-off candidate |
| Cross-user sharing | ~1k+ | product, not kernel |
| Schema service client in daemon | host | optional for solo local |
| Sentry / crash telemetry | host | packaging |
| Homebrew / launchd / distribution | host | packaging |
| App blob upload storage | absent on Mini already | keep 404 |
| Desktop UI / ingestion / discovery | removed 2026-07-12 | archive branch only |

Anything in K3 may be deleted or feature-gated **only if** K0 (and the
product’s chosen K1/K2 subset) stay green.

**Cloud sync peel (2026-07-15):** the first in-tree peel is a Cargo feature,
not a workspace crate split. `fold_db/cloud-sync` compiles the S3/auth sync
engine, sync store decorators, and runtime sync coordinator; feature-off
`fold_db` keeps the local encrypted Sled stack and rejects `cloud_sync`
configuration at construction. `lastdb_node` opts into `cloud-sync` by default
so the shipped Mini daemon continues to build with the product cloud path.

## Constitution test contract

**Location:** `fold_db/crates/core/tests/kernel_constitution_test.rs`

**Run:**

```bash
cargo nextest run -p fold_db --test kernel_constitution_test
```

**Scenario (single integration test, K0 + K1 core):**

1. Create a fresh data directory.
2. Open `FoldDB`.
3. Load a 3-field hash-keyed schema (`Note`: `slug`, `title`, `body`); set Available.
4. Create three notes.
5. Point-query one by hash key; assert title/body.
6. List-style query; assert three rows.
7. Multi-field query returns aligned title+body for one key (K1.1).
8. Update one note’s body (K1.2); re-read.
9. Delete one note (K0.7); default query misses it; schema still listed.
10. Assert schema identity hash is present (K1.4).
11. `shutdown()`, drop DB, open again on the **same path** (K0.8).
12. Schema Available; two live notes remain; deleted note absent on default read.

**Failure policy:** a red constitution is a **kernel regression**, not a flaky
app test. Do not weaken the test to land a peel; re-ratify the doc first.

## How to use this for size reduction

1. **Peel:** propose a `cfg` / crate split; run constitution + (later) Mini smoke.
2. **Nano:** reimplement only K0 (then K1.1–K1.4) in a clean repo; compare LOC.
3. **CodeRings:** classify production source against these tiers so growth is
   attributed to K0 vs K3, not a single blob.

## Explicit non-goals (this document)

- Claiming production Mini is already “small”
- Mandating a rewrite timeline
- Requiring full K1.5–K1.7 or K2 UDS in the first constitution PR
- Data-format compatibility between nano and real `~/.lastdb` day one

## Related

- `lastdb_uds::uds_router::DataRoute` — Mini allowlist
- `lastdb_host` — shared wire/handler contract
- `north-star-lastdb-minimal-node` — product direction
- `north-star-coderings` — size vs complexity measurement
- Archive: `archive/desktop-dmg-pre-removal` — pre-Mini desktop surface
