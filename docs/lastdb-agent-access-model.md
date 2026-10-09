# LastDB access model for agents

> **READ FIRST — LastDB access (can't miss):** Before any schema, query, mutation, dual-write, or heal:
> `brain get concepts-lastdb-agent-access-model`
> · `docs/lastdb-agent-access-model.md` (this workspace)
> · **Requirement:** `docs/lastdb-access-complexity-requirements.md` · `brain get requirement-lastdb-access-complexity`
> · public: https://thelastdb.com/docs/agent-access-model
> · https://thelastdb.com/llms.txt

**Won't-undo:** LastDB is **Dynamo-style NoSQL with access-pattern design**, not SQL.

| Durable homes | |
|---|---|
| **Canonical model (umbrella)** | `docs/lastdb-canonical-model.md` · `concepts-lastdb-canonical-model` |
| **rehydrate / resident graph** | `docs/lastdb-rehydrate.md` · `concepts-lastdb-rehydrate` |
| **Requirement (complexity + no scan)** | `docs/lastdb-access-complexity-requirements.md` · `requirement-lastdb-access-complexity` |
| Brain concept | `brain get concepts-lastdb-agent-access-model` |
| Workspace | `docs/lastdb-agent-access-model.md` (this file) |
| Public | https://thelastdb.com/docs/agent-access-model · https://thelastdb.com/llms.txt |
| Deep internals | `docs/lastdb-storage-schema-to-atoms.html` |

**Audience:** every agent that reads or writes LastDB Mini (`lastdbd` on `~/.lastdb/data/folddb.sock`), fkanban/kanban, brain/fbrain, lastgit metadata, situations, etc.

---

## 1. One-sentence model

**Design for the query you need.** Primary data is keyed one way (e.g. card by slug). Other queries use a **second schema**. Shared payload fields bind into a **protein** and fold sibling tips. Dual-write only a **thin projection** that is a different product shape — same idea as a Dynamo GSI projection the app maintains.

This is intentional NoSQL. It is not “almost SQL.” Do not invent field-equality filters, JOINs, or full-table scans.

---

## 2. Required query complexity (product law)

Full requirement: `docs/lastdb-access-complexity-requirements.md`.

| Operation | Shape | Complexity |
|-----------|--------|------------|
| **Point get** | exact `HashKey` or `HashRangeKey { hash, range }` | **O(1)** |
| **Range under one hash** | `HashKey` on HashRange (partition), `HashRangePrefix`, `HashRangeRange` | **O(log M)** |
| **Multi-get** | K exact keys | **O(K)** |
| **Full scan** | — | **Not supported** |

**M** = number of live keys under **one** hash partition (ordered by range key).

- Hash (partition) is required for every range query.
- Field count is a fixed schema constant — not an asymptotic variable.
- There is **no full scan**. LastDB is like DynamoDB, not SQL.

---

## 3. Dynamo map (use this vocabulary)

| Dynamo | LastDB / fold_db |
|--------|------------------|
| Table | Schema (`DeclarativeSchemaDefinition`) |
| Partition key | HashKey (or HashRange **hash** component) |
| Sort key | RangeKey (HashRange **range** component) |
| Item attributes | Fields (assembled at query time from tips + atoms) |
| `GetItem` | exact `HashKey` / `HashRangeKey` → **O(1)** |
| `Query` on PK (+ SK prefix/between) | hash-scoped range → **O(log M)** |
| GSI / second table for another access pattern | **Second schema** — multi-key same-product payload fields: **node proteins + tip fold**; thin projections may still be app dual-write |
| `Scan` | **Not a product operation** — do not use; design another key |
| LSI/GSI dual-write (platform) | Prefer proteins for multi-key same-product fields; app dual-write only for projections that are not the same field protein |

If you already know Dynamo access-pattern design, you already know 80% of how to use this database.

**Multi-key proteins (2026-07-30+):** When two schemas index the **same product**
under **different keys**, Mini binds shared field identities into proteins and
propagates new mutations by folding sibling tips to the **same atom**. Apps do
**not** reach into proteins and should not dual-write those payload fields after
bind. Guide: fold `docs/app-developers-multi-key-proteins.md` · brain
`concepts-lastdb-protein-write-fold`.

---

## 4. How a “record” is stored (just enough)

You do **not** need the full engine lecture for day-to-day CRUD. Minimum:

1. **Schema** — names fields and keying mode (`Single` | `Hash` | `Range` | `HashRange`). Catalog, not the row store.
2. **Field** — each field has its own **molecule** (tip index): “for this key, which atom is current?” Same key coordinates `(hash[, range])` across fields assemble a logical row at read time.
3. **Atom** — immutable, content-addressed field value (`atom:{uuid}`).
   **Size fence:** each atom's content is hard-capped (default **64 KiB** serialized JSON; env `LASTDB_MAX_ATOM_CONTENT_BYTES`, absolute max 1 MiB). Atoms are structured field values, **not** a blob store — packs/files go in file-blob/CAS. See fold `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
4. **Query** — resolve tips for the keys you care about, batch-fetch atoms, zip fields into the JSON the API returns.

Multi-field “rows” (a Card with title+body+column) are **co-keyed at read time**, not one physical row blob.

Deeper: open `docs/lastdb-storage-schema-to-atoms.html` (encryption layers, `mk:` keys, ENC).

---

## 5. Access patterns — do / don't

### Do

| Intent | How |
|--------|-----|
| Load one card / brain record by slug | **HashKey** on the primary schema (`Card`, Concept, …) via CLI `show` / `get` — **O(1)** |
| List a board / column | **BoardCards** HashRange: hash = board, optional range prefix = `column#` — **O(log M)** for that board; via `kanban list` / `list --column` |
| Point update | Mutate **primary** fields; prefer protein fold for shared fields; dual-write only a thin projection (prefer `kanban` / `fkanban` / `brain` over raw multi-schema surgery) |
| Another list shape | New keyed Hash/HashRange schema — protein fold for shared fields; dual-write only a thin projection; never scan |
| Read several independent things from **different schemas** | **`POST /api/queries/batch`** — `{"queries":[<`/api/query` body>, …]}`, up to 64, replies in request order with one `status` per item. Not a join: items cannot use each other's results. Within one schema use `HashRangeKeys`. See `docs/designs/lastdb-query-batch-route.md` |
| Health / load | `lastdb status` (exit 0 iff the owner socket answers `/health`) + socket `/api/status` request_ops; playbook `sop-lastdb-request-ops-telemetry` |
| Am I compatible with this node? | `GET /api/version` (both sockets, no state) → `{ok, api_version, build, capabilities, instance_id}`. Compare the `api_version` your client was built against; `/health` carries the same `api_version`. A `400 {"kind":"unknown_key"}` on a data route means the node does not know a key you sent — the client is newer than the node; upgrade the node (`brew upgrade lastdb`), do not rewrite the request. The key itself is never echoed (I4) |

### Don't

| Anti-pattern | Why |
|--------------|-----|
| Full scan as list | **Not supported**; not Dynamo Query; historical node meltdowns |
| Field-equality filter like `{ column: "todo" }` as if SQL WHERE | Filters are key-shaped only; fake filters 400 or force client fallbacks |
| Cross-partition range without hash | Same class as scan — design a hash that partitions the list |
| N+1 point-get every slug after a bad list | Load storms |
| “Fix” empty list by bulk re-upserting secondaries while primary HashKey is broken | Destructive dual-write/heal thrash |
| Restart primary for `:9001` / busy errors | Socket is the plane; TCP is retired; busy ≠ dead |
| Upgrade lastdbd by pointing a new binary at live `~/.lastdb` first | `lastdb-safe-upgrade` only |

---

## 6. Primary vs secondary (dual-write)

### Primary

- **Source of truth** for the entity (e.g. Card by slug, including body).
- Access: HashKey(slug) / show / get → **O(1)**.

### Secondary index (here)

Two different product shapes share the “second access pattern” vocabulary:

#### A. Multi-key siblings of the same product (node proteins)

- Two schemas, **different key layouts**, same product (e.g. BoardCards by board vs MilestoneCards by milestone).
- Shared **payload fields** share field identity and bind into **proteins**.
- On **mutate**, the node **folds sibling tips** to the same atom — apps use one write path; both keys stay addressable.
- History when a layout joins: node **tip backfill**, not app dual-write of every row.
- App brief: fold `docs/app-developers-multi-key-proteins.md`.

#### B. Thin denormalized projection (app dual-write)

- A **second schema** for a **different query shape** that is **not** the same field protein (classic thin membership / GSI-style projection).
- Usually a **thin projection** (titles, columns, positions — **not** full body).
- **Dual-written:** on every logical create/move/update of the primary, the app **also** updates the secondary (put new tip keys, delete old sort keys, purge orphans).
- List access: hash-scoped range → **O(log M)**.

### Important: proteins vs dual-write

Atoms/molecules are addressable. **Multi-key same-product fields** now use
**proteins** (shared atom, tip fold) — do **not** re-implement that from the app
and do **not** poke `protein:` / `molprot:` keys.

**Thin projections** that are a different product shape may still be
**denormalized dual-writes** (separate molecules/atoms for list fields). Same
idea as a Dynamo GSI projection the app maintains.

Implications:

- Prefer proteins for multi-key same-product payload; dual-write only where the product still requires a thin projection.
- **Drift** (list ≠ show) remains a failure mode for dual-written projections.
- **Heal** tools re-sync secondary from primary **truth** when dual-write is in play. Only run heals when primary point-reads are healthy.
- Primary remains truth; secondary list structure is disposable when it is a projection.

---

## 7. Product map (EdgeVector daily driver)

| Product | Primary access | Secondary / notes |
|---------|----------------|-------------------|
| **fkanban / kanban** | Card HashKey(slug) **O(1)** | **BoardCards** HashRange(board, `col#pos#slug`) **O(log M)**; prefer CLI over inventing raw queries |
| **brain / fbrain** | Concept (etc.) by slug / type | Search/ask need app capability; after restart may need consent once — never grant from unattended routines |
| **lastgit** | Repo / CR / policy schemas | Chatty CR polling is normal; cheap queries ≠ “node on fire” by themselves |
| **situations** | Situation/index schemas | Preflight before mutating shared systems |

Socket: `~/.lastdb/data/folddb.sock`. Use installed CLIs from `~/.local/bin` (host-track), not random WIP checkouts.

Config hashes for fkanban live in `~/.fkanban/config.json` (`schemaHashes.card`, `board_cards`, …).

---

## 8. Encryption (agent-relevant only)

**Product defaults (Operation Trinity / Mini):**

| Piece | Shipping default | Notes |
|-------|------------------|--------|
| HashKey in `mk:…` | **`blind_v1`** when env unset | Override only with `LASTDB_HASH_KEY_ENCODING=plain` (tests) |
| RangeKey in `mk:…` | **`ope_v1`** when env unset | Override only with `LASTDB_RANGE_KEY_ENCODING=plain` (tests) |
| Atom **content** | Sealed under account E2E key on write | **Dual-read** of legacy plain until reseal; **`LASTDB_ATOM_CONTENT_STRICT=1`** = fail-closed open (Trinity Son) |
| File KDK + local CAS | Per-blob DEK in sealed content; local `cas_blobs` sealed under DEK | Plain CAS writes need `LASTDB_ALLOW_PLAIN_CAS=1` |
| Per-field unique DEK | **Not the model** | Same account `encryption_key` for content seal |
| Schema catalog | Mixed plain / `ENC:…` over history | Open before parse |

**Agent rules:**

1. **Open before parse** — never feed `ENC:…` to `serde_json`. Column-1 deserialize errors usually mean ciphertext hit JSON.
2. Assume product keys are **blind+OPE** unless you deliberately set plain for tests.
3. Do not enable `LASTDB_ATOM_CONTENT_STRICT=1` on a home until atom content is re-sealed (or dual-read is intentional).
4. Depth: `fold_db/docs/DESIGN_OPERATION_TRINITY.md` · brain `project-operation-trinity` · `concepts-lastdb-enc-clean-model` (today vs target).

---

## 9. Agent checklist (copy into plans)

```text
[ ] Access pattern named (point O(1) / hash-scoped range O(log M)) — not “SELECT *”
[ ] Using primary HashKey for single-entity truth
[ ] Using product secondary (BoardCards / CLI list) for board list — never full scan
[ ] Mutations go through product CLI; prefer protein fold for shared fields; dual-write only a thin projection
[ ] No bulk secondary heal while primary reads fail
[ ] Socket health: kanban list / targeted brain get — not brain doctor / :9001
[ ] Load: lastdb status (exit 0 = socket reachable) / ops (or /api/status request_ops) before blaming “node down”
[ ] Version: GET /api/version api_version ≥ what the client needs; a 400 kind=unknown_key is version skew, not a bad request
[ ] Secrets: LastSecrets only — never put secrets in brain/kanban bodies
[ ] ENC: open before parse; keys default blind_v1+ope_v1; content dual-read until STRICT after reseal
```

---

## 10. Related records / docs

| What | Where |
|------|--------|
| **Canonical model (schema→field→molecule→atom→file, proteins)** | `docs/lastdb-canonical-model.md` · `concepts-lastdb-canonical-model` |
| **rehydrate (resident-first miss path)** | `docs/lastdb-rehydrate.md` · `concepts-lastdb-rehydrate` |
| **Access complexity requirement** | `docs/lastdb-access-complexity-requirements.md` · `requirement-lastdb-access-complexity` |
| This concept (brain) | `concepts-lastdb-agent-access-model` |
| Storage depth (HTML) | `docs/lastdb-storage-schema-to-atoms.html` |
| ENC clean model | `concepts-lastdb-enc-clean-model` · this file §8 |
| Ops telemetry | `sop-lastdb-request-ops-telemetry` |
| Safe Mini upgrade | `sop-lastdb-safe-upgrade` / skill `lastdb-safe-upgrade` |
| Repo / workspace map | `concepts-edgevector-repo-layout` |

---

## 11. Framing for humans and agents

> LastDB is **Dynamo-shaped NoSQL**. **Get = O(1). Range under a hash = O(log M). No full scan.** Primary key = one query shape. Second schemas for other shapes: protein fold for shared fields; dual-write only a thin projection. Field/molecule/atom is how values are stored under the hood; day-to-day you think **table + PK/SK + optional projection table**.

That framing is accurate and lower cognitive load than “it’s not SQL so everything is novel.” The novel parts are **protein fold for shared fields**, **app-owned thin-projection dual-write**, **field-assembled items**, and **at-rest content seal** (open before parse; key encodings optional) — not the idea of partition/sort keys.
