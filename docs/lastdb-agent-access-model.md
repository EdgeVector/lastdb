# LastDB access model for agents

Use one schema with named alternate keys for the same records.
Keep reusable field definitions separate from mutable record values.
See [the multi-key schema guide](multi-key-schemas.md).

> **READ FIRST — LastDB access (can't miss):** Before any schema, query, mutation, dual-write, or heal, read this file.
> Then `docs/lastdb-canonical-model.md` (the canonical model), `docs/lastdb-access-complexity-requirements.md`
> (the complexity law), and, for data that grows or is large, the Shape rules below.
> Public: https://github.com/EdgeVector/lastdb/blob/main/docs/lastdb-agent-access-model.md · https://thelastdb.com/llms.txt

**Won't-undo:** LastDB is **Dynamo-style NoSQL with access-pattern design**, not SQL.

| Durable homes | |
|---|---|
| **Canonical model (umbrella)** | `docs/lastdb-canonical-model.md` |
| **Core logical memory and native disk** | `docs/lastdb-rehydrate.md` |
| **Requirement (complexity + no scan)** | `docs/lastdb-access-complexity-requirements.md` |
| **Record lifecycle (delete, purge, TTL, file blobs)** | `docs/lastdb-canonical-model.md` §1 · `docs/lastdb-ttl-sweep.md` · `docs/lastdb-blob-counter.md` |
| This file | `docs/lastdb-agent-access-model.md` |
| Public | https://github.com/EdgeVector/lastdb/blob/main/docs/lastdb-agent-access-model.md · https://thelastdb.com/llms.txt |

**Audience:** every agent that reads or writes LastDB Mini (`lastdbd` on `~/.lastdb/data/folddb.sock`), fkanban/kanban, brain/fbrain, lastgit metadata, situations, etc.

---

## 1. One-sentence model

Design each key for a required read. A schema owns its records: one stored record per primary key.
Named alternate keys address those same records through fields declared in the schema.
An alternate read returns the primary key. Mutations use that primary key.
The native request updates the record and its alternate entries atomically.

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

## 3. Key map

| Required read | LastDB address |
|---|---|
| One record | Primary hash, or primary hash and range |
| Ordered records under one hash | A declared range, with a fixed hash |
| The same records through another key | A named alternate key on the same schema |
| Several known records | A batch of exact keys |
| Relationships with their own values | A separate keyed schema |

Canonical field identity supplies definitions and types. It does not connect record values across schemas.

---

## 4. How a “record” is stored (just enough)

You do **not** need the full engine lecture for day-to-day CRUD. Minimum:

1. **Schema** — names fields and the supported logical key type: `Hash` or `HashRange`. Older template metadata can retain other forms.
2. **Record** — every field of the schema at one primary key, stored as **one value** (`rk:` key). Each field has a stamp (write time, logical counter, device, mutation). A write changes only the fields it names; when two writes meet, each field keeps the newer stamp (the field merge). The record has one metadata block (source file name and metadata map).
   **Record cap:** a record holds at most **64 KiB** — all fields, stamps and metadata together. A list or log that grows is one record per entry in a HashRange schema. A record is **not** a blob store — packs/files go in a file blob with a reference. See the Shape rules below.
3. **Alternate entry** — one `ak:` key per named alternate key of a record. It holds the primary key, not a copy of the record.
4. **Query** — read the record at the key (1 read), a range under one hash (1 range read), or an alternate key (the alternate entries, then one batch of records). The reply carries the primary key and the record metadata. A rewrite keeps no older version, and the home has no `as_of` read; a field history policy can keep copies of old values of one field.

A Card with title+body+column is **one stored record**. The node does not join separate field values at read time.

Deeper: `docs/lastdb-canonical-model.md` (the record, the write path, delete and TTL), `docs/lastdb-rehydrate.md` (memory and disk, written for the field key model).

### Shape rules

Pick the shape of your data before you write the schema. The node enforces the ceiling; it cannot fix a bad shape.

1. **Data that grows without bound is one record per entry** in a HashRange schema. The hash is the partition (an owner, a day) and the range is the entry key. Never rewrite one growing document on every append: each write re-encrypts the whole document, and it fails at the ceiling.
2. **Large opaque write-once bytes go to a file blob** (files, packs, images, PDFs, base64 of any of those). The record holds a reference (hash, size, media type). Upload the blob first and write the record that names it soon after: a blob that nothing references is deleted after a dwell of one hour (`docs/lastdb-blob-counter.md`).
3. **Everything else is one record, up to 64 KiB.** Keep facts that different devices write independently in separate fields: the field merge keeps each field's newer value. Do not pack independent facts into one field value.
4. **A TTL needs an age rule.** A schema declares retention in the app manifest. A HashRange range key must start with 20 digits of unix seconds, zero-padded. Use a day-slice hash for a log. A Hash schema uses its latest write time. Set the policy before the first write, or run the explicitly confirmed personal-schema backfill. It keeps original write ages and registers old hashes. A TTL deletes records for good (`docs/app-manifest-retention.md`, `docs/lastdb-ttl-sweep.md`).
5. **A lease, a claim or a counter that several writers change needs compare-and-set.** Put `expected` on the mutation (`Absent` or `Value`). It is atomic on one node. Across devices last-write-wins decides, and the loser is kept as a conflict record, so run such state on one device. A node that does not know `expected` can ignore it: check the node before you claim a lease.

There is no history, no scan and no sequence of older versions to fall back on. Design the key for the read.

---

## 5. Access patterns — do / don't

### Do

| Intent | How |
|--------|-----|
| Load one card / brain record by slug | **HashKey** on the primary schema (`Card`, Concept, …) via CLI `show` / `get` — **O(1)** |
| List a board / column | Card named key `by_board`: hash = board, optional column range prefix; via `kanban list` |
| Point update | Mutate the primary record. The native request updates its declared alternate addresses. Prefer the product CLI. |
| Remove data | `Delete`, `Purge` (also releases history and conflict records), or a manifest TTL. Each leaves a tombstone so an older write cannot bring the record back, and releases file blob references in the same batch. Send at most the number of keys that the 400 error names. See `docs/lastdb-canonical-model.md` §1 |
| Another list of the same records | Declare a named alternate key from the schema fields. |
| Read several independent things from **different schemas** | **`POST /api/queries/batch`** — `{"queries":[<`/api/query` body>, …]}`, up to 64, replies in request order with one `status` per item. Not a join: items cannot use each other's results. Within one schema use `HashRangeKeys`. See `docs/designs/lastdb-query-batch-route.md` |
| Health / load | `lastdb status` (exit 0 iff the owner socket answers `/health`) + socket `/api/status` request_ops; `lastdb ops` (`docs/lastdb-ops-telemetry.md`) |
| Am I compatible with this node? | `GET /api/version` (both sockets, no state) → `{ok, api_version, build, capabilities, instance_id}`. Compare the `api_version` your client was built against; `/health` carries the same `api_version`. A `400 {"kind":"unknown_key"}` on a data route means the node does not know a key you sent — the client is newer than the node; upgrade the node (`brew upgrade lastdb`), do not rewrite the request. The key itself is never echoed (I4) |

### Don't

| Anti-pattern | Why |
|--------------|-----|
| Full scan as list | **Not supported**; not Dynamo Query; historical node meltdowns |
| Rewrite one growing document on every append | Each write re-encrypts the whole record and fails at the 64 KiB record cap. One record per entry |
| Read a field value by a value id | That read is gone. API version 15 removed it. Read by key |
| Rely on an older version or a history read | A rewrite keeps no older version. There is no `as_of` read |
| Field-equality filter like `{ column: "todo" }` as if SQL WHERE | Filters are key-shaped only; fake filters 400 or force client fallbacks |
| Cross-partition range without hash | Same class as scan — design a hash that partitions the list |
| N+1 point-get every slug after a bad list | Load storms |
| “Fix” empty list by bulk re-upserting secondaries while primary HashKey is broken | Destructive dual-write/heal thrash |
| Restart primary for `:9001` / busy errors | Socket is the plane; TCP is retired; busy ≠ dead |
| Upgrade lastdbd by pointing a new binary at live `~/.lastdb` first | `lastdb-safe-upgrade` only |

---

## 6. Record values and alternate addresses

Keep one source schema for each record type.
Declare alternate keys for different reads of those same records.
The alternate entry stores the primary key and no copy of the record.
An update changes the affected alternate entries in the same native transaction as the record.

Use separate schemas for relationships, counters, tags, links, or files with their own data.
Collect known keys first. Batch independent reads and mutations.
Equal canonical field definitions can hold different values at equal keys in separate schemas.

For existing data, follow [the multi-key schema guide](multi-key-schemas.md).

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

**Current Mini uses encrypted V2 run files. The logical range format remains an explicit home contract.**

| Piece | Shipping default | Notes |
|-------|------------------|--------|
| HashKey in `rk:…` and `ak:…` | `blind_v1` on every home | No setting selects another encoding. |
| RangeKey in `rk:…` and `ak:…` | One canonical ordered scalar payload. No range uses OPE. | The range profile in the record space key bundle selects the payload type. |
| Run data and lookup blocks | Compression, then AES-256-GCM | Main and CAS use separate derived home keys. |
| Record values | Plain logical JSON inside encrypted V2 blocks | No extra record envelope. |
| Portable file blobs | Existing portable ciphertext and access bundles | The enclosing CAS run blocks also use V2 encryption. |
| Schema records | Plain JSON inside encrypted V2 blocks | A native put refuses an `ENC:` or `ENB:` envelope in a catalog collection. The schema store reads plain JSON only. |
| Local `identity.key` | A plaintext 32-byte seed with owner-only permissions | Possession of this seed permits data decryption. |

The block encryption covers logical keys, records, alternate entries, metadata, counters, and control values together.
Clear file descriptors, footers, frame sizes, and object counts remain visible.
The cloud adapter retains the same ciphertext for both native homes. It excludes the local identity file and cloud credentials.
The account-key proof and exact format checks protect startup and native restore admission.
The typed converter uses a stopped compact V2 source and a separate target. Startup never converts or resumes a home.
Current main removes the active V1 path. Do not restore it for typed conversion.

CAUTION: The identity file is a separate key boundary.
A person who reads that seed can derive the file encryption keys. Database encryption does not replace operating-system disk protection.

**Agent rules:**

1. **Open before parse** — never feed `ENC:…` to `serde_json`. Column-1 deserialize errors usually mean ciphertext hit JSON.
2. Read the persisted home and record space contracts. Typed lookup keys use no OPE.
3. Preserve the V2 plain record representation inside encrypted blocks. Do not add a record envelope or a second ordered lookup.
4. See `docs/designs/typed-range-field-contract.md` for the complete contract and release limits. Trinity describes the older record-encryption design.

---

## 9. Agent checklist (copy into plans)

```text
[ ] Access pattern named (point O(1) / hash-scoped range O(log M)) — not “SELECT *”
[ ] Using primary HashKey for single-entity truth
[ ] Using product secondary (BoardCards / CLI list) for board list — never full scan
- Keep alternate keys on the source schema and field definition reuse separate from record values.
[ ] No bulk secondary heal while primary reads fail
[ ] Socket health: kanban list / targeted brain get — not brain doctor / :9001
[ ] Load: lastdb status (exit 0 = socket reachable) / ops (or /api/status request_ops) before blaming “node down”
[ ] Version: GET /api/version api_version ≥ what the client needs; a 400 kind=unknown_key is version skew, not a bad request
[ ] Secrets: LastSecrets only — never put secrets in brain/kanban bodies
[ ] Read the home/profile contract; V2 blocks protect plain records and ordered keys; keep the private identity outside backups
```

---

## 10. Related records / docs

| What | Where |
|------|--------|
| **Canonical model (schema → record → file blob and named keys)** | `docs/lastdb-canonical-model.md` |
| **rehydrate (resident-first miss path, field key model)** | `docs/lastdb-rehydrate.md` |
| **Access complexity requirement** | `docs/lastdb-access-complexity-requirements.md` |
| **Delete, purge, TTL, file blobs** | `docs/lastdb-canonical-model.md` §1 · `docs/lastdb-blob-counter.md` |
| **TTL sweep and manifest retention** | `docs/lastdb-ttl-sweep.md` · `docs/app-manifest-retention.md` |
| **Record cap (64 KiB) and the peer field value limit** | `docs/lastdb-canonical-model.md` §1 · `fold_db/docs/FIELD_VALUE_SIZE_LIMIT.md` |
| ENC model | this file §8 |
| Ops telemetry | `docs/lastdb-ops-telemetry.md` |
| Safe Mini upgrade | skill `lastdb-safe-upgrade` |

---

## 11. Model for humans and agents

A schema owns each record. Each primary key holds one stored record.
Declare named alternate keys for other reads of those records.
Reuse canonical field definitions across schemas while each schema keeps separate values.
Copy an older value layout into a fresh home.
