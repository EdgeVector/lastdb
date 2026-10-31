# LastDB canonical model — schema → field → molecule → atom → file and named keys

> **CANONICAL.** Ratified 2026-07-27, written after the storage redesign to
> re-fix the terms that drifted in translation. If another doc or a code
> comment disagrees with this one, this one wins; fix the other. The atom
> lifecycle (counters, delete, purge, TTL) was updated on 2026-10-21.

**Audience:** anyone designing an app against LastDB (Mini `lastdbd` or
embedded). Agents: also read
`docs/lastdb-agent-access-model.md` for day-to-day usage rules.

---

## 0. What kind of database this is

LastDB is **Dynamo-style NoSQL**. It is not SQL and is not "almost SQL."

- Everything is looked up by a **specific key**: an exact hash key, or a range
  under one hash.
- **There is no scan feature.** Not a slow scan, not a discouraged scan — the
  operation does not exist. If you cannot name the key, you cannot make the
  query; design a key that serves it.
- If you know DynamoDB access-pattern design (table + partition key + sort key
  + projections per query), you already know how to use this database.

Complexity law (`docs/lastdb-access-complexity-requirements.md`):

| Operation | Shape | Complexity |
|---|---|---|
| Point get | exact hash (or hash+range) key | **O(1)** |
| Range under one hash | prefix / between on the range key, hash fixed | **O(log M)** |
| Multi-get | K exact keys | **O(K)** |
| Full scan | — | **not supported** |

---

## 1. The ladder: schema → field → molecule → atom → file

### Schema

An app declares a schema with its field names and key type: `Hash` or `HashRange`.
The schema describes how its field molecules use keys. It does not store the record values.
Older declarations and remote templates can still encode `Single` or `Range`.
The native logical query and write routes reject those layouts before they fetch molecules or change records.

An app can declare its installed schema again:

- **More fields.** The node accepts the new fields in place. The schema keeps its name, key, alternate keys and existing molecules. Each new field gets its own molecule. Old records read a new field as absent.
- **Fewer fields, with equal definitions.** An older app version can declare a subset. The node reuses the installed schema and loads nothing.
- **Any other change.** The node refuses a changed field type or description, a changed key, or changed alternate keys. The app declares a new schema name and copies its records explicitly.

### Field → molecule

**Every field of a schema has its own molecule.** A molecule is an **index**:
under one key configuration — a hash, or a hash plus range — it maps key
coordinates to the current **atom** for that field (the *tip*).
Molecule identity uses SHA-256 over the fixed domain `lastdb:molecule-uuid:v1`, the schema name, and the field name.
Each part has a four-byte length prefix. The digest uses unpadded base64url text.
A writer can name the molecule from the schema and field without a fetch.
Canonical field identity describes a reusable definition. Each schema owns its record values.

### Named alternate keys and field definition reuse

A schema owns its values. Canonical field reuse supplies definitions and types.
Equal field identities do not connect records in different schemas.

The primary key addresses the record. Each named alternate key uses fields declared in that schema.
An alternate molecule maps its key to the primary record key. It stores no payload copy or separate field tip.
The write updates the record and its alternate addresses in one native request.
Queries return the primary key for subsequent mutations.

Each local schema owns its field values. Startup refuses a stored schema that does not own its values.
Copy older data into a fresh home; no in-place conversion exists.
See [the multi-key schema guide](multi-key-schemas.md) and [compound key rules](compound-alternate-keys.md).

### Atom

An **immutable, content-addressed value** (`atom:{uuid}\0b`) holding one field's
data at one key. A write never mutates an atom — it writes a new atom and
repoints the molecule's tip (last-write-wins).

Every atom has one **counter** record (`atom:{uuid}\0c`) in the same hash group
as its body. The counter holds the number of tip records and counted conflict
records that name the atom. **The batch that lowers the counter to exactly zero
deletes the body and the counter, in the same batch.** Nothing else deletes an
atom body. There is no grace window, no audit and no garbage-collection pass. The
counter is the only authority for the life of a body
(`docs/lastdb-atom-counter.md`).

**The ceiling.** A new atom holds at most 256 KiB of content (the JSON value, before
encryption). `LASTDB_MAX_ATOM_CONTENT_BYTES` can change it between 1 KiB and 1 MiB.
V2 compresses and encrypts run blocks that contain the atom. Native backup retains those encrypted bytes.
The content cap is separate from the stored block and backup limits. An atom may pack
many small fields up to the ceiling, but it is not a blob store. Shape rules for
data that grows or that is large: `docs/lastdb-agent-access-model.md`, section
Shape rules.

**Field history.** History stays off until a field policy retains earlier values.
The `keep` policy retains a replaced value. The `ttl` policy sets its expiry.
Each retained value holds an atom reference until its history entry leaves.
A named alternate address has no separate payload history.
See [field history](lastdb-atom-history.md). Conflicting writes have their separate retention rule below.

### File

The atom's data is what points at durable bytes on disk:

- **Physically**, the native disk engine finds the file bytes from the atom identity.
  Native storage uses disk format v2: sorted run files in two run homes.
  File positions remain storage details; molecule tips retain atom identities.
- **File-scale payloads** (packs, attachments, anything past the atom fence)
  are **content-addressed file blobs** (CAS: `blobs/sha256/…`); the atom holds
  the reference, not the bytes.

### Row assembly

A logical record ("row") is **not one stored blob**. It is the set of fields
whose molecules share the same key coordinates. A read walks each field
molecule's tip at the key, batch-fetches the atoms, and zips the fields into
the JSON the API returns. Multi-field records are **co-keyed at read time**.

```text
Schema (catalog: fields + keying mode)
  └─ Field ── molecule (index: hash[, range] → tip)
                 └─ atom (immutable value, content-addressed)
                       └─ file (run file on disk; big bytes → CAS blob)
```

### Delete, purge and TTL

Three ways end a key. All three remove the tip records of the key, and the atom
counter frees each body whose last reference goes, in the same batch.

| Way | Removes the tips | Frees the bodies | Writes a delete barrier | Use |
|---|---|---|---|---|
| Delete | yes | yes, at zero references | yes (`rdel:v2:`), so an older replayed write cannot bring the key back | an ordinary client delete |
| Purge | yes | yes, at zero references | no | hard erasure; a TTL sweep uses it by default |
| TTL sweep | yes | yes, at zero references | only with `LASTDB_TTL_SWEEP_VERB=delete` | expiry of old keys |

- A request reads the tips of its keys by key and stages one batch. The cost follows the
  keys of the request, never the size of the schema.
- A body that another tip or a kept conflict record still names stays. Two keys that
  share one atom keep it until the last one goes.
- A delete frees the record, not the disk bytes. Compaction returns the bytes.
- A TTL is a schema retention policy. The sweep erases the expired keys of every schema
  that has a policy. A HashRange schema needs a range key that starts with 20
  digits of unix seconds. Set the policy before the first write, or
  [explicitly backfill existing personal Hash/HashRange records](lastdb-ttl-sweep.md#schema-shape-that-a-ttl-needs)
  with the owner's `--confirm-expire-existing` confirmation. See
  `docs/lastdb-ttl-sweep.md` and `docs/app-manifest-retention.md`.
- A molecule goes only when its `slots`, `records` and `bindings` counters are all zero.
  A file blob goes after its counter reaches zero and a dwell of one hour passes.
  (`docs/lastdb-molecule-counters.md`, `docs/lastdb-blob-counter.md`.)

**Kept conflicting writes.** When two devices write one key, the database keeps the
losing write as a conflict record for 30 days (`LASTDB_CONFLICT_RETENTION_SECS`). A
conflict record holds one counted reference to each atom it names, so a kept loser keeps
its body. The retention janitor releases a record at the end of its window. A delete,
a purge or a TTL erase of the key releases its records in the same batch (up to 100 for
one request) and hands the rest to the janitor to drain. Conflict is per atom, so
packing independent facts into one atom couples their fate.


---

## 2. Named alternate keys

Use one schema when several keys must find the same record.
For example, Card uses its slug as the primary key and a board key for the board list.
The board key uses the board field and an ordered range from the column, position, and slug fields.
Every required field is present in Card. The key needs no record correspondence with another schema.

- An exact primary read resolves field tips for the supplied primary key.
- An alternate read resolves primary addresses, then reads the requested fields from those records.
- A create adds the primary record and its alternate addresses in one transaction.
- An update removes old alternate addresses and adds the new addresses in that transaction.
- A delete removes the primary tips and their alternate addresses in that transaction.
- Adding or removing a key on existing data uses the explicit owner change command and complete coverage checks.
- An alternate read works in the personal database and in an org database.
- An alternate read refuses while the node receives data from another user. Received records have no alternate addresses. Read them by the primary key.

Canonical field identity controls reusable descriptions and types. It does not select other records for a write.
Unrelated records use separate schemas and retain separate values.
Relationships with their own values also use separate keyed schemas.

---

## 3. Layer boundary

The core stores schemas, field molecules, atoms, and keys.
An app defines what its records and keys mean.
The core updates declared alternate addresses with the source record.
The app owns relationships or summaries that contain different data.

---

## 4. The contract for apps

Design **for the query, before the data**. For every read your app will make,
name the access pattern:

1. **Single entity by identity** → exact hash key on the primary schema, O(1).
2. **A list** → a schema/molecule whose hash partitions the list and whose
   range key orders it; read as a range under that one hash, O(log M).
3. **Another list of the same records** → a named alternate key on the source schema.
   A relationship with different data uses its own keyed schema.

Dynamo vocabulary map:

| Dynamo | LastDB |
|---|---|
| Table | Schema |
| Partition key | hash key |
| Sort key | range key |
| Item attributes | fields (assembled from tips + atoms at read time) |
| `GetItem` | exact key → O(1) |
| `Query` (PK + SK condition) | range under one hash → O(log M) |
| Alternate lookup | A named key on the same schema |
| `Scan` | **does not exist** |

Never:

- Full scan, scan-then-filter, or unscoped enumerate-all — not supported.
- Field-equality filters as if SQL WHERE — filters are key-shaped only.
- JOINs — assemble in the app from keyed reads, or design a key.
- Cross-partition range without a hash — same class as scan.
- N+1 point-gets to fake a list — design the list key.

---

## 5. Memory → disk → cloud

The logical model stays the same across memory, disk, and cloud:

```text
schema → field → molecule → atom → file
```

### Native disk engine

The core model holds current schemas, molecule definitions, keys, tips, and atom values.
Reads and accepted logical writes use that model.
A call captures the core state and its matching disk root.
A miss fetches the required records in batches. The fetched logical records can stay warm.

The recency list controls warm retention under a byte budget, which defaults to 256 MiB.
The byte budget (`LASTDB_CORE_WARM_BYTES`) is the only warm set limit. There is no record count limit (Tom, 2026-10-30).
The October 19 byte-limit decision replaces the old 10,000-logical-record cap.
Each read enters the warm set. The purge removes least recently used clean records.
The purge skips a record that a call still holds or a path reserved by private write preparation.
If those records prevent enough purge, the charge can temporarily exceed the budget.
Status counts that excess; the purge runs again after the owners release their records.
An older call can retain its earlier values. Their memory owners stay charged until that call returns.
These call lifetimes do not provide an old-value query or stored atom history.

The private disk queue retains prepared bytes until flush. Ordinary reads cannot query that queue.
The published `CURRENT` of each run home owns local recovery. It does not replace the current logical memory state.
A commit writes one new immutable run file, makes it durable, then publishes a new `CURRENT`.
Reopen reads that `CURRENT`. Nothing replays.
PR 387 and later PRs removed legacy Cloud Sync. The V2 byte backup uses a separate gate, `LASTDB_BYTE_BACKUP_V2_ENABLED`, that defaults off.
Capture saves an exact cut and a named file hold; its source code does not establish production cloud readiness.
A successful native write completes its disk commit and core memory change before the ack.
The disk queue starts immediately. No collection timer delays the ack.
This order is the design (Tom, 2026-10-30): fast durable writes, then the ack.
An ack always means that the write is on disk. There is no memory ack and no flush interval.
An internal file change clears affected core records before a later call can use them.

Native reads and writes always use the core model. No old memory mode flag selects that route.
The obsolete `LASTDB_RESIDENT_PERSIST_MS` setting has no effect, including at startup.
There is no periodic mutation flusher or delayed durability mode.
`LASTDB_MUTATION_SYNC_FLUSH` and `LASTDB_BACKGROUND_FLUSH_MS` also have no effect.
Explicit flush and the final shutdown barrier remain.
The warm byte budget, definition allowance, selected-key allowance, and write allowance cover separate owners.
They do not define one whole-process memory cap.
Native writes publish clean records after disk commit. They create no dirty warm-record state.
The retained dirty counters and threshold do not grant an early memory ack or reserve a separate dirty share.
See the [byte-limit design](lastdb-warm-set-byte-limit-design.md) and [current native summary](lastdb-disk-native-node.md).

Selected HashRange schemas can load all required membership keys before their first query continues.
Concurrent first queries share that load. Later range calls select complete keys from memory.
Tips and values remain partial. Range-key preload does not load unused atom values or file contents.
The policy remains opt-in through `LASTDB_SCHEMA_PRELOAD_SCHEMAS`.
See the [preload design](lastdb-schema-range-preload-design.md) and [native write result](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/core-owned-writes-20261018/RESULTS.md).

### Engine cutover

Normal FoldDB startup opens the disk format v2 engine. No selection flag is required.
The logical constructor rejects any other physical store before it opens logical namespaces.
An empty home initializes directly. A disk format v2 home can reopen.
A disk format v1 home or an older home receives an explicit refusal (decision D1, 2026-10-26).
No automatic conversion or boot migration runs.
Cloud sync stays disabled for this optimization round.

See [the engine README](../vendor/laststore/README.md) for the run files, commit, merge, and reopen rules.

LastDB uses only disk format v2 from the vendor crate. That crate does not provide an alternate logical query or write engine.
The source review and product receipts must identify the exact native source that they cover.

---

## 6. Molecules and atoms are node-universal (cross-DB) — 2026-08-18

Owner-stated semantics (2026-08-18, near-verbatim): "For all DBs in the
user's node, molecules and atoms are universal. If I copy an entire schema
(with my selection of keys) into an org database, it does not copy the
molecules — it references the same molecules. Same with atoms. They are
cross-DB."

- In one node, molecules and atoms are **universal across all databases**. A
  database boundary does not partition the molecule/atom store.
- A schema copy into another database (for example an org/shared DB) copies
  only the **catalog**: the field list and the chosen keying. It does not copy
  molecules or atoms.
- The copied schema **references the same molecules and the same atoms** as
  the source database. The node stores the data once; databases share it by
  reference.
- Molecule identity uses the framed tuple from §1.
  An explicit schema copy keeps the same molecule identities and atoms.

The local catalog read and write decisions share one native write scope.
The native gate precedes molecule locks for a share or delete.
Instance cleanup uses one complete batch and refuses work above the native operation or 128 MiB limit.
A refusal retains the catalog reference and data. Search receives removals after the complete batch reaches disk.

Instance cleanup retains molecule access-domain wraps because another instance or shared schema can use them.
The `reclaimed_wraps` result therefore remains zero. Cloud sync stays disabled.
Cloud sync needs domain-aware access revocation before it can resume.

Scope note: this universality is **per node**. Cross-user sharing (a shared
DB synced to another person's node) still replicates data to that node.

**2026-08-18 follow-up:** the node-universal rule above is now implemented as a
database catalog with per-molecule key bundles (it replaces the earlier
`{db_hash}:` copy geometry). A catalog entry that names a molecule adds one
`bindings` count to the molecule (`docs/lastdb-molecule-counters.md`).

---

## 7. Durable homes / related

| What | Where |
|---|---|
| **This doc (canonical)** | `docs/lastdb-canonical-model.md` |
| Core logical memory and native disk | `docs/lastdb-rehydrate.md` |
| Agent usage rules and shape rules | `docs/lastdb-agent-access-model.md` · https://github.com/EdgeVector/lastdb/blob/main/docs/lastdb-agent-access-model.md |
| Complexity requirement | `docs/lastdb-access-complexity-requirements.md` |
| Atom counter, delete and purge, crash states | `docs/lastdb-atom-counter.md` |
| Molecule counters and the drain | `docs/lastdb-molecule-counters.md` · `docs/lastdb-molecule-drain.md` |
| Blob counter and the dwell | `docs/lastdb-blob-counter.md` |
| TTL sweep | `docs/lastdb-ttl-sweep.md` · `docs/app-manifest-retention.md` |
| Atom size ceiling | `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md` |
| Counter lifecycle design and what was built | `docs/lastdb-atom-counter.md` |
