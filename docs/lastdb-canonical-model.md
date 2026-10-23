# LastDB canonical model — schema → field → molecule → atom → file, proteins, and the no-scan contract

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

### Field → molecule

**Every field of a schema has its own molecule.** A molecule is an **index**:
under one key configuration — a hash, or a hash plus range — it maps key
coordinates to the current **atom** for that field (the *tip*).
Molecule identity uses SHA-256 over the fixed domain `lastdb:molecule-uuid:v1`, the schema name, and the field name.
Each part has a four-byte length prefix. The digest uses unpadded base64url text.
A writer can name the molecule from the schema and field without a fetch.
An explicit field mapper can share another field's molecule.

The molecule is the only way in. To find data you present the key the molecule
is built on; there is no other read path.

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
A sealed 256 KiB atom is about 341 KiB, which fits the 400 KiB item limit of the
cloud store. Restore replay still accepts 1 MiB, for older atoms. An atom may pack
many small fields up to the ceiling, but it is not a blob store. Shape rules for
data that grows or that is large: `docs/lastdb-agent-access-model.md`, section
Shape rules.

**No history.** A rewrite keeps no older version. The replaced atom loses one
reference, and its body goes when its last reference goes. The home has no `as_of`
read and no per-key history. The one exception is a conflicting write (see
Delete, purge and TTL).

### File

The atom's data is what points at durable bytes on disk:

- **Physically**, the native disk engine finds the file bytes from the atom identity.
  Native storage uses packed files and keyed trees.
  Older physical tools remain separate from the logical database constructors.
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
                       └─ file (segment on disk; big bytes → CAS blob)
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
  that has a policy. A Range or HashRange schema needs a range key that starts with 20
  digits of unix seconds. The policy must exist before the first write. See
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

## 2. Protein — molecules bound into one coherent set

The ladder continues: **atom → molecule → protein**.

The problem proteins solve: several molecules keyed differently over the same
data are, by themselves, unrelated — the data gets copied per key and an
outside job (app dual-write + heal tools) must keep them from drifting. A
**protein** binds them so coherence is intrinsic to the storage model, not
bolted on by the app.

| Term | Meaning |
|---|---|
| **protein** | a record with its own **UUID** holding a **member list** of molecules |
| **member molecule** | the same data *folded* under one particular key configuration |

Rules:

- **Two-way pointers.** The protein lists its members; **every member molecule
  stores its protein's UUID**. The whole set is reachable from any member.
- **One atom set, shared.** Members are indexes into the same atoms, not
  copies. There is no "canonical member" — with one copy of the data there is
  nothing to canonicalize. The protein's UUID is the identity.
- **Write propagation.** A write lands on whichever member the writer
  addressed: new atom, that member's tip repointed. Core then follows the
  member's protein UUID, walks the member list, and repoints every other
  member's tip to the shared atom. O(members), correct by construction — the
  protein is the authority on who must update, so drift cannot happen.
  The native create/update request prepares the related molecule changes
  before one core publication. Its ack does not precede those related tips.
  A mixed create/delete call can also require a separate durable erasure phase.
  This does not promise one atomic change for every mutation verb.
- **App view:** when two schemas share the same set of fields but index them
  differently, their field molecules belong to one protein. A write through
  either schema updates the shared atoms and re-indexes the related molecules
  of the other — automatically, in core.
- **Adding an access pattern = adding a member.** Register a molecule under
  the new key, add it to the member list, set its protein UUID, backfill its
  tips once. This *replaces* "reindex" as a concept.
- **Re-keying** (the one stateful op): when a member's key value changes, that
  member moves partitions (drop old key, insert new). The protein owns it.
- **Not a general graph.** Protein membership is the only molecule→molecule
  link. Arbitrary molecule graphs are not a feature.

**Status (restated 2026-08-17):** proteins are **shipped in core**
(end-to-end proof 2026-07-28). Multi-key
same-product **shared fields** bind into a protein and fold sibling tips
to one atom. App dual-write remains only for a **thin projection** that
is a different product shape, not the same field protein. Prefer protein
fold for shared fields. Dual-write only a thin projection. Some app paths
still write that thin projection by hand — leftover cutover, not the
product model.

---

## 3. Layer boundary

**Core knows no application.** It has atoms, molecules, proteins, and keys —
nothing else. It has no concept of boards, cards, slugs, or brain records. An
app declares schemas, decides which access patterns it needs, and privately
names what each key means; core only ever sees keyed molecules, atoms, and
protein bindings. Secondary indexing is core's responsibility (the protein);
naming and meaning are the app's.

---

## 4. The contract for apps

Design **for the query, before the data**. For every read your app will make,
name the access pattern:

1. **Single entity by identity** → exact hash key on the primary schema, O(1).
2. **A list** → a schema/molecule whose hash partitions the list and whose
   range key orders it; read as a range under that one hash, O(log M).
3. **Another list shape** → another keyed molecule. Shared fields are a
   protein member. Dual-write only a thin projection that is a different
   product shape. Never a filter over everything.

Dynamo vocabulary map:

| Dynamo | LastDB |
|---|---|
| Table | Schema |
| Partition key | hash key |
| Sort key | range key |
| Item attributes | fields (assembled from tips + atoms at read time) |
| `GetItem` | exact key → O(1) |
| `Query` (PK + SK condition) | range under one hash → O(log M) |
| GSI | protein member molecule (shared fields); thin projection may still be app dual-write |
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
protein binds related molecules
```

### Native disk engine

The core model holds current schemas, molecule definitions, keys, tips, and atom values.
Reads and accepted logical writes use that model.
A call captures the core state and its matching disk root.
A miss fetches the required records in batches. The fetched logical records can stay warm.

The recency list controls warm retention under a byte budget, which defaults to 256 MiB.
The October 19 byte-limit decision replaces the old 10,000-logical-record cap.
Each read enters the warm set. The purge removes least recently used clean records.
The purge skips a record that a call still holds or that still awaits flush.
If those records prevent enough purge, the charge can temporarily exceed the budget.
Status counts that excess; the purge runs again after calls end or writes reach disk.
An older call can retain its earlier values. Their memory owners stay charged until that call returns.
These call lifetimes do not provide an old-value query or stored atom history.

The private disk queue retains prepared bytes until flush. Ordinary reads cannot query that queue.
The selected searchable snapshot owns local recovery. It does not replace the current logical memory state.
The writer creates immutable payload files and changed search pages, then publishes one complete snapshot root.
Reopen validates that root and its generation receipt. It performs no internal-journal replay.
The local capture router remains, but no cloud engine attaches during this cutover.
A successful native write completes its disk snapshot and core memory change before the ack.
The disk queue starts immediately. No collection timer delays the ack.
An internal file change clears affected core records before a later call can use them.

Native reads and writes always use the core model. No old memory mode flag selects that route.
The obsolete `LASTDB_RESIDENT_PERSIST_MS` setting has no effect, including at startup.
The warm byte budget, definition allowance, selected-key allowance, and write allowance cover separate owners.
They do not define one whole-process memory cap.
The approved dirty-write share is 25%; the reviewed code reports the threshold but does not yet enforce that share.
See the [byte-limit design](lastdb-warm-set-byte-limit-design.md) and [current source review](lastdb-searchable-snapshot-code-review.md).

Selected HashRange schemas can load all required membership keys before their first query continues.
Concurrent first queries share that load. Later range calls select complete keys from memory.
Tips and values remain partial. Range-key preload does not load unused atom values or file contents.
The policy remains opt-in through `LASTDB_SCHEMA_PRELOAD_SCHEMAS`.
See the [preload design](lastdb-schema-range-preload-design.md) and [native write result](proofs/core-owned-writes-20261018/RESULTS.md).

### Engine cutover

Normal FoldDB startup opens the native packed engine. No selection flag is required.
The logical constructor rejects a non-native physical store before it opens logical namespaces.
An empty home initializes directly. Supported snapshot homes can reopen.
Mandatory snapshot feature `256` replaces the retired native journal feature.
A nonempty older-format home receives an explicit refusal before writer file creation.
No automatic conversion or boot migration runs. This change supplies no converter for old native journal homes.
Cloud sync stays disabled for this optimization round.

See [the snapshot commit protocol](lastdb-snapshot-commits.md) for publication, failure, backup, and compatibility rules.

Older physical-format tools and vendor code do not provide an alternate logical query or write engine.
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
- This extends §2's "one atom set, shared" rule (protein members index the
  same atoms, not copies) up one level: databases also reference shared
  molecules/atoms rather than holding copies.
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
| Agent usage rules and shape rules | `docs/lastdb-agent-access-model.md` · https://thelastdb.com/docs/agent-access-model |
| Complexity requirement | `docs/lastdb-access-complexity-requirements.md` |
| Atom counter, delete and purge, crash states | `docs/lastdb-atom-counter.md` |
| Molecule counters and the drain | `docs/lastdb-molecule-counters.md` · `docs/lastdb-molecule-drain.md` |
| Blob counter and the dwell | `docs/lastdb-blob-counter.md` |
| TTL sweep | `docs/lastdb-ttl-sweep.md` · `docs/app-manifest-retention.md` |
| Atom size ceiling | `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md` |
| Counter lifecycle design and what was built | `docs/design-lastdb-counter-lifecycle.md` |
