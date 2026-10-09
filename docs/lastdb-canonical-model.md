# LastDB canonical model — schema → field → molecule → atom → file, proteins, and the no-scan contract

> **CANONICAL.** Owner: Tom. Ratified 2026-07-27, written after the storage
> redesign to re-fix the terms that drifted in translation. If another doc or a
> code comment disagrees with this one, this one wins; fix the other.
>
> Brain: `brain get concepts-lastdb-canonical-model` · this file:
> `docs/lastdb-canonical-model.md`

**Audience:** anyone designing an app against LastDB (Mini `lastdbd`, the
desktop node, or embedded). Agents: also read
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

Complexity law (`requirement-lastdb-access-complexity`):

| Operation | Shape | Complexity |
|---|---|---|
| Point get | exact hash (or hash+range) key | **O(1)** |
| Range under one hash | prefix / between on the range key, hash fixed | **O(log M)** |
| Multi-get | K exact keys | **O(K)** |
| Full scan | — | **not supported** |

---

## 1. The ladder: schema → field → molecule → atom → file

### Schema

An app-declared **catalog**: it names the fields of a record type and the
keying mode (`Single` | `Hash` | `Range` | `HashRange` — i.e. an exact key, or
a hash plus a range key). The schema stores nothing; it describes how the
molecules below it are keyed.

### Field → molecule

**Every field of a schema has its own molecule.** A molecule is an **index**:
under one key configuration — a hash, or a hash plus range — it maps key
coordinates to the current **atom** for that field (the *tip*). Molecule
identity is deterministic: `sha256(schema:field)`, so a writer that knows the
schema and field can name the molecule without loading anything.

The molecule is the only way in. To find data you present the key the molecule
is built on; there is no other read path.

### Atom

An **immutable, content-addressed value** (`atom:{uuid}`) holding one field's
data at one key. A write never mutates an atom — it writes a new atom and
repoints the molecule's tip (last-write-wins).

Atoms are size-fenced (default 64 KiB serialized; hard max 1 MiB). They are
structured field values, not a blob store.

### File

The atom's data is what points at durable bytes on disk:

- **Physically**, atoms live in hash-sharded **segment files** under the store
  home (storage v2: `atoms/<hh>/<hh>/<seg>.seg`), so disk usage is explainable
  with `du` and compaction is per-shard.
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
  Propagation is **enqueued** (eventually consistent): the entry member and
  atom commit immediately; the other members fold in shortly after; racing
  writes converge by the existing LWW tip semantics.
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
(`design-lastdb-protein-molecule-set`, e2e 2026-07-28). Multi-key
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

## 5. Resident graph: memory → disk → cloud

**Product law (Tom, 2026-07-28):** The full ladder is the **resident graph** —
**primary** and ultra-fast:

```text
Schema → field → molecule → atom (file reference and access metadata)
(+ protein binding molecules)
```

Tier sequence:

```text
T0  resident (primary)  →  schema catalog, molecule tips, and atoms in memory
T1  disk (eventual)     →  dirty resident persists to LastStore
T2  cloud (eventual)    →  sync / export; never on mutation ack
```

**Logical resident set:** tips and atoms in memory are capped by used-record
count (`RESIDENT_KEY_CAP = 10000`), not by a byte budget.
`LASTDB_RESIDENT_BYTES` bounds the ResidentGraph.
`LASTDB_HASH_GROUP_WARM_BYTES` bounds the hash-group warm set for non-logical
collections (indexes, schema_index, atom_ref_edges_v2, keep_small, metadata,
cas_blobs). Do not tune those byte settings to size the logical resident set.

**Fidelity:** what is in resident is **exactly** what will be written later
(isomorphic persist — same tips, atom uuids/content, blob refs, protein
membership). Not a tip-only cache or a different RAM shape.

**Read:** memory first; miss **`rehydrate`s** from disk (lazy; open is not a
full load).

**Owner correction, 2026-09-08:** all schemas with local data stay in memory.
Query fields select molecule keys, then the required atoms. Queries return file
references and access metadata. They never hydrate or cache CAS file bytes.
See `decision-2026-09-08-local-memory-stops-at-atoms`.

**Write:** **`apply`** full ladder in resident → dirty pin → ack → **persist**
serializes the same records → cloud later.

| Op | Role |
|---|---|
| **`apply`** | Mutate resident ladder (hot path) |
| **`rehydrate`** | Miss: disk → faithful resident object |
| **`resolve`** | Hit or rehydrate |
| **persist** | Dirty → disk (same logical records) |
| **`evict`** | Drop **clean** only; never dirty until persisted |
| **`fold`** | Protein tip fan-out on resident (not rehydrate) |

Full brief: brain `concepts-lastdb-rehydrate` · `docs/lastdb-rehydrate.md`.

---

## 6. Molecules and atoms are node-universal (cross-DB) — Tom, 2026-08-18

Owner-stated semantics (Tom, 2026-08-18, near-verbatim): "For all DBs in the
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
- Mechanism fit (§1): molecule identity is deterministic
  (`sha256(schema:field)`) and atoms are content-addressed, which is what
  makes reference-not-copy possible across DBs on one node.

Scope note: this universality is **per node**. Cross-user sharing (a shared
DB synced to another person's node) still replicates data to that node — see
brain `design-databases-are-shareable-org-dissolves`.

**2026-08-18 follow-up:** the node-universal rule above is now an approved
implementation design — brain `design-lastdb-db-catalog-reference-model` (DB
catalog + per-molecule key bundles; replaces the `{db_hash}:` copy-geometry of
fold #1278/#1323). North Star: brain
`north-star-lastdb-db-catalog-reference-model`.

---

## 7. Durable homes / related

| What | Where |
|---|---|
| **This doc (canonical)** | `docs/lastdb-canonical-model.md` · brain `concepts-lastdb-canonical-model` |
| **rehydrate / resident graph** | brain `concepts-lastdb-rehydrate` |
| Agent usage rules | `docs/lastdb-agent-access-model.md` · brain `concepts-lastdb-agent-access-model` · https://thelastdb.com/docs/agent-access-model |
| Complexity requirement | `docs/lastdb-access-complexity-requirements.md` · brain `requirement-lastdb-access-complexity` |
| Protein design | brain `design-lastdb-protein-molecule-set` · `docs/lastdb-native-secondary-indexes-design.html` |
| Protein write/fold | brain `concepts-lastdb-protein-write-fold` |
| Storage depth (HTML) | `docs/lastdb-storage-schema-to-atoms.html` |
| Storage v2 (segments + CAS) | brain `spike-lastdb-storage-v2-sharded-segments` |
| Atom-key partition locality | brain `design-lastdb-atom-key-partition-locality` |
| DB catalog reference model (§6) | brain `design-lastdb-db-catalog-reference-model` · `north-star-lastdb-db-catalog-reference-model` |
