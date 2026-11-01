# LastDB canonical model — schema → record → file blob and named keys

> **CANONICAL.** Ratified 2026-07-27. Rewritten 2026-11-01 for the record model
> (home format 7, decisions D1–D7, Tom, 2026-10-31). If another doc or a code
> comment disagrees with this one, this one wins; fix the other.

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
- If you know DynamoDB key design (table + partition key + sort key
  + projections per query), you already know how to use this database.

Complexity law (`docs/lastdb-access-complexity-requirements.md`):

| Operation | Shape | Complexity |
|---|---|---|
| Point get | exact hash (or hash+range) key | **O(1)** |
| Range under one hash | prefix / between on the range key, hash fixed | **O(log M)** |
| Multi-get | K exact keys | **O(K)** |
| Full scan | — | **not supported** |

---

## 1. The ladder: schema → record → file blob

```text
Schema (catalog: fields, primary key, named alternate keys)
  ├─ record (rk:)  one stored value per primary key: every field, a stamp per field
  │    └─ file blob (CAS)  large bytes that a field or the record metadata names
  └─ alternate entry (ak:)  alternate key → primary key, no copy of the record
```

Record space: `S = base64url(SHA-256(frame("lastdb:record-space:v1", schema)))`. A scoped
key gets its `{scope}:` prefix in front.

Every home is format 7. Field molecules, tips and atoms do not exist (decision D6).

### Schema

An app declares a schema with its field names and key type: `Hash` or `HashRange`.
The schema owns its records and their values.
Older declarations and remote templates can still encode `Single` or `Range`.
The native logical query and write routes reject those layouts.

An app can declare its installed schema again:

- **More fields.** The node accepts the new fields in place. The schema keeps its name, key, alternate keys and records. An old record reads a new field as absent.
- **Fewer fields, with equal definitions.** An older app version can declare a subset. The node reuses the installed schema and loads nothing.
- **Any other change.** The node refuses a changed field type or description, a changed key, or changed alternate keys. The app declares a new schema name and copies its records explicitly.

Canonical field identity describes a reusable definition. It does not connect records in different schemas.

### Record

A **record** is all fields of one schema at one primary key, stored as **one value**
at one record key:

```text
rk:{record space}:{escaped blinded hash}\0{typed range}
```

The record space is a fixed identity of one schema. The key has the same shape as an
alternate key, so the typed range order applies to it.

The record value (`fold_db/crates/model/src/record_value.rs`) is JSON:

```text
{"v":1, "fields":{…}, "stamps":{…}, "meta":{"source_file_name", "metadata":{…}, "stamp"}, "deleted": stamp}
```

- `fields`: the value of each field.
- `stamps`: one stamp for each field, `[written_at_ns, logical_counter, device_id, mutation_id]`,
  compared in that order (decision D1).
- `meta`: the record metadata — the source file name and the metadata map, with its own
  stamp (decision D7). A record has one metadata block. A field has no metadata.
- `deleted`: the delete floor of a tombstone. A tombstone has no fields.

Empty parts are left out.

A write changes only the fields that it names. **Field merge:** when two writes
change one record, each field keeps the value with the newer stamp. So two devices
that change two different fields of one record keep both changes.

A local write replaces only the metadata keys that it sends, and the source file name
only when it sends one. A write without metadata keeps the stored metadata. A peer
write replaces the metadata by stamp, but keeps this device's `file_blob_ref` and
`file_hash`.

**The record cap.** A record holds at most **64 KiB** (`MAX_RECORD_BYTES`): the whole
JSON-encoded record, with the stamps, the metadata and the delete floor (decision D3).
The write checks the final record of each key. A larger record gets
`413 {"error":"atom_content_too_large","scope":"record"}`. A merged peer record over the
cap is skipped, and the stored record stays. Put a large field in a file blob. A growing
list or log is one record per entry in a HashRange schema.

`LASTDB_MAX_ATOM_CONTENT_BYTES` (default 256 KiB, maximum 1 MiB) now limits only one
field value of a peer record. A local write is checked against the record cap only.

### File blob

Large opaque bytes (files, packs, images) go in a **content-addressed file blob**
(CAS). A `$lastdb_file.blob_ref` field value, or the `file_blob_ref` / `file_hash`
record metadata, holds the reference, not the bytes. Each file blob has a counter
(`bref:v3:c:{sha256}`) of the records and field history entries that name it. A write
changes the counter by the net change between the old and the new record. At zero the
file blob waits for a dwell of one hour (`LASTDB_BLOB_DWELL_SECS`), then goes
(`docs/lastdb-blob-counter.md`).

### Reads

| Read | Stored reads |
|---|---|
| By primary key | 1 record read |
| Range under one hash | 1 range read over the record keys |
| By a named alternate key | the alternate entry, then 1 batch of record reads |

A read returns the fields, the record metadata and the **primary key**. Use the
primary key for a later mutation. An alternate read also returns the matched key.
A reply has no molecule id and no writer id. Each field entry of
`metadata` keeps the record's `source_file_name`. `author_pub_key` is `null`.
The node does not assemble a record from separate field values: the record is
stored whole.

### Writes

One native write request:

1. reads the records that its preconditions need (`expected`, `must_exist`, purge) in one batch;
2. reads the current record of each key (memory first, then disk);
3. merges each changed field and its stamp. A write merges only the fields it sends;
   there is no full-replace option;
4. computes the alternate entries from the old and the new fields: it puts the new
   entries and deletes only old entries that still name this record;
5. changes the file blob counters by the net change of the file blob references;
6. puts the records, the alternate entries, field history rows, counter changes, share
   copies, delete audit rows, TTL ages, idempotency entries and HashRange partitions in
   **one batch**.

The ack follows the durable disk commit (section 5).

### Delete, purge and TTL

A delete changes the record into a **tombstone** at the same key. The tombstone
keeps the delete floor, so an older replayed write cannot bring the record back.
No separate delete barrier exists. The owner can raise the home-wide
[replay cutoff](delete-replay-cutoff.md) on a stopped home.
In the same batch the delete removes the alternate entries, lowers the file blob
counters, removes the TTL age rows, copies the floor to share copies and writes a
delete audit row (`dellog`). A delete of an absent record writes nothing.

| Way | Leaves a tombstone | Releases history and conflict records | Use |
|---|---|---|---|
| Delete | yes | at the end of their own windows; a field with a history policy keeps a copy of the deleted value | an ordinary client delete |
| Purge | yes | yes, in the same batch (at most 1,024 history entries for one record) | hard erasure; a TTL sweep uses it by default |
| TTL sweep | yes | as its verb (`purge` by default) | expiry of old keys |

- A request reads its records by key and stages one batch. The cost follows the keys
  of the request, never the size of the schema.
- A delete frees the record, not the disk bytes. Compaction returns the bytes.
- A TTL is a schema retention policy. A Hash record ages by its write time: the newest
  field stamp. A HashRange schema needs a range key that starts with 20 digits of unix
  seconds. Set the policy before the first write, or
  [explicitly backfill existing personal Hash/HashRange records](lastdb-ttl-sweep.md#schema-shape-that-a-ttl-needs)
  with the owner's `--confirm-expire-existing` confirmation. See
  `docs/lastdb-ttl-sweep.md` and `docs/app-manifest-retention.md`.
- A schema drop blocks the schema first, then releases its records (`rk:`), field history
  (`rh:`), alternate entries (`ak:`), conflict records and file blob references in batches,
  then removes the schema. A write during the drop gets a refusal.

### Field history

History stays off until a field policy retains earlier values (decision D5).
The policy is `{"mode":"off"|"keep"|"ttl","seconds"}`. The `keep` policy retains a
replaced or deleted value. The `ttl` policy sets its expiry.
A history entry (`rh:{S}:{sha(record key)}:{field identity}:{id}`) keeps its own copy of
the old field value, its stamp and the reason (`replace` or `delete`). A large old value
keeps its file blob reference, which counts as one reference of that file blob.
An alternate entry has no history. LastDB keeps no history of a whole record.
The route is `/api/atom-history` (a code name) and the CLI is `lastdb history`.

### Kept conflicting writes

When a peer import finds that two devices wrote one field of one record, the losing
value is kept as a **conflict record** (`rcf:` row, with an `rcm:` marker on the record)
for 30 days (`LASTDB_CONFLICT_RETENTION_SECS`). A conflict record names the schema, the
primary key, the field, the losing and the winning mutation. It holds no file blob
reference. The retention janitor releases a conflict record at the end of its window.
A purge releases the conflict records of the record in the same batch.

---

## 2. Named alternate keys

Use one schema when several keys must find the same record.
For example, Card uses its slug as the primary key and a board key for the board list.
The board key uses the board field and an ordered range from the column, position, and slug fields.
Every required field is present in Card. The key needs no record correspondence with another schema.

An **alternate entry** (`ak:` key) maps one alternate key to the primary key of the
record. It holds the primary key and the matched key coordinates, and no copy of the
record (decision D4). So there is one copy of each record, and no copy can drift.

- An exact primary read reads the record at the supplied primary key.
- An alternate read reads the alternate entries, then one batch of the records they name.
- A create adds the record and its alternate entries in one transaction.
- An update removes the old alternate entries and adds the new entries in that transaction.
- A delete removes the record's alternate entries in that transaction.
- Adding or removing a key on existing data uses the explicit owner change command and complete coverage checks.
- An alternate read works in the personal database and in an org database.
- An alternate read refuses while the node receives data from another user. Received records have no alternate entries. Read them by the primary key.

Canonical field identity controls reusable descriptions and types. It does not select other records for a write.
Unrelated records use separate schemas and retain separate values.
Relationships with their own values also use separate keyed schemas.

---

## 3. Layer boundary

The core stores schemas, records, alternate entries and file blobs.
An app defines what its records and keys mean.
The core updates declared alternate entries with the source record.
The app owns relationships or summaries that contain different data.

---

## 4. The contract for apps

Design **for the query, before the data**. For every read your app will make,
name the key:

1. **Single entity by identity** → exact hash key on the schema, O(1).
2. **A list** → a schema whose hash partitions the list and whose
   range key orders it; read as a range under that one hash, O(log M).
3. **Another list of the same records** → a named alternate key on the source schema.
   A relationship with different data uses its own keyed schema.

Keep each record under the 64 KiB record cap. Put large bytes in a file blob.
Two fields that different devices change independently can share one record: the
field merge keeps both changes.

Dynamo vocabulary map:

| Dynamo | LastDB |
|---|---|
| Table | Schema |
| Partition key | hash key |
| Sort key | range key |
| Item | record (one stored value per primary key) |
| Item attributes | fields of the record |
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
schema → record → file blob
```

### Native disk engine

The core model holds current schemas, records and alternate entries.
Reads and accepted logical writes use that model.
A call captures the core state and its matching disk root.
A miss fetches the required records in batches. The fetched records can stay warm.

**The warm set unit is one record at one key** (decision D2). A fetch brings the full
record. It does not bring other keys. Alternate entries are read from disk and do not
enter the warm set (D2 permits them; the current code keeps records only). A file blob
stays outside the warm set. A write request stages its written records up to a quarter
of the byte limit; they enter the warm set after the flush, and the rest enter on a later
read.

The recency list controls warm retention under a byte budget, which defaults to 256 MiB.
The byte budget (`LASTDB_CORE_WARM_BYTES`) is the only warm set limit. There is no record count limit (Tom, 2026-10-30).
Each read enters the warm set. The purge removes least recently used clean records.
The purge skips a record that a call still holds or a key reserved by private write preparation.
If those records prevent enough purge, the charge can temporarily exceed the budget.
Status counts that excess; the purge runs again after the owners release their records.
An older call can retain its earlier values. Their memory owners stay charged until that call returns.
These call lifetimes do not provide an old-value query.

The private disk queue retains prepared bytes until flush. Ordinary reads cannot query that queue.
The published `CURRENT` of each run home owns local recovery. It does not replace the current logical memory state.
A commit writes one new immutable run file, makes it durable, then publishes a new `CURRENT`.
Reopen reads that `CURRENT`. Nothing replays.
A successful native write completes its disk commit and core memory change before the ack.
The disk queue starts immediately. No collection timer delays the ack.
This order is the design (Tom, 2026-10-30): fast durable writes, then the ack.
An ack always means that the write is on disk. There is no memory ack and no flush interval.
An internal file change clears affected core records before a later call can use them.

Native reads and writes always use the core model. No old memory mode flag selects that route.
There is no periodic mutation flusher or delayed durability mode.
Explicit flush and the final shutdown barrier remain.
Native writes publish clean records after disk commit. They create no dirty warm-record state.
See the [byte-limit design](lastdb-warm-set-byte-limit-design.md) and [current native summary](lastdb-disk-native-node.md).

### Engine and format

Normal startup opens the disk format v2 engine with a format 7 home.
A new home is format 7. This release opens no older home: copy the records of an older
home into a fresh home. No automatic conversion or boot migration runs.

See [the engine README](../vendor/laststore/README.md) for the run files, commit, merge, and reopen rules.

### Backup and peer sync

The V2 byte backup copies the encrypted run files of a format 7 home. A restore needs no
source home.
A peer event carries, for each record, only the fields and metadata whose stamp names
the request, and a delete floor. The peer import merges the delete floor, then each field
by stamp, then the metadata, so the import order does not change the result. A peer
import refuses file blob references, shared schemas and non-plain schemas.

---

## 6. Databases on one node

In one node, a schema copy into another database (for example an org or shared
database) copies only the **catalog** entry. The target entry reuses the source schema
instance and adds key wraps for the field, alternate and record space keys. The copied
schema reads the same stored records; the node stores the data once.

A share rule for another person copies the whole record, with its delete floor, to a
share key in the same batch as the write.

Scope note: this is **per node**. Cross-user sharing (a shared database synced to
another person's node) still replicates data to that node.

---

## 7. Durable homes / related

| What | Where |
|---|---|
| **This doc (canonical)** | `docs/lastdb-canonical-model.md` |
| Storage layout summary | `docs/lastdb-storage-layout.md` |
| Agent usage rules and shape rules | `docs/lastdb-agent-access-model.md` · https://github.com/EdgeVector/lastdb/blob/main/docs/lastdb-agent-access-model.md |
| Multi-key schemas | `docs/multi-key-schemas.md` |
| Complexity requirement | `docs/lastdb-access-complexity-requirements.md` |
| Blob counter and the dwell | `docs/lastdb-blob-counter.md` |
| TTL sweep | `docs/lastdb-ttl-sweep.md` · `docs/app-manifest-retention.md` |
| Record model design and decisions D1–D7 | brain `design-lastdb-record-model-20261031` |
| Molecule model (retired in format 7) | `docs/lastdb-atom-counter.md` · `docs/lastdb-molecule-counters.md` · `docs/lastdb-molecule-drain.md` · `docs/design-lastdb-counter-lifecycle.md` |
