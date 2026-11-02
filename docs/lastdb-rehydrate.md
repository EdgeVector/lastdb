# LastDB memory and disk routes

This page describes the memory and disk routes of a format 8 home.
The [canonical model](lastdb-canonical-model.md) defines the record model. It wins where this page differs.

## One model

A format 8 home stores one record at each primary key (`rk:`).
The record holds every field value of the schema, a stamp for each field, and one record metadata block.
Named alternate entries (`ak:`) point to the primary key. They hold no copy of the record.
File contents stay outside the record. A record can name a file blob and its access metadata.

The native engine uses one core model for schema definitions and records.
Reads and accepted writes use that same model.
The warm set holds decoded records, one entry for each used record key (decision D2).
A fetch brings the full record. It does not bring other keys.
Alternate entries and file blobs do not enter the warm set.
The recency list holds references to records. It does not hold another copy of the values.
The warm set uses the byte limit of the [warm set byte limit design](lastdb-warm-set-byte-limit-design.md).

A call holds its selected records. The purge skips those records and the keys that private write preparation reserves.
When the call releases its records, the purge restores the byte limit if clean records permit it.
The call first releases its saved values, so dead allocations do not cause extra purge.

## Read

1. Resolve the schema through the core model.
2. Capture the current disk root and its publication revision.
3. Use warm records only while that revision still matches. Retain the selected records for the call.
4. Fetch the missing records in batches from the held disk root.
5. Admit each successful fetch after its source checks. Purge clean, unused records when the byte charge exceeds the limit.

A read by primary key reads one record. A range under one hash reads the record keys of that hash in bounded pages.
A read by a named alternate key reads the alternate entries, then one batch of the records that they name.
A query reply carries the fields, the primary key, and one `metadata` block for each record.
The block is `{source_file_name, metadata?}`, or `null` when the record has none.
Each record in the reply also carries `has_conflicts` and `conflicted_fields`.
A reply has no field key id and no writer id.

Native writes commit to disk before they publish clean core effects. No dirty memory overlay exists for reads.
The read scope retains its disk root, call-local definitions, selected record sources, and record holds.
A call can retain an earlier value while a later write replaces the current value.
Its memory owner stays charged until that call returns.
This call lifetime does not provide an old-value query. [Field history](lastdb-field-history.md) is a separate policy.
A failed read cannot enter the model as a confirmed missing record.
The call keeps a compact certificate for each decoded record and its exact disk source.
The certificate covers the full value, the metadata, and the stamps.
The call moves its decoded record into the warm set after the certificate check.
The certificate store keeps no second record body.

Exact reads and range reads use the same query controls.
Empty field and numeric condition lists mean no conditions for pages and counts.
Numeric order and numeric conditions keep the exact stored integer value. They do not round it to a float.
Field and numeric conditions read their fields before the result limit, even when the reply omits those fields.
The final sort uses the same value order as key selection. An omitted sort field stays internal and leaves the reply before return.
Query conditions borrow the selected field values. They do not copy a complete reply record.
Time conditions keep milliseconds and negative fractional seconds.

Keyed reads keep the caller's key form through counts and reply metadata.
They do not read a caller key as the disk form of another key.
Only an unfiltered page walk recovers keys. It reads each key from the key fields of the record, because a walk cannot decode a blinded hash.
Hash reads trust the supplied key. Range punctuation has no special meaning.
Cross-hash administrative pages use offsets. A cross-hash `PageAfter` cursor gets a refusal.
Query cursors support ascending keyed reads without field conditions or value order controls.
Other query forms use offsets. A supplied unsupported cursor gets a refusal.
Empty and whitespace hashes stay exact API values.
An unfiltered blinded Hash list refuses a key that it cannot recover. A filtered Hash list can use the caller's hash.

An optional schema count reports `null` when the count fails or is not available.
Only a successful count can report zero records and `has_data: false`.
The schema-definition list uses the owner context. Its optional counts describe the owner's personal database.
Named database queries and key lists use their requested database scope.

## Write and flush

1. Validate the batch and its current preconditions under the native write gate.
2. Read the current record of each key, from memory first and then from disk, in one bounded batch.
3. Merge each changed field by its stamp, and prepare the disk output.
4. Reserve the memory and disk work before publication.
5. Complete the disk commit through the one native disk queue.
6. Publish the clean core change, then send the durable ack.

A write changes only the fields that it names. Each field keeps the value with the newer stamp.
The same batch holds the records, the alternate entry changes, the blob counter changes, the field history entries,
the share copies, the delete audit entries, the TTL ages and the idempotency entries.
A write computes the alternate entries from the old and the new fields.
It puts the new entries and deletes only the old entries that still name this record.

When a write omits declared key fields, preparation supplies their values from the lookup key.
Create and Update refuse a declared key field whose value differs from the caller's key.
The refusal occurs before any member of that batch changes data.
Create and Update refuse the reserved tombstone value and validate ordinary field types.
A missing required key gets a refusal before write preparation. The engine does not invent a key from the record contents.

A successful batch returns one accepted input mutation ID for each caller input, in caller order.
Delete, Purge, and idempotent repeats follow the same rule.
The ID identifies the accepted input. It does not identify the stamp that wins a field.
Change notices use that exact input-to-ID association.

Local mutations in one batch share one revision and get consecutive author times in caller order.
The native create and update batch writes its author clock high-water state in the same durable commit.
That state never moves below an earlier durable reservation.
Imported signed clocks stay unchanged. Random mutation identities do not order repeated local writes.

A present record metadata block in a write replaces only the metadata keys that it sends.
A write without metadata keeps the stored metadata.
Single and batch requests carry the declared source file name through the same write route.

The private disk queue owns prepared bytes and completion work until flush.
Ordinary reads cannot query that queue.
The prepared disk commit owns its disk space reservation directly.
A failed commit keeps that reservation. The sealed queue keeps its pending request owners.
Each queued disk extension reserves only its additional output bytes.
The earlier request keeps its reservations until the extension succeeds.
Acceptance moves those owners without a second charge. Refusal releases only the private extension.

Native requests start disk work immediately. They return a durable ack after the complete disk commit and memory publication.
No collection timer delays the ack.
An internal file change that has no record translation finishes on disk and clears the affected warm records before success.
Write reservations, core metadata and schema definitions have separate allowances.
Their limits do not define one memory cap for the whole process.

A fixed operation or byte limit refuses the whole prepared request before publication.
A read limit also returns a refusal. Neither refusal causes an endless delete retry.
Temporary memory or queue pressure keeps its retry behavior.

## File blobs

Each local file blob has one blob counter (`bref:c:`).
The counter holds the number of records and field history entries that name the blob.
A write changes the counter by the net change between the old and the new record, in the same batch.
All databases on the node share the file blob store. One blob counter counts the references of every scope.

At zero, a blob waits one hour by default, because an upload comes before the record that names it.
The dwell pass then reads the counter again inside a native write scope.
It deletes the blob, its counter and its queue entry in one batch.
A reference that arrives during the dwell cancels the delete.
See [the blob counter](lastdb-blob-counter.md).
A personal restore keeps existing file bytes and their encryption metadata. It adds only files that are absent from the node.

## Delete and purge

A delete changes the record into a tombstone at the same key. The tombstone keeps the delete floor.
In the same batch the delete removes the alternate entries, lowers the blob counters and writes a delete audit entry.
A purge also releases the field history and the kept conflict records of the record in that batch.
Write conditions use the same record reader as queries. They do not guess a record location from the caller's key.
The worker checks the caller's conditions before it prepares a delete.
Delete and Purge check the caller's conditions in the same database scope as the removal.
The `must_exist` option checks the caller's key before removal.
An ordinary Delete skips a key from another layout. A loud Delete or Purge refuses that key.
Search gets the change after the disk commit.

`lastdb liveness explain blob <ref>` reads the counter of one file blob.
It cannot list the referrers of the blob, because the counter holds a number and no list.

## Schema catalog

Schema definitions use one shared namespace on the node.
Writes and deletes read those definitions, then use the caller's database scope for the records.
The definition lookup does not select data from another database.
Ordinary create and update writes keep the registered schema definition. They do not store or reload the schema catalog.
Schema declaration and definition updates keep their catalog publication boundary.
Schema publication sets the fixed encryption keys of every field key of the schema.
Personal and named databases use the same keys for the same field key identity.
Private key creation follows the native write boundary and one lock order.
A failed schema change cannot publish its private keys in the shared cache.

Catalog put, declaration, share, and removal use the same native write boundary.
The catalog entry and the data of a removed instance change together.
Cleanup planning and preparation each refuse a request above the 128 MiB byte allowance.
The native operation limit can also refuse the complete cleanup.
A refusal keeps the catalog entry and all instance data.
Instance removal does not revoke the field key encryption wraps that other instances can use. Its `reclaimed_wraps` result stays zero.
A schema drop releases the records of the schema in batches. See [the schema drop](lastdb-schema-drop.md).

## Memory relief

The memory relief of the node uses the current native core model.
An explicit request first waits for accepted writes at the disk barrier.
It then removes clean records in least recently used order.
Held records and keys that a private write reserves stay protected.
The report gives the remaining bytes and says whether the requested target was met.
Host pressure uses the same native measurements and the same purge.
Held allocations can prevent a relief request from reaching its target.

## Run files

A disk format v2 home merges its run files in the background.
A merged input file goes away after the last read of the older root ends.
Reopen reads the published `CURRENT` of each run home. Nothing replays.
At open, the owner deletes temporary files and run files that `CURRENT` does not name.
The [engine README](../vendor/laststore/README.md) describes the run files.

## Shared responsibilities

| Part | Responsibility | Explicit boundary |
|---|---|---|
| Core model | Current definitions and warm records | A warm record does not prove a complete range. |
| Read scope | One held disk root, publication revision, allowance, and call-local owners | Child reads keep the same scope. |
| Write preparation | One complete change and its prepared disk bytes | A failed preparation does not publish part of that change. |
| Native queue | Prepared disk output and completion work | Ordinary reads cannot use it as another value source. |
| Physical maintenance | Run file merge, backup, and explicit repair | These tools do not select another engine. |

## Engine boundary

Normal startup opens the disk format v2 engine. No selection flag is required.
The CAS wrapper uses the account E2E content key directly. No keyring option selects another key.
The program opens only a format 8 home. An empty home can initialize directly.
Any other node marker gets the generic failure. No boot migration or automatic conversion runs.
Startup namespace errors propagate. An unreadable record in an enforcing encryption check causes a refusal without deletion.
The encryption key proof precedes namespace checks.
No configuration file decodes into the database configuration. Callers build it in code.

The V2 byte backup uses a separate gate, `LASTDB_BYTE_BACKUP_V2_ENABLED`, that defaults off.
Capture saves an exact cut and a named file hold. The [native summary](lastdb-disk-native-node.md#backup-boundary) separates this source from cloud readiness.
Raw physical format tools and vendor code do not provide another engine.

The core owns the record values and their measurements.
Normal status returns a saved sample, normally refreshed every 60 seconds. Native status returns the current counters.
Write gates, record locks, and purge queues control their own work.

## Current key format

A new home records the current key format before it serves requests.
The key format entry is `layout:keys` with the value `typed-ranges`.
The node marker file `laststore-pack-node-v1` holds `format=8\nhome=disk-run\ncas=disk-cas\n`.
A new home records its setup state before it creates the physical store.
An interrupted new setup can resume from that state. It cannot accept user writes before setup completes.
An existing home without the current key format entry or that setup state gets an error.
Startup does not convert data.
Flat keys use the zero-byte separator.
Field key identities use their exact base64url form.
A logical snapshot requires the same key format entry. Another snapshot gets an error before restore writes data.
