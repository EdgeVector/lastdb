# LastDB memory and disk routes

## One logical model

The native engine uses one core model for schemas, molecule definitions, keys, tips, and atom values.
Reads and accepted logical writes use that same model.
The recency list holds references to logical records. It selects the clean records that the purge removes first.
It does not contain another copy of the values.

A normal logical record follows one schema, field, molecule, and key to one atom.
The warm set uses the byte limit defined in the [warm set design](lastdb-warm-set-byte-limit-design.md).
A call holds its selected records. The purge skips those records and paths reserved by private write preparation.
When the call releases its records, the purge restores the byte limit if clean records permit it.
The call first releases its saved values, so dead allocations do not cause extra record removal.
File contents stay outside this model. An atom can return a file reference and its access metadata.
Reads and writes reuse an equal complete atom object when one is already available.
An equal UUID alone cannot combine copies with different atom metadata.
The warm set charges each molecule tip's metadata separately from its shared atom.
Schema aliases at one molecule key share this charge.
A tip update replaces the charge. Absence and removal release it.
Private write metadata retains its separate allowance.

## Read

1. Resolve the schema and its field molecules through the core model.
2. Capture the current disk root and its publication revision.
3. Use current core keys, tips, and atom values only while that revision still matches. Retain selected values for the call.
4. Fetch the missing records in batches from the held disk root.
5. Admit each successful fetch after its source checks. Purge clean, unused records when the byte charge exceeds the limit.

Native writes commit to disk before publishing clean core effects. There is no dirty-model read overlay.
The read scope retains its disk root, call-local definitions, selected atom sources, and path/value holds.
A call can retain an earlier value while a later write replaces the current value.
Its memory owner stays charged until that call returns.
This call lifetime does not provide an old-value query or stored atom history.
Current writes create no atom history or history reverse references.
Private replacement and conflict records remain until their current references reach disk.
Explicit physical cleanup commands can remove existing history files.
A failed read cannot enter the model as a confirmed missing record.
A selected tip requires its atom in every database scope.
The shared body reader checks the result count and refuses a missing atom.
It returns complete atoms, so callers do not contain another skip rule.
The call keeps a compact certificate for each decoded atom and its exact disk source.
The certificate covers the full value, metadata, and time. It excludes no atom field.
The call moves its decoded atom into the warm set after the certificate check.
The certificate store keeps no second atom body. Its metadata cap stays at 4 MiB.
Sources, certificates, and scope bindings each permit up to 10,000 entries within that shared cap.
The former skipped-atom reports remain empty wire fields. They own no counters or record lists.
One shared query check refuses old-value and recursive-depth controls for reads and counts.
The request decoder retains those names for an explicit refusal. Current field readers do not accept those controls.

Each field keeps its current atom in its molecule.
A query assembles a record from those fields in the selected storage scope.
Exact reads and range reads use the same query controls and each field's own atom metadata.
Empty field and numeric condition lists mean no conditions for pages and counts.
Numeric order and numeric conditions retain the exact stored integer value.
They do not round a stored integer to a float before the comparison.
Successful memory tip reads consume the same call allowance as other required reads, including records rejected by visibility checks.
Keyed fields keep each selected tip's writer; the Single-field writer helper does not override them.
Field and numeric conditions fetch their fields before the result limit, even when the response omits those fields.
The final sort uses the same value order as key selection. An omitted sort field remains internal and leaves the response before return.
Query conditions borrow the selected field values. They do not format or copy an entire response record.
Both steps of a field query use the same bounded reader for the other fields.
There is no separate whole-record value or record-compaction write route.
Native file packing combines the disk work without a second logical value representation.
When a write omits declared key fields, preparation supplies their known values from the lookup key.
Create and Update refuse a declared key field whose value differs from the caller's key.
The refusal occurs before any member of that source batch changes data.
Delete and Purge create no key-field atoms.
This keeps the record available to field conditions without a separate membership source.
An atom identity includes its source schema and value. A write through another schema can change that identity for the same value.
Related molecules still share the current selected atom. A normal reopen retains that atom identity.
Internal current-field reads select each tip once, then fetch the selected atom bodies in one batch.
Writes, reads, and related deletion keys use one conversion for text, numeric, and Boolean coordinates.
Slot helpers select coordinates without an encoding change. Only the storage codec changes their representation.
Cold Hash counts and value reads use the same exact tip reader.
That reader keeps the caller's hash and the selected atom location together.
Keyed reads keep the caller's key form through counts and response metadata.
They do not reinterpret a caller key as another key's disk encoding.
Only physical page and sample reads recover keys from their stored form.
Reads and writes use the exact molecule identity from the schema.
Normal reads and deletes do not try another UUID spelling or create another preloaded key set for it.
Only explicit dropped-schema physical cleanup can enumerate historical UUID spellings.
Narrowed reads carry the range form explicitly. Exact caller ranges receive no second decode.
Hash reads trust the supplied key. A HashRange field-value check uses only the declared hash field that established the keys.
Projection fallback establishes its own keys. Range punctuation has no special meaning.
Named-layout membership precedes the result window. Bounded tip pages retain their cursor before value fetch, so rejected or missing values do not end selection early.
Exact HashRange batches check each selected record against its own hash. Counts use the same rule.
Actual primary tip presence controls projection fallback, including an empty page after the cursor or offset.
Cross-hash administrative pages use offsets. Logical `PageAfter` and non-keyed query cursors receive an explicit refusal; keyed query cursors use their API-coordinate window.
Schema metadata supplies definitions. It does not supply a fallback value when the current tip is absent.
A shared key that becomes usable also receives the unchanged fields through their existing atoms.
The absence of an old key does not skip those fields.
A missing, null, array, or object value in an optional peer-key field names no peer key.
If a peer key loses a required scalar value, the write removes its confirmed old association.
The addressed schema still requires scalar values for its own declared key fields.
Old-key removal and related deletion use one current association check.
The peer must expose every source key field with the same canonical field identity and current scalar coordinate.
Every present shared field must also match the source atom, metadata, and author clock.
A moved unchanged field can retain its atom and use the key-change author clock.
Equal field values alone do not establish record ownership.
Partial shared fields that omit the source identity do not authorize related record removal.
Conflicts still choose each field's current atom by its author clock.
A related key uses the field values that the source write accepts.
A rejected field value cannot change that key, even when another field in the same mutation wins.
The source decision precedes related key preparation inside the private native write boundary.
A peer with mixed source identities or conflicting shared-field clocks remains unchanged by related cleanup.
This rule does not introduce unique peer keys or replacement of an entire record on a field write.
Each schema change reads the earlier private changes in the same batch before it prepares related keys and values.
HashRange write conditions and explicit Delete or Purge use the declared key membership rule.
A present declared hash field must prove both coordinates. An absent hash field retains the existing sparse projection rule.
An ordinary Delete skips a key from another layout. A loud Delete or Purge refuses that key.
Create and Update refuse the reserved tombstone value before preparation and validate ordinary field types.

Schema key lists capture the schema, selected tips, and atom values in one read.
The public key list resolves the caller's database membership inside that captured read.
Its selected field, keys, values, and preload use the same database scope.
Value reads, counts, and schema key lists share the resolved schema's blocked-state check.
They load the molecule's encryption keys and fetch selected values in batches.
HashRange membership precedes the key-list limit; rejected records cause a bounded refill.
Empty and whitespace hashes remain exact API values.
The list command preserves those hashes in both requests and continuation commands.
An unfiltered blinded Hash list refuses a key that it cannot recover. A filtered Hash list can use the caller's hash.
Explicit physical helpers can still return storage tokens. They do not define schema query results.
Time conditions retain milliseconds and negative fractional seconds.
Query cursors support ascending keyed reads without field conditions or value order controls.
Other query forms use offsets. A supplied unsupported cursor receives an explicit refusal.
An optional schema count reports `null` when the count fails or is unavailable.
Only a successful count can establish zero records and `has_data: false`.
The schema-definition list uses the owner context. Its optional counts describe the owner's personal database.
Named database record queries and key lists use their requested database scope.

## Range keys

Selected HashRange schemas load complete membership keys before their first query continues.
Concurrent first queries share that load. Later ranges select keys from the complete memory set under one named hash.
Tips and atom values remain partial. Key preload does not fetch every value.
The complete key allowance is separate from the warm set byte limit.
Unselected schemas and refused loads use the bounded disk selector.
A failed optional preload releases its incomplete keys and retains its diagnostic reason.
It does not prevent a later keyed disk read. A required disk read still reports its own error.
An empty hash is valid for both initial key preload and later changes to the complete key set.

## Write and flush

1. Validate the batch and its current preconditions under the native write gate.
2. Prepare the complete logical changes and their disk output.
3. Reserve their memory and disk work before publication.
4. Complete the disk commit through the one native disk queue.
5. Publish the clean core change, then send the durable ack.

Write preparation fetches the exact headers of its changed fields in one bounded batch.
It batches their tip reads within each database scope, with at most 10,000 keys per fetch.
The existing reader resolves generation shadows, current private changes, and Delete barriers for those exact keys.
A missing exact-scope header retains the existing full-load fallback.
The fetched values end with the private preparation. The write gate and durable ack rule stay unchanged.

A successful batch returns one accepted input mutation ID per caller input, in caller order.
Delete, Purge, and idempotent repeats follow the same rule.
The ID identifies the accepted input. It does not identify the atom that wins a field conflict.
Change notices use that exact input-to-ID association. They do not substitute another input's ID.

The private preparation step transfers its schema, atoms, and related changes into the disk stage.
That transfer does not copy those prepared payloads into a second job.
The logical path stage collects the bindings for all changed molecules first.
It reads each distinct schema definition once from the current private view.
Each molecule still requires its own complete binding counter and exact field-to-molecule checks.
The definitions leave memory after that stage. A later stage reads its own current view.
Within that preparation, repeated exact disk reads reuse the already authenticated result from the held root.
The private result store has a 1 MiB key/value/entry allowance and at most 10,000 records.
It omits individual values larger than 16 KiB and falls back to disk when its allowance is full.
Every read checks the current private changes and the accepted prefix before these saved disk results.
Each reuse still charges the complete call's key and returned-byte allowance before it copies a value.
Errors never become saved absences. Ordinary queries do not allocate this result store.
The result store ends with the write preparation. It holds no decoded storage blocks and creates no shared file or block cache.
A private writer can reuse a clean core schema definition when its captured revision still matches.
It checks all three exact sources: the schema, its state, and its successor.
A change in the current request or its accepted prefix requires a fresh captured read.
Private prepared definitions retain priority. Reuse creates no new shared cache or catalog write.
The native preparation report counts core reuse, private prepared reuse, and decode attempts separately.
Ordinary create/update writes keep the registered schema definition and field
molecule identities. They publish changed tips, values and molecule headers through
the native transaction without storing or reloading the schema catalog. Schema
declaration and definition updates retain their catalog publication boundary.
Record writes retain the existing best-effort relationship-binding retry; that
check does not store or reload the unchanged catalog definition.
Native after-commit work sends the Search change after the disk commit.
Related molecule definitions share one protected metadata change.
Concurrent schema declarations cannot create competing proteins for the same unbound molecule.
Schema and protein definitions use one shared namespace on the node.
Related writes and deletes read those definitions, then use the caller's database scope for the records.
The definition lookup does not select data from another database.
Schema publication establishes the fixed encryption keys for every final field molecule.
This includes related molecules before a write reaches them through a protein.
Personal and named databases use the same keys for the same molecule identity.
Private key creation follows the native write boundary and one lock order.
A failed schema change cannot publish its private keys in the shared cache.
Current reads use that one key pair. No previous-key transition or automatic conversion runs.
Preload loads the molecule keys before it captures their encoding identity.
The native write boundary precedes the membership and molecule-liveness guards.

Catalog put, declaration, share, and removal use that same write boundary.
The native write boundary precedes molecule locks and the catalog's last-reference check.
The catalog row, its molecule bindings, and a removed instance's data change together.
Cleanup planning and preparation each refuse a request above the 128 MiB byte allowance.
The native operation limit can also refuse the complete cleanup.

A refusal retains the catalog row and all instance data. Search receives changes only after commit.
Instance removal does not revoke molecule encryption wraps that other instances or schemas can use.
Its `reclaimed_wraps` result remains zero. Cloud reactivation requires a separate domain-aware revocation design.

The private disk queue owns prepared bytes and completion work until flush.
Ordinary reads cannot query that queue.
The prepared disk commit owns its disk-space reservation directly.
A failed commit retains that reservation; the sealed queue retains its pending request owners.
Reopen reads the published `CURRENT` of each disk format v2 run home. Nothing replays.
Private requests retain newer writes until the run commit and clean memory publication complete.
Local mutations in one batch share one logical revision and receive consecutive author times in caller order.
The native create/update batch includes its author-clock high-water state in the same durable commit.
The state never moves below a prior durable reservation. Other write paths retain their explicit clock barrier.
Imported signed clocks remain unchanged. Random mutation identities do not order repeated local writes.
A new winning tip replaces the full author clock and optional key metadata, even when its atom identity stays the same.
The atom identity describes the schema and value. It does not describe the write time or key metadata.
Equal atoms do not create a different-atom conflict or a replacement-history entry.
The same commit retires the prior tip's reference. A reference that the current tip still uses stays active.
A present key-metadata record supplies all its optional fields. An omitted member clears that value.
Only the absence of the entire key-metadata record permits atom creation metadata as a fallback.
Current key metadata can reference a file blob independently of the immutable atom metadata.
Each tip that carries file metadata is one reference to its blob, and so is each atom counter that lists the blob.
The blob counter changes in the same batch as the tip or the atom counter that holds the reference.
All databases on the node share the file blob store, and one blob counter counts the references of every scope.
A scope drop lowers the blob counters that its tips and atom counters held, in the same batch as the rows.

File cleanup uses the blob counter and a dwell (`docs/lastdb-blob-counter.md`).
A blob whose counter reaches zero waits one hour by default, because an upload comes before the atom that names it.
The dwell pass then reads the counter again inside a native write scope and deletes the blob row, its counter and its queue entry in one batch.
A reference that arrives during the dwell cancels the delete.
A personal restore keeps existing node-wide file bytes and their encryption metadata.
It adds only files that are absent from the node.
Single and batch requests carry the declared source file name through the same write pipeline.
Logical query responses omit the physical molecule version. A partial memory molecule cannot supply a stable record version.
All native requests wait for disk and their complete memory publication before success.
An internal file change that lacks a complete logical translation finishes on disk and clears affected core records before success.

User Delete resolves the current related molecule keys inside the same native write boundary as their removal.
Write conditions retain the selected tip's physical atom location through the body fetch.
They use the same required-body reader as queries. They do not guess a location from the caller's key.
The worker checks the caller's conditions before it prepares any deletion.
The current field atoms supply the values for those checks and related keys.
Source and related deletions enter one disk commit, including each schema's cleanup work.
Search receives the deletion after that commit.
Both Delete forms remove tips. The atom counter frees an atom body only when its last reference goes, so shared atom bodies stay. The `must_exist` option checks the caller's key before removal.
Delete crosses Hash and HashRange layouts only for complete records with the same field names and canonical field identities.
Partial membership records keep their separate deletion boundary.
The explicit Purge route retains its separate destructive rules.
Delete and Purge check the caller's conditions in the same database scope as the removal.
Delete and Purge give every tip change to the atom counter planner. The planner lowers one reference for each removed tip and deletes each body whose count reaches exactly zero in the same batch (`docs/lastdb-atom-counter.md`).
A counter that is absent, or lower than the removal, keeps the body and marks the counter `suspect`. The request does not fail for it.
Counters use the selected database scope.
The atom liveness report reads the counter of each named atom: its count, its `suspect` flag, and whether the body exists.
The report cannot list the referrers of an atom, because the counter holds a number and no list.
This diagnostic accepts the default owner scope. It refuses named database handles because it lacks a schema for their alias lookup.
An optional peer key without scalar coordinates receives no related deletion.
The caller's addressed record remains eligible for deletion.
Delete and Purge select related keys under the same native write boundary as their removal.

A fixed operation or byte limit refuses the whole prepared request before publication.
A missing required key also receives a refusal before write preparation.
The declared key fields can supply that key. The engine does not invent one from the record contents.
Stored record keys require valid decoding. Aggregate guards and portable imports propagate a decode failure.
A read limit also returns a refusal. Neither refusal causes an endless delete retry.
Temporary memory or queue pressure retains its retry behavior.

Native requests start disk work immediately and return a durable ack after the complete disk commit.
The old 500 ms collection delay does not control native requests.
Write reservations, core metadata, schema definitions, and complete keys have separate allowances.
Their limits do not define one whole-process memory cap.

The node's memory relief uses the current native core model.
An explicit request first waits for accepted writes at the disk barrier.
It then removes clean logical records in least-recently-used order.
Held records and privately reserved write paths stay protected.
The report gives the remaining bytes and says whether the requested target was met.
Definitions, complete keys, and write metadata keep their separate allowances.
Host pressure uses the same native measurements and clean-record purge.
The warm set enforces its byte limit when clean, unheld records permit removal.
Held allocations can prevent a relief request from reaching its target.

Each queued disk extension reserves only its additional output bytes.
The earlier request keeps its reservations until the extension succeeds.
Acceptance transfers those owners without a second charge. Refusal releases only the private extension.
A failed disk commit retains the complete reservation for its required output.

A disk format v2 home merges its run files in the background.
A merged input file goes away after the last read of the older root ends.
At open, the owner deletes temporary files and run files that `CURRENT` does not name.
The [engine README](../vendor/laststore/README.md) describes the run files.

## Shared responsibilities

| Part | Responsibility | Explicit boundary |
|---|---|---|
| Core model | Current definitions, selected tips, atom values, and confirmed absence | A partial molecule does not prove a complete range. |
| Complete selected keys | Membership for selected schemas | The separate allowance does not preload every atom. |
| Read scope | One held disk root, publication revision, allowance, and call-local owners | Child reads keep the same scope and selected atom location. |
| Write preparation | One complete logical change and its prepared disk bytes | A failed preparation does not publish part of that change. |
| Native queue | Prepared disk output and completion work | Ordinary reads cannot use it as another value source. |
| Physical maintenance | Run-file merge, backup, and explicit repair | These tools do not select another logical engine. |

## Engine boundary

Normal FoldDB startup opens the disk format v2 engine. No selection flag is required.
Factory options name offline maintenance and the Search inbox.
The CAS wrapper uses the account E2E content key directly; no keyring option selects another key.
The factory takes no inactive cloud callback or worker-delay inputs.
The logical constructor rejects an older physical store before it opens the logical namespaces.
An existing native home can reopen. An empty home can initialize directly.
A nonempty older-format home receives an explicit refusal. No boot migration or automatic conversion runs.
Startup namespace errors propagate. An unreadable record in an enforcing encryption check causes a refusal without deletion.
The encryption-key proof precedes namespace checks. A new proof follows only successful checks; optional diagnostic counts stay separate.
Normal page readers contain no plaintext-to-blinded legacy duplicate cleanup.
No configuration file decodes into the database configuration. Callers build it in code.

Legacy Cloud Sync is removed. No configuration can activate it.
The V2 byte backup uses a separate gate, `LASTDB_BYTE_BACKUP_V2_ENABLED`, that defaults off.
Capture saves an exact cut and a named file hold. The [native summary](lastdb-disk-native-node.md#backup-boundary) separates this source from cloud readiness.
Raw physical format tools and vendor code do not provide an alternate logical engine.
Logical writes do not generate tip history. Startup does not schedule a history drain.
Explicit physical history maintenance remains separate from the normal logical routes.

The core owns the logical values and their record measurements.
Status reads those measurements from the native runtime.
Normal status returns a saved sample, normally refreshed every 60 seconds; native status returns the current counters.
`PersistLaneMetrics` measures background purge work; it holds no logical values.
Historical status fields remain at the wire boundary for installed clients.
The native sampler does not maintain their retired graph and loader counters.
Write gates, record locks, and purge queues still control their respective work.
The [write result](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/core-owned-writes-20261018/RESULTS.md) records the preceding measured source and its limits.
Later cleanup or cutover checks must identify their own source and product evidence.

## Current key format

A new home records the current key format before it serves requests.
The current logical marker is `layout:v3:keys` with value `3`.
It covers framed molecule identities and exact binding identities.
The physical node marker remains `laststore-pack-node-v1`.
A new home records its setup state before it creates the physical store.
An interrupted new setup can resume from that state. It cannot accept user writes before setup completes.
An existing home without the current format marker or that setup state receives an error.
Startup does not convert data.
Flat keys use the current zero-byte separator. A read never tries an older colon spelling.
Molecule identities use their exact current base64url form. A hex identity is not an alternate key.
The current atom placement choices remain flat or located by molecule and hash.
The current hash and range encryption choices remain available.
A changed atom placement setting receives an error after home creation.

The old key conversion, key fork, tip conversion, and collection copy commands are removed.
Archived tip files and their repair jobs are removed. Current history events remain.
Version 2 logical snapshots require the same current key format marker.
Older logical snapshots receive an error before restore writes data.
This key format change leaves the current warm set limits unchanged.
