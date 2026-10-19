# LastDB memory and disk routes

## One logical model

The native engine uses one core model for schemas, molecule definitions, keys, tips, and atom values.
Reads and accepted logical writes use that same model.
The recency list holds references to logical records. It selects the clean records that the purge removes first.
It does not contain another copy of the values.

A normal logical record follows one schema, field, molecule, and key to one atom.
The normal warm limit is 10,000 logical records.
A call holds its selected records. The purge skips those records and writes that still await flush.
File contents stay outside this model. An atom can return a file reference and its access metadata.

## Read

1. Resolve the schema and its field molecules through the core model.
2. Capture the current core state and the matching disk root.
3. Use the known keys, tips, and full atom values from that captured state.
4. Fetch the missing records in batches from the held disk root.
5. Admit the fetched logical records when their source checks and memory allowances permit it.

A call can retain an earlier value while a later write replaces the current value.
Its memory owner stays charged until that call returns.
This call lifetime does not provide an old-value query or stored atom history.
Current writes create no atom history or history reverse references.
Private replacement and conflict records remain until their current references reach disk.
Explicit physical cleanup commands can remove existing history files.
A failed read cannot enter the model as a confirmed missing record.
One shared query check refuses old-value and recursive-depth controls for reads and counts.
The request decoder retains those names for an explicit refusal. Current field readers do not accept those controls.

Each field keeps its current atom in its molecule.
A query assembles a record from those fields in the selected storage scope.
Exact reads and range reads use the same query controls and each field's own atom metadata.
Query conditions borrow the selected field values. They do not format or copy an entire response record.
Both steps of a field query use the same bounded reader for the other fields.
There is no separate whole-record value or record-compaction write route.
Native file packing combines the disk work without a second logical value representation.
When a write omits declared key fields, preparation supplies their known values from the lookup key.
This keeps the record available to field conditions without a separate membership source.
An atom identity includes its source schema and value. A write through another schema can change that identity for the same value.
Related molecules still share the current selected atom. A normal reopen retains that atom identity.
Internal current-field reads select each tip once, then fetch the selected atom bodies in one batch.
Schema metadata supplies definitions. It does not supply a fallback value when the current tip is absent.
A shared key that becomes usable also receives the unchanged fields through their existing atoms.
The absence of an old key does not skip those fields. Only a confirmed old association permits its removal.

## Range keys

Selected HashRange schemas load complete membership keys before their first query continues.
Concurrent first queries share that load. Later ranges select keys from the complete memory set under one named hash.
Tips and atom values remain partial. Key preload does not fetch every value.
The key allowance is separate from the 10,000-record warm limit.
Unselected schemas and refused loads use the bounded disk selector.

## Write and flush

1. Validate the batch and its current preconditions under the native write gate.
2. Prepare the complete logical changes and their disk output.
3. Reserve their memory and disk work before publication.
4. Publish the complete core change, then send the permitted ack.
5. Flush the prepared bytes through the one native disk queue.

The private preparation step transfers its schema, atoms, and related changes into the disk stage.
That transfer does not copy those prepared payloads into a second job.
Native after-commit work sends the Search change after the disk commit.

The private disk queue owns prepared bytes and completion work until flush.
Ordinary reads cannot query that queue.
The native journal supports recovery of stored writes. It does not replace the current core state.
Explicit durable requests wait for disk. A memory refusal also forces disk completion before success.
An internal file change that lacks a complete logical translation finishes on disk and clears affected core records before success.

User Delete resolves the current related molecule keys inside the same native write boundary as their removal.
The worker checks the caller's conditions before it prepares any deletion.
The current field atoms supply the values for those checks and related keys.
Source and related deletions enter one disk commit, including each schema's cleanup work.
Search receives the deletion after that commit.
Both Delete forms remove tips and keep shared atom bodies. The `must_exist` option checks the caller's key before removal.
Delete crosses Hash and HashRange layouts only for complete records with the same field names and canonical field identities.
Partial membership records keep their separate deletion boundary.
The explicit Purge route retains its separate destructive rules.
Both Purge paths check shared references after removal of the target tips.
A failed reference check stops publication before atom removal.
An invalid peer-key value receives an explicit refusal before any deletion starts.
That fixed input error does not retain a write for an endless storage retry.

A fixed operation or byte limit refuses the whole prepared request before publication.
A read limit also returns a refusal. Neither refusal causes an endless delete retry.
Temporary memory or queue pressure retains its retry behavior.

The native flush delay is at most 500 ms. `LASTDB_RESIDENT_PERSIST_MS` can select a delay from 1 to 500 ms.
A crash can lose a write that received an ack before its flush.
Write reservations, core metadata, schema definitions, and complete keys have separate allowances.
Their limits do not define one whole-process memory cap.

## Engine boundary

Normal FoldDB startup opens the native packed engine. No selection flag is required.
Factory options name the local encryption keyring and Search inbox.
The factory takes no inactive cloud callback or worker-delay inputs.
The logical constructor rejects an older physical store before it opens the logical namespaces.
An existing native home can reopen. An empty home can initialize directly.
A nonempty older-format home receives an explicit refusal. No boot migration or automatic conversion runs.

Cloud sync stays disabled for this optimization round. Startup and runtime activation must refuse it before side effects.
The local capture router remains. No cloud engine attaches to it during this cutover.
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
The [write result](proofs/core-owned-writes-20261018/RESULTS.md) records the preceding measured source and its limits.
Later cleanup or cutover checks must identify their own source and product evidence.
