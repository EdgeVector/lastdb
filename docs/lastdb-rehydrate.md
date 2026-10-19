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
Reads and writes reuse an equal complete atom object when one is already available.
An equal UUID alone cannot combine copies with different atom metadata.

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
A selected tip requires its atom in every database scope.
The shared body reader checks the result count and refuses a missing atom.
It returns complete atoms, so callers do not contain another skip rule.
The former skipped-atom reports remain empty wire fields. They own no counters or record lists.
One shared query check refuses old-value and recursive-depth controls for reads and counts.
The request decoder retains those names for an explicit refusal. Current field readers do not accept those controls.

Each field keeps its current atom in its molecule.
A query assembles a record from those fields in the selected storage scope.
Exact reads and range reads use the same query controls and each field's own atom metadata.
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
A peer with mixed source identities or conflicting shared-field clocks remains unchanged by related cleanup.
This rule does not introduce unique peer keys or replacement of an entire record on a field write.
Each schema change reads the earlier private changes in the same batch before it prepares related keys and values.

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
The key allowance is separate from the 10,000-record warm limit.
Unselected schemas and refused loads use the bounded disk selector.
An empty hash is valid for both initial key preload and later changes to the complete key set.

## Write and flush

1. Validate the batch and its current preconditions under the native write gate.
2. Prepare the complete logical changes and their disk output.
3. Reserve their memory and disk work before publication.
4. Publish the complete core change, then send the permitted ack.
5. Flush the prepared bytes through the one native disk queue.

The private preparation step transfers its schema, atoms, and related changes into the disk stage.
That transfer does not copy those prepared payloads into a second job.
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

The private disk queue owns prepared bytes and completion work until flush.
Ordinary reads cannot query that queue.
The prepared disk commit owns its disk-space reservation directly.
A failed commit retains that reservation; the sealed queue retains its pending request owners.
The native journal supports recovery of stored writes. It does not replace the current core state.
Explicit durable requests wait for disk. A memory refusal also forces disk completion before success.
An internal file change that lacks a complete logical translation finishes on disk and clears affected core records before success.

User Delete resolves the current related molecule keys inside the same native write boundary as their removal.
Write conditions retain the selected tip's physical atom location through the body fetch.
They use the same required-body reader as queries. They do not guess a location from the caller's key.
The worker checks the caller's conditions before it prepares any deletion.
The current field atoms supply the values for those checks and related keys.
Source and related deletions enter one disk commit, including each schema's cleanup work.
Search receives the deletion after that commit.
Both Delete forms remove tips and keep shared atom bodies. The `must_exist` option checks the caller's key before removal.
Delete crosses Hash and HashRange layouts only for complete records with the same field names and canonical field identities.
Partial membership records keep their separate deletion boundary.
The explicit Purge route retains its separate destructive rules.
Delete and Purge check the caller's conditions in the same database scope as the removal.
Both Purge paths check shared references after removal of the target tips.
A failed reference check stops publication before atom removal.
Reference data uses the selected database scope. Its completion proof covers the whole home and uses unprefixed markers.
A missing completion proof refuses atom removal. An older reference store cannot replace that proof.
An optional peer key without scalar coordinates receives no related deletion.
The caller's addressed record remains eligible for deletion.
Delete and Purge select related keys under the same native write boundary as their removal.

A fixed operation or byte limit refuses the whole prepared request before publication.
A missing required key also receives a refusal before write preparation.
The declared key fields can supply that key. The engine does not invent one from the record contents.
Stored record keys require valid decoding. Aggregate guards and portable imports propagate a decode failure.
A read limit also returns a refusal. Neither refusal causes an endless delete retry.
Temporary memory or queue pressure retains its retry behavior.

The native flush delay is at most 500 ms. `LASTDB_RESIDENT_PERSIST_MS` can select a delay from 1 to 500 ms.
A crash can lose a write that received an ack before its flush.
Write reservations, core metadata, schema definitions, and complete keys have separate allowances.
Their limits do not define one whole-process memory cap.

The node's memory relief uses the current native core model.
An explicit request first waits for accepted writes at the disk barrier.
It then removes clean logical records in least-recently-used order.
Held records and unflushed records stay protected.
The report gives the remaining bytes and says whether the requested target was met.
Definitions, complete keys, and write metadata keep their separate allowances.
Host pressure uses the same native measurements and clean-record purge.
These controls do not enforce the planned warm byte admission limit.

Each queued disk extension reserves only its additional file and journal bytes.
The earlier request keeps its reservations until the extension succeeds.
Acceptance transfers those owners without a second charge. Refusal releases only the private extension.
A failed disk commit retains the complete reservation for its required output.

## Shared responsibilities

| Part | Responsibility | Explicit boundary |
|---|---|---|
| Core model | Current definitions, selected tips, atom values, and confirmed absence | A partial molecule does not prove a complete range. |
| Complete selected keys | Membership for selected schemas | The separate allowance does not preload every atom. |
| Read scope | One captured core state, disk root, allowance, and held owners | Child reads keep the same scope and selected atom location. |
| Write preparation | One complete logical change and its prepared disk bytes | A failed preparation does not publish part of that change. |
| Native queue | Prepared disk output and completion work | Ordinary reads cannot use it as another value source. |
| Physical maintenance | Packed-file rewrite, backup, and explicit repair | These tools do not select another logical engine. |

## Engine boundary

Normal FoldDB startup opens the native packed engine. No selection flag is required.
Factory options name the local encryption keyring and Search inbox.
The factory takes no inactive cloud callback or worker-delay inputs.
The logical constructor rejects an older physical store before it opens the logical namespaces.
An existing native home can reopen. An empty home can initialize directly.
A nonempty older-format home receives an explicit refusal. No boot migration or automatic conversion runs.
Startup namespace errors propagate. An unreadable record in an enforcing encryption check causes a refusal without deletion.
The encryption-key proof precedes namespace checks. A new proof follows only successful checks; optional diagnostic counts stay separate.
Normal page readers contain no plaintext-to-blinded legacy duplicate cleanup.
The configuration reader also refuses obsolete type tags and invalid path values.
It does not convert an older configuration or discard its cloud or engine settings.

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
