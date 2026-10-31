# Logical device capture v1

The V2 native snapshot remains the authority for local reads and crash recovery.
An accepted normal write reaches disk before LastDB sends its ack.
This change adds an optional logical mutation stream to that same native commit.
The stream supplies future multi-device sync work.

`LASTDB_LOGICAL_CAPTURE=1` enables capture on a native home without capture state.
The default stays off.
The private `logical-device-capture-v1` control file holds an Ed25519 signer, a generation UUID, the paired native home identities, and an enrollment state.
Its mode is `0600`.
The origin combines the public key and the generation UUID.

Enrollment writes the prepared control file, commits head zero, then writes the ready state before any runtime becomes available.
The complete prepared control file reaches disk before an atomic rename makes its final name visible.
The next enrollment removes only a private partial file at the fixed temporary path.
A restart can complete a valid prepared enrollment with its original signer and head zero.
After enrollment, the durable capture state keeps capture active across restarts.
The environment flag cannot reset the origin or its sequence.

Capture refuses a foreign home binding or missing established control state.

The first release accepts locally authored, personal Create and Update requests on plain Hash and HashRange schemas.
The schema and each field require canonical identities.
The normal mutation route refuses imports, Delete, Purge, TTL, aggregate operations, mapped fields, share rules, share subscriptions, and file blob references.
It also refuses existing molecule generations and alternate field layouts.
The admission check runs before attribution records or author-clock changes.
The native admission fence also refuses unsupported internal logical writes before their batch can commit.

Ordinary CAS and `must_exist` checks still decide whether the original local request succeeds.

The source delta includes normalized key fields and canonical API coordinates.
After all schema groups stage their writes, capture reads the exact stored tip records in one batch.
It compares the full authored order, including the atom identity and mutation UUID.
It then fetches the selected atom bodies, including metadata that atom deduplication retained.
The event records the final accepted fields, their schema and field identities, their original tips, and their per-key metadata.
Repeated writes to one physical key produce one final field fact.

The event also names every accepted source mutation in the native request.
An accepted request with no final field change can produce an empty outcome list.
An entirely idempotent retry produces no new event or sequence.

The device signs the prepared outcome event with a new outer signature.
This signature binds the origin, sequence, previous hash, source mutation identities, and complete field outcomes.
The original mutation signature does not authenticate a changed, normalized request payload.
The event encoder sorts all JSON object keys in UTF-8 byte order and preserves array order.
It uses `serde_json` number encoding and preserves exact integers, including nanosecond times.
The cloud manifest encoder has different integer limits and does not encode these logical events.

The event contains no source request predicates, blinded storage keys, or native reference counters.
The event, its metadata, the device head, the atoms, and their local reference counts share one native commit.
A capacity or preparation error drops the complete private batch.

Ordinary reads do not read this stream.

The fixed limits are:

- 128 source mutations per request.
- 256 KiB for the serialized source request, with a byte count before buffer allocation.
- 1,024 declared fields across the addressed schemas and 1,024 final field outcomes.
- 1 MiB for encoded candidate outcomes, with a byte count before each retained clone.
- 1 MiB for the selected raw atom bodies, with a bound before a large body can enter memory.
- 1 MiB for a signed event and an explicit export page.
- Four events per export call.
- 64 MiB of encoded event backlog; a full backlog refuses the complete data write.
- Sixteen metadata records in administrative status.

The native storage limits also bound complete request preparation.
Capture does not change the warm set or its memory policy.
The stream uses the V2 native block encryption at rest.
An explicit export returns logical event data and needs a separate authorized transport.
The private signer stays outside the native backup file allowlist.

An enrolled native backup cannot open as a normal node without its original private control file.
Portable restore needs the future device replacement and snapshot adoption contract.
A ready control without its durable head refuses normal open.
A prepared control with a nonzero head also refuses normal open.
A partial or malformed control file requires operator attention; it does not reset the identity.
An offline backup open cannot complete a prepared enrollment.

This release does not silently create a new signer or reset an established sequence.

These encoded byte caps do not claim an exact Rust memory size.

The owner can export original signed events and import events from an explicitly enrolled device.
The owner socket supplies `POST /api/sync/logical-v1` for these operations.
The app socket does not supply this route.
An export includes the original sequence-zero anchor, the current source position, and a bounded event page.
An export does not change the source position.

Before the first import, the owner enrolls the source origin and its sequence-zero anchor on the receiver.
A valid event signature alone does not enroll a device.
The receiver refuses its own origin and a different anchor for an enrolled origin.
The device roster permits at most 32 origins.

Each import page contains events from one origin in source sequence order.
The receiver verifies every complete event, its signature, its hash, and its previous hash.
The receiver refuses a gap or a different event at a known sequence.
An exact repeat does not change data or advance the import position.
The native snapshot retains the last import position and one compact hash receipt per accepted source sequence.

The imported field facts retain their original atoms, tips, per-key metadata, and authored order.
When an original tip has no device identity, the receiver uses the authenticated stream origin for conflict retention.
This change does not alter the tip order.
The receiver checks the canonical schema, field, molecule, and key before it writes the facts.
It computes its own atom reference counts and uses the existing conflict rule.
A later field fact wins under the existing complete tip order; arrival order does not decide the winner.
The receiver also advances its local author clock from the authenticated facts.
It refuses a new field fact at or below the existing home-wide replay cutoff.

App notifications name the accepted keys and original mutation identities.
They use the operation `update`, because a field outcome does not retain the original request verb.
The receiver sends Search the actual accepted fields and stored atom bodies.
A losing field fact does not produce an update notification.

The field facts, conflict records, local reference counts, author clock, and import position share one native commit.
A refused page changes none of these disk records.
An ack follows the durable commit and the core memory publication.
After a crash, the native snapshot supplies both the data and the import position.
Recovery does not read or replay a mutation log.

An import does not create a new local authored event.
Each source remains responsible for its own stream.
A device needs the streams of every source; a third device cannot obtain all source events from one receiver's authored stream.
The owner export returns logical data without transport encryption.
It requires an authorized transport before any cloud transfer.

The peer limits are:

- Four events per export or import page.
- 1 MiB for the encoded event array, including its brackets and separators.
- 1,024 declared fields across the schemas of one import page.
- An additional 8 KiB for the request or response envelope.
- 8 KiB for a control request without imported events.
- 32 enrolled source origins.
- 65,536 compact imported hash receipts across all origins.

A full receipt store refuses the complete import page.
These finite limits bound the first release until snapshot adoption defines safe receipt and log removal.
They do not change the existing warm set or native memory limits.
Encoded byte limits do not describe the exact Rust memory size.

New native backups carry an encrypted [snapshot inclusion map](snapshot-inclusion-map-v1.md).
It binds the producer and peer positions to the exact captured files.
It does not authorize snapshot adoption or log removal.

This release does not supply a complete cloud sync protocol, portable snapshot adoption, log pruning, or device replacement.
Schema administration and other metadata changes also require a future protocol contract.
CI uses fresh nodes for event transfer, duplicate requests, refused pages, conflict results, and crash recovery.
The CI summary and artifacts hold the operational evidence.
