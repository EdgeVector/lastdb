# Logical device capture v1

The V2 native snapshot remains the authority for local reads and crash recovery.
An accepted normal write reaches disk before LastDB sends its ack.
This change adds an optional logical mutation stream to that same native commit.
The stream supplies future multi-device sync work.

`LASTDB_LOGICAL_CAPTURE=1` enables capture on a native home without capture state.
The default stays off.
The private `logical-device-capture-v1` control file holds an Ed25519 signer, a generation UUID, and the paired native home identities.
Its mode is `0600`.
The origin combines the public key and the generation UUID.
After enrollment, the durable capture state keeps capture active across restarts.
The environment flag cannot reset the origin or its sequence.
Capture refuses a foreign home binding or missing established control state.

The first release accepts locally authored, personal Create and Update requests on plain Hash and HashRange schemas.
The schema and each field require canonical identities.
Admission refuses imports, Delete, Purge, TTL, aggregate operations, protein members, mapped fields, share rules, share subscriptions, and file blob references.
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
The event contains no source request predicates, blinded storage keys, or native reference counters.
The event, its metadata, the device head, the atoms, and their local reference counts share one native commit.
A capacity or preparation error drops the complete private batch.
Ordinary reads do not read this stream.

The fixed limits are:

- 128 source mutations per request.
- 256 KiB for the serialized source request, with a byte count before buffer allocation.
- 1,024 declared fields across the addressed schemas and 1,024 final field outcomes.
- 1 MiB for retained candidate outcomes, with a byte count before each retained clone.
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
A crash between private control creation and the first durable head also refuses normal open.
This release does not silently create a new signer or reset a sequence after either condition.

This release supplies local authored outcomes and durable device positions.
It does not supply a complete cloud sync protocol, peer import, portable snapshot adoption, log pruning, or device replacement.
Schema administration and other metadata changes also require a future protocol contract.
The next step uses CI to check native crash boundaries, accepted writes, refused requests, repeated content, and device positions after reopen.
The CI summary and artifacts hold the operational evidence.
