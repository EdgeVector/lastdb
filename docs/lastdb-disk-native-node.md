# Native disk node path

The native disk backend connects the saved home format to LastDB namespaces and keyed reads.
Selection requires `LASTDB_DISK_PACK=1` for a new empty store. An existing native marker selects its saved backend.
An existing legacy store is refused by this constructor. This change does not convert it.
No primary, primary copy, archive, factory, or shared service participates in the local checks.

## Current limits

This is an explicit development path. The default new-node backend remains the legacy format.
The native constructor refuses memory-first acknowledgements until the pre-apply admission gate is complete.
Use `LASTDB_RESIDENT_MODE=off` for the generated-node check. Each write waits for its complete disk directory.
The constructor also refuses a cloud-connected native node until native backup and cloud references are complete.
Namespace removal requires a complete named-key plan and remains refused.
Binary ids remain addressable by exact key. Internal ranges also accept a trailing byte limit marker for metadata.
Reserved and binary prefixes require exact-key reads.

## Home fence

The node store contains `laststore-pack-node-v1`, with the exact bytes `format=1\nhome=disk-pack\n`.
The native home resides in its fixed `disk-pack` child directory.
Both the node store and the new native child contain a legacy layout refusal:
`version=2\nlayout_mode=disk_pack_v1\n` in `laststore-layout-v1`.
The version-1 parser refuses that descriptor before it opens or creates legacy data.
The current legacy opener also refuses either native marker.
Native startup verifies the parent refusal. Incomplete construction never becomes an empty-store guess.

## One read boundary

The runtime publishes an in-process CURRENT only after the journal and complete directory reach disk.
A request captures that root once. All queries in a batch share its aggregate work budget and call-local buffers.
Each query item carries its own failure state. A failed native read cannot become an empty success through an inner fallback.
A resource error remains explicit, and another batch item retains its own result.
The shared call ends after every item completes. It keeps no file handles, hash groups, pages, or blocks afterward.
An absent namespace inside a query remains read-only. Its first mutation registers it in the same complete batch as its stored records.

The backend derives every exact group and hash-range group from the held home descriptor.
Known internal metadata ranges can visit the 1,024 exact directory coordinates under the same finite call limits.
They perform no file walk or legacy group replay. User molecule ranges use their saved hash fanout.
The backend batches known coordinates and probes older key forms only for unresolved slots.
Writes remove an older physical copy only when the same held read proves that copy exists.

The native warm cache stores fetched record bytes. It does not duplicate them in the plaintext seam cache.
A cache hit must match the call's committed frontier. A later root invalidates each changed key before it releases the write.
A read admits a record only when its epoch and frontier still match.
Read admission makes space before insertion. If every record is protected, the returned temporary body does not enter the cache.
The record cap remains 10,000. This cap is not a process memory-size limit.

## One complete mutation

One persist envelope collects its stored atoms, tips, schema, and side records before a native journal batch.
Reads within that envelope see its staged writes and the held committed root.
A compare-and-swap holds the same exclusive write gate as every other mutation.
The write captures the latest complete root after that gate. An older query cannot supply its comparison state.
Only a complete directory publication permits resident dirty clear or a completion reply.
Purge envelopes use the same boundary. A failed native attempt restores its retry state.
The writer retains every generation file. It grants no pack deletion or compaction authority.

Native conversion, memory-first admission, backups, rollback, retained references, and final fresh-node performance remain separate completion gates.
No primary upgrade follows from the development flag or these source checks.

The [generated-node receipt](proofs/disk-native-node-20261015/report.md) records exact, batch, range, update, delete, restart, and resource-limit results.
It also records the small debug comparison. This comparison does not establish a speed improvement or production readiness.
