# Native disk file lifetime and packing

The native snapshot writer uses a shared file lifetime protocol and explicit file maintenance.
The [active snapshot protocol](lastdb-snapshot-commits.md) defines the complete commit and reopen boundary.
This document reflects the reclaim handoff from PR 267 and the later journal removal.
It does not establish a primary result.

## Generation protection

The mandatory `FEATURE_FILE_LIFETIME` bit is 8.
New homes and artifacts set this bit at creation.
Older binaries refuse that home before they trust its root.
Snapshot roots without this bit retain every file.
Every supported root also requires snapshot feature `256` and refuses retired journal feature `2`.
Old journal homes receive an explicit refusal; startup performs no migration.

Each ordinary read takes a shared `LIFETIME_LOCK` before it captures `CURRENT`.
A read with a cached root checks its immutable generation receipt under that lock.
The call releases the lock after its last file access.
The lock works across processes and retains no pages or dependency arrays.
Reclaim takes the same lock exclusively without a wait; active calls cause a defer result.
Internal metadata calls run under the writer lease or an outer generation hold.

A native runtime read uses a typed capability from its exclusive writer.
The call registers under the read-view lock and performs no file access for that protection.
The capability retains the operating-system writer lease through the last read scope.
A separate process cannot acquire the writer lease while that call exists.
An atomic local gate counts the active calls for each generation.
Reclaim does not defer for them: it keeps every generation that an active call holds, with its whole closure.
A call therefore finds every file that its own receipt names, and a tight loop of reads cannot starve reclaim.
Concurrent reclaim permits new holds on the installed root or a newer root.
It refuses roots below its retained generation floor.
Standalone writer maintenance excludes new local holds until reclaim ends.
Standalone reads keep the cross-process `LIFETIME_LOCK` protocol.

Named references use a checksummed kind-12 page and a directory barrier.
The purposes are backup 1, rollback 2, cloud recovery 4, and conversion source 5.
Purpose 5 retains an already published source generation.
Purpose 3 remains reserved for a pending kind-10 conversion output receipt.
An expired reference still protects its complete generation.
Only an explicit owner release removes it.

`generation_inventory` returns exact relative dependency paths, lengths, and creation generations.
It includes the generation receipt and immutable format descriptors.
It excludes mutable `CURRENT`.
A format-2 physical backup encodes the selected root separately and holds its generation throughout each copy.
The bundle contains no journal tail or empty journal stream.

## Packing and space

`HomeSnapshot::compact` uses the existing exclusive writer lease and the selected complete snapshot.
It quotes worst-case uncompressed output from the live map before it writes replacements.
It then streams current records and tombstones into new pack parts and map pages.
Each record keeps its stored body, full key, collection, and physical mutation version.
The byte copy uses the existing block and record format.
The existing plain-frame restriction remains in force.
Physical encrypted frames require an absent key-epoch adapter and remain refused.
Encrypted atom content stays opaque.
Compaction uses one payload writer across hash groups, with parts of at most 8 MiB.
Each hash group retains its separate locator tree.

The writer publishes the complete replacement receipt before it publishes `CURRENT`.
It keeps the existing 65,536 dependency-part limit.
The compaction quote permits at most 65,536 collection/hash-group coordinates and 67,108,864 records across the home.
It retains one cursor path and one record stream, with a 64 KiB copy buffer.
No product full-read query is added.

One space ticket owns the complete quoted output until completion or cancellation.
The ticket includes active reservations and checks filesystem capacity.
The native-root census has a six-level depth and a finite file bound.
The whole-home cap is disabled by default.
`LASTDB_DISK_CAP_BYTES` can set a positive cap for the native node.
The defaults reserve 64 MiB for free space and 64 MiB for replacement work.
Compaction can consume the replacement reserve.
Current files and the complete quoted output must still fit any configured cap and the available filesystem space.
No reservation takes credit for later file removal.

## Reclaim

Reclaim requires a complete selected snapshot and the lifetime feature bit.
It verifies the files for its captured root, named references, active local read generations, and generations inside the grace period.
The writer lease verifies each retained generation once; a later call reads its receipt only, so a drain of many calls does not open every part again.
The retained generations can name at most 262,144 parts. A home above that bound stops reclaim with an error that says so and that nothing was deleted; release stale references to resume it.
The default grace period is 24 hours.
It never infers release from a reference expiry.
It removes only exact obsolete paths recorded by old generation receipts.
It checks regular files, parent paths, creation headers, and file age before removal.
It refuses symlink namespaces.

Old receipt directories move atomically into an owned retirement namespace after their dependencies no longer need retention.
A later call can finish an interrupted receipt cleanup.
Reclaim removes only a receipt that is provably unfinished (see `docs/lastdb-storage-parts.md`).
Any other unreadable old receipt stops reclaim with an error that names its directory, and the directory stays.
This command does not invent ownership for unknown output.
The sweep of part files that no receipt names is a separate step.
It removes those files after a minimum age.

A concurrent reclaim plan holds its captured generation and excludes generations at or above its floor from old-file removal.
Later ordinary writes can publish new generations while cleanup runs.
A concurrent orphan sweep also checks each part's creation generation.
New, incomplete, or unknown output stays outside its removal plan.

## Product commands

`lastdb-pack-maintain` requires an explicit native `--home`.
Its commands are `compact`, `reclaim`, `inventory`, `reference`, `references`, `release`, `hold`, `hold-writer`, and `records`.
`records` prints body hashes and original mutation versions through the supported administrative stream.
`hold` retains a read generation for a bounded period.
`hold-writer` reports the local hold costs and retains the writer lease after the snapshot handle ends.
The mutation commands require the exclusive native writer lease.
They refuse a writer that already owns that home.
No command stops a node or activates this format on the primary.

## Native node maintenance

The existing owner route `POST /api/db/compact` serves native whole-home maintenance.
The route defaults to a preview.
Pass `{"dry_run":false}` to pack live records and reclaim eligible files.
Pass `{"dry_run":false,"reclaim_only":true}` to reclaim without a rewrite.
An optional `grace_ms` sets an explicit grace period.
A named collection is refused because native parts share one committed root.

The runtime owns a separate maintenance gate before it starts a pack or cleanup.
It waits for earlier accepted writes and takes the complete write gate before the pack.
The runtime captures a protected root and releases the gate before the copy.
A full copy or recent-tier copy runs outside the gate.
Bounded catch-up rounds graft newer locators before a short final handoff.
The source hold protects its files, and the orphan sweep defers while private output exists.
See `docs/lastdb-concurrent-compaction.md` for the complete protocol.

After publication, a short read-view write lock installs the new root and captures the reclaim plan.
The runtime then releases the read-view lock, writer mutex, and complete write gate before old-file cleanup.
The maintenance gate still excludes another pack or cleanup.
Ordinary writes and reads can continue during reclaim.
Older active calls retain their complete generations, and new calls can hold the installed root or a newer root.
An external shared lifetime lock still defers removal.

New calls acquire their generation hold before they release the view read lock.
No call can capture an old root after its files retire.
Cancellation of the request does not cancel the owned maintenance work or release its holds early.

The ordinary cap-disabled space check uses filesystem capacity and active tickets only.
It does not enumerate files for each write.
An explicitly enabled whole-home cap still uses a bounded census.
A configured cap therefore adds file-inventory work to each reservation.

## Dependency parts and automatic maintenance

A generation receipt names every part file that the generation needs, and it holds at most 65,536 entries.
Every publication adds entries, and only the pack step removes them. The node counts the parts after each
publication and starts maintenance at its adapted trigger, initially 4,096 parts.
At the 85-percent soft limit a new automatic-maintenance write receives HTTP 503 `compaction_behind`.
A write that exceeds the hard limit still receives `write_resource_limit`.
The ledger, the worker, the settings, the `Storage parts` status line, the sweep of unreferenced part files
and the fail-closed rule of reclaim are in `docs/lastdb-storage-parts.md`.
