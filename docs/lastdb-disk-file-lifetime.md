# Native disk file lifetime and packing

The native writer now supports a shared file lifetime protocol and explicit file maintenance.
The primary remains unchanged.

## Generation protection

The mandatory `FEATURE_FILE_LIFETIME` bit is 8.
New homes and artifacts set this bit at creation.
Older binaries refuse that home before they trust its root.
Existing native homes without this bit retain every file.
This change does not silently convert their compatibility contract.

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
The gate also refuses new holds during reclaim and refuses roots below its retained generation floor.
Standalone reads keep the cross-process `LIFETIME_LOCK` protocol.

Named references use a checksummed kind-12 page and a directory barrier.
The purposes are backup 1, rollback 2, cloud recovery 4, and conversion source 5.
Purpose 5 retains an already published source generation.
Purpose 3 remains reserved for a pending kind-10 conversion output receipt.
An expired reference still protects its complete generation.
Only an explicit owner release removes it.

`generation_inventory` returns exact relative dependency paths, lengths, and creation generations.
It includes the generation receipt and immutable format descriptors.
It excludes mutable `CURRENT` and `JOURNAL_TAIL`.
A physical backup encodes the selected root and a matching empty tail separately.
It also holds the source generation throughout each copy.

## Packing and space

`HomeJournal::compact` first completes the journal checkpoint under the existing exclusive writer lease.
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

Reclaim requires a complete journal checkpoint and the lifetime feature bit.
It verifies the full closure of `CURRENT`, named references, generations that active local reads hold, and generations inside the grace period.
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
Any other unreadable receipt, unknown file, or failed private output stops reclaim with an error that names the directory, and the directory stays; this command does not invent ownership for them.
The sweep of part files that no receipt names is a separate step. It removes those files after a minimum age.

## Product commands

`lastdb-pack-maintain` requires an explicit native `--home`.
Its commands are `compact`, `reclaim`, `inventory`, `reference`, `references`, `release`, `hold`, `hold-writer`, and `records`.
`records` prints body hashes and original mutation versions through the supported administrative stream.
`hold` retains a read generation for a bounded period.
`hold-writer` reports the local hold costs and retains the writer lease after the journal handle ends.
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

The runtime waits for the complete write gate before maintenance starts.
Compaction rewrites the whole home and holds that gate until its work ends.
New writes wait; this path has no incremental job or wall-clock work limit.
The long pack copy permits reads from the old root.
After publication, the runtime takes the read-view write lock and installs the new root.
It then attempts reclaim before it releases that lock.
Older active calls keep their generations: reclaim retains each held generation and deletes nothing in its closure.
Another process that holds the shared lifetime lock still defers reclaim.
New calls acquire their generation hold before they release the view read lock.
Thus no call can capture a root after its files retire.

The ordinary cap-disabled space check uses filesystem capacity and active tickets only.
It does not enumerate files for each write.
An explicitly enabled whole-home cap still uses a bounded census.
A configured cap therefore adds file-inventory work to each reservation.

## Dependency parts and automatic maintenance

A generation receipt names every part file that the generation needs, and it holds at most 65,536 entries.
Every publication adds entries, and only the pack step removes them. The node counts the parts after each
publication and runs pack and reclaim by itself when the count passes 4,096. Above 90 percent of the limit a
new write waits for that work, and a write at the limit is refused with HTTP 503 `write_resource_limit`.
The ledger, the worker, the settings, the `Storage parts` status line, the sweep of unreferenced part files
and the fail-closed rule of reclaim are in `docs/lastdb-storage-parts.md`.
