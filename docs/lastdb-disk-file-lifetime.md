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
An atomic local gate makes internal reclaim defer while any such call exists.
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
Encrypted atom content stays opaque.

The writer publishes the complete replacement receipt before it publishes `CURRENT`.
It keeps the existing 65,536 dependency-part limit.
The administrative stream has a one-million-record work limit.
It retains one cursor path and one record stream, with a 64 KiB copy buffer.
No product full-read query is added.

One space ticket owns the complete quoted output until completion or cancellation.
The ticket includes active reservations and checks filesystem capacity.
The native-root census has a six-level depth and a finite file bound.
The whole-home cap remains disabled until its number is accepted.
The defaults reserve 64 MiB for free space and 64 MiB for replacement work.
Compaction can use the replacement reserve and does not refuse because a disk cap is exceeded.

## Reclaim

Reclaim requires a complete journal checkpoint and the lifetime feature bit.
It verifies the full closure of `CURRENT`, named references, and generations inside the grace period.
The default grace period is 24 hours.
It never infers release from a reference expiry.
It removes only exact obsolete paths recorded by old generation receipts.
It checks regular files, parent paths, creation headers, and file age before removal.
It refuses symlink namespaces.

Old receipt directories move atomically into an owned retirement namespace after their dependencies no longer need retention.
A later call can finish an interrupted receipt cleanup.
Unknown files and failed private outputs remain retained; this command does not invent ownership for them.

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
The long pack copy permits reads from the old root.
After publication, the runtime takes the read-view write lock and installs the new root.
It then attempts reclaim before it releases that lock.
Older active calls retain their generation holds and can defer deletion.
New calls acquire their generation hold before they release the view read lock.
Thus no call can capture a root after its files retire.

The ordinary cap-disabled space check uses filesystem capacity and active tickets only.
It does not enumerate files for each write.
An explicitly enabled whole-home cap still uses a bounded census.
Its hot-path counter needs a separate accepted cap policy before activation.
