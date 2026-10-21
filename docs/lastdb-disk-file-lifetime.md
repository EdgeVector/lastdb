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
Compaction rewrites the whole home and holds that gate until its work ends.
New writes wait; this path has no incremental job or wall-clock work limit.
The long pack copy permits reads from the old root.
After publication, the runtime takes the read-view write lock and installs the new root.
It then attempts reclaim before it releases that lock.
Older active calls retain their generation holds and can defer deletion.
New calls acquire their generation hold before they release the view read lock.
Thus no call can capture a root after its files retire.

The ordinary cap-disabled space check uses filesystem capacity and active tickets only.
It does not enumerate files for each write.
An explicitly enabled whole-home cap still uses a bounded census.
A configured cap therefore adds file-inventory work to each reservation.

## Dependency parts and automatic maintenance

Each published generation has one receipt. The receipt names every file part that the generation needs.
It holds at most `MAX_DEPENDENCY_PARTS` entries (65,536).
A publication carries all earlier entries forward and adds its own:
two journal parts for each batch (data and map), one fresh directory part,
and one fresh locator part for each locator tree that the batch edits.
Only compaction removes an entry from a later receipt.
A restart, an idle wait, and a SIGKILL do not merge parts; a restart adds a few parts for its own writes.
A small durable request makes two or three batches and about 13 parts. A write of ten keys makes about three parts for each key.
A writer that reaches the limit refuses the write with `dependency capacity`.

Every publication writes the whole receipt again (80 bytes for each part, twice for each batch),
and the previous receipts stay on disk until reclaim. The count is therefore a hard limit and a write cost.

The native runtime keeps the count after each publication and runs maintenance (pack, then reclaim) when it passes a high-water mark.
Writers wait on the write gate while maintenance runs, so a writer meets the hard limit only when maintenance fails.
Above 90% of the limit a new write also waits for the worker, for at most one minute, before its own admission.
The pack step copies every live record again, so its cost grows with the live set and with the number of hash groups that hold data; it is the longest hold of the gate.

| Setting | Default | Meaning |
|---|---|---|
| `LASTDB_DEPENDENCY_HIGH_WATER` | 4096 | Parts at which maintenance starts. At most 75% of the limit. |
| `LASTDB_AUTO_MAINTENANCE` | on | `0` or `false` turns the worker off. |
| `LASTDB_AUTO_MAINTENANCE_GRACE_MS` | 0 | Grace for the reclaim of an automatic run. |
| `LASTDB_ORPHAN_MIN_AGE_MS` | 600000 | Youngest unreferenced part file that the sweep removes. |
| `LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS` | 30 | Wait before the first pack of a process. |

The trigger rises to twice the parts that the last run left, so a home with a large live set does not loop.
It also rises with the square root of twenty times the records that the last run copied (14,000 parts for ten million records),
because a larger home costs more to pack and so packs less often.
The first pack of a process waits 30 seconds unless the count is above 90% of the limit, so a restart is not blocked behind it.
The pack step holds the write gate. Reclaim then runs in slices of 32 old generations, oldest first,
and the worker releases the gate between slices, so writers and readers run while old receipts retire.
The worker also runs this reclaim when it starts, so a stop or a crash in the middle of a drain does not leave old receipts until the next run.
A reclaim that active reads defer is retried after 1, 2, 4, 8, 16 and then every 30 seconds, for about 20 minutes.
Reclaim removes only a generation directory that is provably unfinished (empty, or a partial receipt whose first page declares more pages than are present); a complete receipt under another number, an unknown file and a gap in the part numbers stop it with an error that names the directory.
When its drain ends, the worker sweeps part files that no receipt names, such as the output of a publication or a compaction that a crash interrupted.
The sweep removes only files older than `LASTDB_ORPHAN_MIN_AGE_MS` (default ten minutes), and it stops without a removal if any receipt is unreadable.
A failed run backs off for 2 to 60 seconds and keeps the writer healthy when `CURRENT` still names the old root.
A home that already sits at the limit starts, waits for the pack step, and accepts writes when it ends; the first start can take as long as one pack step.

`lastdb status` shows the count, the limit, the trigger, the peak and the last run on the `Storage parts` line.
`GET /api/db/native-status` returns the same fields under `native_disk.parts`.
A write that the limit refuses returns 503 `write_resource_limit` with `retry_after_secs`, and wakes the worker.

Reclaim syncs each changed directory once after a phase, and not once for each removed file.
A phase ends with its barrier before the next phase starts, so a receipt never leaves the live set before the removals that it lists are durable.
A crash between phases leaves unreferenced files, which a later reclaim can still remove through the retired receipts.

