# Receipt part pruning: the measurement plan

The baseline is `08d8436ccce9e2493e187a6b0133dc93f47c410f`.
Both variants use the journal-free writer.
The change removes obsolete dependency parts from each new snapshot receipt.
The automatic worker also schedules periodic reclaim below its compaction trigger.
The matched benchmark keeps automatic maintenance off in both versions to isolate foreground costs.

Both variants use the same compiler, dependency lock, release command, and target directory.
The baseline source stays in a separate clean DEV worktree.
The build receipts record the full source commit, executable hash, build command, and compiler.
The standard host lock covers each build and all six measured runs.

The workload uses 512 generated records with four fields and a 1 KiB body.
The six original phases stay unchanged.
The separate compaction and delete phases also stay unchanged.
The order is before 1, after 1, after 2, before 2, before 3, and after 3.
Each run uses a fresh owned home with automatic maintenance off and a 500 ms flush interval.

The report compares three independent pairs.
It gives disk growth, allocated size, process write bytes, write latency, commit counts, and peak process memory.
It retains each run's samples and does not pool requests across runs.
Every phase checks all known keys and ordered ranges under one hash.
Each run also checks a graceful reopen.

The separate retention operation uses 64 generated keys, repeated replacements, and 16 deletes.
A rollback reference protects the earlier root while current receipts omit its obsolete parts.
The operation compares exact dependency hashes before and after reclaim.
After release, reclaim removes obsolete data parts.
It then checks keyed reads, ranges, a forced stop after flush, reopen, and an earlier backup restore.

The separate large value operation starts with an older snapshot that lacks the new part counts.
Its first new write creates the counted snapshot.
It writes 16 generated random values of 768 KiB, with two keys longer than a search page.
The offered bytes exceed one 8 MiB payload part.
It identifies an actual record that crosses a part boundary and retains that record during partial deletes.
It checks shared payload and map parts through reclaim, then removes their last references and checks their removal.
Large offered inputs stay in the owned temporary home; the receipts retain their byte counts and hashes.
The older binary must refuse reads and writes of the counted format without any physical file change.
A separate 64-key operation checks actual external key chains in both locator leaves and their parent.
It verifies shared overflow references after incremental deletes, replacements, reclaim, and reopen.

The operations use generated data only.
An additional automatic-maintenance operation waits for the startup drain, then creates obsolete files below the compaction trigger.
It sends no manual pack or reclaim request and checks later idle cleanup with exact keys and ranges.
The separate orphan operation creates real native Put/Delete payloads before startup.
It advances the generation floor, observes startup retention below the age cutoff, then requires removal by the repeated automatic sweep.
It does not claim a second physical writer operates beside a live node.
The primary, archive, cloud, and shared services stay unchanged.
The measurements do not establish power-loss behavior or physical SSD write counts.

The [final report](RESULTS.md) identifies the exact candidate and preserves the earlier complete intermediate round separately.
