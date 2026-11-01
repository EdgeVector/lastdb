# V2 sustained cloud benchmark

This benchmark measures the real daemon through its Unix socket. The workflow
uses one standard `ubuntu-24.04` machine in the public repository. It does not
select paid machines, create cloud resources, or use the primary.

The ready PR validates the driver with a 1 GiB seed and 30 seconds at each rate.
That short run provides no sustained-capacity claim. For the full recipe, use GitHub Actions →
**V2 sustained write measurement** → **Run workflow** for later runs.
Manual inputs select 1 or 10 GiB, the phase duration, and the seed duration cap.
The default recipe offers 100, 1,000, and 5,000 record updates per second for
30 minutes each. Reads by key and ranges run at the same time.

## What the sizes mean

The corpus size counts generated body bytes. The default body contains 1,024
bytes. Keys, records, field keys, and other record metadata add bytes. Compression
can reduce file bytes. The report keeps these measures separate.

The initial corpus must reach its size within the seed duration cap. The optional
10 GiB corpus runs only when free temporary space passes the guard. If its seed
cannot reach 10 GiB within the cap, the report records an incomplete large case.
It does not label that case as a 10 GiB capacity result.

## What the measurements mean

The driver schedules writes by time. A fixed limit controls active requests.
There is no unlimited client queue. The result counts offered, admitted,
successful, and skipped work. HTTP rejection and unknown transport outcomes
remain separate. A timeout does not establish whether a write reached disk.

Request duration and scheduled-to-completion duration use separate distributions.
The latter includes delay in the client. Request drain time follows the offered
window and does not increase its reported duration. Read results must use the
requested keys and contain valid generated values. Concurrent reads can return
different valid versions.

Resources are sampled every ten seconds. The sample includes the daemon and
driver memory, CPU time, process file-call bytes, process device bytes, allocated
files, free disk space, and sorted file counts. Device counters can omit delayed
writeback. File samples can miss short peaks. These samples do not establish a
hard disk-size limit.

After the load phases, the driver sends serial durable writes to one dedicated
key. It kills the owned daemon without a flush, restarts the same generated home,
and requires the recovered version to be at least the last acknowledged version.
An in-flight write can also survive. This is a small operational recovery check;
it does not establish power-loss behavior or every possible crash position.

Generated homes use short private temporary paths with ownership records. The
driver stops the owned process and removes its home. Artifacts contain results,
logs, and build metadata. They contain no database home or identity key.

## Limits

This is a finite workload on one Linux cloud machine. It does not establish the
M4 Max limit. The default recipe uses fixed-size bodies, a fixed batch size, and
bounded client concurrency. It does not establish the limits of large file blobs,
long-held reads, cloud sync, or every application schema.

Each phase follows the previous phase in the same home. The report preserves
the order and resource history. Later phases include any work the earlier phase
leaves behind. A short manual duration can check the benchmark path, but only
the full duration can support conclusions about that duration.

Use the Actions summary and uploaded artifacts as the measurement record. Do not
commit generated results or duplicate receipts to the repository.

## Opt-in write and disk diagnostics

The manual `diagnostics` option selects numeric write stages and four disk
checkpoints. The PR recipe selects this option for its short validation run.
For a sustained investigation, select 1 GiB, `rates=1000`, and `seconds=1800`.
The diagnostic recipe disables the optional 10 GiB attempt.

The checkpoints follow the initial load, updates, idle time, and crash recovery.
Idle time is five minutes, or the phase duration when that duration is shorter.
The checkpoints count current keys and stored value bytes by internal collection.
They also separate published, retained, temporary, and unreferenced file bytes.
Resource samples separate the search inbox from native files. The benchmark
does not start a consumer for that inbox. File entries and unique file inodes
have separate byte totals, so hardlinks remain visible without a duplicate count.
The inventory shares a 60-second deadline across both homes. It has a 64 MiB
memory limit, a 512-cursor input limit, and a 50-million-record work limit.
The ordered base files share one cursor, with one base file open at a time.
A partial result remains explicit and makes the investigation incomplete.

The numeric logs separate requests, native write scopes, physical commits,
completion, Search delivery, and background compaction. Completion covers the
delivery, resource release, and ack notice after the native disk commit.
Search delivery includes JSON encode, file write, and rename stages.
Nested stages and concurrent work overlap. Do not add
their durations. Request duration excludes the socket queue wait. Diagnostics
add work, so these times do not establish the uninstrumented write limit.
Request JSON omits zero phase fields. A request stage mean uses only records
with that field present; the report shows this count beside the total count.
The deliberate crash can interrupt a log record. Its exact log byte interval
is excluded and reported; load capture and request count checks remain strict.
The separate recovered-key check still requires the last acknowledged version.

The Actions summary shows the results. `diagnostics.json` retains all numeric
stages, counts, checkpoint totals, coverage checks, and capture errors.
An incomplete initial seed skips all later checkpoints and load phases.
Its interval records `workload_complete=false`, and the report names the skipped checkpoints.
Numeric capture still requires every expected write event. A missing Search checkpoint after that stop does not mean lost write timings.
The workflow still fails when the initial corpus misses its target. Capture completeness does not establish corpus completeness.
Logical bytes precede block compression. Do not subtract them from file bytes.
Normal nodes perform no inventory or numeric log capture without both existing
isolation and timing flags.
