# Parallel prerequisites for durable publication

A foreground native commit retains all four durability barriers.
It changes their schedule without a disk-format change.

The writer first completes the new run's encrypted blocks and footer.
The commit then holds both the commit lock and the publication lock.
The new run metadata and the next manifest remain private.

Two independent prerequisites execute in parallel:

- A joined worker flushes the run file, renames it, and flushes the run directory.
- The caller encodes CURRENT, writes its temporary file, and flushes that file.

The caller joins the worker and requires success from both prerequisites.
Only then can it rename CURRENT and flush the root directory.
The readable root changes after that final barrier.
The native queue still completes memory publication and delivery before the durable ack.

This order preserves the dependency from durable CURRENT to a durable run name and complete run bytes.
The staged CURRENT file is private, even when its file flush finishes first.
The directory barriers retain their existing platform implementations, including F_FULLFSYNC on macOS.

A worker creation failure returns an error and releases its private temporary run.
A run failure removes a successfully staged temporary CURRENT.
A CURRENT staging failure still joins the run worker before it returns.
Unpublished new runs retain the existing discard rule.
An uncertain CURRENT rename or root-directory barrier still poisons the home and retains its output for reopen.
The accepted queue retains its reservation and fails closed after a failed publication.

Compaction retains its existing publication path.
Foreground publication holds the publication lock for both prerequisites, so a compaction cannot change the next manifest underneath it.
The added thread ends before the commit returns. There is no persistent worker, file handle, or new value cache.

The numeric native report imports only its explicitly joined worker.
The parent interval already covers that work; child covered time is not added again.
`v2_parallel_prerequisites_us` measures the shared interval.
`v2_run_current_overlap_us` measures the intersection of run durability and CURRENT staging on the monotonic clock.
It does not claim simultaneous physical device I/O.
`v2_run_build_us` now ends after complete run bytes, before durability.
`v2_run_durable_us` retains the separate durability interval.
Nested and parallel phase durations must not be added.

The CI comparison uses three alternating ordinary pairs from identical stopped generated files.
Each case sends 2,048 durable single-record calls with 16 clients and verifies the final values.
A separate native pair supplies phase and barrier evidence.
The normal fresh-home speed, growth, disk/delete and artifact checks remain active.
The existing Linux and macOS disk-run proofs include reopen and process-interruption recovery.
These checks do not simulate a physical power failure.
