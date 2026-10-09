# Purging allocator

The `lastdbd` binary uses mimalloc by default. Mimalloc returns unused pages to
the kernel after the LRU removes warm groups.

The daemon sets mimalloc's purge delay to zero unless the process environment
contains `MIMALLOC_PURGE_DELAY`. Set that variable to a delay in milliseconds
to select a different policy. Mimalloc treats `-1` as no purge.

Build the comparison binary with the system allocator:

```bash
cargo build -p lastdb_node --bin lastdbd --no-default-features \
  --features cloud-sync,sentry-telemetry
```

The status and self-metric surfaces report the allocator name, committed bytes,
reserved bytes, requested Rust bytes, and a retention upper bound.

The upstream release build disables the detailed allocation counters. The daemon
uses 32 separate counters for requested Rust bytes. A free on another thread
subtracts from that thread's counter. The sum gives the live total.

`malloc_bytes_in_use` reports that total. `malloc_bytes_held_free` reports
committed bytes minus requested Rust bytes, with a lower limit of zero. This
upper bound includes allocator metadata and unused space within active pages.
Native library allocations outside the Rust allocator are not in the live total.
The system allocator build uses the macOS malloc-zone counters when available.

The governor requests a purge before it removes cache groups. It measures the
physical footprint again, even when the purge reports zero committed bytes.
The purge call visits the caller's thread heap and shared arenas. It cannot
force every live thread heap to release pages. The cross-thread test measures
the physical result while the allocation owner remains alive.

A UDS worker also collects its own heap when it finishes a request. That call
runs only when `COLLECT_EPOCH` is ahead and the last footprint tick was over
the 1 GiB slack line (`measured` minus `footprint_net`). It does not collect
another thread. A tick under the slack line does not collect at request end.
Tokio workers still collect from the park hook when the epoch is ahead.

A second purge trigger runs on its own schedule and does not wait for the
footprint to cross the 10 GiB soft line. `footprint_net` is the measured
footprint minus the allocator-held-free bytes that fit inside it. When that
gap reaches 1 GiB, the governor asks the allocator to purge. A one-minute
cooldown limits this trigger, even when a client polls `/api/status` often.
This trigger closes a real gap: a node under the soft line never runs the
eviction path, so without this trigger it never runs a purge either. The live
primary showed the gap on 2026-09-25: `governor_state=under` for hours, with
`malloc_held_free` near 10 GiB the whole time.

The `allocator_memory_proof` test also puts 64 MiB through an 8 MiB resident
budget. It checks eviction, physical memory, schema retention, and dirty atoms.
