# Exact-prefix read simplification review

Baseline: c6b4f7ed9ac5a24169e3f41a66579fdc5ba80de7 (merged PR #269).
Candidate implementation: 155d9192f9b1d682a979343138933671699c5265.

All four prefix callers formerly supplied singleton arrays. The scalar API now
makes that restriction explicit. Prefix reads no longer decode every row to
construct a logical-slot HashSet, and preload reads no longer construct source
priorities, per-batch priority maps, or an output map keyed by decoded slots.
The range preload route also uses the scalar collector.

CoreKeyLease selects unique keys from the domain and advances using an exclusive
last-key bound. A single source cannot overlap another spelling. Rows still sort
by physical base key before return, preserving the previous single-source order.
The 4096-key chunk size is unchanged for a single source. The current lease is
checked after each resolved batch, and unavailable/invalidated preload routes
still fall back to the disk path.

The disk path retains storage-prefix stripping, generation merging, Delete
barrier filtering, and captured changes, in their previous order. Captured
changes select the same single prefix formerly passed as a singleton array.
No storage format, write protocol, key encoding, or query boundary changes.
Non-molecule prefixes retain their prior barrier-only fallback.

No blocking correctness findings in this self-review. Runtime benchmarks cover
public read/write behavior, not concurrent lease replacement, generation
activation, CAS conflicts, power-loss recovery, or every internal fallback.
This is a continuation of exact-key cleanup, smaller than the previous change:
157 Rust lines removed, 82 added, net 75 removed across six files.

## Validation

- Optimized `lastdbd` builds passed for both revisions (`--release --locked`).
- Full workspace non-ONNX clippy lane passed with warnings denied:
  `cargo clippy --workspace --lib --bins --locked --exclude generate_validated_schema_org_seeds --exclude schema_service_worker -- -D warnings`.
- `cargo fmt --all -- --check` passed.
- Changed-file size, function size, no-new-allows, staged build-artifact, and
  whitespace checks passed. Benchmark drivers passed Python syntax compilation.
- All 24 benchmark runs (49,344 timed requests) passed integrity checks.
- No test suite was added or run, following the repository instruction.
