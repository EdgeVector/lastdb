# fold_db bench baseline

`baseline.json` is the **committed performance baseline** for the fold_db
criterion benches (`../query_path_bench.rs`, `../db_operations_bench.rs`,
`../encrypt_scan_bench.rs`).

## Why this exists

Until this landed there was no tracked perf baseline. Regressions were only
catchable by eye, or against hand-typed numbers in the bench doc comments that
went stale silently — which is how the 2026-06-20 wedge (an O(N-schema)
per-query clone that turned the FLAT point lookup into an O(field) read, peaking
at ~13 GB RSS) slipped past the benches even though a bench *existed* for that
exact path. A regression with no tracked baseline stays invisible until it
wedges the node.

## How it's enforced

The `Bench` workflow (`.github/workflows/bench.yml`) runs the benches and then
`scripts/lints/bench-compare.py`, which compares each fresh criterion **median**
against `baseline.json` and fails any case whose measured median exceeds
`baseline_ns × regression_factor` (2.0 by default; the FLAT point-lookup cases
are guarded tighter at 1.6× so an O(field) regression trips well before 2×).

That workflow is **off the merge-queue critical path** — it runs weekly, on
manual dispatch, and on demand when a maintainer adds the `run-benches` label to
a PR. It is *not* in `ci-required`, so it never blocks a merge; a regression is
a loud signal to a human. (The benches seed up to 100k-record corpora and take
~an hour; keeping them off every PR preserves the merge-queue build time that
#959 + #964 halved — see `project_fold_ci_build_time`.)

The absolute numbers are runner-/machine-dependent — treat the baseline as a
**relative** regression ceiling, not an SLA.

## Regenerating the baseline

When a deliberate perf change moves a curve, refresh the affected numbers in the
**same PR**:

For the scheduled db-perf guard, prefer the wrapper so setup/build time and
Criterion measurement time are reported separately:

```bash
scripts/ci/run-db-perf-guard.sh
```

The wrapper prebuilds the two guarded bench targets with a named setup timeout,
then runs the fast guard measurement and `scripts/lints/bench-compare.py`.

```bash
# On a quiet machine (close other load; the numbers are relative but noisy
# baselines cause flaky guards):
for query_group in \
  indexed_point_lookup \
  point_lookup_vs_schema_count \
  realistic_message_lookup \
  indexed_full_read \
  value_filter_scan \
  hashrange_page_list_vs_N \
  count_rows_vs_field_count \
  count_rows_vs_row_count \
  concurrent_list_reads \
  concurrent_mixed_read_write
do
  cargo bench -p fold_db --bench query_path_bench -- "${query_group}"
done
cargo bench -p fold_db --bench db_operations_bench --bench encrypt_scan_bench

# criterion writes target/criterion/<group>/<id>/new/estimates.json.
# Read each median ("median.point_estimate", ns) and update the matching
# `baseline_ns` in baseline.json, explaining the move in the commit message.
```

To save a native criterion baseline for local A/B comparison (independent of
this committed JSON):

```bash
cargo bench -p fold_db --bench query_path_bench --bench db_operations_bench -- --save-baseline main
# later, after a change:
cargo bench -p fold_db --bench query_path_bench --bench db_operations_bench -- --baseline main
```

## What's guarded

See `baseline.json`. Headline cases:

- **`indexed_point_lookup`, `realistic_message_lookup`, `point_read`** — O(1)
  reads that **must stay FLAT** across cardinality (guarded at 1.6×). A
  regression here means the per-key read started materializing the whole field
  again — the 2026-06-20 failure mode.
- **`point_lookup_vs_schema_count`** — the **second** axis of the 2026-06-20
  wedge: a per-query deep clone of the whole schema registry in
  `get_schema_following_supersession`, whose cost scales with the **number of
  schemas / superseded versions loaded**, not corpus size. Holds a fixed 1K
  corpus and sweeps the registry to 1/10/100/500 loaded schemas (some
  superseded), asserting the keyed lookup stays ~flat (guarded at 1.6× per K).
  Every other group loads exactly one schema, so this dimension was invisible —
  the exact blind spot that let the incident through. A curve that climbs steeply
  with schema count is the O(N-schema) registry clone returning.
- **`keyed_update_write`** — per-key write into a populated field; sub-linear,
  relative guard (regressed 3–4× on 2026-06-21, the proximate motivation).
- **`batch_store`** — raw `DbOperations::batch_store_atoms` write throughput at
  batch sizes 1/10/100/1000, with a fresh DB per measured iteration so corpus
  growth cannot hide or invent write regressions. Guarded at the default 2×
  ceiling because this is the storage write path below mutation payload shaping.
  The one-atom case uses a loaded-routine-host baseline from 2026-07-19 because
  fresh tiny Sled stores are especially sensitive to shared-runner noise.
- **`atom_refcount_tip_write_vs_catalog_refs`** — full tip-changing mutation
  cost at 0/16/64 catalog aliases. The path uses one exact durable catalog
  counter read per affected molecule. The sweep catches a return to a
  per-alias molecule-edge scan.
- **`storage_breakdown`** — per-schema logical storage metering attribution via
  `AtomStore::storage_breakdown`, with a fixed 500-atom metered schema inside a
  growing corpus. The current implementation scans canonical atom rows and
  filters to the requested schema, so the curve is expected to be linear in
  total corpus; if it becomes schema-index backed, re-capture and flatten these
  cases.
- **`concurrent_list_reads`** — the concurrent-read load shape that actually
  wedged `:9001`; catches throughput collapse / lock convoys.
- **`indexed_full_read`, `schema_scan`** — result-sized, linear-by-design paths;
  guarded only against gross >2× blowups.
- **`schema_scan_indexed`** — fixed-result schema-index listing through the
  local-only `schema_index` namespace; must stay flat across corpus size
  (guarded at 1.6×). The shared flat 7 ms baseline reflects loaded-runner
  medians from 2026-07-18; a curve that grows with corpus means
  `list_atoms_by_schema` fell back to scanning non-matching atoms.
- **`schema_load_at_boot`, `encrypted_namespace_scan`** (`encrypt_scan_bench`) —
  at-rest-encryption (Gap G1) bulk-scan overhead. Each group runs the same scan
  over an **encrypted** store and a **plaintext control** on the same corpus, so
  the delta is the per-row AES-256-GCM decrypt cost; only the `encrypted/*` ids
  are guarded (linear-by-design, relative ceiling). `schema_load_at_boot` is
  `get_all_schemas()` over the encrypted `schemas` namespace (~2× plaintext,
  sub-ms at realistic schema counts); `encrypted_namespace_scan` is the
  `scan_prefix` per-row-open path the `lineage_forward`/`lineage_reverse`
  traversal lands on (~15× plaintext, ~2.8 µs/row). Guards against a regression
  in the decrypt path (per-row key re-derivation, redundant clone, slower
  cipher). Regenerate with `cargo bench -p fold_db --bench encrypt_scan_bench`.
