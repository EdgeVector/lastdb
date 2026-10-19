# Round 3 measurement plan

The reference is PR217 at `8c0531e6ca032bbc1e5b11379792c3a120f170a6`. Its accepted release is reused. No new reference build occurs. `baseline-identity.json` records all 1,878 matching source inputs and this reuse. `baseline-root.json` names the retained executable-only root. The candidate receives a separate source manifest and executable hash after the source freeze and release build.

Use only fresh synthetic homes. Do not use the primary, archive, an existing home, cloud, tests, or a new fixture suite. Keep the original helpers unchanged. Run all measured operations serially under `scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 -- ...`. Preserve every outcome and stop on a product failure.

## Paired timing matrix

Run the existing `docs/proofs/core-owned-writes-20261018/run_cases.py` with `--receipt-root docs/proofs/core-quality-round3-20261018/benchmark`, both private executable paths, both expected hashes, and both source manifests. This wrapper copies the unchanged V6 `driver.py`, `api.py`, and `workload.py` from `core-memory-state-20261018`. Their bytes match `core-owned-writes-20261018/benchmark-v6` exactly.

The fixed order is `current-on`, `before-on`, `current-off`, `before-off`, then the reverse order. Each case uses a fresh home. Thus each preload state has an A/B pair and a B/A pair. The matrix has eight cases. Historical case times are approximately 28 seconds, so allow 4–6 minutes for the matrix. Receipt review and cleanup occur after the matrix.

Each case uses two independent schemas: Hash and HashRange. Each schema starts with 256 records and four fields. Each body has 1,024 bytes. The HashRange schema has one named hash. The warm set cap is 10,000 logical records. The driver adds 64 new records per schema, so each schema ends with 320 records.

The fixed measurements are:

- Exact Hash reads with widths 1, 16, and 64.
- Exact HashRange reads with widths 1, 16, and 64.
- Bounded HashRange interval reads with widths 16 and 64.
- The first read after a process reopen, five reads of the same keys, and a disjoint set in the same process.
- Selected-schema preload on and off. Preserve the load count and ready-state observations.
- Queued-request write ack and ack through an explicit flush. The conditions are new-key create, update without a prior value read, and update after a prior value read.
- Current values, atom and molecule identities, membership, order, counts, and final normal reopen.

The eight cases provide 696 checked read operations and 112 timed 64-record write batches. The existing paired reader produces 42 read comparisons and 12 write comparisons. Each matched first-read group has two observations. Each repeated-read group has ten observations. Keep all individual observations with the summaries.

The seed phase also sends eight durable 64-record create requests per case. Their ordinary API elapsed times remain in the raw receipts. If reported, label them **durable seed-create ack**. Do not call them durable warm-update samples. The measured write frontier after ack and after flush remains explicit. The ack-through-flush duration includes the status request between ack and the explicit flush.

## Ordinary protein and conditional-write operations

Use the existing ordinary API `Node`, normal native boot, and canonical response helpers. Preserve helper hashes, the exact JSON inputs, all raw requests and responses, and the source and executable identities. Do not add an executable driver or change a frozen helper.

Reuse the accepted per-field HashRange protein and conditional-write scenario from `record-query-delete-authority-20261018/operation-inputs.json` and `final-range-cas`. Use fresh homes with the same requests for the reference and candidate. The sequence creates the source and peer layouts, moves the peer key through the source, writes through the peer, rejects a stale expected value, deletes with the current expected value and `must_exist`, checks the missing-target refusal, and checks a normal reopen. Check every known source, old peer, new peer, and unrelated field. This is product evidence, not a new timing suite.

Report the successful Delete API wall duration from both raw operation receipts only when the requests, initial state, durability, and response contract match. One matched pair is a descriptive observation. It does not support a throughput or percentile claim. Keep refused calls separate from the successful Delete duration.

Use one additional fresh candidate-only home for a nullable peer-key Delete. Declare matching Hash layouts by `id` and `status`, with `status` typed as `OneOf(String, Null)` on both schemas. Create a source with a null status; do not create a peer under a fabricated key. Preserve the exact extra input before execution. The Delete must cause an explicit terminal refusal. Then send a valid status update, send a valid Delete, and read the known keys. This shows that the write queue accepts the next valid write. Do not run the known retry-risk call on the reference. Do not claim a paired latency improvement for this case.

## Summary and limits

Use the unchanged `core-memory-state-20261018/summarize.py`, with its receipt-root value supplied in the invoking process, then `core-owned-writes-20261018/benchmark_tables.py` and `write_frontiers.py`. Preserve the original summary separately as an unaccepted intermediate. Before table generation, replace its historical scope text with exact `reference_scope` metadata: this comparison measures candidate `894c459d1e235aa928973fc6f479182acf459ef3` against the reused PR217 release at `8c0531e6ca032bbc1e5b11379792c3a120f170a6`. Raw measurements remain unchanged. Final tables identify both releases. Do not describe the reference as the old disk engine or attribute all changes to the memory cutover.

The process reopen clears LastDB memory. It does not clear the operating-system file cache. This dataset fits the key cap. It does not measure a 16–20 GiB database, maximum disk throughput, memory pressure, or cloud upload. Compare each request shape only with the same shape in the other release. Hash width 16 or 64 uses a batch endpoint; HashRange uses one keyed query. Two first-read observations provide limited evidence. The host lock serializes this work but does not reserve the machine.

After all nodes stop normally, preserve receipt hashes and exact cleanup previews. The parent reviews the roots before supported exact cleanup. Retain `sp.wrer5mcd` until all reference measurements finish.
