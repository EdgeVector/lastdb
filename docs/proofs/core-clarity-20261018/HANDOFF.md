# Release and ordinary API handoff

The source and compiler checks pass at `a60b27d43f11fd6d1f3cf514d1579229d2256d5c`.
The full-workflow release SHA256 is `bef1ff5b9047e8f76f2b42ec5f581bc385467d99785b6325a5a56a6d3f496f5a`.
Those identities cover the three full API workflows.
The later product commit `436cc86ad82c4ca2486e0b7f55e4a1b840dfeaed` changes only the public error response's selector list.
Its final release SHA256 is `66879950810b0c9b09756ff1ed0d6981b3ce3af849a89ab3436d7f23bf880045`.
The [results](RESULTS.md) separate the full workflows from the final error-response check.
This plan uses no existing database and no new performance comparison.

1. Capture a fresh source identity with `source_identity.py capture`. This pass records separate cohort and final-response identities.
2. Build the release under the host lock with the exact command below and in the source manifest.
3. Finalize the source manifest with `source_identity.py finalize --sha256 <release SHA>`.
4. Preserve the verified program with the unchanged `core-owned-writes-20261018/preserve_program.py` helper.
5. Run `run_clarity.py` under the host lock. Use the frozen V7 driver identity and the final source identity.
6. Use separate fresh homes and receipt directories for logical, protein, and scoped workflows.
7. Require the first create status to show the pending ACK conditions below.
8. Verify the source, program, and driver identities after the workflows. Preserve all refusals and failures.

The first create status precedes the first target value read. Its phase ends with `after-ack-before-first-value-read`.
It requires `accepted_sequence > completed_sequence`, `accepted_batches > 0`, and `core_warm_dirty > 0`.
The default process has no engine, memory-mode, or proof replay choice in its environment.
The existing logical workflow includes normal stops and reopens.
The supplement preserves literal `ENC:`, `ENZ:`, and `ENB:` values through the default JSON codec and reopen.
The unique warmed purge requires key absence and public Atom UUID 404 after ACK and reopen.
The purge proves addressable absence with source-reviewed deletion. It does not prove reclaimed disk bytes or zero-file UUID reads.
V7 is the final driver identity. The V5 and V6 failure receipts stay unchanged.
The plan explains the two narrow driver corrections. Product source stays unchanged between these attempts.

```sh
scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 -- cargo build --release --locked -p lastdb_node --bin lastdbd
```

The node uses its normal default features. No storage or memory mode feature is required.

## Cleanup plan

Each helper creates a private `sp.*` root under the system temporary directory.
The root must have the current user, mode 0700, an exact real path, and a matching `.synthetic-seed-owner.json` marker.
The run preserves its root identity and normal process-stop receipts.

Before cleanup, preserve request logs, program/source hashes, results, and cleanup summaries in this proof directory.
Seal the receipt files with their exact sizes and hashes.
Check each exact root for active PIDs, sockets, and open files. A program-only container needs the same checks.
Prepare the unchanged supported `cleanup_owned.py` preview. Send the exact preview paths to the root agent for review.
Use only its supported exact-path reclaim after that review.
Verify all sealed receipt hashes after cleanup. Record both failures and successful reclaim results.
Do not remove the primary, archive, another agent's root, or a root with a live owner.

The old V6 homes and programs are already reclaimed. This pass must not reconstruct or reopen them.
The source helper uses V6 only as a historical input-list reference. It does not imply a retained V6 executable.
