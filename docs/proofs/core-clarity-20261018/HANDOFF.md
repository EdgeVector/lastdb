# Release and ordinary API handoff

The source still needs its final commit and compiler checks. Do not run these commands before that source freeze.
This plan uses no existing database and no new performance comparison.

1. Capture `candidate-identity.json` with `source_identity.py capture`.
2. Build the release under the host lock with the exact command below and in the source manifest.
3. Finalize the source manifest with `source_identity.py finalize --sha256 <release SHA>`.
4. Preserve the verified program with the unchanged `core-owned-writes-20261018/preserve_program.py` helper.
5. Run `run_clarity.py` under the host lock. Use the frozen v5 driver identity and the final source identity.
6. Use separate fresh homes and receipt directories for logical, protein, and scoped workflows.
7. Require the first create status to show the pending ACK conditions below.
8. Verify the source, program, and driver identities after the workflows. Preserve all refusals and failures.

The first create status precedes the first target value read. Its phase ends with `after-ack-before-first-value-read`.
It requires `accepted_sequence > completed_sequence`, `accepted_batches > 0`, and `core_warm_dirty > 0`.
The default process has no engine, memory-mode, or proof replay choice in its environment.
The existing logical workflow includes normal stops and reopens.
The V5 supplement preserves literal `ENC:`, `ENZ:`, and `ENB:` values through the default JSON codec and reopen.
The unique warmed purge requires key absence and public Atom UUID 404 after ACK and reopen.
The purge proves addressable absence with source-reviewed deletion. It does not prove reclaimed disk bytes or zero-file UUID reads.
Only V5 is a final driver identity. The unrun V1–V4 draft identity files are removed.

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
