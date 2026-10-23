# Active work checkpoint

The current source is commit `6ed71c9ddb3d4dac203151ebf5009a5ff04d76cb`.
Branch: `durable-write-concurrency-20261022`.
PR: https://github.com/EdgeVector/lastdb/pull/282 (draft, never merged).
The source is based on main `bcdcbba38`.

The original durable-write change already merged as PR #278.
The user approved the next steps: concurrent latency, sustained compaction, and removal of redundant unflushed memory guards.
Keep disk before ack. Add no journal or collection timer. Use no tests or fixtures.
Use no brain, situations, kanban, primary data, primary archive, or primary restart/upgrade.
Use no subagents.

All owned data resides in this exact scratch root:
`/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/wq.SL5jRo` (uid 501).
The old `wq.zwzf9Y` disappeared before the new captures. It held only a pinned binary.
Do not use or search that old path.

The current running tool session is exec session `82177`.
It runs the host-lock wrapper around `finish.py --mode metadata`.
It is still queued behind other host work. The current holder is the unrelated
compaction benchmark `rebuild.py` under worktree `lastdb-direct-compaction-benchmark-20261023`.
Do not interfere with it. Other waiters also precede this session.
The queued finish script builds the optimized candidate, pins it, then runs the captures.
It holds the host lock through that whole campaign.
The completed output marker is `metadata-completed.json`.
No `metadata-*` capture exists yet at this checkpoint.

Current source changes:
- Eager FIFO group commit, no timer. Four already-waiting callers can share one device flush.
- Separate logical preparation and physical publication locks.
- Mutation quota binds to the logical frontier and required format features.
  Equivalent physical compaction roots retain owned quota; logical writes invalidate it.
  Compaction checks its replacement dependency count plus the queued quote.
- Native change-feed tips use existing private metadata publication and the durable memory tip.
  This avoids both a tip mutex across the flush and an extra disk tip read.
- Clean cache invalidation and publication follow request order under one view lock.
  Later disk-only fallbacks cannot leave an earlier cached value.
- Dirty path marks, early memory publication, completion traversal, and dead coverage helpers are removed.
  Active read and write holds remain. Dirty status fields stay zero.
- Compaction generation preparation reuses verified immutable facts from the protected cut
  and one prior private output receipt. Conflicting facts fail closed. No format changes.
- Off-gate copy, catch-up, and receipt preparation now use existing complete durability scopes.
  Apple file barriers share a final device drain instead of a full drain for every file.
  The node uses catch_up_and_prepare to cover that work with one off-gate drain.
- Failed private jobs cannot publish after a failed durability scope. Check wrappers and
  failed flag carefully; private prepare does not clear it before the final drain.

Source review and format/size/allow/boundary/redaction checks pass.
Required CI passes on the current source:
https://github.com/EdgeVector/lastdb/actions/runs/39993980713
The latest optimized build and operational checks remain pending behind the host lock.
Earlier required CI also passed on every completed revision.

Completed captures in this directory:
1. Initial source `3be4c9ce7`:
   main single avg17.835 P9952.629; candidate avg20.916 P9958.662.
   main four-client eight-record batches avg112.072 P99343.676;
   candidate avg80.085 P99191.425. 1000 calls each, disjoint schema sets.
   Its sustained run made no compaction progress until writes stopped.
   Disk inventory peaked at 5.984 GB.
2. Frontier source `327c26ebb` (binary sha in final-identity.json):
   repeated main single avg27.651 P9972.567;
   candidate avg30.061 P9953.202.
   candidate concurrent avg90.387 P99146.638;
   repeated main concurrent avg161.641 P99498.820.
   Profile singles: main29.543, candidate28.372; logical prep both about3.95ms;
   device flush both about7.8ms; physical commit about20-21ms.
   Host load increased; no under-20 claim is justified.
   Its sustained run also failed compaction during writes.
   Both revisions recovered all acknowledged values and notices after forced stops.
3. Memory pressure passed twice under 256 KiB warm cap, with held reads protected.
   Initial result: max768 held paths,17394 purges,254736 final warm bytes, zero dirty paths.
   Same-key CAS: exactly one durable winner and three409 responses per round,24 rounds.
   Large1025-mutation notification chunk, Delete/Purge and recovery passed for source327.
4. final-capped used an active1GiB policy cap. 38 calls succeeded, four returned507.
   The helper treated this expected refusal as a campaign failure and stopped normally.
   No recovery check exists yet for this cap case.
   verify_refused() at the end of the pending campaign must check all304 acknowledged
   mutation notices and values, absence of failed new keys, and actual bytes <=cap.
   The507 message incorrectly says the host filesystem is full; actual host free space
   was241GiB. This is a policy refusal, not a full host disk.

The pending metadata campaign uses a fresh executable and fresh generated homes:
metadata-smoke, metadata-sustained4096 calls, metadata-single1000 profiled,
metadata-before-single1000 profiled, metadata-concurrent1000 profiled,
metadata-before-concurrent1000 profiled, metadata-semantics, metadata-boundaries,
and capped-recovery on the existing owned final-capped home.
The semantics helper uses fresh home hmg; boundaries use hc.
All run() homes use the next h-number automatically.

Next work:
- Wait for exec82177, with commentary updates. Never bypass or alter the host lock.
- Inspect metadata-sustained compaction receipts for successes DURING measured writes.
  Compare disk inventory and source revision outcomes. Do not claim indefinite bounded
  total size: retained live mutations/notices and production24h grace still consume disk.
- Analyze metadata profiles with analyze.py. It supports writes.json or writes.json.gz
  and counts each shared physical commit once via first_sequence..sequence.
- If failures remain, fix them within constraints; keep PR draft. No merge permission for
  this new follow-up is assumed from the earlier merge of #278.
- Write RESULTS.md with the full before/after table and limitations, update PR body,
  and preserve receipts. A process kill does not simulate physical power loss.
- Compress large receipts after all helpers finish. compressed.json records raw sha256.
  It is reasonable to gzip repeated JSON over64KiB to reduce the PR review noise.
- Clean all owned scratch homes/binaries/logs only after receipt review. Check exact
  root, realpath, uid, and absence of active owned daemons. Preserve cleanup receipts.
  Use exact supported cleanup or rm -r -- EXACT, never rm -f/-rf or a guard bypass.
- Remove this checkpoint file when the work finishes.
- git add -A for this isolated worktree, run required non-test lints, venue check,
  require clean status before push, update/attach PR (already attached), confirm CI.

Resource coordination update: the lock owner belongs to the task titled "Complete atom lifecycle work"
(id01a16509-b24e-7691-9ade-82fb7596d83a, local). It runs six100k-record captures under one lock.
Read-only app thread inspection established ownership. Do not message it without user authorization.
An asynchronous user request is pending: permission to request a safe handoff between captures.
No answer is present. Elapsed time does not authorize a message.
Other task "Review LastDB disk storage" also waits; it does not own the lock.
Use app thread tools only for this coordination. No subagents were spawned.

Current code additionally coalesces background copy/catch-up/receipt file barriers within
existing complete-native durability scopes. It leaves copy and metadata work off the write gate.
Every scope keeps per-file sync and the final full-device drain. Failed jobs cannot publish.
Required CI passed on current6ed71c9dd source. No optimized executable exists yet for it.
The pending finish script now checks unique storage implementation marker strings in the
compiled executable, plus its hash, to reject a stale storage library.
