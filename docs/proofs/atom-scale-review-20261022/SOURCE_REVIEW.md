# Independent scale and cost source review

Reviewer: Codex scale review agent. Date: 2026-10-22.
Frozen source: `43ca120feb20ae6b95ab36dd3e2a088366522369`.
Separate correction reviewed: `c1413d3d06256f3a37b84d73eed39a18a94d6136`.
Long-key quote correction reviewed: `d5985afd0e98aac1777dcb9252fdc7186c8b1ed1`.
Checked directory decode correction reviewed: `c11e6ad8c51a4147f211c380e4521c59bd7a4708`.
Soft-limit retry correction reviewed: `bb7a4a7f5219265cb660ba8084b72fdbb2833554`.

## Finding and correction

One resource defect appeared in `TierCopy::tree` and `TierCopy::allocator`.
Each touched locator object retained an open private part descriptor until the tier ended.
Concurrent full-copy catch-up can retain multiple old locator objects.
A later tier can touch those objects and exceed the service descriptor limit.
The 64-descriptor read pool did not bound these output descriptors.

Commit `c1413d3d0` closes an existing output descriptor when each tree ends.
It does not create an allocator for an old tree that the copy skipped.
`SparsePages` checks the append length when it reopens an unfinished private tail.
Completed 512-page parts already receive their barrier.
The final `finish` receives the barrier for an unfinished tail.
This correction resolves the source finding.

The frozen workload measures the preceding implementation.
Its data-selection evidence transfers to the descriptor correction.
Its exact times do not transfer: the correction adds close and reopen operations between trees.
The parent performs the final build and product check for that small correction.

## Long-key reservation finding

The original tier quote also undercounts old overflow chains in recent pages.
`TreeEdit::edit_leaf` retains existing descriptors on a value-only update.
Their overflow pages can predate the tier boundary.
`TierCopy::leaf` creates each descriptor again, including those old chains.
The quote originally counts only recent metadata bytes times sixteen.

A 65,535-byte external key requires five 16 KiB overflow pages.
A recent leaf can therefore need more than 4 MiB of replacement overflow.
Sixteen times that leaf page reserves only 256 KiB.
The 8 MiB floor covers small examples, but does not cover enough such leaves.
This can exceed the reserved replacement bytes and a configured disk cap.

The proposed correction walks recent reachable directory and locator pages.
It adds the output overflow charge from each validated descriptor length.
Directory output uses a 128-byte inline threshold; locator output uses 1,024 bytes.
An external key stores a 128-byte prefix in either layout.
The suffix receives complete 16,256-byte payload pages, each with a 16 KiB disk charge.
The quote need not read the full key, old overflow chains, or record payloads.
The parent implements this correction in `maintenance/tier_quote.rs`.
The independent review confirms the output thresholds and complete per-chain page charges.
Receipt decode enforces strict coordinate order, so the age lookup uses a valid binary search.
The page and height bounds keep the walk finite.
Each temporary read call ends after its page read.
The correction resolves the source finding, subject to its build, CI, and actual product check.

Frozen tier times exclude the extra quote metadata reads from this correction.
## Soft-limit retry finding

The large actual run increased selected parts from 1,279 to 1,540 during a successful concurrent full copy.
The post-copy ledger sets its baseline to the result.
The former repeat policy required new growth above that baseline.
At the 85-percent soft limit, refused writes cannot provide that growth.
A refused-write notification cleared `held`, but did not bypass the growth condition.
The worker could therefore leave the home write-refused indefinitely after a successful copy.

Commit `bb7a4a7f5` bypasses both growth suppression and `held` at the soft limit.
A retry there selects a full copy; a recent copy could retain the grafted base.
A successful copy that still leaves the limit active receives a 2-to-60-second exponential back-off.
Refused-write notifications cannot interrupt that rest.
The retry counter resets below the soft limit.
This correction resolves the source finding and avoids an immediate repeated full-copy loop.
An irreducible base above the limit can remain refused under the capacity policy.

The independent normal-worker operation confirms a bounded final-delta refusal, then a successful retry and later accepted writes.
It does not reach the real soft limit.
The growth result establishes the first prerequisite of the defect, not a complete threshold reproduction.

## Review decision

The source review approves `bb7a4a7f5219265cb660ba8084b72fdbb2833554`, including the three corrections above.
The three independent product captures and their exact owned cleanup completed.
The merge still requires the final corrected head's green CI and successful product check from the parent agent.
Frozen source timings remain separate from that final check.

## Cost and bounds

- `CompactionPlan::copy` performs the full quote and copy outside the write gate.
  Both full operations follow all selected native entries.
  A source hold protects their input files.
- A recent quote uses the complete receipt and recent payload bounds.
  It does not enumerate old record keys.
  The tier checks receipt ages before it reads a selected tree page.
- Catch-up uses page ages and original mutation versions.
  It can read old child roots to reject their subtrees.
  It does not enumerate the contents of those old subtrees.
  There are at most eight off-gate rounds.
  The final handoff refuses above 4,096 changed locators or new groups.
- The pack retains at most 4,096 map pages, 256 decoded blocks, 32,768 part records,
  and 64 source descriptors. Cursor tree pages receive periodic removal.
  Additional receipt and allocator state follows the bounded part and tree counts.
  This source check is not an observed process-memory bound.
- The source and latest caught root each keep a typed generation hold.
  The active-copy lease defers orphan removal until private output ends.
  The copy keeps its space ticket through publication or abandonment.
  Delta extensions reserve a conservative tree-output bound.
- The policy consolidates eight recent outputs.
  Recent payload bytes or accumulated changed entries above one quarter of the base start a full copy.
  The entry criterion also covers large deletes.
  A reopen conservatively selects a full copy.
- The automatic part policy normally starts at or below half of 65,536 parts.
  Observed growth and copy duration can select an earlier trigger.
  Reclaim failure retains the prior 75-percent policy and back-off.
  With automatic maintenance enabled, admission returns the named retryable 503 at 85 percent.
  The hard receipt limit remains independent of that soft admission check.
- Private background requests share the 500 ms collection window.
  Ordinary reads cannot select that private prefix.
  Their duty cost includes their own preparation hold and the shared sealed flush hold.
  The open collection window does not enter that cost.

The real 85-percent threshold is not reached by the scale comparison.
No source check proves recovery at that threshold, a power-loss result, or a production latency bound.
