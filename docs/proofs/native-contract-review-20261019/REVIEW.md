# Native memory and disk contract review

Repeated writes in one batch could use an arbitrary order. A later update could lose its key field when the atom value stayed equal.
This change uses one write order and carries the complete current tip through reads, writes, and deletion.
It also removes old read exceptions and an incorrect diagnostic path.

The reviewed source passes the source, build, and ordinary data checks.
The required GitHub checks control the merge.

## Changes

- Conditions and deletion now use the declared key layout. Another mapped layout cannot supply a false record match.
- Local writes in one batch keep caller order and one common batch revision. Equal atom identities can still receive newer tip clocks.
- The winning tip owns its complete optional metadata. A cleared value cannot inherit metadata from an older atom body.
- Single and batch write requests preserve the declared source file name.
- Each accepted tip replacement retires its previous reference. Equal atoms do not create conflict history.
- Numeric conditions and result order preserve large integer differences. Empty condition lists behave as omitted lists.
- A failed optional key preload returns to bounded disk reads. It does not leave a permanent query error.
- A complete metadata read refuses an oversized combined result. It cannot silently return an incomplete result.
- Writes refuse the reserved tombstone value. Ordinary field validation has no tombstone exception.
- The old zero-clock override and the unused physical molecule version response field are removed.
- The atom liveness diagnostic reads exact native reference counts. Missing counts remain unknown; retired edge records cannot prove absence.

## Intentional boundaries

The core model still supplies logical values. The private disk queue supplies prepared disk output and completion work.
Native file packing remains independent of atom history.
The selected key allowance remains separate from the warm value allowance.

The atom liveness diagnostic accepts the default owner scope.
It refuses named database handles because a target identity alone cannot resolve a schema alias.
The CLI reports an unknown count when no exact count is available.

Aggregate writes retain their explicit rule: a new contribution over unchanged source values receives a refusal.
That rule is separate from normal tip clock advancement.

## Evidence

The final source inventory contains 1,892 inputs.
The release uses normal default features and fresh synthetic homes.
The source commit is `6aa67e6ceb30a4e854eca3779601d12d828d78f8`.
The release SHA-256 is `3f1e0fdac6b55c776ad02a19f562cc5b60b30b936cba4d5712459171e2e0f9c2`.

The complete workspace Clippy checks pass with warnings refused. Format and changed-code size checks pass.
Encoded keys, plain keys, and a small key allowance return the same 282 query responses across 102 ordinary HTTP calls.
Each run returns complete records after reopen. Each reference count follows `1, 1, 1, 0, 0`.
The small allowance selects disk reads for two key domains. It retains 2,134 key bytes within its 65,536-byte allowance.

The existing core operations return 3,363 complete records across 74 reads and 386 query responses.
All 36 concurrent responses contain one complete permitted state. All 29 required warm reads make no file calls.
All nine accepted node sessions stop normally.
All nine temporary homes from this round are reclaimed through the supported exact-path command. Their preserved receipts remain unchanged.
The [focused review](final-v3/independent-focused-all-review.json) and [core review](final-v3/independent-core-cohort-review.json) record the independent checks.

The original request data and failed candidates remain separate from the accepted result.
The first requests contain format errors. The first complete candidate loses `c1.id` after a delete and update.
The next candidate fixes those values, but its old diagnostic cannot prove reference counts.

The existing operational helpers remain unchanged. This round adds request data, not an executable check suite.
No tests run. No primary home or archive supplies data.
This round makes no latency or throughput claim.
The optional preload I/O failure and the oversized metadata union receive source review only.
The small allowance run exercises the supported disk fallback, not an injected I/O failure.

The [final result](final-result.json) records the accepted source, release identity, ordinary replies, and exact cleanup receipts.
