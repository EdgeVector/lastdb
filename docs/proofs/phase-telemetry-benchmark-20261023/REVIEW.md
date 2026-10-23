# Phase telemetry declaration review

Baseline: `46e30246f946ac6ca25c8078878e82c01313ac8c` (main after PR #279).
Implementation: `fffb4f2cd5273adcf417e7ec40a85f2dee04d6c9`.

## Change and value

The telemetry model independently listed 49 fields in its struct, display-name
array, struct-to-array conversion, and indexed array-to-struct conversion. A local
small declarative macro now generates those pieces from one ordered declaration.
Each entry retains the field's documentation and pairs its Rust name with its
stable display name. The phase count follows from the declaration.

One Rust file changes: 449 lines deleted, 270 added (net 179 removed). Most added
lines are retained documentation indented inside the declaration. The file shrinks
from 611 to 432 lines. The existing oversized file shrinks, satisfying the source
size rule without an override. No new dependencies, exported macros, unsafe code,
casts, allocations, or runtime dispatch are introduced.

The benefit is maintenance clarity: adding a phase no longer requires updating
four parallel lists and a manually numbered slot. This is a different subsystem
from the prior storage-routing work. No speedup is expected from this refactor.

## Equivalence and risk

`schema-review.json` records a source inspection against the baseline. All 49
field names/types, display names, declaration order, old array slots, and field
documentation match. Generated fields retain exactly the same serde defaults and
zero omission, visibility, and derives. The public array shapes and constant names
stay the same. `from_array` destructures its fixed-length input in the same order
instead of manually indexing it. Rust checks the array length at compile time.

Every handwritten method after the conversions is textually unchanged: empty
checks, saturating sums/subtraction, parent-phase exclusion, socket queue-wait
exclusion, name/value pairing, and rendering. `PhaseTimings` remains fixed-size
and Copy. The request accumulator, dispatch-to-telemetry mapping, durable rollup
read/write fields, and counters are unchanged. Storage, locking, authorization,
wire envelopes, and durability behavior are unchanged.

The tradeoff is macro indirection when navigating the struct's generated methods.
The macro stays private, has one invocation and no conditional branches, and uses
ordinary Rust fields and array expressions. Its declaration makes the common
u64/serde policy uniform; a future exceptional field would require reconsidering
that policy. No blocking correctness issue was found in this self-review.

## Product evidence scope

The driver reuses the committed same-fixture benchmark, capture, and ownership
cleanup helpers from earlier rounds. Baseline/candidate builds use the same
worktree, target directory, compiler, flags, and Cargo lockfile, sequentially.
Heavy Cargo work and the entire measurement window use the host performance lock.

Each configuration starts from one baseline-created synthetic fixture, copied and
content-digested before each measured run. Client and daemon use distinct fixed
CPUs; all four configured daemon workers share one server CPU. The workload
covers fully warm Hash/HashRange points, bounded range/prefix reads, requested
queued batch writes, durable single/batch writes, and forensic status responses.
It validates returned values/count/order, acknowledgements, flush, and reopen.
Telemetry checks require positive known phase names with zeros omitted, matching
mutation samples and cumulative aggregate phase sums. The sampler runs every one
second for both versions; captures respect the status endpoint's cached nature.

This is one host/client and a small warm dataset. OS pages and neighboring host
load are uncontrolled; single-server-CPU timings do not establish production
concurrency/capacity or statistical significance. Timing values naturally differ
between versions, so telemetry validation compares structure and accumulation
invariants, not equal wall-clock measurements. All 49 mappings are inspected in
source; not every recording site is exercised, and this is not power-loss proof.
No test suite is added or run, following repository instructions.

See RESULTS.md for completed build/lint checks, measurements, and merge assessment.

## Final assessment

Both optimized builds and full workspace non-ONNX clippy passed. Source schema,
format/size/allow checks passed. Primary: 24 measured runs, 211,200 timed requests.
An unfavorable short plain durable-write median (+21.0%) triggered another 16 runs
and 6,144 requests with longer write workloads and warmed affected keys. Follow-up
plain single writes measured -2.9% (all four pair p50s lower); preloaded durable
batches measured -3.2% with mixed pair signs. Forensic status was within 0.1%.
All value, telemetry, durability, flush/reopen, and ownership cleanup checks passed.

The follow-up changes warmup/duration and does not prove the earlier short
post-reopen slowdown cannot recur. The source/API equivalence, lack of a repeated
sustained-write penalty, and larger maintenance benefit support merging. Keep as a
clarity refactor, not a performance optimization. Remaining risk: private-macro
navigation and performance under cold/concurrent conditions beyond this evidence.
See RESULTS.md and FOCUSED_RESULTS.md for all results and limitations.
