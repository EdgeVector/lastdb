# Atom refcount grace-window garbage collection

## Contract

A zero live-reference count creates a candidate. It does not delete an atom.
The candidate uses the time of that zero transition as its epoch.

The reaper deletes an atom only when all these conditions are true:

1. The same candidate epoch is older than the grace window.
2. The live reference count is still zero.
3. No pending protein-fold marker holds the atom.
4. A reachability audit marks that exact epoch unreachable.

A positive count deletes the candidate and its audit result in the same durable
batch as the count change. A later zero transition creates a new epoch.

A body write that meets a candidate with an exact zero count record starts a
new epoch with no audit, in its own durable batch before the tip batch of the
same job. The new epoch has a fresh grace window. A replayed legacy tip does the
same before it writes the tip. A count that returns to zero while an old
candidate stays starts a new epoch, because the cancel of that candidate was
lost. The reaper deletes only when the count record is present and an exact
zero. A candidate without a count record is removed, and its body leaks.

A crash can leak an orphan atom. It must not let the reaper delete an atom that
a tip refers to. The re-arm relies on the body batch and its barrier finishing
before the tip batch of the same job starts. The hash groups inside one batch
have no durable order.

## Access shape

The `aref:gc:v1:c:` key gives an exact candidate lookup by atom UUID. The
`aref:gc:v1:q:` key orders candidates by their zero-transition time. A reaper
ranges a bounded queue page. It does not scan atom bodies.

After each candidate passes all gates, the reaper deletes its exact body,
locator, blob edges, live count, candidate rows, and schema marker. Optional
atom-plane compaction removes the dead segment bytes and returns disk space.

The daemon's periodic automatic `gc-atoms` probe supplies the audit gate. A
completed probe generation binds its keyed reachability markers to candidate
epochs that existed when that probe started. The daemon then invokes the
reaper with physical atom-plane compaction enabled.

The schema marker uses a separate namespace. The delete removes that marker
first and retains the durable candidate until the main body transaction
succeeds. A schema-index failure therefore leaves the body and candidate for a
later retry.

## Grace window

The default grace window is 24 hours. This value is far above the measured
protein-fold p99 and leaves time for delayed work after a process restart. The
audit gate remains mandatory after the window expires.

The 2026-09-28 real-data CoW run measured 100 queued sibling folds. It applied
the queue in one bounded batch. The results were:

- p50: 219.071 seconds
- p99: 286.768 seconds
- maximum: 288.003 seconds

The 24-hour default is more than 300 times the measured p99.

## Measured proof

The 2026-09-28 run above used an APFS clone of the real data. That harness and
its ignored host-lane test are removed (Tom, 2026-10-12: nothing copies the
primary home). The numbers stay as the historical measurement. A new
measurement uses a fresh empty home with synthetic protein writes.
