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

## CoW proof

The ignored host-lane test uses an isolated real-data APFS clone. It refuses
the live `~/.lastdb` path. It creates 100 protein writes, applies each queued
sibling fold, and reports p50, p99, and maximum propagation time.

```text
LASTDB_ATOM_KEY_ENCODING=partition_prefix \
LASTDB_ATOM_GC_COW_HOME=<isolated-clone> \
cargo test -p fold_db \
  host_lane_protein_fold_propagation_p99_on_real_data_clone \
  -- --ignored --nocapture
```

The focused LastStore test also proves these results:

- A candidate stays present before its grace deadline.
- A reference bounce removes the candidate and prevents deletion.
- An audit that finds a root prevents deletion.
- An unreachable audited atom loses its body after the grace window.
- Atom segment allocation decreases after compaction.
