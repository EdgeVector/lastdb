# Explicit node cleanup for V2

`lastdb db cleanup` starts one durable cleanup request for the main store and
the file-blob store. `lastdb db cleanup --status` reads its status.
`lastdb db cleanup --wait --json` waits for a completion receipt. A client
timeout does not cancel the request. The node resumes a pending request after
restart, after its normal initialization and restore checks.

The owner socket exposes the same flow at `POST /api/db/cleanup` and
`GET /api/db/cleanup`. A POST accepts an empty object or an optional UUID
`request_id`. A repeat with that id returns the same latest request. An empty
POST reuses an active request. A different id cannot replace active work.
An app socket cannot request or inspect this maintenance operation.

## Completion rule

The node first drains earlier accepted writes through the normal write gate.
It saves the two target frontiers in one bounded authenticated checkpoint.
It then calls the V2 cleanup step on both stores. Each step preserves normal
publication order, file authentication, disk barriers and old read lifetimes.
Foreground writes can continue between steps. A continuous stream of writes
can prolong cleanup; there is no fixed completion deadline.

The node records `complete: true` only when all of these facts hold together:

- Earlier accepted writes reach disk, and the final write gate excludes a new
  publication during the completion check.
- Both stores have no tiered run or base merge left.
- The readable roots adopt the completed disk roots.
- Old reads release the replaced active files.
- The active run directories contain no removable unnamed run or temporary
  file; every required unlink and directory barrier succeeds.
- The completed frontiers cover both saved target frontiers.
- The completion receipt reaches disk with its own file and directory barriers.

The receipt names `completed_through`. Later writes do not invalidate that
past receipt. A new cleanup request covers later work. A read hold reports
`waiting_for_reads`. An IO failure reports `retrying` and retries after 30
seconds. An uncertain native publication reports `recovery_required`; it
cannot report completion. A corrupt cleanup checkpoint also reports recovery
as required, rather than an empty or completed request.

## Scope

Cleanup removes old versions and tombstones that normal atom and blob rules
already delete. The atom counter remains the only authority for atom deletion.
This operation does not delete a live CAS record because it appears unused.
Recovery of a failed final CAS delete is a separate operation.

Backup hardlinks and saved remote backups retain bytes independently. Cleanup
does not release a backup or promise erasure from those copies. The status
states these exclusions. It certifies the two active stores, not all copies
of the bytes. It is an explicit node operation, not a new chat automation or
a change to the automatic merge size thresholds.
