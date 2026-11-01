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

The receipt names `completed_through`, with both frontiers and file generations.
An older restored generation cannot reuse a newer completion receipt.
Later writes do not invalidate that
past receipt. A new cleanup request covers later work. A read hold reports
`waiting_for_reads`. An IO failure reports `retrying` and retries after 30
seconds. An uncertain native publication reports `recovery_required`; it
cannot report completion. A corrupt cleanup checkpoint also reports recovery
as required, rather than an empty or completed request.

## Scope

Cleanup removes old versions and tombstones that normal record and blob rules
already delete. The record counter remains the only authority for record deletion.
This operation does not delete a live CAS record because it appears unused.
Recovery of a failed final CAS delete is a separate operation.

Backup hardlinks and saved remote backups retain bytes independently. Cleanup
does not release a backup or promise erasure from those copies. The status
states these exclusions. It certifies the two active stores, not all copies
of the bytes. It is an explicit node operation, not a new chat automation or
a change to the automatic merge size thresholds.

## Nightly CI

The Disk v2 node proof workflow runs each day at 09:27 UTC on Linux and macOS,
with the nightly CI schedule. It creates temporary homes with generated records
and requests cleanup through the owner API. It checks completion, repeated
requests, and the saved record after restart. The runner removes each temporary
home after the operation. Nightly and manual runs do not cancel one another.

The Disk run proof workflow also runs each day at 09:27 UTC on Linux and
macOS. Its existing node store operation fills both stores with 512 records
each, deletes 256, and checks the remaining values and absent keys. It also
checks old reads, later writes, cleanup resume, and the saved completion
record after restart. Its existing run home operation checks physical file
removal.

These checks cover native CAS cleanup after logical deletion. Blob upload
and dwell decisions remain separate. Nightly and manual runs use separate
run IDs. PR updates cancel only older runs for the same PR.

## Interrupted CAS deletion

A compound native batch writes CAS additions, main records, then CAS deletions.
Before either store changes, an authenticated local intent saves the deletion keys and both exact frontiers.
Admission reserves its bounded disk space before the request enters the accepted queue.
The intent contains no file body or encryption key.

A failure after main publication stops the native runtime and reports an uncertain commit.
The caller must not treat that error as a rejected mutation with no disk effect.
The next normal open resolves the intent before any read, write, or merge worker starts.
If main publication did not occur, it keeps the blobs.
If main publication completed, it finishes the CAS deletion or recognizes its already-complete frontier.
An unexpected home identity or frontier stops recovery without a guessed deletion.

A stopped-home backup refuses an unresolved intent and changes neither store.
A live capture and the cleanup completion check also refuse an unresolved intent.
A recovered native frontier outside the complete mutation log requires a new snapshot through the existing gap rule.
This recovery does not remove old cloud backups or repair unrelated historical CAS orphans.

The native status includes the recovery result for the current boot.
It reports the observed outcome, intent key count, and exact main and CAS frontiers.
It includes no blob keys or content.
An ordinary second boot reports no repeated intent recovery.

Tom approved a CI-only interruption operation on October 28.
The existing Linux and macOS node workflow runs it on fresh generated homes.
It uploads 257 distinct files and writes 258 records, then purges 256 records.
Two live records share the remaining file and field value.
The normal 120-second blob dwell remains in force.
An external observer stops only its owned process after it observes real intent and main publication files.
It never writes native control state or substitutes a commit result.
The authenticated production recovery report must confirm the actual interrupted cut.
Modification times alone do not prove that cut because a merge can also replace CURRENT.
The operation permits four bounded attempts and fails if none captures a main-published, CAS-pending cut.
Normal reopen must complete the pending deletion, keep every live value and file, and remove all 256 deleted files.
Exact counter verification and native cleanup must complete.
A second boot must preserve the result and perform no repeated intent recovery.
This operation does not claim proof of every possible interruption cut or malformed-intent refusal.
