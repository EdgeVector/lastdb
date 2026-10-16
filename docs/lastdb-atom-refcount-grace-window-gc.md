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

## Cadence, lap restart and visibility

The daemon runs one local cadence for automatic `gc-atoms`. It needs no cloud
sync engine. It does no work for `LASTDB_GC_ATOMS_START_DELAY_SECS` (600) after
boot. After that a step runs every `LASTDB_GC_ATOMS_STEP_INTERVAL_SECS` (30).
The probe, the audit and reap, and the delete pass each get a budget of
`LASTDB_GC_ATOMS_STEP_BUDGET_SECS` (5). A probe lap walks every reference plane
and then the atom plane, and persists a checkpoint after each call. A lap that
finishes starts the next lap `LASTDB_GC_ATOMS_RELAP_INTERVAL_SECS` (3600) after
it finished.

A completed lap may authorize a delete only while its view is current. Before
any reap or delete, a step restarts the lap (the generation increments) when
the lap:

- finished before this process started (`lap-finished-before-boot`);
- claims to have finished after the current time, because the clock moved back
  (`lap-completed-in-the-future`);
- finished a maximum lap age or more ago (`lap-older-than-max-age`). The maximum
  is the grace window, and never less than the relap interval; or
- finished a relap interval or more ago (`lap-older-than-relap-interval`).

Only the new lap's audit then counts. A very small home can finish the new lap
inside the restarting step. The reap in that step then uses the new lap's own
markers and audit identifier, so it is not stale. A checkpoint that cannot be
read fails the step and deletes nothing. The status shows `stale_lap_restarts`
and the last reason, and each restart writes one `info` line. Without this, a
node that was stopped for longer than the grace window could delete on an audit
that predates the stop (no writer marked a reference while the process was
down, and an offline tool may have installed rows).

The last rule is waived while the lap's own work is progressing. The audit
recording a verdict, the reaper deleting, or a delete batch scanning the atom
plane counts as progress, and the status shows it as
`last_progress_at_unix_secs`. A restart would reset the delete cursor and the
audit, so at a large scale (the plane walk needs several relap intervals) the
late keys would never be reached. The writers keep marking for the lap's
generation while it is active, and every delete still passes the grace, audit
and edge gates. The first three rules always apply, progress or not. The
maximum lap age is the hard bound: a pass that is still making progress when its
lap reaches it restarts the lap, and no delete happens until the new lap
completes and the audit runs again. Writer markers cover only `mk:` rows, so the
bound keeps a long pass from finishing on verdicts older than the grace window.
A single step runs at most about two step budgets, so a step that starts inside
the bound can end a few seconds after it.

## What may delete an atom

Two things delete an atom, and both need the grace window and the audit of the
current lap:

- The reaper takes up to `LASTDB_GC_ATOMS_REAP_CANDIDATE_CAP` (256) zero-count
  candidates per step, oldest first. A candidate needs a zero epoch older than
  the grace window and an unreachable verdict that the CURRENT lap recorded for
  that epoch. A verdict from an older lap counts as no verdict
  (`retained_stale_audit`). The audit stops at the step deadline after at least
  one verdict. While any candidate in the page still waits for an attempt, the
  step deletes nothing (`audit-incomplete`). An audit that fails keeps its
  candidate unaudited (`audit_failed`) and does not stop the rest; a candidate
  that fails to revalidate, or whose queue record does not decode, is kept and
  counted (`retained_error`). The audit deletes nothing and needs no backup cut.
  The reaper deletes bodies, so it runs under the packing lock, taken only when
  a candidate is ready (`atom_gc_reap_ready`) and reused by the delete pass; a
  held backup cut ends the step (`cut-held`). The reaper refuses to run when
  batch writes have no durable barrier (`LASTDB_TXN_DEFERRED_FLUSH=1`).
- The cap-armed pass starts only when the last completed lap measured at least
  `LASTDB_GC_ATOMS_MAX_UNREFERENCED_BYTES` (128 MiB). The cap decides when a
  pass starts. It walks the atom plane once per lap and deletes an orphan only
  if the same grace and current-lap audit gates clear it. An atom with no
  zero-count row has no zero epoch, so this pass keeps it (`no_zero_epoch`). It
  also removes the candidate row and the live-count row of the atoms it deletes.
  A batch holds the write locks of its candidates together: the body lock and
  the count lock of each, taken in stripe order (256 stripes of each). The
  default batch is `LASTDB_GC_ATOMS_DELETE_CANDIDATE_CAP` (64), and a larger
  value is clamped to 256 (`AUTOMATIC_GC_ATOMS_DELETE_BATCH_MAX`). At 64
  candidates about 57 of 256 stripes are held, so about 22 percent of writes can
  wait for one batch. The hold time of the slowest batch is
  `slowest_delete_batch` in the status. The reaper locks one atom at a time.

The reaper no longer compacts the `atoms` collection after a step. The plane
compactor returns the bytes on its overhang trigger.

### Conflict writes stay (Tom, 2026-10-13)

When two devices write the same key, LastDB keeps each conflicting write, and
no cleanup may remove those atoms. What protects them today:

- The replay conflict recorder (`store_merge_conflicts`, legacy physical `mk:`
  replay) writes a `history:` row and a `conflict:` row for the loser. Its
  reverse edges now go through the compact v2 path, so `has_active_atom_refs`
  sees them. Older conflicts hold v1 edges only. Both deleters now also keep any
  atom with an active v1 edge (`has_active_legacy_atom_ref_edge`), in addition
  to the lap markers that the History and Conflicts phases write.
- `clear-history` never selects a row marked as a conflict or a kept Put, for
  any `--keep-last` value (`conflict_rows_kept` in its report).
- Replay drops node-local `gc-atoms` scratch keys, so a peer cannot plant a
  foreign checkpoint or marker set.
- The manual orphan collectors already treat `conflict_loser_atom` as a root.

What does not protect them: the logical replay path (`MutationIntent`, the path
modern peers use) records no conflict on either side. A replayed write that
loses to a different device's tip is dropped at the tip decision, and a write
that displaces a local atom archives nothing (thin tips keep no `tv:` row). Both
atoms are then ordinary zero-count candidates. The peer-apply interlock below
keeps every deleter off such a node until replay records those writes.

### Peer-apply interlock

Unreferenced atoms are deleted only on a node that has NEVER applied a mutation
a peer wrote. One durable row, `peer-apply:v1`, holds the answer:

| State | Row | Meaning |
|---|---|---|
| `never` | `never` | Written once, by the boot that creates a brand-new home. |
| `seen` | `seen` | Written and flushed BEFORE the first peer data is applied. |
| `unknown` | absent | Every home an older build created, and any home whose row a copy dropped. Unknown counts as seen. |

`never -> seen` is the only transition. No durable proof of "never" exists in
an older home, so none is used: a cloud config, a sync cursor, the pin log and
`history:` or `conflict:` rows can each be absent after a trim, a reset or a
restore, and a daemon's peer counters read zero after every start. An operator
who knows the home is single-device uses the override below.

The row is node-local. It is on the capture skip list, replay drops it, and a
snapshot neither carries nor restores it, so no other node can set or clear it.
A snapshot also neither carries nor restores the `gcatoms-*` probe checkpoints
and markers, which describe the lap of the node that wrote them.
It lives in the default plane for unrouted main keys (`tips`). Backup carries
that plane, compaction keeps live rows, and no reclaim verb selects an unknown
prefix.

**Where it is written** (`mark_peer_apply_seen`, unless noted):

1. `SyncEngine::replay_entry_with_schema_refresh` (`replay/fence.rs`), just
   before `replay_entry_inner`. Every replayed entry reaches it: the cloud
   download replay, the bootstrap tail, mutation-log segment apply (the pin-log
   replay and `run_mutation_log_peer_apply_cycle`), `MutationIntent`, a logical
   commit and the legacy physical `mk:` Put. An entry that cannot change state
   (`PhysicalDigest`, an unknown tag) and an entry the retired-intent preserver
   takes do not set it.
2. `SyncEngine::restore_bootstrap_snapshot` (`bootstrap/snapshot.rs`), after the
   install. The install clears this node's row and writes none, so the row is
   rewritten (`rewrite_peer_apply_seen`), ignoring the in-process flags.
3. `.peer_derived`, the marker file that `Host::boot` reads and answers with
   `seen` (`record_boot_peer_state`). The marker is written, durably, before any
   data of another home lands in the home. Two writers exist:
   - `lastdb restore` (`prepare_destination`), before it installs anything. This
     covers a chunk-backup restore, the cloud-generation restore and their tail
     replay, which runs in the restore tool with no serving store.
   - The cloud-purge adoption switch (`cloud_purge_adoption/switch.rs`), just
     before `switch_data` installs the staged candidate.
4. A share seed import (`cloud_share_refresh/import.rs`), before it writes.
5. `Host::boot`, for a brand-new home only: `never`
   (`stamp_peer_apply_never_on_new_home`, the same boot and the same emptiness
   proof as the fresh-home stamp).

A physical restore does not trust the row it installs. A chunk install copies the
source's `tips` plane, so the adopter inherits the source's `never`, and that
says nothing about the adopter's lineage. The marker makes `seen` win over the
inherited row. The one case with no marker is the selected writer in a cloud
purge: it keeps its own data (`selected()` installs nothing), so its own row
stays true.

**Ordering.** All writers of `seen` share one rule, in `AtomStore`:

- "Present" and "durable" are separate. `mark_peer_apply_seen` reads the row,
  writes it when it is not `seen`, and ALWAYS flushes before it reports success.
  Only a flush that returned `Ok` sets the durable flag that makes later calls
  free. After a failed flush a retry flushes again, so replay cannot apply peer
  data while the row is unflushed.
- One lock covers the `seen` write and the `never` stamp (probe and put). Sync
  can start before the stamp runs. If a replay marked `seen` first, the stamp
  finds the row (or the flag) and writes nothing; the stamp never overwrites
  `seen`, and no peer data applies while the stamp holds the lock.

**Where it is read:**

1. The automatic cadence, every step (`peer_apply_gate`): it publishes the state
   in the status and, when the state is `seen` or `unknown`, ends the step after
   the probe lap with `skipped: peer-applied` or `skipped: peer-state-unknown`.
   Probe laps and the unreferenced figure keep running; nothing audits, reaps or
   takes a lock.
2. Backstop in the stores (`require_no_peer_apply_for_atom_delete`): a
   non-dry-run `reap_audited_atom_gc_candidates`, `delete_orphan_atoms_from_probe`
   and the manual walker `gc_orphan_atoms_with_roots_for_schema_at_cut` refuse.
   The walker asks again for every page, just before it deletes that page's
   queue: it can run for a long time, and a peer mutation can be applied during
   it. When the answer changed it drops the queue and stops with the same
   refusal. The opt-in reclaim janitor, which calls the walker, is covered by it.
3. `lastdb status` and `GET /api/status` (`status.gc_atoms.peer_apply_state`,
   `allow_after_peer_apply`, `peer_interlock_skipped_steps`).

**Override.** `LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY=1` (also `true`, `yes`,
`on`) for the daemon, off by default. With it set, automatic delete runs after a
peer apply, so the atoms of a LOST multi-device write can be deleted. The status
shows `ALLOW_AFTER_PEER_APPLY=ON` and a warning is logged once. The manual verb
needs its own explicit option (below).

**Other deleters of unreferenced atoms:**

- `lastdb db gc-atoms --execute` (and the schema-scoped form) honors the
  interlock unless the operator passes `--allow-after-peer-apply`. A dry run is
  always allowed. It is the same unreferenced-atom delete as the automatic one,
  so leaving it open would be a quiet path around the interlock. The refusal is
  HTTP 409 with the reason.
- The override travels in the request body of `POST /api/db/gc-atoms`. That route
  is `Scope::Owner` in the route table, so the app socket does not serve it, and
  the handler refuses a caller that is not the owner principal (HTTP 403).
- The opt-in reclaim janitor (`LASTDB_ATOM_RECLAIM_MS`) honors it through the
  walker; only the daemon-wide env lifts it.
- Not unreferenced-atom deleters, so not gated: purge and hard erasure, the TTL
  sweep (both delete the atoms of named keys, an explicit user action),
  `clear-history` (deletes history rows, never atoms), `reap-dropped-schema`,
  the offline `lastdb_local_maintain atom-gc` reap (removes redundant storage
  copies of a body that survives), and `gc-file-blobs` (blob rows).

The TTL sweep purges by default, so it frees atom bytes at once, also on a node that has applied
a peer mutation. With `LASTDB_TTL_SWEEP_VERB=delete` it leaves the atoms of the expired keys to this
garbage collection and frees no atom bytes there until the override above is set. See
`docs/lastdb-ttl-sweep.md`.

**Later (not built).** O1: record a history entry on replay when a tip is
replaced or a peer atom is dropped, with a retention window, then lift the
interlock for nodes that record it. O2: carry the replaced tip (causal context)
in the mutation envelope so only truly concurrent writes are kept.

Cost of the grace window: garbage waits 24 hours, then the next lap, so the
held amount is about one day of churn plus one lap interval and one lap
duration. At 100 MB of churn per day that is about 100 MB held at all times,
and about 105 to 125 MB for a lap interval of 1 hour and a lap duration of 1 to
5 hours. The default cap (128 MiB, about 134 MB) sits just above that held
amount, so at that churn the cap arms the pass only when more than about 10 MB
is also overdue. The reaper does not wait for the cap.

The delete options need a grace gate to be built: `AutomaticGcAtomsDeleteOptions`
has no `Default`, and its constructors are `gated(...)` and
`ungated_emergency(...)`. The second one exists only for the
`LASTDB_GC_ATOMS_BYPASS_GRACE=1` switch, so a call site cannot drop the gate by
accident.

Operator switches (all off by default):

- `LASTDB_GC_ATOMS_GRACE_WINDOW_SECS` longer than 86400 is always honored. A
  shorter value is ignored unless `LASTDB_GC_ATOMS_ALLOW_SHORT_GRACE=1` is also
  set. That pair exists so a synthetic home can age candidates in seconds.
- `LASTDB_GC_ATOMS_BYPASS_GRACE=1` is the emergency switch for a full disk. The
  cap-armed pass then deletes orphans with no grace window and no audit, as it
  did before. The status line shows `BYPASS_GRACE=ON`.

## Conditions that had stopped deletes without a message

Four conditions had stopped deletes:

- A finished lap pushed its own restart time one interval ahead of `now` on
  every step, so the restart never arrived. The first lap a home completed was
  the last. The restart time is now set once.
- The delete pass keeps every candidate while the compact reverse-edge plane is
  not proven complete (`aref:v2:m:complete` and `aref:v2:m:history-complete`).
  The backfill that wrote them was removed, so a home created by that build
  never held them. The first boot that mints an identity over an empty store
  now stamps both markers with the value `fresh-home` (nothing else stamps).
  The stamp is dropped, and flushed, before a personal photograph restore
  installs rows and again after it. Any error in the proof of emptiness means
  "not fresh", and a failed stamp never fails boot. A home that holds data and
  lacks the markers stays blocked, and the status says so.
- Every step over a finished lap audited every zero-count candidate again. Each
  audit writes two durable rows, so 400 candidates used more than the whole
  step budget and the delete pass behind them never started. A candidate that
  the current lap already judged is skipped.
- A probe scan call visited one physical handle and persisted a checkpoint. The
  marker clear phases, the reference planes and the atom plane now scan up to
  8 handles per call. The row limit still bounds a call over dense groups. The
  prologue and the delete scan stop in the middle of a page and resume from
  one handle, so they keep one.

Reverse edges that protect an atom the lap markers do not: `tv:` rows carry a
`TipVersion` edge, `history:` rows a `MutationHistory` edge, and a `conflict:`
row is written in the same batch as a history event whose edge names the loser.
The writer-side lap marker covers `mk:` rows and pin-log roots. The final check
under the per-atom locks reads those edges. A row installed without edges is
the one thing the check cannot see, which is why a restore drops the
`fresh-home` stamp first.

The probe keeps its checkpoints (`gcatoms-meta:`) and one reference marker per
referenced atom (`gcatoms-probe-ref`, `gcatoms-probe-tv-skip:`) in the main
store. These rows are node-local scratch. They are on the capture skip list
(`CAPTURE_SKIP_MAIN_KEY_PREFIXES`), so they never enter the mutation log or the
sync pin log. Before this, each lap would have captured two records for every
referenced atom, plus one per checkpoint write, and a node with a cloud engine
would have uploaded them.

`lastdb status` and `GET /api/status` (`status.gc_atoms`) show the cadence:

- `loop`: `running`, `starting`, `disabled`, `not-started` or `STALE`;
  `in_step` names a step that is still running. `grace` is the window in force.
- `unreferenced`: the bytes and atoms the last completed lap found, with the
  lap number, its age and its duration. The delete pass arms at `cap`.
- `probe lap`: the lap in progress, its phase and its page count.
- `outcome` and `skip_reason`: `below-threshold`, `cut-held`,
  `packing-lock-busy`, `atom-ref-reads-not-ready`, `audit-incomplete`,
  `start-delay`, `no-eligible-candidates`, `delete-pass-running`,
  `probe-running`, `disabled`, `failed` (with `last_error` and its age).
- `last_delete_pass`: how many orphan candidates the last delete step rechecked
  and why it kept the rest (`marker`, `body_changed`, `no_zero_epoch`,
  `within_grace`, `audit_not_clear`, `reads_not_ready`, `active_edge`,
  `pending_hold`, `retired_intent`, `unreadable`).
- `stale_lap_restarts` and the reason of the last one, when any happened.
- `slowest_delete_batch`: the longest time one delete batch held its locks.
- `last_delete`, `total_deleted` and the last reap counts
  (`retained_before_grace`, `retained_without_audit`, `retained_stale_audit`,
  `retained_audit_reachable`), `audit_deferred` and `audit_cut_short_steps`.
- `sync_engine`: `attached` or `absent`. The cadence runs in both cases.

Each completed lap writes one `info` line under target `fold_db::gc_atoms`
(`automatic gc-atoms lap complete`). A held backup cut writes one line when it
begins. The packing lock wait is bounded to 60 seconds, so a long holder shows
as `packing-lock-busy` and does not stop the cadence.
