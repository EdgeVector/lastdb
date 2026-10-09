# Delete returns the bytes: dead-record residue and the compaction trigger

North Star: `north-star-lastdb-delete-returns-the-bytes`.

## The chain

1. Live `Delete` stamps a resident tombstone and ACKs. Reads hide the key.
2. The persist lane converges the slot: LastStore deletes the `mk:` tip and its
   `tv:` chain. A LastStore delete is an **append** of a delete marker.
3. `gc-atoms` frees the atom bodies no tip names. Also appends.
4. Compaction rewrites the plane. This is the only step that returns bytes.

Steps 1 to 3 make the plane larger. Step 4 must run, or a delete returns nothing.

## The signal step 4 needs

Before this change every automatic compaction trigger measured filesystem
block slack: `st_blocks * 512 - st_size` per segment file. A delete does not
change that number. A plane could hold gigabytes of deleted rows and read as
0% reclaimable, so unattended compaction never fired for deleted content.

LastStore now counts **record-byte residue** per hash group:

| Counter | Meaning |
|---|---|
| `live_bytes` | record bytes the live index addresses |
| `dead_bytes` | superseded puts, the puts of deleted ids, and delete markers |

Both are plaintext record lengths, so the ratio holds under at-rest sealing
and compression. The counters are exact for a resident group (maintained on
every index change and every delete-marker append), rebuilt exactly by the
segment scan when a group loads, and persisted in the group's id sidecar
(`keys-v1.idx`, framing `LSKI3`) on eviction and at shutdown. A v2 sidecar
(`LSKI2`) still answers ids; it is rewritten with residue on the next persist.

`LastStore::collection_residue` sums the plane without loading a cold group:
resident groups from memory, cold groups from their sidecar, and everything
else under `unknown_bytes`, which is never counted as dead.

## The trigger

`CollectionDiskUsage` carries both measurements. The expected reclaim is

```
reclaimable = slack + (apparent - unknown) * dead / (live + dead)
```

and every automatic trigger (`tips`, `atoms`, order log, residual planes,
locators, the photograph-aligned pass) fires on that number against the
existing ratio and floor knobs. `lastdb status` shows `dead=` and `reclaim=`
per plane under `automatic_compactions`; `lastdb db compact --collection X`
(dry run) prints a `residue:` line and projects `bytes_after` from it.

## Proof

- `vendor/laststore/tests/residue.rs`: counters through put, overwrite,
  delete, compact; cold reload equals incremental; sidecar answers without a
  load; encrypted groups.
- `fold_db/crates/core/tests/delete_returns_bytes_test.rs`: create N rows,
  delete them, run `gc-atoms`, assert both planes report dead bytes with zero
  filesystem slack, compact both, assert on-disk bytes fall and residue reads
  zero, and the atoms rewrite left its retirement record for the next manifest
  cut.
- Measured on a copy-on-write clone of the primary (2026-09-07, build
  `b112ee096-dirty`): `atoms` 3,273,093,491 B -> 2,996,045,022 B after one
  rewrite (8.5% dead); a second probe measured every group with
  `residue_unknown_bytes = 0`.

## File blobs (`cas_blobs`)

`gc-file-blobs` removes a blob row with `LastStore::delete`, which is an append,
so the local file-blob plane kept every byte it ever stored. `cas_blobs` joined
`COMPACT_ALLOWLIST` on 2026-10-08, so `lastdb db compact --collection cas_blobs
--execute` returns them. A bare `compact` and `compact --all` do not walk it
(`COMPACT_NAMED_ONLY`); the operator names it, and the `--all` answer lists it
under `named_only_not_walked`.

A blob row is the only copy of its bytes until a cloud copy exists, so the
rewrite touches as little of the plane as it can.

| Question | Answer |
|---|---|
| Cloud isolation | Required. The plane is captured (`PhysicalDigest` put, `Delete` op) and backed up as `Mutable` chunks, and no regression proves its rewrite capture-neutral. `--execute` pauses Cloud Sync if it was on. With a sync engine it holds the photograph packing lock; with no engine there is no backup cut to exclude, so it runs without the lock. |
| Which groups | Only groups that hold dead bytes (`LastStore::compact_collection_dead_groups`). The verb loads each group once, reads its exact residue, and leaves a group with no dead bytes byte-identical. Other planes still rewrite every group. |
| Disk order | Write and sync `<seq>.seg.tmp`, rename it, sync the directory, and only then remove the old segments, lowest sequence first. The first removal error stops the walk and is returned (a missing file is not an error). The key sidecar goes first, so an emptied group cannot keep one that matches its next first segment. A failed write or rename removes the temp file, and the next rewrite sweeps a leftover one. A crash leaves the old segments, the new one, or the new one plus a suffix of the old ones, and every state replays to the same live set. Not mitigated: a filesystem that reorders the directory operations of the removal could persist a later unlink before an earlier one, and a delete marker could go while the put it covers stays. |
| Self-compaction | None, on purpose. An isolated plane would make the daemon pause Cloud Sync on a timer, and the automatic headroom gate reserves a flat 64 MiB that does not cover this rewrite. `gc-file-blobs` frees nothing yet while `blob_complete=false`, so a timer has nothing to reclaim. `automatic_compactions` in `lastdb status` has no `cas_blobs` entry for this reason. |
| Host pressure | The manual path has no host-pressure gate, unlike the retired-group path (`compact_retired_groups`). That is why a bare `compact` skips the plane. A named run under high pressure is the operator's call; the help says not to. |
| Memory | One hash group at a time, never a bounded frame. MEASURED (`vendor/laststore/tests/compact_plain_large_rows_memory.rs`, debug build, synthetic 8 x 2 MiB rows): the extra peak is about 1.9 x the group when the group is already resident, and about 2.6 x when the verb loads it cold (the daemon case), so about 3 x the group in all. A sealed row is the body base64-encoded inside a JSON row, about 4/3 of its size, so a 16 MiB slab is ~22 MB and a group holds a few of them; the largest group on the primary was 79 MB (DERIVED: about 150 MB extra). A local PUT can store a body up to the UDS cap, 128 MiB (`lastdb_uds::uds_http::MAX_BODY_LEN`); one sealed row then reaches ~171 MB and one group about 510 MB at the peak (DERIVED, not measured). |
| Stalls | A put, get or delete on a group waits for that group's rewrite, which holds the group lock (DERIVED from `compact_loaded_group`, not measured). A PUT that ends with a store-wide flush locks each dirty group and can wait behind a compacting one. |
| IO | A run reads every group once and rewrites only the groups with dead bytes, so a plane with no deletes costs a read and no write. The next backup cut may re-upload the rewritten chunks (not measured). |
| Proof | `vendor/laststore/tests/compact_dead_groups.rs`: clean groups byte-identical, crash states with several segments, emptied group and stale sidecar, failed rewrite, a damaged row, and a threaded put, get and delete race. `storage/laststore/cas_blobs_compaction_tests.rs`: sealed rows through the real `blob_cas` API; delete, compact, disk bytes fall, survivors open also after reopen, clean groups untouched. `exec/compact_cloud_lock_tests.rs`: the route on a populated plane through the host stack, and the `--all` walk. |

To make it automatic later: prove the rewrite capture-neutral through the
production stack (as for `tips`), size the headroom gate from the largest group,
and re-arm the tests in `sync::policy`.

## Known limits

- Automatic compaction runs in `PlaneCompactor` (`sync/capture/plane_compactor.rs`),
  which also runs on a node booted without `cloud_sync.json` and so without a
  sync engine. `cas_blobs` has no automatic compaction on any node; the owner
  verb is its only path.
- `gc-atoms` pages its prologue by physical handle. One manual call may
  return before it frees anything; call until `prune_more_remaining` is false.
- Two rows with the same body share one content-addressed atom, and the second
  put re-appends the whole body. That residue is now visible and reclaimable.
