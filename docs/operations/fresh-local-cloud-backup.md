# Fresh Local Cloud Backup

This plan makes the current local database the source for a new cloud backup.
The cloud backup reads the primary files without a second local database.
The safe upgrade uses a test copy.
The primary must run the new Mini code and the new storage service code.

## Cloud scope

CAUTION: The normal backup does not upload B2 file blobs.
Keep the B2 `cas/sha256/` objects, R2 `thumbs/loose/sha256/` objects, and legacy `files/` objects.
Keep them during this operation.
The remote restore does not prove that a B2 file blob is readable.

Clear the old normal `backup/latest` pointer and cloud mutation log in the exact database scope.
Check that `backup/latest` is absent and the cloud mutation log is empty.
Keep the committed S0 rescue pointer, manifest, and chunks.
Keep the S0 hold and its pin pages.
The fresh cut accepts only chunks named by that exact S0 manifest.
It rejects extra or missing chunk names and any size mismatch.
The later source-free restore checks the stored bytes.
If extra chunk keys exist, stop and use a reviewed scoped cleanup plan.
The S0 hold blocks normal chunk deletion.
Do not clear a whole account or database prefix.
Old recovery objects do not enter the new manifest.
Keep the S0 rescue until the new remote restore passes.

## Start and finish

CAUTION: Remote restore rejects a recovery home that contains database files.

1. Keep Cloud Sync Off on the primary.
2. Deploy the storage service with the fresh pointer condition before the Mini upgrade.
3. Make a durable local backup and test the new Mini on a copy before the primary upgrade.
4. Set the primary resume marker and restart the daemon under the safe upgrade procedure.
5. Run `lastdb cloud resume-primary start --fresh-from-local`.
6. Wait for the job status `verified_backup`.
7. Create a mode-700 recovery home with links to the primary credentials. Do not copy secret bytes or the primary `data/` folder.
   Replace `<primary-home>` with the full path to the primary home in these commands.
   Set `recovery_home="$(mktemp -d "$TMPDIR/lastdb-recovery.XXXXXX")"` and run `chmod 700 "$recovery_home"`.
   Run `ln -s "<primary-home>/identity.key" "$recovery_home/identity.key"`.
   Run `ln -s "<primary-home>/cloud_sync.json.paused" "$recovery_home/cloud_sync.json.paused"`.
   Run `ln -s "<primary-home>/cloud_sync.json" "$recovery_home/cloud_sync.json"`.
   Only one cloud config link resolves at a time. Keep `laststore_high_water.json` out of the recovery home.
8. Run `lastdb --data-dir "$recovery_home" restore --remote-latest --db-hash <db-hash> --manifest-sha256 <sha> --into <new-home>`.
   Use the exact database hash for the cleared scope and the `cut.manifest.manifest_sha256` from step 6.
9. Check restored records by key. Check a key that you deleted while Cloud Sync was Off.
10. Run `lastdb cloud resume-primary finish --restore-manifest-sha256 <sha> --restore-home <new-home>`.

The resume marker keeps the cloud workers Off through the cut, upload, and restore check.
The finish command checks the remote restore marker against the exact new manifest.
The finish job also checks the exact cloud pointer and local backup mirror before it clears the resume marker.

## Known local damage

The normal fresh command fails if the local atom history has a missing file.
The `--accept-local-damage` option makes a backup of the local files that exist.
This option does not repair unreadable records.
It marks the job result as `owner_accepted_local_damage` and requires an owner damage check before finish.
The owner must read selected healthy keys from the primary and the restored home.
The owner must compare the results and write `<new-home>/.fresh_backup_degraded_check.json` with these fields:

- `version`: `2`.
- `manifest_sha256`: the exact SHA-256 from the fresh backup manifest.
- `source_missing_atom_groups`: the exact count from that manifest.
- `owner_accepted_local_damage`: `true` after the owner accepts the known local damage.
- `healthy_key_reads`: the positive count of healthy keys that the owner read in both homes.
- `healthy_keys_match`: `true` if those reads returned the same records.

The code validates these fields. It does not count every unreadable key.

## Limits

The fresh cut stops primary writes while it reads the local files.
The file read can stop writes for more than 30 seconds on a large local store.
Schedule a quiet period and measure the pause.
The remote restore test uses the normal backup; it does not test B2 file blobs.
The restored target has no sync engine. Its file blob fetch route returns 409.
