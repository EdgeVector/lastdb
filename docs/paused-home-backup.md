# Backup while Cloud Sync is Off

The primary has Cloud Sync Off. A local write changes memory before the flush puts it on disk.

CAUTION: A live snapshot can miss a write that gets an ack during the snapshot. Use a stopped copy for this backup.

CAUTION: A peer write can restore a record that this primary deleted while Cloud Sync was Off. Keep Cloud Sync Off.

## Prepare the copy

CAUTION: The approved one-time rescue uses the LastStack #256 v2 stopped-copy marker and a separate publisher. This CLI accepts only a v1 marker and cannot publish a backup. Do not upgrade the primary only to create a v1 receipt for this rescue.

1. Use a safe upgrade to install the candidate daemon.
2. Check that the candidate keeps Cloud Sync Off.
3. Stop the candidate through the supervised LastStack action.
4. Require `.shutdown_flush_ready` for that daemon session. The daemon writes it after requests and node work stop and the final flush succeeds.
5. Copy the stopped home. Restart the primary before cloud work.
6. Require `.cloud_backup_source_copy` in the copy. It must match the daemon PID and session start time in `.shutdown_flush_ready`.

The copy has no socket file. The primary has no backup request file.

## Check the copy

Use the checked release CLI from the clean Fold worktree. Set `FOLD_RELEASE_CLI` to its absolute path.

```sh
"$FOLD_RELEASE_CLI" --data-dir "$STOPPED_COPY" cloud backup-while-off --json
```

The command requires an explicit copy path. It checks the copy marker, shutdown receipt, identity key, store layout, and paused cloud file.

The command refuses a home with a live session or a socket path. It never runs on the primary.

CAUTION: The command currently stops before any cloud write. The cloud lacks a separate rescue backup with a rule that keeps its manifest and files.

The command reports `server_enforced_peer_writer_fence_unavailable`. Treat this result as a blocked backup.

## Required cloud result

The future rescue backup must be separate from normal `backup/latest`. It must keep the old backup and every file that a rescue manifest names.

The rescue backup must bind the manifest to an encrypted recovery descriptor. A fresh restore must use that descriptor and skip old peer writes.

CAUTION: The stopped copy covers only writes that reached disk before the stop. A later live write does not enter this backup while Cloud Sync stays Off.

CAUTION: The primary reported two unreadable records across six links on 2026-10-05. Keep the older backup. One new snapshot cannot prove a complete restore.

## Restore after the source disk is lost

This path requires an immutable S0 rescue cut in the account cloud root. The backup command confirms the encrypted recovery file before it commits the rescue cut.

Use the recovery phrase to create a separate home. Do not start the daemon for this home.

```sh
lastdb --data-dir <recovered-credentials-home> connect --env prod
```

Enter the recovery phrase on standard input. This command creates `identity.key`, `cloud_sync.json`, and `data/.device_id`. The restore command accepts these files. It rejects a source database.

```sh
lastdb --data-dir <recovered-credentials-home> restore --remote-s0-only --into <fresh-target-home>
```

If the account has several databases, add `--db-hash <hash>` to select one. If a database has several rescue cuts, add `--manifest-sha256 <hash>`.

The service supports one rescue cut per database now. A later service change can add several cuts. The command rejects a target with any existing file.

The CLI lists the rescue cuts. It reads the selected cut again and checks the encrypted recovery file. It then checks the restored manifest and the v2 `s0_only` marker.

The target gets `cloud_sync.json.paused` and `.cloud_resume_required`. Cloud Sync stays Off, even if the restore fails after it creates the target.

After a successful restore, the target gets `.rescue_s0_restore_ready`. Its database hash, manifest hash, and counter match the JSON report. The report also states `restore_mode: s0_only` and `cloud_sync_off: true`.

CAUTION: The cloud can hide a newer rescue cut. The recovered identity alone cannot prove the newest backup. Keep an external receipt or known manifest hash if you need that proof.

CAUTION: The backup proves only the snapshot cut. It does not prove writes after that cut. Do not turn Cloud Sync On until the Delete repair and peer check are complete.
