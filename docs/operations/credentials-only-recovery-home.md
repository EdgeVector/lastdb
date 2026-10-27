# Prepare a credentials-only recovery home

`lastdb cloud prepare-recovery-home` creates a fresh home for a cloud-only restore.
It reads the source identity and cloud credentials inside the product.
It does not open the local database or call a cloud service.
It does not change the source home, register an account, or start a daemon.

Use an explicit absolute source path and an absent absolute destination path.
The destination parent must exist.
The command refuses a path overlap, a symlink component, or an existing destination.
The source identity and `cloud_sync.json.paused` must be regular owner-only files.

The selected environment must match the saved source URL exactly.
`--env prod` uses the compiled PROD URL, regardless of runtime endpoint overrides.
The API key from stdin must match the saved source API key.
This is an offline credential check; it makes no remote account claim.

For the primary PROD home, use the stored API-key locator at the point of use:

```bash
set -o pipefail
lastsecrets get lastdb-primary-cloud-api-key-prod |
  lastdb --data-dir /absolute/source/home cloud prepare-recovery-home \
    --into /absolute/absent/recovery-home --env prod --api-key-stdin --json
```

The command emits a receipt with no credential values or identity hashes.
The new home has mode `0700`.
It contains only three files, each with mode `0600`:

- `identity.key`: the same 32-byte identity seed.
- `cloud_sync.json.paused`: the same API key and the checked URL.
- `.cloud_resume_required`: the durable Cloud Sync Off marker.

The command creates no `data` directory.
Use this home as the `--data-dir` of `lastdb cloud backup-v2 restore`.
Give restore a different fresh `--into` home and the exact `--stream`.
The restore reads the recovery phrase from stdin and reads the backup from the cloud.
The restore command requires `LASTDB_BYTE_BACKUP_V2_ENABLED=1`.

The preparation needs no recovery phrase, local database copy, or cloud account registration.
A failed preparation removes only the destination files that this command created.
If the destination changed, the command refuses success and preserves unrelated files.
