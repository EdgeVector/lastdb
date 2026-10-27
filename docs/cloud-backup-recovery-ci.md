# Cloud backup recovery CI

The `Cloud backup recovery` workflow checks recovery from an accepted DEV backup through the ordinary product commands.
It builds the exact accepted main client and records its source, program hashes, package checksum, download link, and artifact expiration.
It then restores the fixed backup, checks its files and known records, and opens the restored home twice.
Both daemon sessions must stop cleanly.

The recovery reads actual R2 and B2 objects.
It selects one published backup by account generation, stream, commit ID, and accepted commit hash.
It never selects the latest head as a replacement.
The known data contains 16 records and one 8 MiB file.
The expected record values, file length, and full file hash must match after both boots.

PRs run the recipe syntax check without cloud credentials.
Cloud-related pushes to main run the live recovery.
A manual dispatch also requires main.
Live operations use the protected `cloud-backup-dev` environment and one serialized workflow lane.
The environment permits only the main branch.
The job receives no AWS or provider administration credential.

The environment contains three secrets:

- `LASTDB_CLOUD_DEV_API_KEY`: access to the existing synthetic DEV account.
- `LASTDB_CLOUD_DEV_RECOVERY_PHRASE`: the recovery phrase for that same account.
- `LASTDB_CLOUD_DEV_BASELINE`: the exact accepted backup selection and expected data.

The baseline JSON contains these fields:

```text
account
stream_id
account_generation
commit_id
commit_sha256
schema
records: [{id, note}, ...]
file_bytes
file_sha256
```

The account must be the allowlisted synthetic DEV account in the recipe.
The endpoint comes from `folddb_profile/environments.json`.
The baseline contains no file encryption key.
A baseline update requires a published DEV backup, provider verification, and independent checks of its expected data.
Keep the selected accepted backup while CI depends on it.

The recipe keeps credentials in a separate private home.
It passes the recovery phrase through stdin.
The restored home starts with cloud backup off.
The recipe uses fresh, owned runner directories and verifies the daemon process before each stop.
It removes only its own stopped temporary homes.
It uploads the executable package, not database files, credentials, raw command replies, or daemon logs.
The GitHub summary records the recovery result.

The automatic operation performs no Connect, Start, Resume, enrollment, source capture, upload, or provider deletion.
It creates no retained cloud objects or quota reservations.
It makes no primary change and uses no primary copy.
An error fails the workflow; it does not create another backup or select another baseline.

This lane proves that the accepted backup remains recoverable with the current client and service.
Capture, upload interruption, new acceptance, and allocation changes still require the separate real DEV backup procedure before a deployment that changes those paths.
The completed DEV procedure covers a fresh backup, the same-cut resume, source-free restore, both providers, native file hashes, restart, and allocation reconciliation.

Automatic fresh backups need a supported retention operation first.
The current V2 service retains accepted backups and has no accepted-backup deletion route.
Repeated uploads or new accounts would consume more retained storage.
Account deletion and bucket deletion rules do not reconcile V2 allocation records.
Do not use them as CI cleanup.
