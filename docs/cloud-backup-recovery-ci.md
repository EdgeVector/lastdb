# Cloud backup recovery CI

The `Cloud backup recovery` workflow runs real DEV backup and restore operations with the exact accepted main client.
It records the source, program hashes, package checksum, download link, and artifact expiration.
The CI summary records the result. Database files, credentials, raw command replies, and daemon logs stay private on the runner.

PRs check the recipe syntax without cloud credentials.
Cloud-related pushes to main run the live operation.
A manual dispatch also requires main.
The protected `cloud-backup-dev` environment permits only main, and one workflow lane serializes all accepted-main operations.
The job receives no AWS or provider administration credential.
PRs also read actual ownership archives with a read-only GitHub token and no cloud credentials.
This operation uses the same archive reader as the protected main operation.

## Real operations

The operation first restores the fixed accepted DEV backup through R2 and B2.
It checks all native file hashes before the first boot, 16 known records, and one complete 8 MiB file.
It reads the data after two boots. Both sessions must stop cleanly.

That restored home then becomes the explicitly connected DEV source.
The connection uses the existing account and performs no enrollment.
Automatic backup uses a 60-second log interval and a separate 86400-second snapshot interval.
The daemon must publish a new snapshot and release its retained source bytes.

The source stores a new 64 KiB file through the ordinary file command.
One batch updates 15 records to reference that file and deletes the other record.
The automatic mutation log must publish those writes and cover the complete current journal head.
Its accepted native file extents must route main files to R2, CAS files to B2, and client metadata to R2.
The log must leave both the last snapshot time and the next daily snapshot time unchanged.
The source then stops and restarts with automatic backup enabled. Its saved policy must remain enabled with the same intervals and revision.
Its next snapshot time must also remain unchanged after the restart.

The operation changes the snapshot interval to 60 seconds and requires a later automatic snapshot.
It then restores the daily interval, disables automatic backup, and stops the source cleanly.
Two separate absent destinations restore the exact selected mutation-log checkpoint and the later snapshot.
Both destinations must return the exact updates, preserve the deletion, and return the complete new file bytes after two boots.
The restored homes must contain no active backup credentials or automatic backup job.

The source also declares one bounded HashRange schema for the atom lifecycle check.
Two expired range keys and one live range key share the same field value and the original 8 MiB file.
The initial snapshot retains all three keys while the TTL worker stays off.
After the source restarts with TTL on, the ordinary TTL purge must erase the two expired keys.
The delete ledger and complete journal frontier must confirm those removals before the log selection.
Two more expired keys pass through the same TTL path before the later snapshot.
The selected snapshot must cover their complete main and CAS frontiers.

Three absent destinations restore the initial snapshot, the selected log checkpoint, and the later snapshot.
The initial snapshot must retain its two original expired keys.
The log and later snapshot must preserve their expired-key removals.
Every destination must return the exact live key, shared field value, and complete original file after two boots.
TTL stays off on every restored daemon, so a later local purge cannot conceal a missed cloud deletion.
The operation uses real owner commands and native TTL mutations, without a service substitute.

The log restore checks its frozen base, exact selected head, completed replay cursor, and expected journal position.
It records hashes for the final native files and checks the product receipt and complete native file count and size.
The snapshot restore also compares every file with the original authenticated per-file proof.
These checks use the real product and DEV services. They use no service substitute or fixture system.

## Protected inputs

The environment contains three secrets:

- `LASTDB_CLOUD_DEV_API_KEY`: access to the existing synthetic DEV account.
- `LASTDB_CLOUD_DEV_RECOVERY_PHRASE`: the recovery phrase for that account.
- `LASTDB_CLOUD_DEV_BASELINE`: the exact accepted backup selection and expected data.

The non-secret repository variable `LASTDB_CLOUD_DEV_BASELINE_SCOPE` contains only the fixed account, generation, and baseline stream.
It gives the public metadata operation an independent baseline exclusion without a cloud credential or expected record contents.
The protected main operation requires those fields to equal its secret baseline before cloud access.

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

The account must match the allowlisted synthetic DEV account in the recipe.
The endpoint comes from `folddb_profile/environments.json`.
The baseline contains no file encryption key.
A baseline update requires an accepted DEV backup, provider verification, and independent checks of its expected data.
Keep that selected backup while CI depends on it. CI never replaces it with the latest head.

## Limits and cleanup

The initial source must contain at most 24 MiB of native files.
The operation limits its observed accepted checkpoints to eight and their total declared object bytes to 64 MiB.
The service also enforces the account quota and reservation rules before an allocation.
The recipe adds no cloud encryption or native format change.
It accesses no primary, primary copy, archive, or PROD service.

The workflow first resumes an earlier saved CI cleanup, before it registers a new stream.
It then refreshes verified ownership, retires eligible older CI streams, and proves the kept latest snapshot.
The fixed baseline, latest successful CI stream, active holds, shared files, and unknown data stay protected.
A quota or integrity block fails CI before a new upload.
CI does not create replacement accounts, increase quotas, or bypass a block.
It does not use account deletion or bucket deletion as cleanup, because those operations do not reconcile V2 allocations.
The paired service must preserve every retained snapshot and log dependency before a provider delete.
Source changes alone do not prove live cleanup. The paired DEV release and real cleanup result remain required before activation.

## CI stream ownership

After all source-free restores succeed, the operation writes a small CI ownership receipt outside Git.
The receipt names the exact repository, workflow, main source, run, attempt, DEV account, generation, and new stream.
It also names the observed accepted points and the exact base snapshot, log, and later snapshot that the restores verified.
The receipt excludes record contents, credentials, file keys, and provider grants.
CI retains it for 90 days and records its download link, checksum, source, and expiration in the summary.
Only a successful operation uploads the receipt. A cleanup caller must also verify the associated workflow result.

The source must stop cleanly at least twice, and every observed point must use one new stream in the fixed DEV account and generation.
That stream must differ from the pinned baseline stream.
This requirement identifies completed CI streams without an account-wide ownership guess.
It does not classify an old stream merely because it has an automatic request ID.
The receipt describes observed points, not a complete frozen directory, and grants no provider deletion authority.

The CI retention rule keeps the fixed baseline stream and the latest successful CI stream.
The cloud service must validate a complete frozen directory before it retires an older owned CI stream.
It must preserve shared files, active restores, uploads, and uncertain allocations, then confirm exact provider absence and release quota once.
An expired or missing ownership record supplies no automatic deletion authority.
General user retention and PROD cleanup remain separate from this CI-only policy.

After pending cleanup, CI reads at most four recent successful main workflow runs.
It verifies each available ownership archive against GitHub's digest, exact source, workflow, run, attempt, account, and generation.
The exact attempt's successful recovery-job completion time orders reruns correctly.
Missing or expired ownership records grant no cleanup authority.
Malformed, unsupported or unverifiable older records also remain unknown; they do not block a valid newer ownership record.
GitHub transport and directory failures stop the operation before a new upload.
This read-only step does not classify a complete directory or delete cloud files.

Before a new restore or upload, CI requires the isolated DEV ownership capability.
It registers verified recipe-bearing historical receipts through the existing account key.
The service independently verifies the immutable GitHub artifact and accepted backup points.
CI then refreshes every durable registration page before it proceeds.
A partial refresh or a service error fails the operation; it grants no deletion authority.

After the successful receipt upload, CI saves that exact archive in the durable service registry.
This non-destructive step runs within the existing serialized workflow.
The service saves the active workflow attempt as `waiting_run`; this state cannot retire a stream.
A later pre-upload refresh verifies the exact completed successful attempt before promotion.
This preserves ownership after archive expiration, and for reruns of an old workflow that falls outside the bounded history.
CI supplies no provider administration credential, GitHub token, or recovery phrase to these routes.
The adapter permits only the compiled DEV endpoint and the fixed CI account.
It requires bounded HTTPS responses, checks the exact registration identity, and refuses redirects.
Ownership registration and refresh make no provider deletions or quota changes.

Before a new upload, CI reads the ordinary account usage ledger.
It checks reserved bytes, retained bytes, other used bytes, and the current quota.
CI requires at least 96 MiB of observed free quota for its bounded 64 MiB checkpoint allowance and control overhead.
Insufficient space stops the operation before upload. CI does not increase or reset quota.
This read supplies an observation, not a reservation; normal service admission still enforces quota for each allocation.
The pre-upload retirement operation reclaims only verified old CI data before this check.

The service waits 30 minutes after the logical retirement before it deletes an eligible file.
CI respects that delay and reads the saved status in waits of at most 30 seconds.
A request checks a five-second budget between at most 16 saved steps.
The client checks the retirement hash, planned allocation counts, and planned byte counts as progress continues.
After the plan exists, planned counts stay fixed. An assigned retirement hash stays fixed.
Released bytes cannot exceed the plan.
CI allows one hour for the control work and 75 minutes for the full pre-upload step.
The complete job has a 150-minute limit, which also covers the build and the existing recovery checks.
An expired limit fails CI. The next run resumes the same saved request before it starts new work.

CI must also acquire durable ownership before its first upload, so a failed restore cannot leave an unregistered stream.
The worker supports a preassigned `LASTDB_CLOUD_CI_STREAM_ID` for the exact dedicated CI account.
It uses that ID only when it creates the first stream record.
An existing saved stream must match the ID, source home, and account scope; a mismatch refuses work, including resume.
The default path uses its existing stream choice when this variable is absent.
This changes local backup-job metadata only. It changes no native file format or encryption.
The recovery command supplies this variable only to its owned source after trusted startup registration and admission.

The startup receipt uses `lastdb.cloud-ci-start.v1` and only the existing owner, account, generation, and fresh stream identity.
Its artifact is `cloud-ci-start-<run_id>-<run_attempt>` and contains only `ci-start.json`.
`prepare_start.py` prepares that claim on protected main; `register_ownership.py --phase startup` verifies its actual GitHub archive and calls `ci/start`.
The caller requires `startup_registered`, the exact claim/receipt identity, and open admission before the source can start.
This claim makes no restore-success or provider-deletion assertion.
The service must refuse adoption of any pre-existing stream without the exact prior claim.
The pre-upload cleanup stage precedes startup registration, so an earlier frozen request can resume before any new effect.
Before the first startup claim, the service can return an explicit unsaved headroom observation.
That response must attest no prior startup claims, no provider deletion, retained unknown data, and sufficient space.
CI records this first-admission result as incomplete cleanup. It does not call it a completed plan.

The recovery command requires the private startup admission record and its exact uploaded receipt.
It checks the source, workflow, run, attempt, fixed baseline, and receipt hash before the source receives its stream ID.
The live context must require startup ownership and support both startup registration and terminal R2 uploads.
Restored nodes receive neither source setting.

New final receipts carry an explicit recovery recipe with the verified base snapshot, mutation log, and final full snapshot.
The recipe stores the changed file size and hash. The service saves this recipe with the trusted artifact identity.
After retirement, the kept-latest helper selects the recipe's exact final full snapshot.
The helper refuses absent or unsupported recipes, inconsistent hashes, or inconsistent point order.
It derives the expected record updates and complete file hash from the saved source revision.
It checks the restored native inventory before the first boot, then checks all baseline keys and full file bytes after two boots.
The helper accesses only a fresh private DEV restore. The pre-upload controller calls it before a new source starts.
Historical receipts without this recipe stay protected and do not become new cleanup inputs.

The PR readback checks at most two recent successful pushes and two successful manual runs.
It reports the actual verified count. A run with no current archives reports no proof and permits the main producer to refresh them.
The main backup job does not depend on this readback, so expired archives cannot prevent that refresh.
If archives exist but none can be verified, the readback fails with a fixed code.

The client requires an explicit terminal R2 upload contract for CI streams.
`LASTDB_CLOUD_CI_TERMINAL_R2_UPLOADS=1` sets the hash-bound `Begin.r2_multipart` field to true for a new request.
The default omits that field and preserves old accepted hashes.
False is invalid. An opted-in R2 upload rejects a direct-PUT grant before it sends bytes.
The same uploader supports the existing B2 multipart grants and the new R2 multipart grants.
CI enables this setting only after the live CI capability confirms support and the matching startup claim opens admission.
These are transport controls. They change no native file format or content encryption.

The recipe uses fresh private runner directories and checks the daemon identity before each stop.
It removes only its own stopped temporary homes after a complete result.
An uncertain automatic operation fails CI and retains its private local capture for recovery on the runner.
It preserves accepted backups and uncertain provider obligations and reports the failure without raw product output.
The Actions log reports a fixed stage name and failure code. Product replies and credentials stay private.
