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

## Daily check

The `Daily cloud recovery check` workflow runs at 10:23 UTC each day.
A manual run also requires main.
It uses the short-lived GitHub token with Actions write permission and source read permission.
It receives no DEV credential, recovery phrase, AWS credential, or provider credential.

The driver first checks for an active or queued recovery on main.
It does not add a second run while that recovery exists.
It then checks successful main runs from the existing push and manual event paths.
A fresh result requires the actual `Automatic DEV backup and recovery` job to succeed within the previous 24 hours.
The exact attempt must also show successful client build, package, recovery, ownership archive, and durable ownership steps.
Recipe syntax success alone does not count.

The driver checks exact-attempt completion times, including a recent rerun of an old source.
It checks at most 1000 records per directory, with an 80-request limit and a 180-second deadline.
An incomplete active-run or job directory, an API error, or an unknown result fails the daily check.
A history limit records a bounded lack of fresh proof. It does not establish complete historical absence.

If the bounded search finds no fresh proof, the driver checks again before one dispatch to the existing workflow on main.
An active recovery still prevents that dispatch.
The summary states the search limit, examined counts, and whether the successful-run directory was complete.
The dispatched event remains `workflow_dispatch`; the trusted ownership event contract stays unchanged.
GitHub has no atomic check-and-dispatch operation.
The existing recovery lane still serializes any concurrent main request.
An uncertain dispatch outcome fails visibly and receives no automatic retry.
A successful dispatch is not a cloud recovery result. The dispatched real DEV operation must succeed separately.

## Real operations

The operation first restores the fixed accepted DEV backup through R2 and B2.
It checks all native file hashes before the first boot, 16 known records, and one complete 8 MiB file.
It reads the data after two boots. Both sessions must stop cleanly.

That restored home then becomes the explicitly connected DEV source.
The connection uses the existing account and performs no enrollment.
Automatic backup uses a 60-second log interval and a separate 86400-second snapshot interval.
The daemon must publish a new snapshot and release its retained source bytes.
That exact selected snapshot must report at least one metadata pack and one pack with more than one file.
CI records those counts with the selected request and commit, then restores that same snapshot.
No exact pack count or minimum per-log pack count is required.

The source stores a new 64 KiB file through the ordinary file command.
One batch updates 15 records to reference that file and deletes the other record.
The selected automatic log chain must publish those writes.
The current source must separately reach its complete journal head.
The accepted native file extents across that chain must route main files to R2, CAS files to B2, and client metadata to R2.
The log must leave both the last snapshot time and the next daily snapshot time unchanged.
After the file and baseline changes, the source creates two keys that belong only to that CI stream.
It updates one key and deletes the other, then deletes the remaining key.
Each batch must return a durable ack with exact operation counts and unique mutation IDs.
The source records the complete log position after each durable ack.
The log restore verifies all three positions in the same complete selected chain.
The source and every restored start must read both temporary keys as absent.
Complete status also requires no pending request, no retry, and zero retained source bytes.
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

The log restore checks its frozen base, exact selected head, completed replay cursor, and authenticated selected journal position.
It records hashes for the final native files and checks the product receipt and complete native file count and size.
The snapshot restore also compares every file with the original authenticated per-file proof.
These checks use the real product and DEV services. They use no service substitute or fixture system.

## Fixed backup selections

The recipe freezes each accepted backup that meets the requested kind and minimum native frontiers.
Later source status reads never replace that selection.
Publication and catch-up share one 900-second deadline.
Catch-up requires all six journal conditions, an idle published request, zero retained source bytes, and matching completed/current/accepted request identities.
Every status read passes through the same checkpoint and byte-budget checks.

File creation and record updates can enter separate automatic logs.
CI checks the complete selected log chain, which excludes the base snapshot from its positive main/CAS log-byte requirement.
It does not require both writes in one final log.
Automatic backup and the mutation journal stay enabled during those writes.

Before log selection, CI saves the full written journal position: epoch, sequence, hash, and native frontiers.
The restore checks that exact position within the authenticated selected prefix.
Its expected final position comes from the selected cloud chain, not a later source status head.
The frozen selection remains the restore target even if source catch-up needs another automatic log.

The restore client saves one small private route receipt for each accepted log link.
It saves that receipt only after every page, file, object receipt, destination, and complete extent coverage passes verification.
The receipt binds the link index, commit hash, root receipt, inventory root, native totals, and role counts.
It also retains the link boundaries and at most 128 authenticated entry hashes.
An existing receipt must equal the recomputed receipt on resume.
The receipt alone does not prove a complete replay.

Object counts are unique within each accepted inventory.
The chain total can count a shared receipt more than once.
It does not claim the provider's unique object count or fresh storage allocation.
Older completed sessions receive no receipt backfill.

CI requires every selected link's receipt, the exact base and head, a completed replay cursor, and the final restore receipt.
It also keeps all native file hashes, keyed reads, file-byte checks, deletion checks, two boots, and strict clean stops.
Route evidence is product-validated; the Python recipe does not independently decode encrypted manifests.
It no longer reads temporary source upload plans that the next automatic backup can retire.
The existing eight-checkpoint and 64 MiB limits include accepted ancestors found during restore.
Dependency inspection rejects more than eight retained points before the selected restore starts.

These receipts stay in the owned restore session, outside Git.
The old temporary-file count allowance and total byte allowance stay unchanged.
A separate count permits only the new canonical route receipt names within the existing chain limit.
Native files, cloud payloads, encryption, and the primary stay unchanged.

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
The manual baseline mode is the only supported update path (see "Baseline move").

## Baseline move

A native format change can make the fixed baseline unreadable by the current client.
The client restores only home format 7, so an older baseline fails the first restore.
The manual `baseline` mode makes a new fixed baseline. It does not run the normal recovery.

```text
gh workflow run cloud-backup-recovery.yml -R EdgeVector/lastdb --ref main -f mode=baseline
```

The mode uses the same protected environment, build, package, cleanup, and start claim as the normal operation.
`baseline.py` then starts a fresh home of the current format with the fixed CI identity.
The recovery phrase supplies the identity seed, and the daemon must report the fixed CI account.
The home receives the `CloudCiBaselineF7` Hash schema, 16 records, and one 8 MiB file.
Automatic backup uploads exactly one snapshot to the claimed stream. No mutation log enters that stream.
The source stops cleanly. The snapshot restores to an absent folder, and two boots read every record and the complete file.
Only then does the step write `ci-baseline.json`. The workflow uploads it as `cloud-ci-baseline-<run_id>-<run_attempt>`.

`register_ownership.py --phase baseline` sends that archive to the DEV service route `ci/baseline`.
The service requires all of these conditions:

- The `baseline_move` capability. It requires the startup ownership mode.
- The open start claim of this run for the new stream. The claim must name the current baseline.
- The exact GitHub archive of the active run on main.
- An accepted point at the head of the new stream, with no open commit.
- No open cleanup freeze.

The service then saves the new baseline and adds the replaced baseline to `retained_baselines`.
Cleanup protects every retained baseline stream. Its deletion is a separate later decision.
The policy keeps at most eight retained baselines. The move makes no provider deletion and no quota change.

The record notes and the file bytes derive from the recovery phrase and the public stream ID (`baseline_data.py`).
The run summary shows only public identifiers and `secret_sha256`, a SHA256 commitment of the exact secret value.
After a successful move, an operator updates the two protected inputs:

1. Derive the secret with `baseline_data.secret` from the phrase, the new point, and the file hash.
2. Compare `baseline_data.commitment` of that value with the `secret_sha256` of the run summary.
3. Set `LASTDB_CLOUD_DEV_BASELINE` in the `cloud-backup-dev` environment from stdin. Do not print the value.
4. Set the `LASTDB_CLOUD_DEV_BASELINE_SCOPE` variable to the new account, generation, and stream.
5. Run the normal recovery on main.

Update both inputs before the next normal recovery.
The daily check does not count a baseline-mode run as a recovery result.

## Limits and cleanup

The initial source must contain at most 24 MiB of native files.
The operation limits its observed accepted checkpoints to eight and their total declared object bytes to 64 MiB.
The service also enforces the account quota and reservation rules before an allocation.
The recipe adds no cloud encryption or native format change.
An early restore failure or strict-stop failure does not retain API credentials.
Cleanup removes only the known API credential files and clears the operation's key reference and credential environment variables.
It also removes canonical private pending connection files after a bounded read of the two owned homes.
Published consent records, identity files, and native files stay unchanged.
Uncertain native data and recovery receipts stay private when the operation cannot confirm a clean stop.
No secret values enter the summary.
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

The CI retention rule keeps the fixed baseline stream, each retained earlier baseline, and the latest successful CI stream.
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
A complete result requires one confirmed release for every eligible allocation and no unresolved upload bytes.
Protected allocation counts remain separate, and legacy or unknown streams stay retained.
CI allows 140 minutes for the control work and 150 minutes for the full pre-upload step.
A pending cleanup and the next plan can each need the full 30-minute service delay.
Each pass also validates the complete saved metadata directory and allocation plan.
A shared 70-minute limit can expire after one request completes and before the next request finishes.
The complete job has a 210-minute limit, which also covers the build and the existing recovery checks.
An expired limit fails CI. The next run resumes the same saved request before it starts new work.
The failure summary retains the last validated phase, allocation counts, headroom, fixed retry codes, and request count.
Each request starts with an unobserved state until a saved state passes validation.
These observations identify the last known progress. They do not claim a completed cleanup.

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

The client restores only home format 7. The first such main source is `2cdc5531c1f056891ff17e16fbe25b52a5c4a201` (#929).
A kept stream from an older main source holds a format 4 to 6 snapshot, which this client refuses by design.
The job reads the full main commit history without file contents and checks the ancestry of the kept owner source.
For an older source, the controller records the kept stream and its source, and attempts no restore.
The service still keeps that stream. A later successful format 7 CI stream replaces it under the normal retention rules.
A kept source outside the main history fails CI. The recovery step recomputes this check from the validated kept record.

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
The Actions log reports a fixed stage name and failure code.
It also records allowlisted pause categories, policy intervals, cleanup counts, and quota totals.
The full CI summary remains the validation record.
Raw status, product replies, credentials, file keys, and provider grants stay private.
