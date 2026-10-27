# Convert an existing development home to typed ranges

Use the product inspection and conversion commands for one stopped V2 home.
The converter creates a separate encrypted target and preserves the source.
The source must use blinded hashes. Its ranges can use plain text or legacy OPE-v1.
Existing typed homes need no conversion.

CAUTION: These commands refuse the primary, the legacy primary, and archived-primary paths.
Do not copy the primary or use a copy of the primary.
Use independently created development data for this procedure.

## Select the source and target

Use the same CI-built program revision for inspection, conversion, and the target daemon.
Select a new target path whose parent already exists.
Do not place either home inside the other home.

The target contains the account identity and converted native data.
Other source-home files remain only in the source.
Review those files and configure any required app or cloud state separately before a switch.
The converter starts no daemon and sends no cloud request.

Confirm the old node's hash and range settings from its configuration.
The source encoding flags are explicit assertions. Stored key appearance cannot establish those settings.
Do not guess a source encoding from a key.

Stop only the selected development daemon with its normal shutdown procedure.
It must leave a clean session and a successful shutdown flush receipt.
Keep the source stopped through inspection and conversion.

## Inspect the stopped home

Use your actual absolute development paths in these variables:

```sh
SOURCE_HOME=/absolute/path/to/stopped-development-home
TYPED_HOME=/absolute/path/to/new-typed-development-home

lastdb --data-dir "$SOURCE_HOME" db inspect-range-conversion \
  --source-hash-encoding blind_v1 \
  --source-range-encoding ope_v1 \
  --target-range-encoding plain \
  --target-home "$TYPED_HOME" \
  --json
```

Use `--source-range-encoding plain` only when the source used that setting.
The inspection creates no target and changes no source data.

Read `inventory.range_fields` for each schema's range field, actual declared type, and proposed profile.
Read `inventory.molecules[].blockers` for shared-molecule conflicts, unsupported types, invalid values, unresolved references, and key collisions.
`Any`, a missing declaration, and unsupported types block conversion.
The tool does not infer a type from stored values.
A declared String remains a String, even when its values resemble numbers or dates.
Correct a declaration through the normal schema procedure before another clean shutdown and inspection.
Legacy homes refuse Date and DateTime range declarations.
Keep an existing valid String declaration. A change to a temporal domain needs a separate explicit conversion.
Do not rewrite stored keys or profile metadata by hand.

Read `readiness.ready_to_convert` and `readiness.blockers`.
A successful inspection can report blockers; `ok: true` does not mean conversion is ready.
Readiness requires an absent target and the explicit blinded-hash assertion.
Inspection without `--target-home` retains the metadata-only inventory behavior.

## Check temporary disk space

Target-aware readiness reads the complete held source through bounded pages.
It measures the translated logical bytes and counts target records, including the new profiles and progress record.
It does not keep all atom or file contents in memory.

The space report separates:

- Source native file lengths and file count. These files already occupy space.
- Estimated target bytes: twice the translated logical bytes, plus 128 bytes per target record and a 1 MiB allowance.
- Estimated temporary merge bytes: another full estimated target.
- Free-space reserve: the native policy's 64 MiB free reserve plus 64 MiB replacement reserve.
- Additional required free bytes: target estimate, merge estimate, and reserve.
- Retained source plus estimated target bytes. This excludes temporary merge output.

The free-space check uses the target filesystem, including when the source uses a different filesystem.
It does not charge the existing source bytes against free space again.
These are planning estimates, not guaranteed physical peaks or allocated-block measurements.
The normal settled 1.5× database limit does not describe conversion space.
Other processes can consume disk space after inspection.
An explicit `LASTDB_DISK_CAP_BYTES` must also fit the estimated target, merge output, and replacement reserve.
The report exposes that cap and `within_disk_cap` separately from filesystem free space.

The converter repeats the free-space check before target creation and before it opens target writers.
Each copy commit and merge also checks current free space through the native reservation policy.
An insufficient-space refusal preserves the source and leaves any incomplete target unavailable to normal startup.
Resume uses the full fresh-conversion space allowance; it does not subtract an existing partial target.

Readiness checks the source format, type mappings, physical record translation, and estimated capacity.
It does not certify every atom reference or counter. Final conversion verification performs those checks.
`database_converted` and `activation_ready` remain false in an inspection report.

## Convert into the separate target

Proceed only after you resolve the inspection blockers:

```sh
lastdb --data-dir "$SOURCE_HOME" db convert-typed-ranges \
  --source-hash-encoding blind_v1 \
  --source-range-encoding ope_v1 \
  --target-home "$TYPED_HOME" \
  --json
```

Keep the source encoding choice consistent with inspection.
If you select custom record or metadata limits, pass the same limits to both commands.
Do not raise a limit merely to suppress a declaration, key collision, or reference error.

The converter locks the stopped source and binds its fixed publications to the target job.
It copies both encrypted native homes and translates the required range references.
It preserves hashes, molecule identities, atom values, and file content.
It then reopens both target homes and checks exact records, counts, references, counters, and the replay floor.
Only successful verification publishes the supported typed format marker.

Require `conversion.database_converted: true`, `conversion.activation_ready: true`, and `conversion.source_unchanged: true`.
These fields certify the inactive target. They do not switch an app or a daemon to that target.

## Resume an interrupted conversion

Keep the source stopped and unchanged.
Use the same source, target, encoding flags, program revision, and limits:

```sh
lastdb --data-dir "$SOURCE_HOME" db convert-typed-ranges \
  --source-hash-encoding blind_v1 \
  --source-range-encoding ope_v1 \
  --target-home "$TYPED_HOME" \
  --resume \
  --json
```

Normal startup refuses an incomplete target.
It never resumes the converter or removes the conversion fence.
A different source publication, target identity, or job configuration receives a refusal.
Target-aware inspection requires a new absent target; use explicit converter resume for an existing job.

## Check the target and switch the development app

Remove legacy encoding overrides from the target daemon's environment.
Start only the selected target, with explicit development socket paths:

```sh
env -u LASTDB_HASH_KEY_ENCODING -u LASTDB_RANGE_KEY_ENCODING \
  lastdbd --data-dir "$TYPED_HOME" \
  --socket-path "$TYPED_HOME/data/folddb.sock" \
  --full-socket-path "$TYPED_HOME/data/folddb-full.sock"
```

Use another terminal for the target's normal app operations.
Check known exact keys, ordered ranges, cursors, and the app's expected field values.
Check all range types that the selected schemas use.
Check one normal write, its keyed read, a durable flush, and a clean restart.
Run the existing reference check against the selected target:

```sh
lastdb --data-dir "$TYPED_HOME" db verify-refs --json
```

Direct the development app to the target's explicit socket only after these checks pass.
Keep the stopped source for recovery. Do not run the source and target as concurrent writers for the same app.

CAUTION: The source becomes stale after new writes reach the target.
A return to the old source can lose those new writes.
Stop the target and resolve those writes before any return to the source.
Primary conversion and primary activation require a separate plan and explicit authorization.
