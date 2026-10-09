# Durable local backup GC jobs (receipt versions 1 and 2)

This is the local GC repair, P1, with the P2 PR-I receipt fields. It is not the cloud-owned deletion protocol.
The daemon uses the existing publication fence and single executor.
A changed manifest can stop a job. The job retains its result.

## CLI and owner route

- `lastdb cloud backup-gc --execute` accepts a job and returns its ID.
- Omit `--execute` for a durable preview job.
- Add `--wait` to wait for a successful terminal receipt.
- Use `--job UUID --wait` to attach after disconnect.
- Use `--status` to read the latest job without cloud work.
- Use `--request-id UUID` to retry acceptance after a lost response.

The CLI prints the request ID before submission. The daemon binds that ID to the trigger and preview mode.
An ID with different parameters fails. A repeat ID never starts a second executor.
Acceptance and status use separate short socket requests. A CLI disconnect does not cancel the daemon task.

`POST /api/sync/backup-gc` accepts `dry_run` and `request_id`. It returns HTTP 202 after durable admission.
The route does not read a manifest or await the publication lock, cloud list, or DELETE.
The worker resolves the keep set and applies the existing exact identity checks.

The same route accepts `job_id`, or `status:true`, for status. It returns HTTP 200.
`after` is an exclusive object receipt sequence. `limit` defaults to zero and has a maximum of 256.
The status path uses exact local keys. It does not wait for cloud work.
A local-only boot can retrieve and reconcile old receipts without cloud credentials.

Without `--wait`, exit zero means admission or a successful status read. It does not mean GC succeeded.
With `--wait`, only a valid `completed` receipt gives exit zero.
`failed`, `partial_failure`, `superseded`, and `interrupted` give a nonzero exit after the receipt output.
A missing, unsupported, malformed, or wrong-job receipt gives a nonzero exit.

## Durable data and crash order

The sidecar directory is `<node-home>/backup_gc_jobs`. It is not a schema, mutation log, or replicated keep set.
No schema persist lane or global LastStore transaction changes.

- `index.json`: a format version, at most 16 active IDs, one latest ID, and two archive capacity counters.
- `<job-id>/summary.json`: version, trigger, mode, state, phase, timestamps, proof identities, counts, bytes, stop reason, and the version 2 fields below.
- `<job-id>/<sequence>.json`: one exact object key, listed size, intent or outcome, and the version 2 fields below.

Each file replacement uses a temporary file, file sync, rename, and directory sync.
Admission syncs the summary and index before it returns the ID. The cached index changes only after disk persistence succeeds.
An unindexed acceptance becomes `interrupted` on exact retry or status. It cannot remain queued without an executor.
A failed index write or directory barrier invalidates the cache. The next access reloads the durable counters.
Recovery preserves earlier accepted jobs that still have a live local owner. A failed admission never receives that owner.

Before each DELETE, the worker writes the object intent and the summary pointer to that intent.
After the response, it writes the object outcome before the summary counts.
Recovery reads at most 16 active jobs and each job's exact pending object receipt.
It reconciles an outcome that reached disk before its summary. It applies the byte count once.
An intent without an outcome becomes `unknown`. Every unfinished job becomes `interrupted`.
Recovery never submits a DELETE or reuses a persisted keep set.

The process-kill test kills a separate test process at admission, dispatch intent, and outcome-before-summary boundaries.
It reopens the receipts twice and checks identity, state, counts, and bytes.
Separate engine and owner-route tests cover the product admission and executor path.

## Engine lifetime

One daemon shares a receipt manager by the canonical sidecar path.
The shared manager owns the receipt cache, accepted-job liveness, executor lock, and publication lock.
A detached task retains its engine. An empty Host engine slot does not prove that the task stopped.
Status without a Host engine uses the same manager and does not interrupt a live task.

A new engine revokes the old engine's GC lease. Both engines share the publication turn.
An in-flight DELETE therefore retains the turn before a replacement can publish.
While engine instances overlap, GC refuses selection and stops before the next DELETE.
After the old engine drops, a new job needs fresh validation under the replacement engine.
Cloud-off retains its existing proof revocation and publication-turn barrier. Cloud-on does not restore that old proof.
The last engine's removal invalidates runtime liveness. A later read reconciles unfinished work without a DELETE.
The replacement tests pause presign, replace the coordinator engine, and query both status paths before the old task exits.

## Receipt versions

Each summary and object file carries its own `version`. The reader accepts 1 and 2.
A file with version 3 or higher, or with no version, fails with `GC_RECEIPT_VERSION_UNSUPPORTED`.
The daemon then does no recovery and no dispatch for that job. The CLI applies the same gate.

A file is version 1 when it carries no version 2 field. The writer keeps that file byte-for-byte as P1 wrote it.
A file becomes version 2 when a version 2 field carries a value. A version 1 archive reads under the version 2 reader with the same decoded values.
No version 1 field changes its meaning. The version 2 fields are:

- Summary `stop_code`: a typed code beside the free-text `stop_reason`. Values: `publication_state_changed`, `newer_generation`, `object_failures`, `metadata_capacity_required`, `task_panic`, `execution_error`, `daemon_lost`, `acceptance_interrupted`, `unindexed_acceptance`.
- Summary `dispositions`: counts and exact listed bytes per version 2 outcome: `deleted_now`, `already_absent`, `failed`, `protected`, `uncertain_write`. Present once a version 2 outcome reconciles.
- Object `instance_id`: the exact cloud instance. `key` is digest-derived and can name a replacement with the same digest.
- Object `provider_version`: `"unversioned"` for an explicit unversioned locator, or `"exact:<id>"` for an exact immutable provider version. Absent when the receipt has no version knowledge.
- Object `outcome` gains `deleted_now`, `already_absent`, `failed`, `protected`, and `uncertain_write`. The version 1 outcomes stay.

`index.json` gains `version`. A P1 index without it reads as version 1. The daemon writes the newest version it can produce.
The index reader refuses a version outside the read range with the same named error.

The local executor writes only version 1 outcomes and no instance fields. Its summaries carry a `stop_code` on every stop.
A later cloud-owned job (P5) can write the version 2 object fields. This change touches no wire format.
The contract items in the cloud GC plan are not yet approved; this is a local sidecar format only.

### Outcome mapping to the P0 protocol model

| Receipt outcome | P0 source | Meaning | Version 1 counter it feeds |
|---|---|---|---|
| `delete_acknowledged` | none (version 1) | a bare 2xx DELETE response | `delete_acknowledged` |
| `failed_before_dispatch` | none (version 1) | no DELETE was sent | `failed_before_dispatch` |
| `unknown` | none (version 1) | a crash or timeout lost the outcome | `unknown` |
| `deleted_now` | `Disposition::DeletedNow` | the provider removed this exact instance | `delete_acknowledged` |
| `already_absent` | `Disposition::AlreadyAbsent` | the exact instance was absent at dispatch | `delete_acknowledged` |
| `protected` | `Disposition::Protected` | a hold or head change protected the instance; no DELETE effect | none |
| `failed` | `WriteOutcome::NotApplied` | terminal provider closure after dispatch; no effect | none; `report.failed` |
| `uncertain_write` | accepted write with no `WriteOutcome` | dispatched, no terminal closure; may still complete | `unknown` |

`report.deleted` stays the acknowledgement count. `report.failed` adds `failed` to the version 1 sum.
A `protected` object is not a failure and not an acknowledgement. `objects_reconciled` counts it.
The CLI `completed` check keeps its version 1 invariants: no `unknown`, no `failed_before_dispatch`, and `objects_reconciled` equal to `delete_acknowledged`.
A completed job therefore has no `failed`, `protected`, or `uncertain_write` object under either version.

## Meaning of results

`delete_acknowledged` counts successful provider DELETE responses. `acknowledged_bytes` sums their exact listed sizes.
These values do not distinguish a newly deleted object from an object already absent.
They do not certify physical erasure or reconciled billing.

`failed_before_dispatch` means that no DELETE was sent for that receipt.
`unknown` means that a DELETE may still complete remotely, or a crash lost its outcome.
No unknown bytes receive acknowledgement credit. A retry requires new admission and fresh safety checks.
The old `report.deleted` field remains an acknowledgement count for compatibility.
The report's billed and reclaimable sizes describe the selection inventory, not a post-delete billing receipt.

## Bounded metadata lifecycle

P1 retains terminal receipts. It has no automatic expiry, prune, or ID reuse.
This protects unknown dispatch evidence and keeps a terminal ID retrievable.

The archive reserves at most 4096 job slots and 1,048,576 object receipt slots.
Admission reserves a job slot before it writes the summary. Failed and unindexed admissions consume capacity too.
The manager reserves object slots in batches of 256. Crash gaps and unused slots consume capacity conservatively.
These are entry-count limits, not a provider quota or an exact disk allocation bound.
File allocation overhead depends on the filesystem. Monitor the sidecar directory's physical size before production activation.

At a limit, admission or the next dispatch fails with `GC_METADATA_CAPACITY_REQUIRED`.
Status and existing receipt reads remain available. The manager prunes no evidence and issues no further unrecorded DELETE.
A saturated archive needs an explicit export and migration decision. P1 supplies no automatic archive reset.
Do not remove the directory, reset its counters, or expire unknown receipts to restore capacity.
P5 can replace this conservative local archive only after it preserves exact IDs and resolves or retains unknown dispatch evidence.

## Limits

This protocol protects the local daemon across engine replacement. It does not fence another device or a late provider DELETE.
The cloud protocol must add durable object-instance intents before it replaces the local executor.
No production cloud call, primary restart, upgrade, or deployment forms part of this repair's tests.
