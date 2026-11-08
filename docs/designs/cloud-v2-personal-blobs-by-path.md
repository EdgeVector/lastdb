# Personal record blobs by path

Status: implementation contract for the client. The service routes
(`blobs/release`, blob uploads with `TransferInput.blob`, and blob reads)
live in exemem-infra.

Design: `design-lastdb-blob-files-on-demand-20261107`.
Decisions: `decision-2026-11-07-lastdb-blob-files-on-demand` and
`decision-2026-11-07-lastdb-blob-optimistic-delete`.

## What a personal backup holds

A personal snapshot or mutation log holds the main run home (`disk-run`) only.
It holds no CAS runs: the run profile has no `cas` home, a log position has
one frontier (`main`), and a log entry has one part. A dedicated shared
database is out of scope and still backs up its CAS home with each snapshot.

Each record file version is one B2 object at the path that every device
derives from the record:

```text
BlobPath { database: <main run home id>, space, record: sha256(record key), write_id: <stream>:<seq> }
```

The object holds the sealed row exactly as the record format reads it from
`cas_blobs` (the storage envelope removed). `fetch-blob` installs those bytes.

## Local markers in `cas_blobs`

Each marker key is a prefix plus the suffix of the version's `fb:` key.
Only the presence of a marker matters.

| Prefix | Meaning | Written by |
|---|---|---|
| `fp:` | Created here, not uploaded yet | The commit that adds an `fb:` row without an `fu:` row |
| `fu:` | In the cloud | The node after the service accepts the uploading job; a download writes it with the row |
| `fr:` | Dropped; the next job releases it | The CAS delete commit that retires the `fb:` row |

## Uploads

1. Only the device that created a version uploads it. A download writes `fb:`
   and `fu:` in one batch, so it never queues an upload.
2. A job plans its uploads after its capture. Every version that a captured
   record names already has its `fp:` row, because the commit that wrote the
   record wrote that row.
3. A snapshot first queues every `fb:` row with neither `fu:` nor `fp:`. The
   first snapshot after the upgrade therefore uploads every local version.
4. The job state keeps the plan as an index and pages of 64 frozen transfer
   inputs, so a restart resumes the same transfer. A job takes at most
   4096 versions; later ones wait for the next job.
5. After the service accepts the job, the node writes `fu:` and removes
   `fp:` for each version with a verified receipt.

## Releases

1. Releases come from record writes, not from local file rows. The format
   retires a dropped version through one path, and the CAS delete commit
   that retires it runs after the main publication that dropped it. That
   commit also writes `fr:` and removes `fu:` and `fp:`. A restored node has
   no `fb:` rows but still queues the releases of the versions it drops.
2. A job plans its releases once, before its first capture, so every planned
   release belongs to a write that the job's backup holds.
3. The job sends them to `blobs/release` in pages of at most 64 paths before
   it publishes. The service deletes the objects when it accepts the job.
4. After acceptance the node removes the job's `fr:` rows.
5. A release of a path that was never uploaded is harmless.

Optimistic delete: an older kept recovery point that names a released
version cannot restore that file.

## Restore and join

Restore installs the main home and an empty CAS home. The CAS home opens and
works with zero runs. The restore proof checks that each record that names a
file version has the matching marker and that the marker derives a valid
blob path; it does not read `fb:` rows. The first `fetch-blob` of a record
downloads its version from B2 and installs it.

## Versions

There is no conversion code. The run profile and log formats changed:

- backup client version 8 and state format 5;
- log roots version 5, log index and entries version 2, kept under a new
  `main-v2` directory;
- restore log cursor and route proof version 2.

The automatic policy is version 3. A version 2 policy is retired once by the
new program: its setting (enabled, cadence, connection) stays exactly, and
its backup baseline is dropped: the backed-up frontiers, the pending job, the
local recovery point, the active job marker and that job's snapshot capture.
The next run takes a full snapshot at once. A snapshot capture of an older
capture format is released before a new capture starts. The new policy is
written last, so a failed step leaves the old file and the next tick retries;
the upgrade never turns automatic backup off.

An older cloud backup carries the CAS home and is restored only by the
program that wrote it. The first snapshot of the new program is the first
restorable point for it. A job that the previous program left open on the
service makes the new job wait (busy, retried) until the service closes it.
