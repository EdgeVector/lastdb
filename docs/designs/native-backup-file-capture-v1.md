# Native backup file capture, first release

Status, 2026-10-26: the V1 capture code that this page describes is removed. The file dependency records in the Atomic directory section are current.

This source change extends Backup Capture with a file dependency directory and exact source proof.
It does not activate a cloud worker, publish a backup, or change a native pack file.
The shared protocol remains `native-cloud-backup-protocol-v1.md`.

## Atomic directory

The record counter creates each supported file dependency record in the same native batch as the body, counter, and tips.
The record counter removes the three fixed dependency keys when the last file reference removes the record body.
An ordinary write does not fetch or decrypt a file blob.

The bare key is `fdep:v1:{record_id}\0{main|thumbnail|poster}`.
The normal body scope also prefixes the dependency key.
The current native route places the record in `records`.
The normal account encryption protects the record.
The record stores no per-file encryption key.

Each record binds the following facts:

- The record identity and file kind.
- The SHA-256 of the exact whole decrypted pointer value, with its `$lastdb_file` wrapper.
- The original blob reference, plaintext digest, owner scope, selected encryption suite, and expected stored size.

The shared canonical JSON helper defines the pointer digest.
Main access uses the existing wrapper fields.
Thumbnail and poster access uses the existing nested `thumbnail.access` fields.
The directory does not guess a stored ciphertext digest at write time.

Unsupported local writes stay valid.
Their absent directory record cannot certify a later backup.
The first release refuses incomplete pointers, extra metadata-only references, unsupported key modes, and files outside the existing format and wire bounds.
It also refuses old pointer records without a matching directory record.
This source change adds no automatic backfill or migration.

## Exact retained source

`prepare_native_backup_capture` selects a complete native root and a durable named reference under the write gate.
The gate covers only the cut and hold metadata.
Hash passes, directory proof, file authentication, and cloud work follow gate release.
The full root requires equal data and journal frontiers.
There is no later journal suffix in this release.

`certify_file_dependencies` examines the retained native record inventory in bounded pages.
This is an explicit internal backup operation, not a logical query.
It visits one explicit native group at a time and uses `[0xff]` as the upper bound.
An empty end would describe an empty native range.
Each continuation seeks only that group; it does not reopen all 1024 groups per record.
For each body, it derives the exact scope and validates all three known dependency keys in one bounded batch.
An unexpected or absent kind refuses certification.
The final coverage check also examines live tip and active generation metadata.
Those file references must match a certified pointer on the selected record.
The check uses the existing selection, merge, and delete rules and the exact scoped record directory.
It does not rewrite a separate metadata map.
A record-only proof cannot cover independent metadata after record deduplication.
Capture saves a separate metadata coverage flag before it admits cloud eligibility.
An older record-certified sidecar can obtain this proof without loss of frozen copy or receipt progress.

The capture directory uses one durable lookup for each `(whole_pointer_digest, kind)`.
Several records can name one pointer.
Every record still needs its own valid atomic directory records.
The capture reuses one coverage entry and its frozen part transfers and receipts for that pointer and kind.

For a local CAS record, capture copies only the exact `cas_blobs` key at the held root into private scratch.
Its proof digest names the raw outer account-encrypted record bytes.
The native inventory already includes those bytes.
Capture does not add a second BlobPart for them.

The native copy checks the complete record header, key, mutation version, and record digest before success.
It returns the exact stored body length and digest without a body-sized allocation.
The proof opens the account envelope and the inner per-file envelope through bounded buffers and private disk files.
It uses the original pointer DEK and checks the plaintext digest and size.
Authentication requires the final encryption tag and exact EOF.
Compressed data also requires its complete deflate end marker and no trailing bytes.
Private output cannot become a successful proof before those checks finish.

The directory suite remains exactly the original pointer suite.
The pointer suite and the local CAS label must both be the account-v2 suite.
The verifier refuses every other suite.

## Backup-owned external bytes

Capture copies an external file only when the exact held CAS key is absent.
Corrupt local bytes or a failed key check cause refusal, rather than a cloud fallback.
The source order is:

1. An exact verified ciphertext file already in this capture's spool.
2. The local file recovery directory from an earlier restore.
3. The existing authorized main or thumbnail download API.

The local recovery lookup checks the home, whole pointer digest, kind, stored digest, size, and original key.
Thus a restored home can make its next backup after the original owner's object disappears.
An absent external source causes refusal.
There is no external-owner retention promise.

The S3 client writes authorized response bytes into a new private file.
It checks the declared response length when present and always checks the exact body length.
It uses a bounded buffer and syncs the complete file before success.
The existing version-one file envelope must authenticate under the original pointer key.
The plaintext digest, blob reference, suite, and complete stored size must match.
Capture then hashes the complete encrypted source bytes.

Capture saves and authenticates the complete ciphertext in its private job directory before it freezes the transport parts.
Each part contains at most 64 MiB of the original ciphertext.
One file has at most 512 ordered parts under the existing manifest format.
Each part has its own saved request identity, exact digest, length, and authoritative receipt.
Each part uses an ordinary complete-object transfer, without a provider multipart upload.

A retry keeps those identities and bytes.
A different remote ciphertext cannot replace a frozen part.
One unconfirmed external file is active at a time.

The capture API exposes the following operations:

- `prepare_external_file` obtains and authenticates the complete private ciphertext.
- `file_part` returns one saved `BackupFilePart` and its transfer input.
- `open_file_part` reads only that part through a bounded reader.
- `confirm_file_part` accepts its exact authoritative authenticated `TransferView::Verified`.
- `external_file_dependency` returns ordered manifest extents only after all part receipts exist.

A locally computed receipt does not establish authority.
Capture saves each verified receipt before it changes the complete-file counters.
It removes the local ciphertext only after all part receipts reach disk, then repeats the directory barrier.
Pending, rejected, missing, and uncertain responses retain the exact source copy.
The service obligation protects a confirmed object until the job reaches its authoritative terminal result.

The private directory therefore holds one active file, not all external bytes of the backup.
The one GiB metadata allowance does not cap that file's payload size.
Payload and decode scratch files require separate filesystem capacity under the native space policy.
The next worker must connect these source APIs to the shared transfer and acceptance APIs.

## Source bounds

| Item | First release bound |
|---|---:|
| External file ciphertext | At most 512 parts of 64 MiB; manifest page bytes also apply |
| Native stored record body | At most `u32::MAX` bytes in the existing native V1 format |
| Record stored envelope or decoded byte buffer | 4 MiB |
| Each charged record JSON tree | 32 MiB |
| Directory plaintext | 4 KiB |
| Returned tip record body | 4 MiB |
| Tip entry controls | 16 KiB |
| Each local dependency entry or lookup file | 16 KiB |
| Restore external identity set | 8 MiB / 43,690 pointer and kind pairs |
| Concurrent source streams, including capture work | 4 |
| Each source stream buffer | 1 MiB |
| Capture metadata and native catalog allowance | 1 GiB |
| Active ciphertext and decode scratch | Separate filesystem capacity admission |
| Native copy run | 16 decoded blocks, then cache release |
| Native payload file handles | 32 |

The [resource policy](native-backup-resource-policy-v1.md) identifies the shared source for these values.
The normal local file API has no separate configured file size cap.
Native record-format bounds and shared memory admission remain in place.
Metadata admission includes dependency records, aliases, part receipts, atomic-write scratch, and the native catalog.
The exact count depends on the complete encoded metadata size.
The first byte, count, format, or disk bound controls admission.
Metadata proof refuses noncanonical file references and unsupported stale overlay metadata.
It also refuses returned tip bodies or entry controls above the limits, even when those records are unrelated to a file.

Native `ReadBudget.bytes` counts cumulative file, decoded, and returned bytes.
It is not a retained-memory limit.
The native CAS lookup uses a bounded ordinary read budget and one exact key.
`HomeRead::copy_body_to` then uses a separate cumulative work allowance derived from that exact record.
It keeps bounded block and metadata caches as the copy advances.
The complete copy costs remain visible in the read receipt.

The new `get_bounded` checks the located body size before payload I/O or body allocation.
The three directory reads use `get_many_bounded` before a batch payload plan.

The source uses separate phases:

- Native CAS copy: bounded block runs copy the exact stored record body into private disk scratch.
- Account decode: bounded encryption and deflate buffers write private JSON scratch.
- CAS JSON decode: a bounded parser copies the selected inner envelope into a private file.
- Per-file proof: bounded encryption and deflate buffers check the original key, plaintext digest, size, tag, and EOF.
- Record decode: at most two charged 32 MiB JSON trees, small raw buffers, and bounded canonical JSON byte buffers.

The caller drops the native read before CAS decode.
The caller also drops record JSON before CAS proof or external authentication.
At most three other one-MiB source buffers coexist with capture work.
File length does not select the buffer allocation.

The 96 MiB cloud work policy does not cap the whole LastDB process.
These source bounds do not establish measured memory or transfer speed.

## Restart, terminal result, and cleanup

The existing `cloud-export-v1/job.json` path and format-one job schema stay unchanged.
The public API name changes to `backup_capture::NativeBackupCapture`.
File progress uses the private `dependencies-{commit_id}` directory with its own checksummed format-one records.
The directory records contain job and root identities, but no credentials or DEKs.
New multi-part progress keeps the whole ciphertext digest and counts in the dependency record.
Each `chunk-{dependency}-{part}.json` file keeps one bounded transfer input and receipt.
Existing single-object `BackupOwned` records remain readable.
`open_file_object` and `confirm_file_transfer` retain their part-zero compatibility path.

A partial certification can discard only its exact owned local metadata and repeat the held-root proof.
A complete certification retains its per-file copy and receipt progress across restart.
Normal dependency status reads saved counters, fixed state files, and only the one active external entry.
Overall capture status still checks each native inventory page's file size.
The explicit `audit_file_dependencies` method checks every local entry and byte count.
The next worker must call that audit after restart and before manifest construction; it must not poll that audit.
The active-file intent precedes its first source request.
An uncertain file deletion repeats its directory barrier before the next file starts.

A lost native source still permits metadata-only close and terminal confirmation through the existing capture coordinator.
The saved job can never substitute the current root.
Source release still requires an authoritative published or closed result.
The source and all readers share the process guard with restore.
Owned work guards also outlive an async caller when its file operation still runs on a blocking thread.
Terminal cleanup removes only recognized members of this exact job's private flat dependency directory.
It also works when its local dependency state is absent or corrupt.

## Remaining release gates

The Mini worker, manifest assembler, service client, and product commands still need integration.
The S2 service acceptance, discovery, and read-grant source exists separately.
Activation, a cache-free product restore, and provider behavior remain separate gates.
No live cloud result follows from these source checks.
The new disk-stream source path removes the former 16 MiB backup authentication guard.
It does not enlarge native V1 physical parts or the existing cloud object and extent limits.
Old uncertified pointer records need an explicit migration decision or a clear refusal in the product command.
GC stays disabled until a complete retained-root and external-object closure passes its own release gate.
