# Native backup file capture, first release

This source change extends Backup Capture with a file dependency directory and exact source proof.
It does not activate a cloud worker, publish a backup, or change a native pack file.
The shared protocol remains `native-cloud-backup-protocol-v1.md`.

## Atomic directory

The atom counter creates each supported file dependency record in the same native batch as the body, counter, and tips.
The atom counter removes the three fixed dependency keys when the last file reference removes the atom body.
An ordinary write does not fetch or decrypt a file blob.

The bare key is `fdep:v1:{atom_id}\0{main|thumbnail|poster}`.
The normal body scope also prefixes the dependency key.
The current native route places the record in `atoms`.
The normal account encryption protects the record.
The record stores no per-file encryption key.

Each record binds the following facts:

- The atom identity and file kind.
- The SHA-256 of the exact whole decrypted pointer value, with its `$lastdb_file` wrapper.
- The original blob reference, plaintext digest, owner scope, selected encryption suite, and expected stored size.

The shared canonical JSON helper defines the pointer digest.
Main access uses the existing wrapper fields.
Thumbnail and poster access uses the existing nested `thumbnail.access` fields.
The directory does not guess a stored ciphertext digest at write time.

Unsupported local writes stay valid.
Their absent directory record cannot certify a later backup.
The first release refuses incomplete pointers, extra metadata-only references, unsupported key modes, and files above its byte limits.
It also refuses old pointer atoms without a matching directory record.
This source change adds no automatic backfill or migration.

## Exact retained source

`prepare_native_backup_capture` selects a complete native root and a durable named reference under the write gate.
The gate covers only the cut and hold metadata.
Hash passes, directory proof, file authentication, and cloud work follow gate release.
The full root requires equal data and journal frontiers.
There is no later journal suffix in this release.

`certify_file_dependencies` examines the retained native atom inventory in bounded pages.
This is an explicit internal backup operation, not a logical query.
It visits one explicit native group at a time and uses `[0xff]` as the upper bound.
An empty end would describe an empty native range.
Each continuation seeks only that group; it does not reopen all 1024 groups per record.
For each body, it derives the exact scope and validates all three known dependency keys in one bounded batch.
An unexpected or absent kind refuses certification.

The capture directory uses one durable lookup for each `(whole_pointer_digest, kind)`.
Several atoms can name one pointer.
Every atom still needs its own valid atomic directory records.
The capture reuses one coverage entry, frozen transfer, and receipt for that pointer and kind.

For a local CAS record, capture reads only the exact `cas_blobs` key at the held root.
Its proof digest names the raw outer account-encrypted record bytes.
The native inventory already includes those bytes.
Capture does not add a second BlobPart for them.

The proof opens the account envelope and the inner per-file envelope.
It uses the original pointer DEK and checks the plaintext digest and size.
The directory suite remains exactly the original pointer suite.
The local constructor labels its shared at-rest codec with the convergent suite, even for a legacy pointer DEK.
The verifier permits that specific legacy-pointer/current-local-label pair.
The label alone does not prove how the key was derived.
Legacy local CAS records without per-file encryption remain uncertified.

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

The S3 client checks the response length and enforces a streamed byte cap before buffer growth.
The existing version-one file envelope must authenticate under the original pointer key.
The plaintext digest, blob reference, suite, and complete stored size must match.
Capture then hashes the complete encrypted source bytes.

Capture saves the verified ciphertext in its private job spool before it saves the frozen transfer input.
A retry uses the same request identity and exact ciphertext digest.
A different remote ciphertext cannot replace a frozen input.
One unconfirmed external file is active at a time.

`confirm_file_transfer` accepts an exact authoritative authenticated `TransferView::Verified` from its caller.
A locally computed receipt does not establish authority.
Capture saves the verified receipt before it removes the local copy and repeats the directory barrier.
Pending, rejected, missing, and uncertain responses retain the exact source copy.
The service obligation protects a confirmed object until the job reaches its authoritative terminal result.

The spool therefore holds one active file, not all external bytes of the backup.
Backup data can exceed one GiB when the metadata reservation still fits.
The next worker must connect these source APIs to the shared transfer and acceptance APIs.

## Source bounds

| Item | First release bound |
|---|---:|
| File plaintext | 16 MiB |
| Remote file ciphertext | 16 MiB + 29 bytes |
| Native CAS stored or decoded record | 32 MiB |
| Atom stored envelope or decoded byte buffer | 4 MiB |
| Each charged atom JSON tree | 32 MiB |
| Directory plaintext | 4 KiB |
| Each local dependency entry or lookup file | 16 KiB |
| Restore external identity set | 8 MiB / 43,690 pointer and kind pairs |
| Concurrent source streams, including capture work | 4 |
| Each source stream buffer | 1 MiB |
| Combined durable capture spool | 1 GiB |

The capture reserves two 16 KiB metadata files per unique dependency, plus three crash slots and one MiB of fixed metadata.
It also includes the existing native catalog bytes and two maximum external ciphertext buffers in the disk reservation.
This conservative admission can refuse roughly thirty thousand unique dependencies before the wire-format record maximum.
The exact count depends on the native catalog size.
It does not claim support for every possible wire-format maximum at once.

Native `ReadBudget.bytes` counts cumulative file, decoded, and returned bytes.
It is not a retained-memory limit.
The native CAS read uses 128 MiB of cumulative work, 512 metadata pages, and one exact key.
The new `get_bounded` checks the located body size before payload I/O or body allocation.
The three directory reads use `get_many_bounded` before a batch payload plan.

The source allocation bounds use separate phases:

- Native CAS read: at most 32 MiB of body, about 32 MiB of decoded blocks, eight MiB of metadata pages, and bounded maps.
- Account decode: the owned helper drops the stored buffer before decrypt, then drops ciphertext before bounded inflate.
- Flat CAS JSON decode: at most 32 MiB of JSON bytes and 32 MiB of owned strings; it then drops the JSON bytes.
- Per-file decode: the helper consumes the inner envelope string before decrypt or inflate; plaintext stays at most 16 MiB.
- Atom decode: at most two charged 32 MiB JSON trees, small raw buffers, and bounded canonical JSON byte buffers.

The caller drops the native read transaction before CAS decode.
The caller also drops atom JSON before CAS proof or external authentication.
The fixed-capacity inflate loop rejects the first excess bytes before a vector can double past its cap.
At most three other one-MiB source buffers coexist with capture work.
These source bounds leave space below the 96 MiB cloud work cap.
They are source conclusions, not measurements from a live node.

## Restart, terminal result, and cleanup

The existing `cloud-export-v1/job.json` path and format-one job schema stay unchanged.
The public API name changes to `backup_capture::NativeBackupCapture`.
File progress uses the private `dependencies-{commit_id}` directory with its own checksummed format-one records.
The directory records contain job and root identities, but no credentials or DEKs.

A partial certification can discard only its exact owned local metadata and repeat the held-root proof.
A complete certification retains its per-file copy and receipt progress across restart.
Normal file status reads saved counters, fixed state files, and only the one active external entry.
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
Files above 16 MiB need an existing-format streaming authentication API before this release can admit them.
Old uncertified pointer records need an explicit migration decision or a clear refusal in the product command.
GC stays disabled until a complete retained-root and external-object closure passes its own release gate.
