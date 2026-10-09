# Design: Cloud file blobs upload-only sync

| Field | Value |
|-------|-------|
| Status | Proposed contract |
| Date | 2026-07-16 |
| Product owner | Tom |
| Repo | `EdgeVector/fold` |
| North Star | `north-star-lastdb-file-blobs-on-demand-sync` |
| Related | `north-star-storage-metering-correctness`, `docs/designs/store-level-log-based-cloud-sync.md` |

## Summary

Personal file bytes are stored as encrypted content-addressed blobs in B2 under
`{scope}/cas/sha256/...`. LastDB cloud sync uploads newly-created blobs and
replicates structured metadata through the normal DB log and snapshot path, but
ordinary sync never bulk-downloads file blobs.

After a device join or bootstrap, the second device can list, query, and reason
about file-bearing records from sled metadata alone. Bytes reach local disk only
after an explicit request for a specific `$lastdb_file` pointer. A successful
fetch verifies the blob hash and caches the bytes in the local `cas_blobs` tree.

## Goals

1. Upload newly-created personal file blobs to B2 CAS after sealing them with a
   per-blob DEK.
2. Store enough `$lastdb_file` metadata in the DB for later explicit open:
   `blob_ref`, `file_hash`, `cipher_suite`, `dek`, encrypted size, name, and
   media type when known.
3. Keep ordinary cloud sync and bootstrap metadata-only for file blobs: log
   entries and snapshots may move pointers, but they do not list or GET
   `cas/sha256` objects.
4. Treat missing local blob bytes as a normal metadata state. Query, list, and
   schema operations must not fail just because local CAS lacks the payload.
5. Fetch bytes through one explicit on-demand path for a named pointer, then
   cache the verified bytes locally.
6. Preserve honest storage metering. Uploaded CAS objects count as
   `file_reference_bytes`; client estimates are never the permanent source of
   truth.

## Non-goals

- Bulk restore-on-sync or "download all my files" during ordinary cloud sync.
- Desktop, Tauri, DMG, or file-browser product work.
- Unmetered or display-only files.
- Storage v2 document-store migration.
- Changing the B2/R2 bucket layout for non-file sync objects.
- Using the primary `~/.lastdb` home for dogfood or destructive validation.

## Product Contract

### Write / upload path

When an app attaches a personal file, the durable outcome is:

1. Compute the plaintext SHA-256.
2. Seal the plaintext with a random per-blob DEK using
   `lastdb-file-blob-dek-v1:aes-256-gcm`.
3. PUT the ciphertext to B2 CAS at the content-addressed file object.
4. Best-effort confirm the upload for metering.
5. Store a `$lastdb_file` pointer and its `FileBlobAccess` metadata in the DB.

The pointer is the sync unit. The ciphertext is reachable from B2, but the DB
metadata is sufficient for structured reads and later explicit open.

Mini exposes the app-facing write path on the owner socket as
`POST /api/db/file-blob`. The JSON body is:

```json
{
  "schema": "namespace/Schema",
  "field": "file_reference_bytes",
  "key": { "hash": "record-id", "range": null },
  "bytes_b64": "<base64 plaintext>",
  "mutation_type": "update",
  "name": "optional-filename.ext",
  "media_type": "application/octet-stream",
  "cache_local_plaintext": false,
  "additional_fields": {}
}
```

`writer_pubkey` is optional; when absent, Mini uses the node public key, matching
the ordinary mutation route. The route is bounded by the UDS request body limit
and does not log or embed plaintext bytes in the DB record. A successful
response returns `file_blob.pointer`, `file_blob.access`, `blob_ref`,
`file_hash`, `mutation_ids`, and the plaintext byte count. If cloud sync is not
compiled in the route returns `501`; if the node has no cloud sync configuration
it returns `409`. The existing `POST /api/db/fetch-file-blob` route is the
corresponding explicit read path.

For large files, clients can avoid base64 expansion. Send
`Content-Type: application/octet-stream`, the raw plaintext bytes as the body,
and the same JSON fields except `bytes_b64` in the compact
`X-LastDB-File-Blob-Metadata` header. The response keeps the JSON report shape.
For fetch, send `Accept: application/octet-stream` with the existing pointer
JSON request. Mini returns the verified plaintext bytes and supplies
`X-LastDB-File-Blob-Ref`, `X-LastDB-File-Hash`, and
`X-LastDB-File-Blob-Bytes` response headers. The `lastdb` CLI exposes these
forms with `db put-file-blob --raw` and `db fetch-file-blob --raw`.

Limits. The socket reader caps one request body at 128 MiB
(`lastdb_uds::uds_http::MAX_BODY_LEN`). Base64 spends 4 wire bytes per 3
plaintext bytes, so the JSON form admits a blob of at most 96 MiB; the raw form
admits 128 MiB. The raw form also drops one encode and one decode of the whole
blob, so peak memory per request falls. The metadata rides in one header line
and is therefore bounded by `MAX_LINE_LEN` (8 KiB), not by the body cap: a
caller that packs a large `additional_fields` object into it receives a framing
error. Put such fields in the JSON form instead.

### Local blob plane (no sync engine)

The `cas_blobs` plane is local. Cloud sync does not own it. A node without a
sync engine can store and read blobs, and the cloud routes above keep their
behavior (`POST /api/db/file-blob` still answers `409` without an engine).

`POST /api/db/put-blob-local` (owner socket only) stores one blob and writes
**no record**. The body is the raw bytes (`Content-Type:
application/octet-stream`; optional `name` and `media_type` as JSON in
`X-LastDB-File-Blob-Metadata`) or JSON `{ "bytes_b64": "...", "name"?,
"media_type"? }`. Any other key is a `400`. The answer is
`file_blob.{pointer, blob_ref, file_hash, bytes, stored}`; `stored` is false
when an intact row for the same bytes was already there. The CLI form is
`lastdb db put-blob-local [--file PATH]` (stdin when `--file` is absent); it
prints the pointer.

- A blob is at most 16 MiB (one LastGit slab) unless the owner sets
  `LASTDB_LOCAL_FILE_BLOB_MAX_BYTES`. The route answers `413` above the limit,
  before it decodes or copies the body. The limit bounds memory: a sealed row
  is about 1.33 times the plaintext, one put holds the body, the sealed text and
  its serialized form at once, and the store layers copy the value again (seal,
  base64, group buffer). Store larger files as slabs.
- The row is sealed under the convergent key (the DEK is derived from the
  plaintext hash), so identical bytes give an identical `blob_ref`, DEK and
  pointer. Without `name` and `media_type` the pointer is identical too, so a
  second record that holds it adds one tip and no atom.
- The route syncs the group that holds the row before it answers. It does not
  flush the whole node: the sync names only that group, so other dirty groups
  do not slow the put. A durable record batch flushes only the groups it wrote,
  not `cas_blobs`, so without this sync a crash could keep a record that names a
  blob the node lost. Call the route, then write the pointer as the whole value
  of a field of type `Any`. **A pointer inside a JSON string is not a
  reference**: `gc-file-blobs` reclaims that blob once its row is older than
  600 s.
- A repeat put of the same bytes writes nothing while the row is younger than
  300 s. An older row is written back with a new `stored_at` (the same sealed
  text, no second seal); this keeps a retried push from losing its blob to
  `gc-file-blobs` (grace window 600 s) between the put and the pointer write.
  Nothing compacts `cas_blobs` automatically, so a rewrite leaves one dead copy
  on disk until an owner compacts that collection.
- A legacy plain row (written by the photo migration, which reads it without a
  key) stays plain. The put keeps an intact one, or renews its `stored_at`; it
  never seals it. A row that does not hash to its key is replaced by a sealed
  row.
- A full disk on the row write is `507 storage_full`, and capture backpressure
  is `503`, as for every other write route.
- `POST /api/db/fetch-file-blob` reads the local row first. With no sync engine
  it never asks the cloud: a missing row is `404`, a row that fails the
  sha256, `blob_ref` or size check is `422`. The code needs the `sharing`
  feature. The node binary builds with `cloud-sync` today, so "no sync engine"
  means a `cloud-sync` build with no engine configured.
- Nothing uploads a local blob to the cloud object store (the B2 CAS object).
  The whole-node backup does not exclude the `cas_blobs` plane
  (`BACKUP_EXCLUDED_EXACT` in `backup_manifest.rs`).

### Ordinary sync path

Ordinary sync moves DB state:

- sealed log entries
- snapshots
- sync cursors and replay metadata

It does not enumerate or download B2 CAS file objects. A device with no local
blob bytes after bootstrap is healthy as long as the `$lastdb_file` pointers and
all non-blob record fields are present.

### Explicit fetch path

The only cloud file-blob download in this design is an explicit request for one
pointer. `resolve_file_bytes_on_demand` is the current code anchor:

1. Check local `cas_blobs` for the requested `blob_ref`.
2. If present, return the local bytes without contacting B2.
3. If absent, require `FileBlobAccess` metadata on the pointer.
4. Request one presigned download for that `file_hash`.
5. Open the ciphertext with the per-blob DEK.
6. Verify the plaintext SHA-256 and `blob_ref`.
7. Store the verified bytes into local `cas_blobs`.

Wrong DEK, unsupported cipher suite, hash mismatch, or ref mismatch is a hard
failure. There is no silent fallback to garbage bytes.

### Delete / GC path

Deleting file metadata from the DB removes or tombstones the pointer through the
normal sync path. Deleting the B2 object is a separate explicit delete or GC
operation:

1. Presign delete for the file hash.
2. DELETE the B2 object.
3. Best-effort confirm delete for metering.

Peers that never downloaded a blob do not perform object deletes as a side
effect of ordinary sync.

## Invariants

| ID | Invariant |
|----|-----------|
| F1 | Ordinary `do_sync`, bootstrap, and device join issue zero B2 GETs for `cas/` and zero `presign_file_download` calls. |
| F2 | Structured file metadata lives in DB state and replicates through log or snapshot, not by object-listing B2 CAS. |
| F3 | Missing local blob bytes is not an error for list, query, schema, or metadata reads. |
| F4 | Blob download happens only through an explicit resolve/fetch for a named pointer. Delivery-slice import may intentionally carry companion blobs, but personal cloud sync does not. |
| F5 | On-demand fetch verifies plaintext hash against the pointer, caches on success, and does not re-fetch on local cache hit. |
| F6 | Upload metering is confirmed by server/object truth, not permanently by a client estimate. |
| F7 | Validation uses throwaway `LASTDB_HOME` and mock or non-primary storage. The primary Mini home is never the dogfood target. |

## Current Code Anchors

| Concern | Anchor |
|---------|--------|
| B2 upload/download/delete helpers | `fold_db/crates/core/src/sync/engine/file_blob.rs` |
| File pointer and access metadata | `fold_db/crates/core/src/sharing/delivery_wire.rs` |
| Local CAS tree | `fold_db/crates/core/src/sharing/blob_cas.rs` |
| On-demand fetch | `fold_db/crates/core/src/sharing/query_slice.rs` |
| Presign upload/download/delete auth calls | `fold_db/crates/core/src/sync/auth/ops/presign.rs` |

`file_blob.rs` already states the key sync posture: personal CAS file blobs are
uploaded and explicitly deleted, but ordinary sync/bootstrap/device-join must
not list or bulk-download `{scope}/cas/sha256/*`.

## Test Plan

### Unit and focused integration tests

- Upload stores ciphertext through the file upload presign path and confirms
  metering after object PUT.
- Upload/delete tests prove the backup path does not list or download CAS blobs.
- Explicit download opens with the returned DEK and rejects wrong DEK or hash
  mismatch.
- On-demand fetch stores a local cache entry and a second resolve hits local CAS
  without a second B2 GET.
- Query/list tests cover `$lastdb_file` pointers whose local bytes are absent.

Existing nearby tests live in `fold_db/crates/core/src/sync/engine/tests.rs`.
Future PRs should extend those tests rather than relying on prose only.

### Product harness

The North Star terminal proof should use a throwaway multi-device setup:

1. Device A writes a file-bearing record.
2. Device A uploads the ciphertext and stores the pointer in DB state.
3. Device B syncs from cloud.
4. Device B can list/query the record with no local CAS bytes.
5. Sync traffic records zero file CAS GETs and zero `presign_file_download`.
6. Device B explicitly fetches one pointer and performs exactly one blob GET.
7. Device B fetches the same pointer again and uses the local cache.

The harness should fail if the default sync cycle turns into a hidden bulk
restore path.

## Implementation Phases

| Phase | Outcome |
|-------|---------|
| P0 | This design contract lands on `main`. |
| P1 | Write path creates `$lastdb_file` pointer + B2 upload + metering confirm. |
| P1 | Sync cycle regression tests enforce zero file CAS downloads by default. |
| P1 | Structured reads work with pointer-only local state. |
| P2 | Mini/CLI/UDS exposes explicit fetch for one file pointer. |
| P2 | Device-join proof demonstrates metadata-only bootstrap. |
| Capstone | North Star harness writes `PASS` proof for `north-star-lastdb-file-blobs-on-demand-sync`. |

## Acceptance

- `docs/designs/cloud-file-blobs-on-demand-sync.md` is present on `main`.
- The invariants above are the reference contract for later file-blob PRs.
- Later implementation cards can cite this document instead of re-deriving
  upload-only sync policy from unit tests.
