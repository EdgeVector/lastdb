# signed_history_relay_e2e

Bundle bootstrap and restore proof against a live relay. It is the executable behind
`LASTDB_RELAY_E2E_BIN` in `EdgeVector/exemem-infra` (`prove_dev_relay.py`, exemem-infra#927).
It needs no secrets: it mints a fresh publisher, database and reader on every run.

## Build

```
cargo build --locked -p signed_history_relay_e2e --bin signed_history_relay_e2e --release
export LASTDB_RELAY_E2E_BIN=$PWD/target/release/signed_history_relay_e2e
export RELAY_URL=...   # relay origin, e.g. the DEV relay or a local reference_relay
$LASTDB_RELAY_E2E_BIN
```

Run CI builds only (this repo's rule); do not build it on a developer machine.

## What exemem-infra `deploy.yml` does

1. Check out `EdgeVector/lastdb` at the Phase 2 SHA (or the pinned fold submodule once it points there).
2. Build the bin with the same Rust toolchain as lastdb (`rust-toolchain.toml`) using the command above.
3. Export `LASTDB_RELAY_E2E_BIN=$PWD/target/release/signed_history_relay_e2e`, then run `prove_dev_relay.py`.

## Contract

Environment (flags exist for each):

| Variable | Default | Meaning |
|---|---|---|
| `RELAY_URL` | required | Relay origin, set by the proof script |
| `LASTDB_RELAY_E2E_TOTAL_BYTES` | 300 MiB | Synthetic record text to upload |
| `LASTDB_RELAY_E2E_CONCURRENCY` | 4 | Blobs in flight, clamped to 1..=8 |
| `LASTDB_RELAY_E2E_LIMIT_SECONDS` | 60 | Each phase must finish under this |
| `LASTDB_RELAY_E2E_WORK_DIR` | temp dir | Scratch FoldDB for the restore; removed on exit |
| `LASTDB_RELAY_E2E_STREAMS` | off | `true` also proves `log.streams` (see "Opt-in steps") |
| `LASTDB_RELAY_E2E_BLOB_FILE_BYTES` | 0 (off) | Also puts and gets one generated file of this size through the blob routes, from and to disk |
| `LASTDB_RELAY_E2E_CLEANUP` | true | Delete the blobs the run uploaded at the end (see "Cleanup"); `false` keeps them |

stdout ends with exactly one JSON object; progress goes to stderr. Exit 0 only when both phases are under
the limit. Failures print `{"ok": false, "error": "..."}` and exit 1.

```json
{"ok": true, "error": null, "upload_seconds": 0.0, "restore_seconds": 0.0, "limit_seconds": 60.0,
 "bytes_uploaded": 0, "record_text_bytes": 0, "records": 6400, "member_records": 1, "bundles": 7,
 "roles": ["read", "write", "revoked"], "grant_revoke_ok": true, "revoked_http_status": 403,
 "streams_discovered": 3, "history_seconds": 0.0, "materialize_seconds": 0.0,
 "generate_encrypt_cpu_seconds": 0.0, "database_id": "<64 hex>",
 "cleanup": {"ok": true, "error": null, "blobs_dropped": 7, "released_bytes": 0, "databases": [
   {"database_id": "<64 hex>", "ok": true, "blobs_recorded": 7, "bytes_recorded": 0, "blobs_listed": 7,
    "bytes_listed": 0, "blobs_dropped": 7, "released_bytes": 0, "absent": 0, "drop_calls": 1,
    "readback": {"blob_list_after": 0, "blob_get_not_found": 7, "object_gone": 7,
                 "object_answers": ["403 AccessDenied"]},
    "publisher_log_entries_kept": 17}]}}
```

`upload_seconds` covers key and birth setup, generating and encrypting every bundle, `blob.put` of every blob,
and `log.append` of the chain. `restore_seconds` covers stream discovery, `log.read`, `blob.get`, decrypt,
`AcceptEntry` and the FoldDB materialize. Checks run after the timer stops.

`blobs_seconds` is the wall time of the bundle puts alone. `blob_puts` has one object per bundle: `index`,
`bytes`, `begin_seconds`, `put_seconds` and `commit_seconds`, and `attempts`, each try of the presigned PUT with
its `seconds`, `sent_bytes` and `outcome` (`HTTP 200`, `HTTP 503`, or the stall or transport error). The PUT goes
through `relay_transfer`, the uploader the node uses: a fresh connection each time, a failure after 30 s without
progress, and at most 3 tries. stderr prints the same per bundle, and each retried relay call.

## Cleanup

On by default (`LASTDB_RELAY_E2E_CLEANUP=true`). After the steps end, also after one failed, the run deletes the
blobs it uploaded, as the publisher of the database it minted (`src/cleanup.rs`; the database id comes from a fresh
random key, so the run acts on nothing it did not write):

1. `blob/list` names every committed blob of the database; `blob/get` gives a presigned GET of each object, and a
   one-byte ranged GET of it must succeed (the control for step 4).
2. One `snapshot` entry goes on a fresh publisher device stream, because `blob/drop` names one. Its manifest digest
   is never uploaded, so no blob has to stay.
3. `blob/drop` drops the listed blobs (at most 25 per call), naming the head of every publisher stream from
   `log/streams` as its fence; a 409 reads the heads again (3 tries).
4. Read-back: `blob/list` is empty, `blob/get` of each digest is 404, and the same unexpired presigned GET is refused
   by the object store: 404, or 403 when the signing credential may not list the bucket (the store then hides
   whether the key exists). The control in step 1 shows the URL itself was good.

The report goes under `cleanup`. A failed cleanup sets `ok` to false and exits 1; it fills `error` only when no
step failed first, so the first failure is never hidden. Log entries stay (a few KB): `log/trim` drops only sealed
entries of a personal database, and no relay route deletes birth, grant or bundle entries. The `log.streams`
database holds no blob. The Exemem relay allows `blob/drop` to the publisher of any database; `reference_relay`
allows it only on a personal database (no grants), so CI's reference_relay runs set `LASTDB_RELAY_E2E_CLEANUP=false`.

## Opt-in steps

Both are off by default, so a relay that lacks the feature keeps passing the existing steps. The JSON report
carries `streams_steps` (the step names that passed, or `null`) and `blob_file_stream` (or `null`).

- `LASTDB_RELAY_E2E_STREAMS=true` runs its own publisher, reader, member and stranger on its own database and proves
  `log.streams`: `streams_empty_writer`, `streams_two_devices`, `streams_granted_callers_list`,
  `streams_head_advances`, `streams_contiguous_head` (a gap keeps the head back; filling it moves it),
  `streams_nongrantee_refused` and `streams_revoked_refused` (each refused with the same 403 code and field as an
  all-devices `log.read`). `streams_contiguous_head` appends seq 1 and 3 before seq 2, which a relay that enforces
  gapless appends refuses; such a relay must skip that step.
- `LASTDB_RELAY_E2E_BLOB_FILE_BYTES=<n>` generates an `n`-byte file in the work dir, puts it with `blob.put`
  streaming from disk, gets it back streaming to disk, and compares SHA-256 of both files. The relay's peak
  memory is checked by `scripts/signed_history/check_relay_process.py` in CI (it needs the relay's pid).

## Protocol used

- Every call carries `nodeSignature`: Ed25519 over
  `POST\n<path>\n1\n<node_key>\n<database_id>\n<body_sha256>\n<issued_at>\n<nonce>` with headers
  `X-LastDB-Relay-Protocol`, `-Node-Key`, `-Database-Id`, `-Device-Id`, `-Issued-At` (unix seconds), `-Nonce`,
  `-Signature`. The device id is a Hex32 (64 hex) in headers, JSON, and signed bytes; it is not signed in the request.
- Roles used: `read` (the restoring node), `write` (a member), `revoked` (that member after the revoke). The
  publisher writes directly.
- Grant and revoke: the publisher grants `write` to a member, the member reads the publisher stream, checks it with
  AcceptEntry, and appends one proposal. A direct entry from the member must get HTTP 403, because only the
  publisher signs direct entries. The publisher then appends an incorporation (a direct entry that cites the proposal)
  and a `revoke` grant. Once the revoke is stored, the member's next proposal `log.append` and `blob.put` must both
  get HTTP 403 (`grant_revoke_ok`). The member's proposal stays stored, and its record survives the restore because
  the publisher incorporated it.
- Blobs follow `blob.put` begin, presigned PUT, commit, then the `bundle` entry is appended; readers use
  `blob.get` and a presigned GET.

## How 300 MiB is generated

- Record `i` is the JSON `{"v": "<48 KiB text>"}`; the text is the base64 of 36 KiB from a ChaCha generator seeded by `i`.
  Incompressible, deterministic, and under the 64 KiB record cap.
- 6400 records (300 MiB of text) go into 7 bundles of at most 1000 records. Each bundle blob is about 49 MB, under the
  64 MiB cap, and AES-256-GCM encrypted (HKDF of the database key seed, a fresh random nonce per blob, so digests differ).
  About 315 MB of ciphertext is uploaded in total.
- Bundles alternate between two device streams of the publisher node. The last entry is a publisher `heads`
  entry citing both tips.
- Up to `LASTDB_RELAY_E2E_CONCURRENCY` bundles generate, encrypt, and transfer at once; generation and encryption run on
  blocking threads.

## Restore and discovery

The invite carries the database id, the publisher node key, one root device id, and the bundle secret. The restorer
reads the root stream from seq 1, accepts it, then follows accepted publisher `heads` tips (the publisher's other
device) and the `incorporated_from` of an incorporation (the member's device) to the remaining streams. No device-id list is passed. Bundle entries are applied after the blob hashes to the entry's
digest and decrypts; a missing or wrong blob fails the restore. Afterwards nothing may be pending, deferred or
rejected, and every tip in the latest `heads` entry must match the held hash. The winners are written into a fresh
FoldDB in batches of 256 creates (one native commit each), then the history equals the generator for all records
and 128 keyed reads match.

## Notes for reviewers

- The relay needs the OpenAPI signature (database id signed). The in-repo `reference_relay` verifies it, so this bin
  also runs against it: `reference_relay --listen 127.0.0.1:0 --data-dir DIR`, then `RELAY_URL=http://<addr>`.
- The blob framing and key derivation here are local to this bin. Swap in the shared client crates once they expose
  the same operations.

## PR-ready blurb

> Adds `signed_history_relay_e2e`, the `LASTDB_RELAY_E2E_BIN` used by exemem-infra#927 `prove_dev_relay.py`.
> Build: `cargo build --locked -p signed_history_relay_e2e --bin signed_history_relay_e2e --release`
> (artifact `target/release/signed_history_relay_e2e`). It signs requests per the relay OpenAPI `nodeSignature`
> with Hex32 device ids, uploads ~300 MiB as 7 encrypted bundles of at most 64 MiB, restores them with stream
> discovery from birth/grant/heads into FoldDB, and prints one JSON object last with `upload_seconds` and
> `restore_seconds` (each must be under `limit_seconds`, default 60 s).
