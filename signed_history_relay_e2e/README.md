# signed_history_relay_e2e

Bundle bootstrap and restore proof against a live relay. It is the executable behind
`LASTDB_RELAY_E2E_BIN` in `EdgeVector/exemem-infra` (`prove_dev_relay.py`, exemem-infra#927).
It needs no secrets: it mints a fresh publisher, database and reader on every run.

## Build

```
cargo build --locked -p signed_history_relay_e2e --bin signed_history_relay_e2e --release
# artifact: target/release/signed_history_relay_e2e
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

stdout ends with exactly one JSON object; progress goes to stderr. Exit 0 only when both phases are under
the limit. Failures print `{"ok": false, "error": "..."}` and exit 1.

```json
{"ok": true, "error": null, "upload_seconds": 0.0, "restore_seconds": 0.0, "limit_seconds": 60.0,
 "bytes_uploaded": 0, "record_text_bytes": 0, "records": 6400, "bundles": 7, "streams_discovered": 2,
 "history_seconds": 0.0, "materialize_seconds": 0.0, "generate_encrypt_cpu_seconds": 0.0,
 "database_id": "<64 hex>"}
```

`upload_seconds` covers key and birth setup, generating and encrypting every bundle, `blob.put` of every blob,
and `log.append` of the chain. `restore_seconds` covers stream discovery, `log.read`, `blob.get`, decrypt,
`AcceptEntry` and the FoldDB materialize. Checks run after the timer stops.

## Protocol used

- Every call carries `nodeSignature`: Ed25519 over
  `POST\n<path>\n1\n<node_key>\n<database_id>\n<body_sha256>\n<issued_at>\n<nonce>` with headers
  `X-LastDB-Relay-Protocol`, `-Node-Key`, `-Database-Id`, `-Device-Id`, `-Issued-At` (unix seconds), `-Nonce`,
  `-Signature`. The device id is a Hex32 (64 hex) in headers, JSON, and signed bytes; it is not signed in the request.
- Roles used: `read` (the restoring node). The publisher writes directly.
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
reads the root stream from seq 1, accepts it, then follows accepted publisher `heads` tips and grant cutoffs to
the remaining streams. No device-id list is passed. Bundle entries are applied after the blob hashes to the entry's
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
> `restore_seconds` (each must be under 60 s).
