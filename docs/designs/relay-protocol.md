# LastDB relay protocol (open)

Status: design for review (2026-11-19). Companion to
`docs/designs/cloud-api.md`. Wire: `docs/designs/relay-protocol.openapi.json`.

**Goal:** smallest open protocol so anyone can stand up a relay if ExeMem
goes down. Signed data is relay-independent. ExeMem = conforming relay +
account/billing service. No dual paths, fallback, or unit tests.

## Trust

Acceptance is **local** (`AcceptEntry`). Relays only store and serve.
**Logs are append-only** — relays never delete log entries. Grants/revokes
exist only as signed chain entries; no revoke intent or index. Any grant
cache for anti-spam is derived, optional, never authoritative. People
carries signed heads for comparison only.

Snapshots are a restore-speed optimization, never authoritative (see
`cloud-api.md`). File blobs may be dropped only after a publisher
`blob_delete` chain entry.

## Surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/blob/put
POST /relay/v1/blob/get
POST /relay/v1/snapshot/put
POST /relay/v1/snapshot/get
```

No `retention.request`. Batching/caps match cloud-api. Auth:
`nodeSignature` + `X-LastDB-Relay-Protocol: 1`.

### `log.append` / `log.read`

Store key `(database_id, node_key, device_id, writer_seq)`. Entries:
`birth`|`data`|`grant`|`blob_delete`|`heads`|`relays`|`resolve`.

Read by device chain or opaque delivery cursor (not canonical order).
Nodes run `AcceptEntry` on read.

### `blob.put` / `blob.get`

Content-addressed by sha256. **`blob.get` returns exactly one download
path: a presigned URL** (plus headers, expiry, bytes, sha256). After a
verified `blob_delete` for that digest, get may return `not_found`;
readers treat it as intentionally gone.

### `snapshot.put` / `snapshot.get`

Publisher uploads snapshot bytes (manifest digest = `restore_point_id`)
and records covered per-device heads in the snapshot metadata.
**`snapshot.get` returns exactly one download path: a presigned URL.**
Nodes must still fetch the full log and verify replay (`cloud-api.md`).

## v3 map

| v3 | Relay |
|---|---|
| `push`/`mutations`, `fetch`/`mutations_since` | `log.*` |
| `push`/`file`, `fetch`/`file` | `blob.*` |
| `push`/`snapshot`, `fetch`/`snapshot` | `snapshot.*` |
| `manage`/`grant`, `revoke`, `access` | out — `log.append` of signed grants |
| `manage`/`create` | `log.append` of signed `BirthEntry` |
| `manage`/`delete` | ExeMem product tombstone only; log stays |
| `account/*`, `maintenance/*` | not protocol |

## Discovery and failover

Publisher-signed `relays` entry; People bootstraps first URL. Multi-relay
push/read; compare heads; switch with no migration.

## Defaults

| Topic | Default |
|---|---|
| Reference relay | In `lastdb` for CI |
| Public `grant#any` reads | Open on every conforming relay |
| Relay list | Publisher-signed `relays` + People |
| Log GC | Never |
| Blob GC | Only after chain `blob_delete` |
| Download | Presigned URL only |
| Accounts | Not required on relay |

## CI conformance

Same scenarios as `cloud-api.md` against ExeMem and the in-repo reference
relay (failover, fork/withhold, snapshot mismatch, lamport, cutoffs).
Account paths ExeMem-only.
