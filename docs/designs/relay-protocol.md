# LastDB relay protocol (open)

Status: design for review (2026-11-19). Companion to
`docs/designs/cloud-api.md`. Wire: `docs/designs/relay-protocol.openapi.json`.

**Goal:** smallest open protocol so anyone can stand up a relay if ExeMem
goes down. Signed data is relay-independent. ExeMem = conforming relay +
account/billing service. No dual paths, fallback, or unit tests.

## Trust

Acceptance is **local** (`AcceptEntry`). Relays only store and serve.
**Logs are append-only and never trimmed.** Grants/revokes exist only as
signed chain entries. Any grant cache for anti-spam is derived, optional,
never authoritative. People carries signed heads for comparison only.

**No snapshots.** File blobs may be dropped only after a publisher
`blob_delete` chain entry. All other relay policy is anti-spam / quota.

## Surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/blob/put
POST /relay/v1/blob/get
```

Batching/caps match cloud-api. Auth: `nodeSignature` +
`X-LastDB-Relay-Protocol: 1`.

### `log.append` / `log.read`

Store key `(database_id, node_key, device_id, writer_seq)`. Entries:
`birth`|`data`|`grant`|`blob_delete`|`heads`|`relays`|`resolve`.

Read by device chain from `from_seq` (default 1) or opaque delivery cursor
(pagination only; never expires). Nodes run `AcceptEntry` on read.

### `blob.put` / `blob.get`

Content-addressed by sha256. **`blob.get` returns exactly one download
path: a presigned URL** (plus headers, expiry, bytes, sha256). After a
verified `blob_delete` for that digest, get may return `not_found`.

## v3 map

| v3 | Relay |
|---|---|
| `push`/`mutations`, `fetch`/`mutations_since` | `log.*` |
| `push`/`file`, `fetch`/`file` | `blob.*` |
| `manage`/`grant`, `revoke`, `access` | out — `log.append` of signed grants |
| `manage`/`create` | `log.append` of signed `BirthEntry` |
| `manage`/`delete` | ExeMem product tombstone only; log stays |
| `account/*` | not protocol |
| former `snapshot` / `retention` / snapshot_proof | **removed** |

## Discovery and failover

Publisher-signed `relays` entry; People bootstraps first URL. Nodes may
append and read across several relays; compare publisher-signed heads;
switch with no migration. A relay that serves a shorter or forked log is
detected by head/hash-chain compare — no snapshot involvement.

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
relay, including failover, fork/withhold, bootstrap replay equals source,
and restore-from-start. Account paths ExeMem-only.
