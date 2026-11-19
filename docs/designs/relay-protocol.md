# LastDB relay protocol (open)

Status: design for review (2026-11-19). Companion to
`docs/designs/cloud-api.md` (`AcceptEntry`, device chains, grants,
revocation cutoff, publisher-signed heads). Wire:
`docs/designs/relay-protocol.openapi.json`.

**Goal:** smallest open protocol so anyone can stand up a relay if ExeMem
is down. Signed data is relay-independent. ExeMem = conforming relay +
separate account/billing service. No dual paths, fallback, or unit tests.

## Trust

Acceptance is **local** (`AcceptEntry` in `cloud-api.md`). Relays only
store and serve. Optional anti-spam/quota is policy, not truth. Grants and
revocations exist only as signed chain entries; a relay must not expose a
revocation intent or index. Any grant cache for anti-spam is derived from
the log, optional, and never authoritative. People is not a revocation
channel — signed heads for comparison only.

## Surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/blob/put
POST /relay/v1/blob/get
POST /relay/v1/snapshot/put
POST /relay/v1/snapshot/get
POST /relay/v1/retention/request
```

Batching/caps match cloud-api. Auth: `nodeSignature` +
`X-LastDB-Relay-Protocol: 1`.

### `log.append` / `log.read`

Store key: `(database_id, node_key, device_id, writer_seq)`.
Entries are opaque signed bytes (`birth`|`data`|`grant`|`heads`|`relays`|
`resolve`). No checkpoint kind.

Read by device chain (`writer_key` + `device_id` + `from_seq`) or opaque
delivery cursor (not canonical order). Nodes run `AcceptEntry` on read.

### `blob.*` / `snapshot.*`

Content-addressed by sha256. Snapshot `restore_point_id` = manifest
digest. Presigned URLs optional; conforming relays that omit them return a
download URL or equivalent byte transfer (implementation choice).

### `retention.request`

**Required** for conformance. Publisher-signed (`lastdb-retention-v1`).
Accepted **only** for data older than a publisher-signed snapshot the
relay still holds (or `delete_database`).

## v3 map

| v3 | Relay |
|---|---|
| `push`/`mutations`, `fetch`/`mutations_since` | `log.*` |
| `push`/`file`, `fetch`/`file` | `blob.*` |
| `push`/`snapshot`, `fetch`/`snapshot` | `snapshot.*` |
| `manage`/`grant`, `revoke`, `access` | out of surface — same signed `grant` chain entries via `log.append`; no revoke side channel |
| `manage`/`create` | `log.append` of `birth` |
| `manage`/`delete` | `retention.request` |
| `account/*`, `maintenance/*` | not protocol (ExeMem service) |

## Discovery and failover

Publisher-signed `relays` entry lists base URLs. People bootstraps the
first URL. Nodes push/read several relays; compare publisher-signed heads;
switch with no migration.

## Defaults

| Topic | Default |
|---|---|
| Reference relay | In `lastdb` for CI conformance |
| Public `grant#any` reads | Open on every conforming relay |
| Relay list | Publisher-signed `relays` + People bootstrap |
| Retention | Required; only older than a publisher-signed snapshot |
| Freshness | Head comparison only |
| Accounts | Not required on the relay; ExeMem bills via its account service |

## CI conformance (DEV E2E)

Same scenarios as `cloud-api.md` CI against ExeMem and the in-repo
reference relay, including failover and fork/withhold detection. Account
paths ExeMem-only.
