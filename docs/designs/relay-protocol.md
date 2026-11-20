# LastDB relay protocol (open)

Status: design for review (2026-11-19). Companion to
`docs/designs/cloud-api.md`. Wire: `docs/designs/relay-protocol.openapi.json`.

**Goal:** smallest open protocol so anyone can stand up a relay if ExeMem
goes down. Signed data is relay-independent. ExeMem = conforming relay +
account/billing service. No dual paths, fallback, or unit tests.

## Trust

Canonical acceptance is **local** (`AcceptEntry`). Relays store, serve, and
apply a **spam/cost gate** from publisher-signed grant metadata. That gate
is **not** AcceptEntry: the relay does **not** decrypt payloads, check
Lamport/`seen_heads`, or run full merge. Nodes still verify everything
locally, so the trust model is unchanged — a relay that lets junk through,
or wrongly refuses, **cannot** make honest nodes disagree. It can only
withhold, which is the same as data loss, already handled by failover
through the publisher-signed `relays` entry.

**Logs are append-only and never trimmed** (except product erase).
Grants/revokes exist only as signed chain entries. The relay’s grant index
is **derived** from publisher-signed birth/grant entries it has verified
for gating — never a side channel. People carries signed heads for
comparison only.

**No snapshots.** File blobs may be dropped after a publisher
`blob_delete` chain entry that does not name a digest still referenced by
an accepted log entry (including `bundle` blobs). Product erase: ExeMem
`manage`/`delete` erases that database’s logs and blobs on the relay
(`database_id` stays tombstoned); `account`/`deactivate` erases all of
that publisher’s relay data.

## Surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/blob/put
POST /relay/v1/blob/get
```

Batching/caps match cloud-api. **Every** relay request is signed
(`nodeSignature` + `X-LastDB-Relay-Protocol: 1`), including reads.

### Request signature (`nodeSignature`)

Minimal signed request (Ed25519 over the node key). Companion headers
carry the fields; the signature covers:

| Field | Role |
|---|---|
| `method` + path | e.g. `POST` and `/relay/v1/log/append` |
| `node_key` | caller’s Ed25519 public key (hex) |
| `database_id` | target database |
| `body_sha256` | SHA-256 of the raw request body |
| `issued_at` | unix seconds (skew ±60s) |
| `nonce` | replay protection (≥2× skew retention) |

Wire lines (big-endian / hex as elsewhere):

```
POST\n<path>\n<protocol>\n<node_key>\n<database_id>\n<body_sha256 hex>\n<issued_at>\n<nonce>
```

Bad or missing signature → **401** `unauthorized`. Device id may still
appear as a companion header for append stream identity; it is **not**
part of the grant gate (grants are by `node_key`).

### Relay-side access gating (from grant entries)

The relay keeps a **per-`database_id` grant index**, updated as it accepts
publisher-signed `birth` / `grant` entries (including role reductions and
revokes) in **chain order**.

**Publisher.** Always allowed. The relay learns the publisher node key from
the `birth` entry, verified against `database_id` (the db public key) and
the birth signature.

**`log.append` / `blob.put`.** Accepted only when the signing `node_key` is
the publisher, **or** a publisher-signed grant in that database’s chain
currently gives that key **`write`** (direct/`bundle` appends and blob
puts) or **`submit`** (proposal `data` appends only), and no later revoke /
role reduction has removed that authority. A **revoke cuts off appends
immediately** (new requests → **403** `forbidden`). Entries the relay
already stored before the revoke **stay**; the relay does **not**
retroactively delete them. Nodes judge those with the existing per-device
cutoff rules.

**`log.read` / `blob.get`.** Require a current **read** grant for the
signing `node_key`, or an active **`grant#any` / `read`**. Meaning of
`grant#any`: **any caller with a valid request signature** may read — not
unsigned/anonymous access. There is no unsigned public path.

**What the relay verifies.** Publisher signatures on `birth`, `grant`
(including revoke / role-reduction), only — enough to update the grant
index. It does **not** decrypt record payloads or run full `AcceptEntry`.

**Readable grant metadata.** Because the relay must gate on them, grant /
revoke / role-reduction entries expose **grantee node key, role, and
cutoffs** as signed plaintext metadata the relay can see (not encrypted
inside the payload).

**Rejected alternative:** a shared database secret for append auth —
revoked members would keep it.

**Bootstrap / failover.** A new or empty relay builds the grant index by
reading the database’s logs from the `birth` entry onward and verifying
publisher-signed birth/grant entries in order. A failover relay rebuilds
the same way from the logs it is given (or fetches from another relay);
until birth+grants are verified, it refuses non-publisher writes and
non-`grant#any` reads.

### `log.append` / `log.read`

Store key `(database_id, node_key, device_id, writer_seq)`; `device_id` is
Hex32 (64 lowercase hex, 32 bytes in `signed_bytes`), never a UUID. Entries:
`birth`|`data`|`bundle`|`grant`|`blob_delete`|`heads`|`relays`.

Read by device chain from `from_seq` (default 1) or opaque delivery cursor
(pagination only; never expires). Nodes run `AcceptEntry` on read
(pending until `seen_heads` deps — and for `bundle`, a valid blob —
arrive).

### `blob.put` / `blob.get`

Blob keys are scoped per `database_id`, content-addressed by sha256 of
the stored (encrypted) bytes. Bundle writers `blob.put` the encrypted
bundle body (presigned; parallel or multipart allowed) **before**
`log.append` of the `bundle` entry. **`blob.get` returns exactly one
download path: a presigned URL** (plus headers, expiry, bytes, sha256).
After a verified, valid `blob_delete` for that digest, get may return
`not_found`. No new endpoints for bundles.

**Dedupe:** encryption with the db key plus a per-blob nonce means
identical plaintext yields different digests — no cross-entry digest
dedupe and no refcount. A `blob_delete` for a digest still referenced by
an accepted `bundle` (or other entry) is invalid; nodes ignore it.

## v3 map

| v3 | Relay |
|---|---|
| `push`/`mutations`, `fetch`/`mutations_since` | `log.*` |
| `push`/`file`, `fetch`/`file` | `blob.*` |
| `manage`/`grant`, `revoke` | out — `log.append` of signed grants |
| `manage`/`create` | `log.append` of signed `BirthEntry` |
| `manage`/`delete` | Erase that DB’s relay logs + blobs; id tombstoned |
| `account`/`deactivate` | Erase all publisher relay data (ExeMem-driven) |
| `account/*` (other) | not protocol |
| former `manage`/`access`, `snapshot`, `retention` | **removed** |

## Discovery and failover

Publisher-signed `relays` entry; People bootstraps first URL. Nodes may
append and read across several relays; compare publisher-signed `heads`
(and tip hashes); switch with no migration. A relay that withholds an
entry cited by `seen_heads` or `heads` is detected when the dependency
never arrives — no snapshot involvement. Equivocation freezes that stream;
recovery is a new `device_id`, not a relay feature. Failover relays rebuild
the grant index from verified publisher birth/grant entries in the logs.

## Defaults

| Topic | Default |
|---|---|
| Reference relay | In `lastdb` for CI |
| Public `grant#any` reads | Any **signed** caller may read when `grant#any`/`read` is active |
| Relay list | Publisher-signed `relays` + People |
| Log GC | Never (product erase via delete/deactivate only) |
| Blob GC | After valid chain `blob_delete`, or product erase |
| Bundle blob max | 64 MiB / 10_000 changes (node-enforced) |
| Download | Presigned URL only |
| Fork recovery | New `device_id` (frozen stream; no resolve) |
| Accounts | ExeMem: publisher account for quota/billing only; grantees need none. Pure relays need no accounts. |
| Grant checks | Per-db grant index from publisher birth/grant entries (spam/cost only) |
| Quota | Charged to the database’s **publisher** (publisher pays) |

## Errors

| HTTP | code | notes |
|---|---|---|
| 400 | `invalid_request` | arg paths; `grants.any.role` (any must be read); signatures |
| 400 | `batch_too_large` | batch caps |
| 401 | `unauthorized` | bad or missing request signature |
| 403 | `forbidden` | no current grant for this `node_key` / role |
| 404 | `not_found` | missing log/blob (incl. after `blob_delete`) |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | quota / rate limits (publisher-billed) |
| 500 | `unavailable` | unknown or service Dynamo errors; retry (same as 429) |

`payment_required` when ExeMem billing refuses the publisher. Relay grant
refusals use **403** `forbidden` (spam/cost gate), not AcceptEntry. A 500
`unavailable` is not a chain conflict and is not final.

## CI conformance

Same scenarios as `cloud-api.md` against ExeMem and the in-repo reference
relay, including failover, fork/withhold, bootstrap via bundles, restore-
from-start, the bundle few-hundred-MB under-a-minute upload/restore check,
**non-grantee append refused (403)**, and **revoked member append refused
(403)**. Account paths ExeMem-only.
