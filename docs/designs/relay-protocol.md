# LastDB relay protocol (open)

Status: design for review (2026-11-19). Not shipped. Companion to
`docs/designs/cloud-api.md` (trust model, `AcceptEntry`, signed history,
grants, checkpoints). Wire shapes:
`docs/designs/relay-protocol.openapi.json`. When this document and that
OpenAPI file differ, OpenAPI wins for paths, auth headers, and error shapes.

**Goal:** the smallest open protocol so that if ExeMem goes down, anyone
can stand up another relay. Signed data is relay-independent. ExeMem is
one conforming implementation plus a separate **account/billing service**,
not a second API.

No dual paths, fallback, migration shims, or unit tests.

## Trust boundary

**Acceptance is a local node rule.** Relays neither enforce nor know about
it. The deterministic function `AcceptEntry` is defined once in
`cloud-api.md` § Trust model; this protocol only stores and relays signed
bytes. Nodes re-run `AcceptEntry` on everything they read.

| Relay may do (policy) | Relay must not be trusted for |
|---|---|
| Store and return bytes | Acceptance / authorization |
| Optional anti-spam (signature shape, chain shape, quota, “key known”) | Canonical order or completeness |
| Billing / rate limits | Integrity of history |

A different conforming relay may skip anti-spam entirely. Nodes must not
rely on a relay accept or reject for correctness. Publisher hash chains and
signed heads (see `cloud-api.md`) detect withholding and forks.

## Relationship to Cloud API v3

| Layer | What it is |
|---|---|
| **Relay protocol** (this doc) | Append/read signed log entries; put/get content-addressed blobs and snapshots |
| **ExeMem account service** | `account/*`, payment, devices, app registry, billing — ExeMem-specific |
| **Cloud API v3** | ExeMem’s HTTP façade = **relay protocol + account service** under `/api/cloud/v3/*` |

One protocol. Nodes speak relay semantics everywhere. ExeMem may keep the
familiar `fetch` / `push` / `manage` / `account` action names as a thin
mapping onto relay intents; other relays implement the relay intents
directly.

### v3 → relay map

| v3 intent | Relay | Notes |
|---|---|---|
| `push`/`mutations` | `log.append` | Store signed entries; optional anti-spam only |
| `fetch`/`mutations_since` | `log.read` | Serve stored entries; delivery cursor is pagination, not order |
| `push`/`file` begin/commit | `blob.put` | Content-addressed by sha256 |
| `fetch`/`file` | `blob.get` | Optional presigned URL is a relay optimization |
| `push`/`snapshot` begin/commit | `snapshot.put` | Manifest blob + restore metadata |
| `fetch`/`snapshot` | `snapshot.get` | Latest or by `restore_point_id` |
| `manage`/`grant`, `revoke` | **out of relay surface** | Publisher-signed `grant` log entries; nodes run `AcceptEntry` |
| `manage`/`access` | **out of relay surface** | Readers verify birth + grant fold locally |
| `manage`/`create` | `log.append` of birth | Birth certificate is signed data; no opaque create RPC |
| `manage`/`delete` | `retention.request` | Publisher-signed deletion/GC request (relay policy) |
| `account/*` | **not protocol** | ExeMem account service only |
| `maintenance/*` | **not protocol** | ExeMem DEV only |

## Smallest relay surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/blob/put
POST /relay/v1/blob/get
POST /relay/v1/snapshot/put
POST /relay/v1/snapshot/get
POST /relay/v1/retention/request
```

Batching: one envelope or array 1–90; request ≤ 256 KiB; response ≤ 8 MiB;
per-item errors; same caps as v3. Auth: `nodeSignature` over the HTTP
request (same header scheme as cloud-api), **plus** per-entry writer
signatures inside log payloads (verified by **nodes** via `AcceptEntry`).

### `log.append`

Args: `database_id`, `entries[]` (1…90) — each a full signed entry
(`data` | `grant` | `checkpoint` | `birth` | `relays`) as in
`cloud-api.md`.

**Correctness:** none. The relay stores opaque signed bytes.

**Optional anti-spam / quota (policy, not conformance for truth):**

1. Request signature valid.
2. Cheap signature/chain shape check.
3. Quota / billing.
4. Optional door: signer looks like publisher or a key that appears in a
   grant entry this relay already stored. **Nodes must not rely on this.**

Result: `{stored, duplicate?}`. Policy errors (if any) are transport-only.

### `log.read`

Args: `database_id`, `writer_key?`, `from_seq?`, `cursor?`, `limit?` 1…90.

- If `writer_key` set: read that writer’s chain from `from_seq` (default 1).
- Else: opaque delivery `cursor` pagination (not canonical order).

Result: `cursor?`, `entries[]`. The reading node runs `AcceptEntry` on
each entry. Relay read policy (open vs gated) is relay-specific — see Open
questions for public databases.

### `blob.put` / `blob.get`

Content-addressed. Handle = `sha256` hex of ciphertext bytes.

**put** (`begin`|`commit` or single-shot): declare `bytes` + `sha256`,
optional upload URL; commit verifies size/hash for storage integrity of
that blob (hash match is physical, not history acceptance).

**get:** bytes or optional presigned GET. Presigned URLs are a **relay
optimization**, not required for conformance.

### `snapshot.put` / `snapshot.get`

Restore point: encrypted manifest blob (`restore_point_id` =
sha256(manifest)) plus named file digests. Canonical meaning stays
node-side. Relay stores and serves.

### `retention.request`

Args: publisher-signed retention body (`lastdb-retention-v1`) binding
`database_id`, publisher key, `mode` (`delete_database` |
`gc_before_snapshot`), `restore_point_id?`, `writer_seq`, signature.

Relay **policy** may GC older than a publisher-signed snapshot it still
holds, or drop a database after `delete_database`. Other relays may be
append-only (Open question). Nodes must not assume universal GC.

## What lives in signed data (not relay RPCs)

| Concern | Where |
|---|---|
| Birth / owner | `birth` log entry (`lastdb-db-birth-v1` material, chained) |
| Grants / revocations | `grant` entries on publisher spine |
| Ordering / canonical state | Writer hash chains + publisher checkpoints |
| Incorporation | Publisher `data` entry with `incorporated_from` |
| Heads / fork evidence | `last_seen_publisher_*`; People / device compare |
| Relay discovery | `relays` publisher entry + People out-of-band |
| **Acceptance** | **`AcceptEntry` on every node** (`cloud-api.md`) |

### Birth as a log entry

Creating a database = append a birth entry to one or more relays. No
separate create RPC in the protocol. ExeMem `manage`/`create` is sugar
that only appends that entry.

### Relay list entry (`kind: relays`)

Domain `lastdb-relays-v1`. Publisher-signed. Payload: ordered relay base
URLs (optional relay keys). Discovery: People bootstrap → read publisher
chain → latest `relays` entry. Switching relays needs **no migration** —
entries are self-contained.

## Failover and mirroring

- Push the same entry to several relays (identical `entry_hash` →
  duplicate).
- Read from any relay; verify with `AcceptEntry` + chain rules locally.
- Compare signed publisher heads across relays. Shorter tip → withholding;
  divergent hash at same seq → fork (`cloud-api.md` § Forks).
- Switch using the `relays` list or People emergency list. No
  import/export.
- Relays need not gossip for conformance; nodes may mirror by read+append.

## Protocol vs relay-specific

**Protocol (conformance):**

- Entry kinds, signed-bytes layouts, `entry_hash` / `prev_hash` rules
- Blob digest = sha256(bytes); snapshot `restore_point_id` = manifest digest
- `log.append` / `log.read` / `blob.*` / `snapshot.*` semantics (store/serve)
- Idempotent duplicate append of identical `entry_hash`
- Error codes for transport failures

**Relay-specific (policy):**

- Whether and how to anti-spam appends
- Read openness
- Quotas / billing (ExeMem: publisher pays)
- Presigned URLs
- Retention aggressiveness
- Rate limits

## ExeMem account service (not protocol)

`register`, `payment`, `deactivate`, `add_device`, `remove_device`,
`databases`, `status`, app registry, billing meters — ExeMem only.

**Relay accounts:** not required. Auth is node-key request signatures +
signed entries. A relay may map publisher keys to billing customers
without putting that map in the protocol.

## Auth headers

Same `nodeSignature` suite as `cloud-api.md`, with
`X-LastDB-Relay-Protocol: 1` on relay paths:

```
POST
<path>
<protocol or ->
<node key hex>
<device id>
<issued-at ms>
<nonce hex>
<body sha256 hex>
```

## Errors (transport / policy)

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `birth_signature` |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.signature`, `request.issued_at`, `request.nonce` |
| 403 | `forbidden` | `relay.policy` |
| 403 | `payment_required` | `payment.subscription` (billing relays) |
| 404 | `not_found` | `database.id`, `blob.sha256`, `restore_point_id`, `writer.seq` |
| 409 | `conflict` | `entries.writer_seq` (policy), `blob.unverified` |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | `relay.append`, `relay.read` |

`history.fork` / `history.withheld` / `history.equivocation` are **node /
app** signals from local verification and head compare, not required relay
responses.

## CI conformance (DEV E2E, not unit tests)

Design a suite (do not build in this PR) that runs the **same** scenarios
against ExeMem and a second **minimal reference relay**. Correctness is
always asserted at the **node** (`AcceptEntry`).

1. Append signed data/grant/checkpoint; read back; node accepts matching
   chains.
2. Blob put/get by sha256; snapshot put/get.
3. Reordered `log.read` → identical canonical replay.
4. Tampered entry: even if a relay stores it, the node rejects on read.
5. Forged grant: node rejects; cannot authorize writes.
6. Revocation + checkpoint → member write rejected by `AcceptEntry`.
7. **Failover:** append to relay A, read from B after mirror; heads match.
8. **Fork/withhold:** relay C serves divergent/gapped tip → node detects
   via head compare with A/B; stops canonical advance.
9. Switch relay using `relays` entry; no migration tooling.
10. Account/payment paths **only** against ExeMem account service.

## Open questions for Tom

1. **Reference relay home** — in `lastdb`, separate repo, or design-only
   until needed?
2. **Public reads** — open on every conforming relay for `grant#any`
   databases, or always relay-local policy?
3. **Relay list in signed data** — confirm publisher-signed `relays`
   entries (+ People bootstrap)?
4. **Birth entry shape** — chained `kind: birth` only, with ExeMem
   `manage`/`create` as append sugar?
5. **Retention intent** — required for conformance, or may a relay be
   append-only forever?
