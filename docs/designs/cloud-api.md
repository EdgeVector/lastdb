# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). Not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Companion: `docs/designs/relay-protocol.md` (open relay). No dual paths,
fallback, migration, or unit tests.

Tom (2026-11-19): action-based interface. Node Ed25519 key is identity.
Every change carries a **writer signature**. Cloud/relay stays blind to
contents.

## Trust model

ExeMem and every relay are **untrusted storage and transport**. Honest
nodes reach the same canonical state from **signed data only**. Relays do
not authorize.

| Trusted for | Not trusted for |
|---|---|
| Availability | Order, completeness, acceptance, integrity |

### Acceptance (local node rule)

**Acceptance is a local node rule.** Relays neither enforce nor know about
it. Optional relay checks are anti-spam/quota only.

**Revocations live only in the chain.** A revocation (including its
per-device cutoff heads) is a signed `grant`-kind entry on a publisher
device chain. It is ordered by the same deterministic merge as every other
entry and covered by the publisher’s signed heads. There is **no** separate
revocation record or side channel — not on the relay surface, not in ExeMem
rows, not in People. People may carry **signed heads for comparison only**.
If a relay or ExeMem keeps grant data for anti-spam, that copy is **derived
from the chain**, optional, and never authoritative. A reader that receives
a revocation only via a side channel and not as a verified chain entry
**must ignore it**.

`AcceptEntry(E, S) → accept | reject` — `S` is the node’s verified signed
history for `E.database_id`:

1. **Signature.** Verify `E.signature` over `signed_bytes(E)` with
   `E.node_key`.
2. **Database bind.** `E.database_id` matches `S`.
3. **Device chain.** `(E.writer_seq, E.prev_hash)` extends
   `(E.node_key, E.device_id)`, or identical `entry_hash` duplicate.
   Out-of-order delivery is buffered.
4. **Publisher authorization.**
   - Owner (from birth) → authorized.
   - Else `kind=data` with a publisher-signed grant in the merged publisher
     spine under the grant-window + revocation-cutoff rules below.
5. **On reject:** do not apply; keep evidence; stop that device chain at
   the gap.

**Canonical merge:** all accepted entries sorted by
`(lamport, node_key, device_id)`. At equal Lamport for grants to the same
grantee, **`revoked` wins**.

**Publisher-signed heads:** each device tip `(device_id → writer_seq,
entry_hash)` is signed (the tip entry itself). Nodes compare publisher
heads across their own devices and via People. On mismatch → stop advancing
canonical state (`history.fork` / `history.withheld`).

**Guarantee:** after a reader has a publisher-signed revocation for a
member, **nothing new** from that member is accepted (revocation cutoff).
A hacked relay may delay the revocation (liveness); signed-head comparison
detects the withhold.

**Limit:** relays can delay or stop delivery. They cannot make honest nodes
silently disagree once heads meet.

## Minimal signed history (Decisions 42–43)

1. A **writer signature** on every change.
2. One **hash chain per device**, key `(node_key, device_id)`, gapless.
3. **Grants and revocations** are signed entries on publisher device chains.
4. Readers run `AcceptEntry` and merge by `(lamport, node_key, device_id)`;
   tie → revoke wins.
5. Publisher **signs heads**; nodes compare; stop on mismatch.
6. **No checkpoints.** No member-head lists. Publisher need not be online
   for member writes to become final.
7. A member **direct** write is final once it passes the grant-window rule
   against the publisher state it binds (`seen_publisher_heads`). A write
   whose chain had already seen a revocation is rejected.
8. **Proposals** count only after a publisher device **incorporates** them.

### Device chains

- `writer_seq` ≥ 1, +1 each time; `prev_hash` links within the device.
- `entry_hash = SHA-256(signed_bytes || signature_raw_64)`.
- Dedupe `(node_key, device_id, writer_seq)`. Same seq on **different**
  devices is fine. Equivocation = two hashes at one device position.
- Stolen node key may mint a new `device_id` (Decision 21).

### Signed bytes (common header)

Length-prefixed, big-endian. Domain label + `0x00`, then:

`database_id`(32) · `node_key`(32) · `device_id`(16) · `writer_seq`(u64) ·
`prev_hash`(32) · `authored_at_ms`(u64) · `lamport`(u64 = max seen + 1) ·
`seen_publisher_heads` (u16 count + (`device_id`, `writer_seq`,
`entry_hash`)× sorted by `device_id`).

Then kind fields. Wire `signature`: base64url, 64 bytes.

| Kind | Domain | Notes |
|---|---|---|
| `birth` | `lastdb-db-birth-v1` material in chain | **First** publisher-device entry (`writer_seq=1`) |
| `data` | `lastdb-mutation-v1` | `stream` direct\|proposal; payload hash; optional file; optional `incorporated_from`; optional `grant_at` |
| `grant` | `lastdb-grant-v1` | `grantee`, `role` (read\|submit\|write\|revoked); revoke carries **cutoff** |
| `heads` | `lastdb-heads-v1` | Compact publisher head vector for People exchange (optional; tip entries also suffice) |
| `relays` | `lastdb-relays-v1` | Publisher-signed relay URL list |
| `resolve` | `lastdb-resolve-v1` | Fork resolution: chosen `(node_key, device_id, entry_hash)` |

**Data:** publisher direct needs no grant. Member direct binds
`grant_at = {device_id, writer_seq}` of the write grant and
`seen_publisher_heads`. Proposal: no grant_at; becomes content only via
publisher incorporation (`incorporated_from`).

**Birth (default):** create = append `birth` as the first entry on a
publisher device chain (`db_salt`, `birth_signature`, owner). ExeMem
`manage`/`create` is sugar that appends it.

### Grant window (no checkpoints)

Member direct write `W` with `grant_at = G` and `seen_publisher_heads = H`:

1. Fold publisher grants in merge order through the publisher prefix
   implied by `H`. At `G`, effective role for `W.node_key` must be
   **`write`**.
2. No later grant in that prefix (through `H`) sets the role away from
   `write`. If `H` already covers a revocation, reject (`must have known`).
3. Apply **revocation cutoff** (below) for any revoke in `S`.

### Revocation cutoff

A revocation is **only** a `grant` entry with `role = revoked` on a
publisher device chain (same `lastdb-grant-v1` bytes, same merge order,
same head coverage as any other entry). It carries the revoked member’s
**per-device heads** the publisher accepts:

`cutoffs[]`: `(device_id, writer_seq, entry_hash)`.

- Readers apply cutoffs only after that revoke entry is in verified `S`
  via the chain — never from a side channel.
- Accept that member’s entries **only up to** those heads (matching hash
  at that seq).
- Reject everything after, whatever position the entry claims.
- A member device the publisher has not listed has **cutoff 0** (no entry
  from that device is accepted).
- Per-member data inside that member’s own revocation — **not** a member
  list.

`manage`/`revoke` (ExeMem) is transport sugar: it appends the same signed
chain entry a `log.append` would. ExeMem must not store an authoritative
revoke row beside the chain.

**Guarantee (plain):** once a reader has the revocation **in the chain**,
nothing new from that member is accepted. Delayed chain delivery is a
liveness failure detected by signed-head comparison.

### Fork resolution (default)

Publisher signs a `resolve` entry naming the chosen tip
`(node_key, device_id, entry_hash)`. Nodes keep evidence of the discarded
branch and resume merge on the chosen tip. Fillable delivery gaps buffer
and resume without resolution.

### Publisher-signed heads

Nodes exchange tip descriptors for publisher devices (and may use
`kind=heads`). Mismatch → stop canonical advance. **Default:** no timed
freshness signal — head comparison only.

## Shape

```
POST /api/cloud/v3/{fetch,push,manage,account,maintenance}
```

Product envelope: `{database_id, intent, args}` → `{database_id, intent, result}`.
Account/maintenance: no `database_id`. Batch 1–90; req ≤256 KiB; resp ≤8 MiB.
HTTP 200 with per-item errors; whole-request auth/version/size failures.

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `file`, `snapshot` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |
| `account` | `register`, `payment`, `deactivate`, `add_device`, `remove_device`, `databases`, `status` |
| `maintenance` | snapshot_proof_* (DEV, `githubOidc`, no batch) |

`manage`/`grant` and `manage`/`revoke` carry signed `grant` entries and
only append them to the publisher device chain (transport sugar). Under
the open relay they are plain `log.append`. No ExeMem/relay revoke index
is authoritative. Authorization is only `AcceptEntry` over the chain.

## Auth (`nodeSignature`)

Headers: `X-LastDB-Node-Key`, `Device-Id`, `Issued-At`, `Nonce`,
`Signature`, and `X-LastDB-Cloud-Protocol: 3` (not on account).

Signed lines: `POST\n<path>\n<protocol|->\n<node key>\n<device id>\n
<issued-at>\n<nonce>\n<body sha256 hex>`.

Skew ±60s; nonces ≥2× skew. Account keys need a registered device (except
`add_device`); account-less keys skip device check. Request signatures
authorize the HTTP call only — not entry acceptance.

## Database keys (Decision 36)

- `db_salt` 32 bytes; `db_seed = HKDF-SHA256(node seed, salt, "lastdb-db-key-v1")`.
- `database_id` = db public key. Birth signature by db key over
  `lastdb-db-birth-v1\n<database_id>\n<owner>\n<db_salt>`.
- Db key signs nothing else. Owner needs no grant. Tombstones; no rotation.
- Nodes verify birth into `S` before treating anyone as owner.

## Requirements (Decision 38)

Publisher pays for storage and transfer on its databases. Lapsed publisher:
reads work; push/grant/create/snapshot → `payment_required`; delete always
allowed.

## Decisions (Tom, 2026-11-19)

1–3, 5–30, 32–41 as in #1800 (protocol v3, envelopes, upload begin/commit,
account surface, no per-device keys, publisher pays, cursors as delivery
pagination only, etc.).

4. **Superseded by 43.** Grants are publisher-signed chain entries.
31. **Superseded by 43.** Public read = signed `grant` `any` / `read`.
34. Create carries birth material (chained `birth` entry; manage sugar OK).
35. Levels: `read` | `submit` | `write` (+ signed `revoked`).
42. **Writer signatures + device chains** `(node_key, device_id)`.
    Acceptance local (`AcceptEntry`). No checkpoints.
43. **Signed grants** on publisher device chains; revocation **cutoff**;
    merge `(lamport, node_key, device_id)`; revoke wins ties; publisher
    signed heads compared via devices/People.

### Defaults (not open questions)

| Topic | Default |
|---|---|
| Freshness | Head comparison only — no timed latest-head signal |
| Reference relay | Lives in `lastdb` for CI conformance |
| Public reads | `grant#any` reads are open on every conforming relay |
| Relay discovery | Publisher-signed `relays` entry; People bootstraps the first URL |
| Retention | Relays must support `retention.request`; accept only for data older than a publisher-signed snapshot |
| Birth | First chained entry on a publisher device |
| Fork resolution | Publisher `resolve` entry names chosen tip |

## Intent notes (deltas from #1800)

- `push`/`mutations` / `fetch`/`mutations_since`: full signed entries; no
  checkpoint kind. Delivery cursor is pagination only.
- `manage`/`grant`/`revoke`: `entries[]` of signed `grant` entries
  appended to the chain; revoke includes cutoffs. No side-channel revoke
  store.
- `manage`/`access`: UX cache only; not authorization truth.
- Account/maintenance: ExeMem service (see relay doc).

## Errors (closed set)

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `grants.any.role`, `birth_signature`, `cursor` |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.*`, `device.*`, `account.retired` |
| 403 | `forbidden` | `grant.absent`, `grant.role` |
| 403 | `payment_required` | `payment.subscription` |
| 404 | `not_found` | `database.*`, `file_handle`, `restore_point_id`, `snapshot_upload` |
| 409 | `conflict` | `database.id`, `account.exists`, `device.*`, `entries.writer_seq`, `file.unverified`, `history.fork`, `history.equivocation`, `history.withheld` |
| 409 | `cursor_expired` | `cursor` |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | `account.register`, `read.public` |

`history.*` are primarily node/app signals.

## CI plan (DEV E2E, not unit tests)

Node-side assertions (`AcceptEntry`). No unit tests. Also run against the
in-repo reference relay (`relay-protocol.md`).

1. Tampered payload: node rejects on read even if relay stored it.
2. Cross-db / field tamper: node rejects.
3. Incorporation mismatch: node rejects.
4. True one-device equivocation: `history.equivocation`.
5. Multi-device offline same `writer_seq`: **not** equivocation.
6. Forged grant: cannot authorize.
7. **Backdated write after revoke:** revoke is in the chain with cutoffs;
   revoked member presents old `seen_publisher_heads` / low lamport past
   the cutoff → node rejects.
8. Write that had seen a revocation in its `seen_publisher_heads`: reject.
9. **Side-channel revoke ignored:** a revocation delivered only outside
   the chain (ExeMem row, People message, relay index) and not as a
   verified chain entry → reader ignores it; cutoffs do not apply.
10. Bad signature: that device chain stops.
11. Reordered delivery → same merge result.
12. Divergent publisher heads → `history.fork`; stop advance.
13. Withheld chain revocation detected by head compare (liveness).

## Build work

**Relay/ExeMem:** store/serve opaque signed entries; optional anti-spam;
account service remains ExeMem-specific.

**Node:** `AcceptEntry`, device chains, grant fold + cutoff, head compare,
`resolve`, birth verify, incorporation.
