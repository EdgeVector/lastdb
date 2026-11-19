# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

The model is the LastDB app API: one prose contract, one OpenAPI 3.1 machine
spec, one version number, and CI that keeps them honest. See `docs/app-api.md`,
`lastdb_node/app-api.json`, `scripts/lints/api_surface.py`, and
`scripts/lints/app_api_types.py`.

Tom (2026-11-19): **action-based** interface — three verbs, nine intents,
node nouns only. Cloud storage may change without LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
Paths name cloud storage. The pin lint only checks a new node against the
pinned cloud, not the next cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` dispatch | Live route table |
| `cloud_backup_protocol` | Wire types and path constants |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/` | Product call sites |
| `docs/designs/shared-database-mutation-logs.md` | Mutation vs proposal log rules |
| CI scripts under `scripts/cloud_backup/` | DEV/ops-only callers |

## Hard rules

- No dual paths, aliases, fallback, or migration. Clean cutover via protocol v3.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, and Org exist only in People. The cloud stores generic config.
- **No route segments or request fields that name cloud storage resources**
  (log, blob, inbox, bucket, B2/R2 key, delivery).

## Shape: three actions, nine intents

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
```

Each body and each 200 response is a **tagged union** on `intent`. The node
states intent in LastDB nouns. The cloud chooses storage.

| Action | Intents (exact set) |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `snapshot`, `file` |
| `manage` | `create`, `grant`, `revoke` |

**Why not resource routes:** a `/logs` or `/blobs` path freezes cloud storage
into the node. With actions + opaque handles, the cloud may move backends
without a new route family. Extensibility is a new `intent` (plus a version
bump when old nodes must stop) — never a new path.

## Decisions (Tom, 2026-11-19)

1. Protocol starts at **v3**.
2. Version in **header + token**; drop redundant body `version`.
3. Upgrade → **426** `upgrade_required` + `minimum_version`.
4. Members are signed `grant#<node key>` entries — **no list, no revision**.
   Self-access is not a separate intent (see open questions).
5. **No** protocol header on auth-service.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof stays, marked **`x-lastdb-environment: dev`**.
8. **Action-based** surface: only `fetch` / `push` / `manage`.
9. **`push`/`mutations` has no `kind` field.** The cloud lands the append from
   the caller’s grant: direct write (publisher, or grant with write) goes to
   the database mutation stream; otherwise the entries land as proposals. The
   response reports `landed: direct | proposal`. Proposals are not a separate
   intent.
10. **`fetch`/`mutations_since` visibility.** ExeMem returns **direct + proposal**
    mutations to the **publisher**, and **only direct** to **receivers**
    (members). ExeMem decides from the caller’s grant. The node sends **no**
    kind or filter. Each returned entry is tagged `stream: direct | proposal`.
    Accepting a proposal means the publisher pushes it via `push`/`mutations`
    (it lands `direct`). Settle of accepted proposals folds into that push —
    no separate settle intent.

## Final nine intents

### `fetch` / `mutations_since`

Always returns the **latest cursor** (folds former `head`). Visibility is
Decision 10 (ExeMem-owned).

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `cursor?` (opaque; absent = from start), `limit?` |
| **Response** | `intent`, `cursor` (latest), `mutations[]` |

`Mutation` (response item): `stream` (`direct` \| `proposal`), `cursor`,
`writer_node_key`, `device_id`, `writer_seq`, `authored_at_ms`, `namespace`,
`payload_b64`, `incorporated_from?` (`node_key`, `device_id`, `writer_seq`).

### `fetch` / `snapshot`

Describe a restore point, or read one part via an opaque part handle.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `restore_point_id?`, `part_handle?` |
| **Response (describe)** | `intent`, `result: describe`, `restore_point_id`, `part_handles[]` |
| **Response (part)** | `intent`, `result: part`, `part_handle`, `bytes_b64`, `length` |

If `part_handle` is set, the cloud returns part bytes. Otherwise it describes
the restore point (and may mint `part_handles`).

### `fetch` / `file`

Byte range of a record file (former `delivery/object` / file_range).

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `file_handle`, `offset`, `length` |
| **Response** | `intent`, `file_handle`, `offset`, `length`, `bytes_b64` |

### `push` / `mutations`

Append sealed mutation entries. Cloud chooses direct vs proposal from grant
(Decision 9). No `kind` field.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `device_id`, `entries[]` |
| **Response** | `intent`, `landed: direct \| proposal`, `cursor`, `accepted` |

`MutationEntry` (request item): `writer_seq`, `authored_at_ms`, `namespace`,
`payload_b64`, `incorporated_from?`.

### `push` / `snapshot`

Prepare and publish a backup restore point via an opaque ticket.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `stage: prepare \| publish`, `ticket?`, `restore_point_id?` |
| **Response** | `intent`, `stage`, `ticket`, `restore_point_id?`, `upload_url?` |

### `push` / `file`

Upload record-file bytes (former `blob/bytes`). Begin / complete with an
opaque ticket; complete returns `file_handle`.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `stage: begin \| complete`, `ticket?`, `size_bytes?`, `content_sha256?` |
| **Response** | `intent`, `stage`, `ticket`, `file_handle?`, `upload_url?` |

### `manage` / `create`

Publish (create) a database.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id` |
| **Response** | `intent`, `database_id` |

### `manage` / `grant`

Put one or more signed `grant#<node key>` entries. No list. No revision
document.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `grants[]` |
| **Response** | `intent`, `ok` |

`GrantEntry`: `node_key`, `signed_grant`.

### `manage` / `revoke`

Remove grant(s) for node key(s).

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `node_keys[]` |
| **Response** | `intent`, `ok` |

## Use cases → intents

| Use case | Node call site(s) | Lands on |
|---|---|---|
| Create database | `native.rs` CREATE | `manage`/`create` |
| Append mutations | `accepted.rs` APPEND | `push`/`mutations` (`landed: direct`) |
| Member propose | `contributions.rs` APPEND | `push`/`mutations` (`landed: proposal`) |
| Read mutations / head | `cloud.rs` HEAD, LIST | `fetch`/`mutations_since` |
| Publisher read proposals | `run.rs` inbox/list | `fetch`/`mutations_since` (`stream: proposal`) |
| Accept proposal | APPEND after intake | `push`/`mutations` (`landed: direct`) |
| Upload file | `signed_blob.rs` blob/bytes | `push`/`file` |
| Read file bytes | `restore.rs` delivery/object | `fetch`/`file` |
| Snapshot backup | `snapshot/transport.rs` prepare/publish | `push`/`snapshot` |
| Snapshot restore | read / read-for-restore / download | `fetch`/`snapshot` |
| Grant / revoke | `named_grant/exchange.rs` set | `manage`/`grant`, `manage`/`revoke` |

## Where dropped v2 work lands

Checked against `lastdb_node` and `shared-database-mutation-logs.md`.

| Dropped | Verdict |
|---|---|
| `head` | **Folds** into `fetch`/`mutations_since` (always returns latest `cursor`). |
| Proposal **append** | **Folds** into `push`/`mutations`. Cloud routes from grant (Decision 9). |
| Proposal **list** (inbox/list) | **Folds** into `fetch`/`mutations_since` for the publisher (Decision 10). Entries tagged `stream: proposal`. |
| Proposal **settle** (accepted) | **Folds** into publisher `push`/`mutations` of the accepted entries (Decision 10). No separate settle intent. |
| Proposal **discard / reject** | **Open question** — see below. Today settle sends `Discarded` with **no** mutation push (`contributions/intake`). |
| File **upload** (blob/bytes) | **Folds** into `push`/`file`. |
| File **read** (delivery/object) | **Folds** into `fetch`/`file`. |
| `self_access` / members/read | **Open question** — see below. |
| `database_access` / ACCESS | **Open question** — see below. Also used for contribution status. |
| Settings get/set | **Open question** — see below. Tom: settings do not belong under fetch. |
| Members get-one | **Dropped.** Confirmed. No `get` intent. |
| Members list / revision | **Dropped.** Decision 4. |
| `settle-submissions` | **Dropped.** No node caller. |

### Publisher proposal read / settle (code)

`log_sync/run.rs` `drain_inbox` today: inbox **list** → apply → APPEND
accepted (`incorporated_from`) → inbox **settle** with `Published` /
`Discarded`.

Under Decisions 9–10: list folds into `fetch`/`mutations_since` (publisher
sees `stream: proposal`); accept folds into `push`/`mutations` (lands
`direct`). ExeMem owns merging streams behind one opaque cursor.

**Discard** still has no home: intake returns `ProposalResult::Discarded`
without a mutation append, and settle clears those entries. That remains an
open question (below).

## Ops and DEV (unchanged homes)

### `POST /api/cloud/v3/ops` (off product surface)

Owner reclaim / retention (`retention.rs`, `blob_retention.rs`). Tag `ops`.

Intents: `retention`, `automatic_retention`, `reclaim`, `reclaim_covered`,
`cleanup_retired`, `reclaim_files`, plus DEV `track_dev`, `cleanup_dev`,
`cleanup_receipt_dev` (`x-lastdb-environment: dev`).

### `POST /api/cloud/v3/dev/snapshot-proof` (dev-marked)

`provision`, `retire`, `status`, `inspect`.

### Auth-service

`/api/developer/*`, `/api/auth/*` — **out of scope** for product schema
strictness; included for surface inventory. **No** protocol header.

## Snapshot actions

| Current | Disposition |
|---|---|
| prepare, publish | **Keep** → `push`/`snapshot` |
| read, read-for-restore, download | **Keep** → `fetch`/`snapshot` |
| retention, automatic-retention, reclaim, reclaim-covered, cleanup-retired, reclaim-blobs | **Move** → `ops` |
| track_dev, cleanup_dev, cleanup_receipt_dev | **DEV-only** on `ops` |
| index-status, index-history | **Absent** |

## Old → new

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `manage`/`create` |
| `…/mutation-log/access` | **open** (database_access / status) |
| `…/mutation-log/append` | `push`/`mutations` (`landed` from grant) |
| `…/mutation-log/head` | `fetch`/`mutations_since` (latest `cursor`) |
| `…/mutation-log/list` | `fetch`/`mutations_since` |
| `…/mutation-log/inbox/list` | `fetch`/`mutations_since` (publisher; `stream: proposal`) |
| `…/mutation-log/inbox/settle` (accepted) | folds into publisher `push`/`mutations` |
| `…/mutation-log/inbox/settle` (discard) | **open** (reject without push) |
| `…/mutation-log/settle-submissions` | **dropped** |
| `…/mutation-log/blob/bytes` | `push`/`file` |
| `…/mutation-log/delivery/object` | `fetch`/`file` |
| `…/mutation-log/snapshot/bytes` prepare/publish | `push`/`snapshot` |
| `…/mutation-log/snapshot/bytes` read / read-for-restore / download | `fetch`/`snapshot` |
| `…/mutation-log/snapshot/bytes` retention/reclaim/* | `ops` |
| `…/mutation-log/snapshot/bytes` *_dev | `ops` (dev) |
| `…/share/members/set` | `manage`/`grant` and `manage`/`revoke` |
| `…/share/members/get` | **dropped** |
| `…/share/members/list` | **dropped** |
| `…/share/members/read` | **open** (self_access) |
| `…/share/members/settings/set` | **open** (settings) |
| `…/share/members/settings/get` | **open** (settings) |
| `…/dev-snapshot-proof/{op}` | `dev/snapshot-proof` |

## Auth

| Scheme | Wire | Where |
|---|---|---|
| `accountBearer` | `Authorization: Bearer` | auth-service |
| `apiKey` | `X-API-Key` (`em_…`) | `manage`/`create`, `push`/`file` (as today) |
| `lastDbNode` | `Authorization: LastDBNode …` | signed storage actions |
| `githubOidc` | Bearer JWT | DEV snapshot-proof |

Header `X-LastDB-Cloud-Protocol: 3` on fetch/push/manage/ops/dev. Not on
auth-service.

## Errors

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

Out of date → HTTP **426**
`{ "code":"upgrade_required", "field":"cloud.protocol", "minimum_version":3 }`.

## CI plan

(a) Spec matches dispatch (intent ↔ handler). (b) Types from tagged unions.
(c) Next cloud vs last stable released node (closes #1799). No unit tests.

## Where the node still names a resource

| # | Issue | Recommendation |
|---|---|---|
| R1 | Presigned `upload_url` may reveal object-store host | Keep as opaque string in v3.0; no bucket fields |
| R2 | Node still builds `BlobPath` today | Replace with opaque `file_handle` from `push`/`file` |
| R3 | Snapshot multipart layout | Only opaque `part_handle`s from `fetch`/`snapshot` |
| R4 | Range size ↔ storage limit | Return `max_range_bytes` on begin/`file` if needed — no store name |
| R5 | DEV snapshot-proof | Keep marked `dev`; proof ids only |

---

## Open questions for Tom

1. **Reject / discard a proposal without pushing.** Code still settles
   `Discarded` proposals with no mutation append (`ProposalResult::Discarded`
   in `contributions/intake`, then inbox settle). Accepted settle is folded
   (Decision 10). Discard is not.
   **Recommend:** add an optional `discard_cursors[]` (opaque) on
   `push`/`mutations` for the publisher only — still one intent, no settle
   route. Alternative: ExeMem auto-expires proposals the publisher has fetched
   and not accepted within a TTL (weaker, needs product OK).

2. **`self_access` / members/read.** Not in the nine.
   **Recommend:** return the caller’s own grant summary on `manage`/`grant`
   for the publisher; for members, a one-shot field on the first
   `fetch`/`mutations_since` page, or local-only until a later intent.

3. **`database_access` / ACCESS.** Enrollment + contribution status today.
   **Recommend:** enrollment on `manage`/`create` response; contribution
   status as a later follow-up (not in the nine).

4. **Settings (link `read`, and today’s `write` / `member_writes`).** Tom: not
   under fetch. Write is moving onto the grant entry; link `read`
   (`members` \| `anyone`) still needs a home.
   **Recommend:** optional signed settings on `manage`/`grant` (and initial on
   `manage`/`create`).

5. **Members get-one.** **Dropped.** Confirmed unless Tom objects.
