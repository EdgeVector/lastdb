# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

The model is the LastDB app API: one prose contract, one OpenAPI 3.1 machine
spec, one version number, and CI that keeps them honest. See `docs/app-api.md`,
`lastdb_node/app-api.json`, `scripts/lints/api_surface.py`, and
`scripts/lints/app_api_types.py`.

Tom (2026-11-19): **action-based** interface — three verbs, node intent,
cloud chooses storage. Storage may change without LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
Paths and bodies name cloud storage (`mutation-log`, `blob`, `delivery`,
`snapshot/bytes`, `inbox`). The pin lint only checks a new node against the
pinned cloud, not the next cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` `http.rs` + blobs/snapshots/logs/auth | Dispatch |
| `cloud_backup_protocol` | Wire types and path constants |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/` | Product call sites |
| CI scripts under `scripts/cloud_backup/` | DEV/ops-only callers |

## Hard rules

- No dual paths, aliases, fallback, or migration. Clean cutover via protocol v3.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, and Org exist only in People. The cloud stores generic config.
- Plain language. One concept in one system.
- **No route segments or request fields that name cloud storage resources.**

## Shape: three actions (not resources)

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
```

Each body is a **tagged union** on `intent` (closed enum). The node states
**intent** in LastDB nouns. The cloud decides which store, key, or backend
to touch.

### How this differs from resource routing

| Resource-based (rejected) | Action-based (this design) |
|---|---|
| `/v3/logs/list`, `/v3/blobs/read`, `/v3/snapshots/…` | `/v3/fetch` with `intent` |
| Request names the store (`log`, `blob`, `inbox`) | Request names the job (`changes_since`, `file_range`) |
| New store → new route family | New store → invisible; new job → new `intent` + version bump |
| Node and cloud share a storage vocabulary | Node owns product nouns; cloud owns storage |

**Why storage can change freely:** the node never asks for “the mutation
log” or “B2 object X”. It asks for “changes since this cursor” or “this
file range”. Cursors, upload tickets, and restore handles are **opaque
cloud-issued strings** the node returns unchanged. The cloud may move from
one log layout to another, swap object stores, or split proposal storage —
the intents stay the same.

### Node nouns (allowed in the request)

database · change · proposal · file · backup / restore point · member grant ·
settings · cursor · (opaque) handle / ticket

### Cloud storage nouns (forbidden in routes and request fields)

log · blob · snapshot object · inbox · bucket · B2/R2 key · delivery ·
mutation-log path segments

## Use cases → intents

| Use case | Node call site(s) | Action + intent |
|---|---|---|
| Publish (create) a database | CREATE | `manage` / `create_database` |
| Enrollment / access view | ACCESS | `manage` / `database_access` |
| Append mutation changes | APPEND (mutation) | `push` / `changes` |
| Read mutation head | HEAD | `fetch` / `head` |
| Read mutation changes | LIST | `fetch` / `changes_since` |
| Propose | APPEND (proposal) | `push` / `proposal` |
| List proposals | inbox/list | `fetch` / `proposals_since` |
| Settle proposals | inbox/settle | `push` / `settle` |
| Upload file bytes | blob context/prepare/complete | `push` / `file` (+ opaque tickets) |
| Read file bytes | delivery/object | `fetch` / `file_range` |
| Snapshot backup | snapshot prepare/publish | `push` / `backup` (+ opaque tickets) |
| Snapshot restore | read / read-for-restore / download | `fetch` / `restore_point`, `restore_part` |
| Grant members | members/set | `manage` / `grant` |
| Revoke members | members/set (clear entry) | `manage` / `revoke` |
| Read own access | members/read | `fetch` / `self_access` |
| Set settings | settings/set | `manage` / `set_settings` |
| Get settings | settings/get | `fetch` / `settings` |
| Owner reclaim / retention | retention CLI | **ops** (off product surface) |
| DEV snapshot proof | CI | **dev** (marked) |

Cut: `settle-submissions` (no node caller). Cut: grant **list** and any
MemberList **revision** document (approved decision).

## Final action + intent list

### `POST /api/cloud/v3/fetch`

| intent | Meaning |
|---|---|
| `head` | Latest change cursor for a database |
| `changes_since` | Changes after a cursor |
| `proposals_since` | Proposals after a cursor |
| `file_range` | Byte range of a record file (via opaque `file_handle`) |
| `restore_point` | Describe / open a backup restore point |
| `restore_part` | Bytes for one restore part (via opaque `part_handle`) |
| `self_access` | Caller’s own grant view |
| `settings` | Current settings config |

### `POST /api/cloud/v3/push`

| intent | Meaning |
|---|---|
| `changes` | Append mutation changes |
| `proposal` | Append a proposal |
| `settle` | Settle proposal ids |
| `file` | Upload record-file bytes (ticket stages) |
| `backup` | Publish a backup restore point (ticket stages) |

`file` and `backup` use opaque tickets: the first call returns a ticket (and
optionally a short-lived upload URL); later calls pass the ticket back. The
node does not name object keys or multipart layouts in the request schema.

### `POST /api/cloud/v3/manage`

| intent | Meaning |
|---|---|
| `create_database` | Create / publish a database |
| `database_access` | Enrollment / access view for one database |
| `grant` | Put one or more signed `grant#<node key>` entries |
| `revoke` | Remove grant(s) for node key(s) |
| `set_settings` | Replace settings config |

No `list` grants. No revision counter on a member list document.

### Ops (off public product surface)

`POST /api/cloud/v3/ops` — owner reclaim / retention. Same action style so the
CLI stays consistent, but **not** part of the product fetch/push/manage
surface. Tag: `ops`.

| intent | Was |
|---|---|
| `retention` | snapshot retention |
| `automatic_retention` | automatic-retention |
| `reclaim` | reclaim |
| `reclaim_covered` | reclaim-covered |
| `cleanup_retired` | cleanup-retired |
| `reclaim_files` | reclaim-blobs (renamed to node noun) |

### DEV-only (`x-lastdb-environment: dev`)

| Path | intents |
|---|---|
| `POST /api/cloud/v3/dev/snapshot-proof` | `provision`, `retire`, `status`, `inspect` |
| `POST /api/cloud/v3/ops` | `track_dev`, `cleanup_dev`, `cleanup_receipt_dev` |

### Auth-service (unchanged; no protocol header)

`/api/developer/*`, `/api/auth/*` — tag `auth-service`.

## Snapshot actions

| Current action | Disposition |
|---|---|
| `prepare`, `publish` | **Keep** as `push` / `backup` (ticket stages) |
| `read`, `read-for-restore` | **Keep** as `fetch` / `restore_point` |
| `download` | **Keep** as `fetch` / `restore_part` |
| `retention`, `automatic-retention`, `reclaim`, `reclaim-covered`, `cleanup-retired`, `reclaim-blobs` | **Move** to `ops` |
| `track_dev`, `cleanup_dev`, `cleanup_receipt_dev` | **DEV-only** on `ops` |
| `index-status`, `index-history` | **Absent** |

## Old → new (every current route)

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `manage` `create_database` |
| `…/mutation-log/access` | `manage` `database_access` |
| `…/mutation-log/append` (mutation) | `push` `changes` |
| `…/mutation-log/append` (proposal) | `push` `proposal` |
| `…/mutation-log/head` | `fetch` `head` |
| `…/mutation-log/list` | `fetch` `changes_since` |
| `…/mutation-log/inbox/list` | `fetch` `proposals_since` |
| `…/mutation-log/inbox/settle` | `push` `settle` |
| `…/mutation-log/settle-submissions` | **retired** |
| `…/mutation-log/blob/bytes` | `push` `file` |
| `…/mutation-log/delivery/object` | `fetch` `file_range` |
| `…/mutation-log/snapshot/bytes` prepare/publish | `push` `backup` |
| `…/mutation-log/snapshot/bytes` read / read-for-restore | `fetch` `restore_point` |
| `…/mutation-log/snapshot/bytes` download | `fetch` `restore_part` |
| `…/mutation-log/snapshot/bytes` retention / reclaim / cleanup-* | `ops` (see intents) |
| `…/mutation-log/snapshot/bytes` *_dev | `ops` (dev) |
| `…/share/members/set` | `manage` `grant` / `revoke` |
| `…/share/members/get` | **open** — see questions (no public get-one in v3.0 list) |
| `…/share/members/list` | **retired** (no list / revision) |
| `…/share/members/read` | `fetch` `self_access` |
| `…/share/members/settings/set` | `manage` `set_settings` |
| `…/share/members/settings/get` | `fetch` `settings` |
| `…/dev-snapshot-proof/{op}` | `dev/snapshot-proof` `{op}` |

## Extensibility

A new capability is a **new `intent` value** (and a protocol version bump when
old nodes must stop). Never a new route family. Opaque handles absorb
storage-shape changes without a wire bump when the intent stays the same.

## Auth

| Scheme | Wire | Where |
|---|---|---|
| `accountBearer` | `Authorization: Bearer <enrollment key>` | auth-service |
| `apiKey` | `X-API-Key` (`em_…`) | `manage` create; `push` file (as today) |
| `lastDbNode` | `Authorization: LastDBNode …` | signed storage actions |
| `githubOidc` | Bearer JWT | DEV snapshot-proof |

Protocol header `X-LastDB-Cloud-Protocol: 3` on fetch/push/manage/ops/dev
storage actions. **Not** on auth-service.

## Protocol version

- Version **3**. Header + matching token field.
- Drop redundant body `version` fields.
- Out of date → HTTP **426**
  `{ "code":"upgrade_required", "field":"cloud.protocol", "minimum_version":3 }`.
- Live OpenAPI home after implementation:
  `cloud_backup_protocol/cloud-api.openapi.json`.

## Errors

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

## CI plan

(a) Spec matches dispatch (intent enum ↔ handler). (b) Types from tagged
unions. (c) Next cloud vs last stable released node (closes #1799).
No unit tests. E2E DEV ops remain the proof.

## Decisions (Tom, 2026-11-19)

1. Protocol starts at **v3**.
2. Version in **header + token**; drop redundant body `version`.
3. Upgrade → **426** `upgrade_required` + `minimum_version`.
4. Members are signed `grant#<node key>` entries and self-access — **no list,
   no revision**. Settings are separate intents, not Share-named paths.
5. **No** protocol header on auth-service.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof stays, marked **`x-lastdb-environment: dev`**.

**Shape (Tom, same day, refined):** exactly three product actions — fetch,
push, manage — with intent enums. Action-based, not resource-based. Cloud
storage may change without the node knowing.

---

## Where the node still names a resource, and why

These are places today’s node (or a naive port) still touches a **cloud
storage** concept. Each is an **open question for Tom**.

### R1 — Presigned upload URLs may reveal bucket / object host

Today `push` file / backup returns a presigned PUT URL the transport calls
directly (`cloud_backup_client` `put_presigned`). The URL hostname and path
can expose the object store.

**Recommendation:** keep short-lived opaque upload URLs as a **transport
detail** outside the intent schema’s named fields (type them as
`upload_url: string` with no bucket fields). Prefer cloud-mediated upload
(`push` accepts bytes or a ticket-only flow) in a later protocol bump if
URLs leak too much. For v3.0, document URLs as opaque strings.

### R2 — File identity is still `BlobPath`-shaped in the node

`lastdb_node` builds `BlobPath` (scope + content layout) for uploads and for
`delivery/object` reads. That is a cloud key shape.

**Recommendation:** replace with an opaque `file_handle` issued when a change
that owns the file is accepted (or returned from `push`/`file`). The node
stores and returns the handle; it does not compose storage keys. v3 request
fields use `file_handle`, never `blob_path` / bucket / key.

### R3 — Snapshot multipart / part layout

Restore download today walks part indexes. If the wire lists part numbers or
object keys, the node knows snapshot object layout.

**Recommendation:** `fetch`/`restore_point` returns opaque `part_handle`s;
`fetch`/`restore_part` takes only those handles. No part-index or object-key
fields in the OpenAPI.

### R4 — Range size tied to storage limit

`file_range` length is bounded by the cloud object read limit. That couples
the node to a storage max.

**Recommendation:** cloud returns `max_range_bytes` once (on `head` or first
`file` ticket) as a protocol constant field, not a store name. Node clamps
ranges to that number. Open whether the constant lives in the OpenAPI as
`x-lastdb-max-range-bytes` or only in responses.

### R5 — DEV snapshot-proof

CI provision/retire/inspect still talk about snapshot proof homes and may
name DEV buckets in operator docs.

**Recommendation:** keep the **dev** action path; mark
`x-lastdb-environment: dev`. Request bodies use proof ids / opaque handles
only — no bucket fields in the public schema.

### R6 — Members `get` (single grant) without `list`

Node `exchange.rs` calls members/get and members/list today. List is
retired by decision. Get-one is not in the three-verb illustrative set.

**Recommendation:** do **not** add `fetch`/`grant` in v3.0. Publisher uses
`manage`/`grant` (put) and local copies; members use `fetch`/`self_access`.
If get-one is required for publisher UX, add `fetch`/`grant` later as a new
intent (version bump).

---

## Other open questions for Tom

1. **Retire `settle-submissions` with no replacement?**
   Recommended: **yes**.

2. **Is `ops` the right home for reclaim/retention (outside fetch/push/manage)?**
   Recommended: **yes**.

3. **`manage`/`database_access` vs folding enrollment into `fetch`/`self_access`?**
   Recommended: keep **`database_access`** under manage — it is enrollment /
   publisher access metadata, not the member’s grant view.

4. **Single `push`/`file` intent with ticket stages vs separate begin/complete intents?**
   Recommended: **one intent** + opaque ticket field (fewer enum slots, same
   extensibility).
