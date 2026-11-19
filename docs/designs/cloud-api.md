# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

The model is the LastDB app API: one prose contract, one OpenAPI 3.1 machine
spec, one version number, and CI that keeps them honest. See `docs/app-api.md`,
`lastdb_node/app-api.json`, `scripts/lints/api_surface.py`, and
`scripts/lints/app_api_types.py`.

Tom (2026-11-19): **action-based** interface — three verbs, node nouns only.
Cloud storage may change without LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
The pin lint only checks a new node against the pinned cloud, not the next
cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` dispatch | Live route table |
| `cloud_backup_protocol` | Wire types and path constants |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/` | Product call sites |
| `docs/designs/shared-database-mutation-logs.md` | Mutation vs proposal rules |
| `scripts/cloud_backup/` | lastdb DEV snapshot-proof CI |

## Hard rules

- No dual paths, aliases, fallback, or migration. Clean cutover via protocol v3.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, and Org exist only in People.
- No route segments or request fields that name cloud storage resources
  (log, blob, inbox, bucket, B2/R2 key, delivery).

## Shape

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
```

Each request and 200 response is a **tagged union** on `intent`.

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `snapshot`, `file` |
| `manage` | `create`, `grant`, `revoke`, `access` |

**Added intent:** `manage`/`access` — members poll grant state every sync turn
(`received.rs` → today’s members/read). Create/grant responses alone cannot
cover that recurring read. One small manage intent; no fetch settings.

## Decisions (Tom, 2026-11-19)

1. Protocol starts at **v3**.
2. Version in **header + token**; drop redundant body `version`.
3. Upgrade → **426** `upgrade_required` + `minimum_version`.
4. Members are signed `grant#<node key>` entries — **no list, no revision**.
5. **No** protocol header on auth-service.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof stays in the contract, marked
   **`x-lastdb-environment: dev`** (called by lastdb CI
   `scripts/cloud_backup/snapshot_attempt_account.py`, not only ExeMem).
8. Action-based surface: only `fetch` / `push` / `manage`.
9. **`push`/`mutations` has no `kind`.** ExeMem lands direct vs proposal from
   the caller’s grant. Response: `landed: direct | proposal`.
10. **`fetch`/`mutations_since` visibility.** Publisher gets direct + proposal;
    receivers get only direct. ExeMem decides from the grant. Node sends no
    filter. Entries tagged `stream: direct | proposal`. Accepting a proposal =
    publisher `push`/`mutations` (lands `direct`). No separate settle intent
    for accepts.
11. **Silent rejections.** No discard/reject call and no discard field. A
    proposal the publisher never pushes is simply not accepted. The node’s
    current explicit `Discarded` settle (`ProposalResult::Discarded` + inbox
    settle) **goes away**.
12. **Link rules and access under manage.** Optional signed `rules` on
    `manage`/`create` and `manage`/`grant` (link `read` and any remaining
    settings). Caller role/grant on create/grant responses where natural, plus
    standalone `manage`/`access` for the recurring member read. Not under fetch.
13. **Cleanup is ExeMem-internal.** Retention, automatic retention, reclaim,
    reclaim-covered, cleanup-retired, reclaim-files, and `*_dev` track/cleanup
    jobs are **not** part of this contract. No public `ops` endpoint.
14. **Ingested proposals may be deleted.** Once the publisher’s node has
    ingested a proposal, ExeMem may delete it whether accepted or not.
    Ingestion is signaled **only** by the publisher’s `fetch`/`mutations_since`
    request `cursor` moving past that proposal — no extra call or field.
    Acceptance remains a later (or never) `push`/`mutations`. Deletion is an
    internal ExeMem cleanup job, not part of this contract. Safe with the
    node’s checkpoint pattern: `run.rs` advances the cursor only after durable
    apply (`store_cursor` + `mark_prefix`); a failed apply does not move it.

## Final intents

### `fetch` / `mutations_since`

Always returns the latest `cursor` (folds `head`). Visibility: Decision 10.

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `cursor?`, `limit?` |
| **Response** | `intent`, `cursor`, `mutations[]` |

`Mutation`: `stream` (`direct`\|`proposal`), `cursor`, `writer_node_key`,
`device_id`, `writer_seq`, `authored_at_ms`, `namespace`, `payload_b64`,
`incorporated_from?`.

### `fetch` / `snapshot`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `restore_point_id?`, `part_handle?` |
| **Response** | `result: describe` → `restore_point_id`, `part_handles[]` · or `result: part` → `part_handle`, `bytes_b64`, `length` |

### `fetch` / `file`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `file_handle`, `offset`, `length` |
| **Response** | `intent`, `file_handle`, `offset`, `length`, `bytes_b64` |

### `push` / `mutations`

No `kind`. ExeMem routes from grant (Decision 9). Accepts fold former settle
(Decision 10). No discard field (Decision 11).

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `device_id`, `entries[]` |
| **Response** | `intent`, `landed`, `cursor`, `accepted` |

### `push` / `snapshot`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `stage: prepare\|publish`, `ticket?`, `restore_point_id?` |
| **Response** | `intent`, `stage`, `ticket`, `restore_point_id?`, `upload_url?` |

### `push` / `file`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `stage: begin\|complete`, `ticket?`, `size_bytes?`, `content_sha256?` |
| **Response** | `intent`, `stage`, `ticket`, `file_handle?`, `upload_url?` |

### `manage` / `create`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `rules?` (signed link rules / settings) |
| **Response** | `intent`, `database_id`, `access?` (`role`, `state`) |

### `manage` / `grant`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `grants[]`, `rules?` |
| **Response** | `intent`, `ok`, `access?` |

`GrantEntry`: `node_key`, `signed_grant`.

### `manage` / `revoke`

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `node_keys[]` |
| **Response** | `intent`, `ok` |

### `manage` / `access` (added)

Recurring “what’s my access” for members and publisher enrollment checks.
Also covers today’s ACCESS contribution-status when `mutation_id` is set
(same path family, no tenth product verb beyond this manage intent).

| | Fields |
|---|---|
| **Request** | `intent`, `database_id`, `mutation_id?` |
| **Response** | `intent`, `state` (`active`\|`revoked`\|`absent`), `role?`, `signed_grant?`, `contribution_status?` |

## Use cases → intents

| Use case | Lands on |
|---|---|
| Create database | `manage`/`create` |
| Append / propose | `push`/`mutations` (`landed` from grant) |
| Read mutations (+ publisher proposals) | `fetch`/`mutations_since` |
| Accept proposal | publisher `push`/`mutations` |
| Reject / ignore proposal | **silent** — never `push`/`mutations`; after the publisher’s fetch cursor moves past it, ExeMem may delete (Decision 14) |
| Upload / read file | `push`/`file`, `fetch`/`file` |
| Snapshot backup / restore | `push`/`snapshot`, `fetch`/`snapshot` |
| Grant / revoke | `manage`/`grant`, `manage`/`revoke` |
| Link rules / settings | optional `rules` on create/grant |
| Self-access / enrollment / contribution status | `manage`/`access` |
| Reclaim / retention / `*_dev` cleanup | **ExeMem-internal** (not in contract) |

## Snapshot actions

| Current | Disposition |
|---|---|
| prepare, publish | `push`/`snapshot` |
| read, read-for-restore, download | `fetch`/`snapshot` |
| retention, automatic-retention, reclaim, reclaim-covered, cleanup-retired, reclaim-blobs, track_dev, cleanup_dev, cleanup_receipt_dev | **ExeMem-internal** (Decision 13) |
| index-status, index-history | Absent |

## Old → new

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `manage`/`create` |
| `…/mutation-log/access` | `manage`/`access` |
| `…/mutation-log/append` | `push`/`mutations` |
| `…/mutation-log/head` | `fetch`/`mutations_since` (latest cursor) |
| `…/mutation-log/list` | `fetch`/`mutations_since` |
| `…/mutation-log/inbox/list` | `fetch`/`mutations_since` (publisher; `stream: proposal`) |
| `…/mutation-log/inbox/settle` (accept) | folds into publisher `push`/`mutations` |
| `…/mutation-log/inbox/settle` (discard) | **removed** — silent rejection; ExeMem deletes after ingest cursor advances (Decisions 11, 14) |
| `…/mutation-log/settle-submissions` | dropped (no node caller) |
| `…/mutation-log/blob/bytes` | `push`/`file` |
| `…/mutation-log/delivery/object` | `fetch`/`file` |
| `…/mutation-log/snapshot/bytes` prepare/publish | `push`/`snapshot` |
| `…/mutation-log/snapshot/bytes` read / read-for-restore / download | `fetch`/`snapshot` |
| `…/mutation-log/snapshot/bytes` retention/reclaim/* / `*_dev` | ExeMem-internal |
| `…/share/members/set` | `manage`/`grant` (+ `revoke`) |
| `…/share/members/get` | dropped |
| `…/share/members/list` | dropped |
| `…/share/members/read` | `manage`/`access` |
| `…/share/members/settings/set` | optional `rules` on `manage`/`grant` (or create) |
| `…/share/members/settings/get` | via `manage`/`access` / grant responses |
| `…/dev-snapshot-proof/{op}` | `dev/snapshot-proof` (dev-marked) |

## DEV snapshot-proof

`POST /api/cloud/v3/dev/snapshot-proof` — intents `provision`, `retire`,
`status`, `inspect`. Marked `x-lastdb-environment: dev`. Kept because
**lastdb** CI calls it (`snapshot_attempt_account.py`).

## Auth

| Scheme | Where |
|---|---|
| `accountBearer` | auth-service |
| `apiKey` | `manage`/`create`, `push`/`file` |
| `lastDbNode` | signed storage actions |
| `githubOidc` | DEV snapshot-proof |

Header `X-LastDB-Cloud-Protocol: 3` on storage actions. Not on auth-service.

## Errors

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

426 → `upgrade_required` + `minimum_version`.

## CI plan

(a) Spec ↔ dispatch. (b) Types from tagged unions. (c) Next cloud vs last
stable released node (#1799). No unit tests.

## Where the node still names a resource

| # | Issue | Recommendation |
|---|---|---|
| R1 | Presigned `upload_url` may reveal object-store host | Opaque string; no bucket fields |
| R2 | Node still builds `BlobPath` today | Opaque `file_handle` from `push`/`file` |
| R3 | Snapshot multipart layout | Opaque `part_handle` only |
| R4 | Range size ↔ storage limit | Optional `max_range_bytes` on begin — no store name |

## Open questions for Tom

None. Cursor-as-ingestion is consistent with `run.rs` (advance only after
durable apply). Decisions 11–14 close the prior open list.
