# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Tom (2026-11-19): action-based interface — three verbs, node nouns, one
standard envelope. Cloud storage may change without LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
The pin lint never checks the next cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` | Dispatch |
| `cloud_backup_protocol` | Wire types |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/` | Call sites |
| `scripts/cloud_backup/snapshot_attempt_account.py` | lastdb DEV snapshot-proof |

## Hard rules

- No dual paths, aliases, fallback, or migration.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, Org exist only in People.
- No cloud storage nouns in routes or request fields.

## Shape

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
```

### Standard envelope (Decision 16)

Every fetch / push / manage body uses one shape. `database_id` and `intent`
are never redefined per intent. Auth stays in headers and the LastDBNode
token — not in the envelope.

**Request**

```json
{
  "database_id": "<uuid>",
  "intent": "<closed enum for this action>",
  "args": { }
}
```

**Response (200)**

```json
{
  "database_id": "<uuid>",
  "intent": "<same>",
  "result": { }
}
```

**Errors** stay `{ "code", "field" }` (+ `minimum_version` on 426).

`manage`/`create` uses the same envelope: the **node chooses** `database_id`
(UUID) today (`native.rs` `CreateBody` / `CloudLog::open` require a UUID) and
sends it; ExeMem does not allocate the id.

OpenAPI: shared envelope fields plus per-intent `args` / `result` schemas,
tagged by `intent` (`oneOf` + discriminator).

### Intents

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `snapshot`, `file` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |

**Added:** `manage`/`access` — members poll grant state every sync turn.
**Added:** `manage`/`delete` — publisher deletes the database and all cloud
data to reclaim storage (Decision 15).

## Examples (one per action)

### fetch

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations_since",
  "args": { "cursor": "opaque-cursor", "limit": 100 }
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations_since",
  "result": {
    "cursor": "opaque-latest",
    "mutations": [
      {
        "stream": "direct",
        "cursor": "opaque-1",
        "writer_node_key": "…",
        "device_id": "…",
        "writer_seq": 1,
        "authored_at_ms": 0,
        "namespace": "…",
        "payload_b64": "…"
      }
    ]
  }
}
```

### push

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations",
  "args": {
    "device_id": "device-1",
    "entries": [
      {
        "writer_seq": 1,
        "authored_at_ms": 0,
        "namespace": "…",
        "payload_b64": "…"
      }
    ]
  }
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations",
  "result": { "landed": "direct", "cursor": "opaque-2", "accepted": 1 }
}
```

### manage

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "delete",
  "args": {}
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "delete",
  "result": { "status": "accepted" }
}
```

## Decisions (Tom, 2026-11-19)

1. Protocol **v3**.
2. Version in **header + token**; no body `version`.
3. **426** `upgrade_required` + `minimum_version`.
4. Signed `grant#<node key>` — **no list, no revision**.
5. **No** protocol header on auth-service.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof stays, marked **`x-lastdb-environment: dev`** (lastdb CI).
8. Action-based `fetch` / `push` / `manage` only.
9. `push`/`mutations`: no `kind`; `landed` from grant.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers direct
    only; accept = publisher push; entries tagged `stream`.
11. **Silent rejections** — no discard call/field; node’s `Discarded` settle
    goes away.
12. **Link rules + access under manage** (`rules` on create/grant;
    `manage`/`access`).
13. **Cleanup ExeMem-internal** — no public ops endpoint.
14. **Ingested proposals deletable** when the publisher’s fetch `cursor` moves
    past them (internal cleanup). Safe: node advances cursor only after durable
    apply (`run.rs`).
15. **`manage`/`delete`** — publisher deletes the database and all its cloud
    data (mutations, proposals, files, snapshots, grants, link rules) so the
    owner reclaims storage. ExeMem performs deletion (sync or background);
    `result.status` is `done` or `accepted`. This is the user-facing reclaim
    path; retention/cleanup jobs stay internal (Decision 13).
16. **Standard envelope** — every fetch/push/manage call uses
    `{database_id, intent, args}` / `{database_id, intent, result}`.

## Intent args and results

### fetch / `mutations_since`

| args | `cursor?`, `limit?` |
| result | `cursor` (latest; folds head), `mutations[]` |

`Mutation`: `stream` (`direct`\|`proposal`), `cursor`, `writer_node_key`,
`device_id`, `writer_seq`, `authored_at_ms`, `namespace`, `payload_b64`,
`incorporated_from?`.

### fetch / `snapshot`

| args | `restore_point_id?`, `part_handle?` |
| result | `result: describe` → `restore_point_id`, `part_handles[]` · or `result: part` → `part_handle`, `bytes_b64`, `length` |

(Nested `result` tag inside the envelope `result` object distinguishes describe
vs part.)

### fetch / `file`

| args | `file_handle`, `offset`, `length` |
| result | `file_handle`, `offset`, `length`, `bytes_b64` |

### push / `mutations`

| args | `device_id`, `entries[]` |
| result | `landed` (`direct`\|`proposal`), `cursor`, `accepted` |

### push / `snapshot`

| args | `stage` (`prepare`\|`publish`), `ticket?`, `restore_point_id?` |
| result | `stage`, `ticket`, `restore_point_id?`, `upload_url?` |

### push / `file`

| args | `stage` (`begin`\|`complete`), `ticket?`, `size_bytes?`, `content_sha256?` |
| result | `stage`, `ticket`, `file_handle?`, `upload_url?` |

### manage / `create`

| args | `rules?` |
| result | `access?` (`state`, `role?`, `signed_grant?`) |

### manage / `grant`

| args | `grants[]` (`node_key`, `signed_grant`), `rules?` |
| result | `ok`, `access?` |

### manage / `revoke`

| args | `node_keys[]` |
| result | `ok` |

### manage / `access`

| args | `mutation_id?` |
| result | `state`, `role?`, `signed_grant?`, `contribution_status?` (`submitted`\|`published`\|`unknown`) |

### manage / `delete`

| args | `{}` (empty object) |
| result | `status` (`done`\|`accepted`) |

Publisher only. Deletes all cloud data for `database_id`.

## Use cases

| Use case | Intent |
|---|---|
| Create database | `manage`/`create` |
| Delete database / reclaim storage | `manage`/`delete` |
| Append / propose | `push`/`mutations` |
| Read mutations (+ publisher proposals) | `fetch`/`mutations_since` |
| Accept proposal | publisher `push`/`mutations` |
| Ignore proposal | silent; ExeMem deletes after fetch cursor advances |
| Upload / read file | `push`/`file`, `fetch`/`file` |
| Snapshot backup / restore | `push`/`snapshot`, `fetch`/`snapshot` |
| Grant / revoke | `manage`/`grant`, `manage`/`revoke` |
| Link rules | optional `rules` on create/grant |
| Self-access / enrollment / contribution status | `manage`/`access` |
| Retention / reclaim jobs / `*_dev` cleanup | ExeMem-internal |

## Snapshot actions

| Current | Disposition |
|---|---|
| prepare, publish | `push`/`snapshot` |
| read, read-for-restore, download | `fetch`/`snapshot` |
| retention, reclaim*, `*_dev` | ExeMem-internal |
| index-status, index-history | Absent |

## Old → new

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `manage`/`create` |
| *(no delete route)* | `manage`/`delete` |
| `…/mutation-log/access` | `manage`/`access` |
| `…/mutation-log/append` | `push`/`mutations` |
| `…/mutation-log/head` | `fetch`/`mutations_since` |
| `…/mutation-log/list` | `fetch`/`mutations_since` |
| `…/mutation-log/inbox/list` | `fetch`/`mutations_since` (publisher) |
| `…/mutation-log/inbox/settle` (accept) | publisher `push`/`mutations` |
| `…/mutation-log/inbox/settle` (discard) | removed (silent; Decision 11/14) |
| `…/mutation-log/settle-submissions` | dropped |
| `…/mutation-log/blob/bytes` | `push`/`file` |
| `…/mutation-log/delivery/object` | `fetch`/`file` |
| `…/snapshot/bytes` prepare/publish | `push`/`snapshot` |
| `…/snapshot/bytes` read* / download | `fetch`/`snapshot` |
| `…/snapshot/bytes` retention/reclaim/`*_dev` | ExeMem-internal |
| `…/share/members/set` | `manage`/`grant` / `revoke` |
| `…/share/members/get` / `list` | dropped |
| `…/share/members/read` | `manage`/`access` |
| `…/share/members/settings/*` | `rules` on create/grant; read via access |
| `…/dev-snapshot-proof/{op}` | `dev/snapshot-proof` |

## DEV snapshot-proof

`POST /api/cloud/v3/dev/snapshot-proof` — `provision` / `retire` / `status` /
`inspect`. Marked `x-lastdb-environment: dev`. Kept for lastdb CI. Uses its
own body (proof ids); not the product envelope.

## Auth

| Scheme | Where |
|---|---|
| `accountBearer` | auth-service |
| `apiKey` | `manage`/`create`, `push`/`file` |
| `lastDbNode` | signed storage actions |
| `githubOidc` | DEV snapshot-proof |

`X-LastDB-Cloud-Protocol: 3` on storage actions. Not on auth-service.

## Errors

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

## CI plan

(a) Spec ↔ dispatch. (b) Types from tagged envelopes. (c) Next cloud vs last
stable released node (#1799). No unit tests.

## Where the node still names a resource

| # | Issue | Recommendation |
|---|---|---|
| R1 | Presigned `upload_url` may reveal host | Opaque string |
| R2 | Node still builds `BlobPath` today | Opaque `file_handle` |
| R3 | Snapshot multipart layout | Opaque `part_handle` |
| R4 | Range size ↔ storage limit | Optional `max_range_bytes` |

## Open questions for Tom

None.
