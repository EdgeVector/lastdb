# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

The model is the LastDB app API: one prose contract, one OpenAPI 3.1 machine
spec, one version number, and CI that keeps them honest. See `docs/app-api.md`,
`lastdb_node/app-api.json` (`api_version` 42), `scripts/lints/api_surface.py`,
and `scripts/lints/app_api_types.py`.

## Why

Today the cloud surface is a hand-dispatched path tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version
(the JSON field named `version` only accepts the integer `2`).
`deny_unknown_fields` is used everywhere, so a field rename on one side is an
opaque failure on the other. The pin lint
(`scripts/lints/lint-cloud-protocol-pin.py`) only checks a **new node** against
the **pinned cloud**. It never checks the **next cloud** against the **last
stable released node**. That gap is why lastdb #1799 (token / wire cleanup)
broke 0.23.40 nodes and was caught only by People DEV sharing.

## Sources (verified)

Route inventory and auth were verified against:

| Source | Role |
|---|---|
| `EdgeVector/exemem-infra` `exemem_service/lambdas/storage_service/src/http.rs` | Top-level dispatch |
| Same repo `…/auth/members.rs`, `…/blobs/mod.rs`, `…/snapshots/mod.rs`, `…/logs/mod.rs` | Action lists |
| Same repo `cdk/lib/storage-service-http.ts`, `cdk/lib/exemem-http-api.ts` | API Gateway routes |
| Same repo `docs/cloud-byte-service-v2.md` | Prose (see note below) |
| `cloud_backup_protocol` (this repo) | Wire types, path constants, `ProtocolError` |
| `lastdb_node` cloud client paths | Which credentials each route uses |

**Prose vs code:** `docs/cloud-byte-service-v2.md` still names snapshot actions
`index-status` / `index-history` and a `delivery/snapshot` spelling. Those are
**not** in the current `storage_service` dispatch or snapshot `action` enum.
This design follows the Rust dispatch tables, not the stale prose.

**Auth service routes** on the same HTTP API (`/api/developer/*`,
`/api/auth/*`) are inventoried below. They are not storage routes. The OpenAPI
includes them as a separate tag so one file covers the whole ExeMem HTTP surface
a node talks to.

## Contract model (mirror the app API)

After acceptance, the live contract is three committed artifacts (names can
move out of `docs/designs/` when the work lands):

1. **Prose** — this document, later promoted beside `docs/app-api.md`.
2. **OpenAPI 3.1** — `docs/designs/cloud-api.openapi.json` (this PR).
   `info.version` is the cloud protocol version as a decimal string.
3. **Lint + generated types** — CI scripts modeled on `api_surface.py` and
   `app_api_types.py` (plan below; not written in this docs-only PR).

Rules:

- The OpenAPI file is the explicit definition. Prose explains it.
- Every served storage path appears in the OpenAPI `paths` object.
- Every auth scheme appears under `components.securitySchemes`.
- Every stable error code appears under `components.schemas`.
- Unknown JSON fields stay refused (`additionalProperties: false` on request
  bodies), matching today's `deny_unknown_fields`.

## Hard rules (owner)

- **No fallback, compatibility, migration, or dual paths.** Old and new routes
  do not run side by side. A cutover is a clean break, guarded by the protocol
  version check.
- **No unit tests** in the plan. End-to-end DEV checks in CI are fine.
- **One concept, one system.** Share, Deliver, and Org exist only in People.
  The cloud stores and returns generic signed config (`read`, `write`,
  `member_writes`, member rows). It does not name presets and does not own
  invitation or org product logic.
- Keep it simple and scalable. Plain language.

## Current inventory (v2)

All storage routes today are **POST** under `/api/cloud/backups/v2/`. API Gateway
attaches two proxies plus optional DEV proof routes.

### Mutation-log proxy — `POST …/mutation-log/{proxy+}`

| Suffix | Purpose |
|---|---|
| `create-database` | Native create (publisher; API key on create) |
| `access` | Access / enrollment view for one database |
| `append` | Append to mutation log or proposal log (by settings) |
| `head` | Head / writer tip |
| `list` | Page of mutation-log entries |
| `settle-submissions` | Publisher settles older opaque submission position |
| `inbox/list` | Publisher lists proposal-log page |
| `inbox/settle` | Publisher settles proposal-log page |
| `blob/bytes` | Cold blob: `context` / `prepare` / `complete` |
| `snapshot/bytes` | Snapshot actions (see below) |
| `delivery/object` | Bounded ciphertext byte range of one record file |

**Snapshot `action` values** (body discriminator on `snapshot/bytes`):
`read`, `read-for-restore`, `prepare`, `download`, `publish`, `retention`,
`automatic-retention`, `reclaim`, `reclaim-covered`, `cleanup-retired`,
`reclaim-blobs`, plus DEV-only `track_dev`, `cleanup_dev`,
`cleanup_receipt_dev`.

**Blob `action` values** (body discriminator on `blob/bytes`):
`context`, `prepare`, `complete`.

### Members proxy — `POST …/share/members/{proxy+}`

| Suffix | Purpose |
|---|---|
| `set` | Publisher writes signed member entries |
| `get` | Read one member entry |
| `list` | Publisher pages every member entry |
| `read` | Member-list revision; optional self-access header |
| `settings/set` | Publisher writes signed settings statement |
| `settings/get` | Read settings statement |

### DEV-only — `POST …/dev-snapshot-proof/{operation}`

`provision`, `retire`, `status`, `inspect`. JWT authorizer
(`token.actions.githubusercontent.com`, audience `lastdb-dev-snapshot`).
Attached only on the DEV storage stack.

### Auth service (same HTTP API, different Lambda)

| Method | Path |
|---|---|
| GET, POST, DELETE | `/api/developer/api-keys` |
| POST | `/api/developer/validate-key` |
| POST | `/api/auth/cli/register` |
| POST | `/api/auth/payments/webhook` |
| GET | `/api/auth/payments/status` |
| POST | `/api/auth/payments/checkout` |
| POST | `/api/auth/payments/portal` |

### Auth schemes in use today

| Scheme | Wire | Used for |
|---|---|---|
| `em_` API key | Header `X-API-Key` | Native create; dual-auth on blob context/prepare/complete; developer key routes |
| `LastDBNode` | Header `Authorization: LastDBNode <token>.<sig>` | Signed storage POSTs; token JSON is `NodeReadToken` |
| GitHub OIDC JWT | Header `Authorization: Bearer …` | DEV snapshot-proof only |

`LastDBNode` binds method, path, raw body SHA-256, nonce, realm, and node key.
Clock skew is ±60s. Replay uses the nonce.

### Errors today

Body shape from `cloud_backup_protocol::ProtocolError`:

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

Codes: `invalid_input`, `unsupported_capability`, `unauthorized`, `not_found`,
`generation_conflict`, `identity_conflict`, `predecessor_conflict`,
`resource_limit`, `quota_exceeded`, `missing_dependency`, `integrity_mismatch`,
`provider_pending`, `outcome_unknown`, `retry_later`.

HTTP status mapping (storage): 404 not_found, 403 unauthorized, 409 conflicts,
413 limits/quota, 202 provider_pending, 503 outcome_unknown/retry_later,
else 400. There is **no** typed “upgrade required” answer today.

### Concept-boundary callouts

These routes are **config storage**, not People Share/Deliver/Org logic. Keep
them. Rename the path prefix so “share” does not look like the People preset:

- `share/members/*` and `settings/*` store publisher-signed member rows and the
  generic `(read, write, member_writes)` statement. The service already does not
  interpret `member_writes`. That is correct.
- Do **not** add named presets, invitations, circles, albums, or org membership
  to this API. Those stay in People / the LastDB owner and app surfaces.

## Target shape (protocol 3)

### URL and method scheme

Keep **POST + JSON body** for every storage operation (same pattern as the app
API and today’s signed cloud POSTs). Drop the nested `backups/v2` and the
mutation-log dumping ground.

Prefix: `/api/cloud/v3/`.

| Resource | New path | Notes |
|---|---|---|
| Databases | `POST /api/cloud/v3/databases` | create |
| | `POST /api/cloud/v3/databases/access` | access view |
| Mutation log | `POST /api/cloud/v3/mutation-log/append` | |
| | `POST /api/cloud/v3/mutation-log/head` | |
| | `POST /api/cloud/v3/mutation-log/list` | |
| | `POST /api/cloud/v3/mutation-log/settle-submissions` | |
| Proposals | `POST /api/cloud/v3/proposals/list` | renamed from inbox |
| | `POST /api/cloud/v3/proposals/settle` | |
| Blobs | `POST /api/cloud/v3/blobs` | body `action`: context/prepare/complete |
| Snapshots | `POST /api/cloud/v3/snapshots` | body `action`: current snapshot actions |
| Delivery | `POST /api/cloud/v3/delivery/object` | out from under mutation-log |
| Members | `POST /api/cloud/v3/members/set` | was share/members |
| | `POST /api/cloud/v3/members/get` | |
| | `POST /api/cloud/v3/members/list` | |
| | `POST /api/cloud/v3/members/read` | |
| Settings | `POST /api/cloud/v3/settings/set` | was share/members/settings |
| | `POST /api/cloud/v3/settings/get` | |
| DEV proof | `POST /api/cloud/v3/dev-snapshot-proof/{op}` | DEV only |

Auth-service paths stay under `/api/developer` and `/api/auth` (no `/v3`
rename in this design unless Tom wants one file version over everything).

### Old → new route table

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `…/databases` |
| `…/mutation-log/access` | `…/databases/access` |
| `…/mutation-log/append` | `…/mutation-log/append` |
| `…/mutation-log/head` | `…/mutation-log/head` |
| `…/mutation-log/list` | `…/mutation-log/list` |
| `…/mutation-log/settle-submissions` | `…/mutation-log/settle-submissions` |
| `…/mutation-log/inbox/list` | `…/proposals/list` |
| `…/mutation-log/inbox/settle` | `…/proposals/settle` |
| `…/mutation-log/blob/bytes` | `…/blobs` |
| `…/mutation-log/snapshot/bytes` | `…/snapshots` |
| `…/mutation-log/delivery/object` | `…/delivery/object` |
| `…/share/members/set` | `…/members/set` |
| `…/share/members/get` | `…/members/get` |
| `…/share/members/list` | `…/members/list` |
| `…/share/members/read` | `…/members/read` |
| `…/share/members/settings/set` | `…/settings/set` |
| `…/share/members/settings/get` | `…/settings/get` |
| `…/dev-snapshot-proof/{op}` | `…/dev-snapshot-proof/{op}` |

No dual routing. After the cutover deploy, v2 paths return the typed upgrade
error (or plain 404 only before the version gate lands — prefer the typed
error on every storage Lambda entry).

## Protocol version

### Single wire version

One integer: **cloud protocol version**. First cutover value: **3**.
`info.version` in the OpenAPI is `"3"`. Rust constant (proposed name)
`CLOUD_PROTOCOL_VERSION: u32 = 3`.

There is no separate “API version” and “token version”. Body fields that
today hard-code JSON `2` become this same integer, or are removed when the
header alone is enough (see open questions).

### Where the version travels

**Required request header:**

```
X-LastDB-Cloud-Protocol: 3
```

Checked first on every storage route, before body decode.

**Also bound into `LastDBNode`:** the token JSON carries the same integer in
`version`, and that value is covered by the signature digest. Header and token
must match. A mismatch is `invalid_input` / `cloud.protocol.mismatch`.

API-key-only auth-service routes that are not storage may omit the header until
Tom decides they share the same number (recommended: omit; they are not the
backup wire).

### Out-of-date node

If the header is missing, lower than the service’s minimum, or higher than the
service supports, the reply is **one clear typed error** — never an opaque
serde failure:

```
HTTP 426
{ "code": "upgrade_required", "field": "cloud.protocol", "minimum_version": 3 }
```

`minimum_version` is required on this code. The node surfaces it to the owner
as “upgrade LastDB to continue using cloud.” No silent retry.

Add `upgrade_required` to the stable error code set. Map it only to HTTP 426.

### How the version is bumped

| Change | Version action |
|---|---|
| Add a route or a required request field | Bump `CLOUD_PROTOCOL_VERSION` |
| Remove a route or change a field meaning | Bump version **and** raise `CLOUD_PROTOCOL_MIN_VERSION` |
| Add an optional response field the old client ignores | Still bump the version under today’s deny-unknown clients; clients that deny unknown fields cannot ignore new response keys |
| Rename a path | Bump; cutover only — no alias |

`CLOUD_PROTOCOL_MIN_VERSION` is what the service accepts. For a clean-break
release it equals `CLOUD_PROTOCOL_VERSION`. It only lags when Tom later chooses
a deliberate multi-release floor — and even then there is still **one** path
tree, not two.

Stable releases publish the protocol version the shipped node speaks. That
number is what the reverse pin lint reads (check c).

## Errors (stable)

All storage refusals use one object:

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

Optional on upgrade only: `minimum_version` (u32).

| Code | Typical HTTP |
|---|---|
| `invalid_input` | 400 |
| `unauthorized` | 403 |
| `not_found` | 404 |
| `generation_conflict` / `identity_conflict` / `predecessor_conflict` | 409 |
| `resource_limit` / `quota_exceeded` | 413 |
| `unsupported_capability` | 400 |
| `missing_dependency` / `integrity_mismatch` | 400 |
| `provider_pending` | 202 |
| `outcome_unknown` / `retry_later` | 503 |
| `upgrade_required` | 426 |

Payment remains `unauthorized` with `field: "payment.subscription"` (existing
convention). Do not invent a second payment shape.

## Auth (target)

Unchanged schemes, documented in OpenAPI:

1. **`apiKey`** — `X-API-Key` (`em_…`). Create-database and blob dual-auth;
   developer key management.
2. **`lastDbNode`** — `Authorization: LastDBNode <base64url(token)>.<base64url(sig)>`.
3. **`githubOidc`** — DEV snapshot-proof only.

Dual-auth routes list both `apiKey` and `lastDbNode` as required.

## CI plan (three checks)

No unit tests. Source lints and DEV end-to-end operations only.

### (a) Spec matches the dispatch table

New lint (name proposal: `scripts/lints/cloud_api_surface.py`), modeled on
`api_surface.py`:

- Parse OpenAPI `paths` for `/api/cloud/v3/…`.
- Parse `storage_service` route match arms (and members / snapshot / blob
  `action` enums) from the pinned `exemem-infra` tree, or from a committed
  path list generated when the fold pin moves.
- Fail when a served route or action is missing from the OpenAPI, or when the
  OpenAPI lists a path the service does not serve.
- Fail when `info.version` ≠ `CLOUD_PROTOCOL_VERSION`.

### (b) Types generated from the spec

New generator (name proposal: `scripts/lints/cloud_api_types.py`), modeled on
`app_api_types.py`:

- Input: the OpenAPI file.
- Output: committed generated Rust request/response types (or a thin module
  that re-exports checked shapes) used by `cloud_backup_protocol` / the node
  client.
- `--check` in CI; `--write` for developers.
- No third-party codegen package required if we follow the app API script’s
  constrained JSON Schema subset.

### (c) Next cloud vs last stable released node

Close the #1799 gap. Extend or pair
`scripts/lints/lint-cloud-protocol-pin.py`:

- Today: **new lastdb** vs **pinned exemem fold** (keep).
- Add: **candidate cloud / protocol tree** vs **protocol tree at the latest
  stable LastDB release tag** (for example `0.23.40`).
- Fail when the candidate would reject that stable node’s requests or when
  that stable node would reject the candidate’s responses, under
  `deny_unknown_fields` rules already encoded in the pin helper.
- When a break is intentional, the PR must raise
  `CLOUD_PROTOCOL_MIN_VERSION` / `CLOUD_PROTOCOL_VERSION` and the stable-release
  process must ship a node that speaks the new version **before** or **with**
  the cloud deploy. The lint’s job is to make the break visible in CI instead
  of in People DEV.

DEV end-to-end: reuse existing cloud shared-daily / snapshot proof operations
against the new paths after cutover. No new unit test target.

## Cutover

1. Land the OpenAPI + prose (this PR).
2. Implement protocol 3 paths and version gate in `cloud_backup_protocol` +
   `exemem-infra` + `lastdb_node` in one coordinated release train.
3. Deploy cloud that speaks only v3; ship a LastDB stable that speaks only v3.
4. Remove v2 path constants. Do not leave aliases.

## Open questions for Tom

1. **Protocol number for the cutover — start at 3?**
   Recommended: **yes** (current wire field is already `2`; a new tree should
   not reuse it).

2. **Keep `version` inside every JSON body and token, or header-only plus token?**
   Recommended: **header + token field**; drop redundant body `version` fields
   where the path already implies the protocol, to shrink deny-unknown churn.

3. **HTTP status for upgrade — 426?**
   Recommended: **426** with `code: upgrade_required` and `minimum_version`.

4. **Rename `share/members` → `members` and `settings`?**
   Recommended: **yes**, so the cloud URL does not look like the People Share
   preset. Behavior stays config pass-through.

5. **Should auth-service (`/api/developer`, `/api/auth`) share the cloud
   protocol version header?**
   Recommended: **no** — separate surface; document in the same OpenAPI file
   under a distinct tag only.

6. **Where should the live OpenAPI live after implementation —
   `cloud_backup_protocol/` or `lastdb_node/`?**
   Recommended: **`cloud_backup_protocol/cloud-api.openapi.json`**, because both
   node and service consume that crate; prose can stay under `docs/`.

7. **DEV snapshot-proof and DEV snapshot cleanup actions — keep inside the
   public contract file?**
   Recommended: **yes**, marked `x-lastdb-environment: dev`, so the dispatch
   lint cannot forget them.
