# The app API: one read, one write

The explicit definitions are in `lastdb_node/app-api.json`.
When this document and that file differ, the file wins.

Status: this is the contract (Tom, 2026-11-16). This document describes
what `lastdbd` serves today and names what is not built yet.

The app API is the interface for apps installed through the app registry. It
has two routes plus the `changes` extension. Each takes a list; one item is a
list of one. The owner socket also serves databases, sharing, status, the
schema catalog, and admin. Record reads and writes go through these routes.

## Read

```
POST /v1/read
{ "reads": [
    { "schema": "notes/Note", "key": { "hash": "n1" } },
    { "schema": "notes/Tag",  "range": { "hash": "work" }, "limit": 100, "keys_only": true },
    { "schema": "notes/Note", "via": "by_author", "range": { "hash": "tom" } },
    { "database": "work", "schema": "notes/Note", "key": { "hash": "n1" } },
    { "schema": "notes/Note", "keys_only": true, "limit": 100 }
] }

200
{ "results": [
    { "records": [ { "key": { "hash": "n1", "range": null }, "revision": "9c2e…", "created_by": "ab12…", "updated_by": "ab12…", "record": { … }, "file": null } ], "next_cursor": null },
    { "keys": [ … ], "next_cursor": "b3f1…" },
    { "records": [ … ], "next_cursor": null },
    { "error": { "code": "not_found", "message": "…" } },
    { "keys": [ { "hash": "n1", "range": null }, … ], "next_cursor": "7a09…" }
] }
```

- A read of records names a schema and exactly one of `key` or `range`.
- `key` is one record: `hash`, and `range` when the schema has one.
- `range` is the records under one `hash`: all of them, those with a `prefix`,
  or those from `start` up to `end`.
- `via` reads through a named alternate key. The reply holds the primary keys.
- `limit` bounds a page. `next_cursor` is an opaque string; send it back as `cursor`.
- `keys_only` returns keys without records. An app can list the keys of a schema
  a page at a time. There is no filter on a field value.
- A `keys_only` read with neither `key` nor `range` lists the primary keys of the
  schema's records (Tom, 2026-11-17). A page holds 100 keys unless `limit` says
  otherwise, and at most 1000. Follow `next_cursor` until it is null for all of
  them. It takes no `via`, no `file`, and no `file_dir`. An app that keeps
  something derived from a schema uses it to build and to rebuild.
- `database` selects a database. Omit it to use the personal database. The read still returns one result per item.
- A received database uses the same request and the same reply as an owned database: a keyed read, a range, `keys_only`, paging, and `via`. The node reads the receiver store. `via` uses that store's alternate keys. An alternate the schema does not declare is the same error as on an owned database.
- The received snapshot must already be durable. Until sync has stored that marker, the read item is `busy` and its message is `database snapshot is not durable`. If the receiver store cannot be opened, the message is `database store is unavailable`.
- An unknown identifier is `not_found`. Its message is `database is not registered on this node`. A registration that is not a signed log gets that same message. A bad identifier is `invalid_request` and its message is `invalid database identifier`.
- On the app socket the usual grant check still applies. The schema must be in the app's `access.read`, or the app must have `"*"`.
- Each read gets its own result or its own error, in order. One list holds at most 64 reads.

## Write

```
POST /v1/write
{ "mutations": [
    { "schema": "notes/Note", "op": "create",  "key": { "hash": "n4" }, "record": { … } },
    { "schema": "notes/Note", "op": "replace", "key": { "hash": "n1" }, "revision": "9c2e…", "record": { … },
      "alternates": { "by_author": { "hash": "tom", "range": "n1" } } },
    { "schema": "notes/Tag",  "op": "delete",  "key": { "hash": "work", "range": "2026-11-16" }, "revision": "41ab…" }
] }

200
{ "written": 3, "revisions": [ "77d0…", "a3f1…", "c09e…" ] }
```

Every write carries a condition, so two writers cannot overwrite each other without knowing it.

- `create` writes only when no record holds the key. It carries the record and no revision.
- `replace` writes only when the record is still at `revision`, the revision the caller read. It carries the complete record.
- `delete` deletes only when the record is still at `revision`. It carries no record.
- A mutation whose condition does not hold refuses the whole list with `409 conflict`. The reply names that mutation and why. The caller reads again and decides.
- `index` is the zero-based position of the first mutation whose condition failed. `reason` is `exists` when a `create` finds a record at the key, `absent` when a `replace` or a `delete` finds no record, or `revision` when the record is at another revision.
- `{ "error": { "code": "conflict", "message": "mutation 1: the record is at another revision", "index": 1, "reason": "revision" } }`
- A schema that declares alternate keys takes their addresses in `alternates` on a `create` and a `replace`.
- The list is all or nothing. Every mutation in one list names the same database. Omit `database` to use the personal database.
- A write that names a received database commits only when that database's `write` setting is `direct` and this node is an active member with the Submit role. The change is stored on this node and appended to the mutation log with this node's key. Otherwise the status is 403 and the code is `permission_denied`. The message is `a received database is read only`. A prefix that is not durable answers 503 with `database snapshot is not durable`. An unknown identifier is `not_found` with the message `database is not registered on this node`. While `write` is `proposals`, a receiver does not use this route. The proposal route is on the owner socket. See Sharing.
- The reply comes after the write is on disk. `revisions` holds the new revision of each record, in order.

A read returns each record's `revision` beside its key. It also returns `created_by` and `updated_by` when the record has them. Each value is the lowercase hex node key of a writer. The node omits a key when the record has no value for it. It does not send null. A record written before the values existed omits both keys, and so does a node on an older build at this API version. `created_by` is set only when the write creates the record. A replace keeps the previous `created_by`, including when that value is absent. `updated_by` is the writer of this revision. A write that names `created_by` or `updated_by`, on the write item or inside `record`, is refused.

## Files

A file is a path in and a path out. No file bytes pass through the app API.

```
{ "schema": "notes/Note", "op": "create", "key": { "hash": "n5" }, "record": { … },
  "file": { "path": "/Users/me/Pictures/whiteboard.jpg", "name": "whiteboard.jpg", "media_type": "image/jpeg" } }
```

- A `create` or a `replace` can carry a `file` with the path of a file the caller can read. `name` and `media_type` are optional.
- LastDB keeps a reference to the file where it is and makes no local copy. The bytes go up at cloud upload.
- `lastdb mutate` names that file by path and does not copy it. The file must stay where it is. On a replace, `"file": null` clears the record's file. A request with no `file` key sends no file.
- A path inside the LastDB home is refused, through any symlink. That covers the
  key file and every database in the home. Every file that cannot be linked gets
  the same answer.
- A `replace` that omits `file` keeps the record's file. A `replace` whose `file` is JSON null clears it. The node frees the bytes when no record names them. A `create` names a file by path or omits `file`. A `delete` carries no `file`.
- Every record in a read reply has `file`: null, or `name`, `media_type`, `size` and `local`.
- In a read reply, `file: null` means the record has no file. That is the reply. On a `replace`, sending `file: null` is the request that clears the file.
- `local` is the path on this device when this device holds the file as a reference and a regular file of the recorded size is still there. It is null when that original is gone or has another size, and when the file is not on this device.
- A read item with `"file": true` also gets `path`. For a reference, `path` is that same local path. When the original is gone or has another size, that read item answers `not_found`.
- For a file LastDB holds as its own bytes, `local` is null until a copy sits in a folder this read may use. On the app socket that folder is `<data>/files` when the grant names `data`. The node creates `files` as mode 0700 if it is missing, and the file name is the content hash. `path` is null when the grant has no `data`. On the owner socket `path` is null unless the read names `file_dir`.
- `file_dir` is an absolute path of a directory that already exists, is owned by the calling user, and is not group or world writable. The node does not create the directory and does not change its mode. It writes the plain bytes to `<file_dir>/<content hash>` and returns that path. The file is mode 0600. The node writes a temporary name in that folder and renames it into place, and it does not follow a symlink at the destination. A regular file of that name and size is reused. The reply sets `local` to the same path. `file_dir` requires `"file": true`. On the app socket `file_dir` is `invalid_request`: the app's folder is fixed by its grant. A missing directory, a symlink, a directory owned by someone else, or a directory that is group or world writable is `invalid_request`.
- A reference to a file that already lives on this device is not copied into `file_dir` or into the app data folder. `path` stays that original path. When the original is gone or has another size, a read with `"file": true` is `not_found`.
- A read that does not set `file` to true does not write. It reports `local` only when the file is already there.
- The reply has no `cloud` field. The node has no local record of whether this exact file version is in the cloud.


## Extensions

Read and write are the core. Anything more is an extension that an app asks for
by name, so a new feature does not raise a version.

- An extension has a name, one lowercase word, and no version beside it.
- It is served at `POST /v1/x/<name>` with a JSON body.
- It uses the same error shape as the core and the same optional `database` field.
- Adding an extension changes no version number. `scripts/lints/api_surface.py`
  lists extensions by name in `lastdb_node/api-surface.json` and refuses the
  removal of one.
- A node lists the extensions it has in `extensions` on `GET /api/version`.
  An installer reads that list.
- An extension is frozen once an app in the registry names it. A change that
  would break those apps is a new name beside the old one.

### changes

LastDB keeps one numbered list of changed record keys for each database. An app
that keeps something derived from records remembers the last number it handled
and asks for everything newer.

```
POST /v1/x/changes
{ "after": 1200, "limit": 500, "schemas": ["brain/Design"] }

200
{ "id": "9f3c…", "head": 1234, "through": 1210, "gap": false,
  "changes": [ { "seq": 1201, "schema": "brain/Design",
                 "key": { "hash": "design-x", "range": null }, "deleted": false } ] }
```

- `after` is the last number the app handled. Omit it and the read starts at 0. The node keeps the newest 250000 entries. When the head is past that window, `after` 0 makes `gap` true and the reply has no entries.
- `limit` is at most 500. Zero reads `head` only.
- `schemas` is optional. Without it the read returns every schema.
- An entry holds no record values. The app reads the changed records through `/v1/read`.
- The app saves `through` as its new position. When `id` differs from the one it
  saved, or `gap` is true, it rebuilds from key pages.
- The owner uses the same `POST /v1/x/changes` call.
- `POST /v1/x/changes` does not serve a received database. That request is `not_found` and its message is `a received database is not served by /v1/x/changes`. The node does not open the receiver store. An unknown identifier is `not_found` with the message `database is not registered on this node`. Omit `database` to use the personal database.

### databases

An app lists and creates named databases, places a schema in one, and uses a
whole-database share. The owner routes stay where they are. This call runs
the same handlers.

```
POST /v1/x/databases
{ "action": "create", "name": "notes", "sharingMode": "whole_database" }

200
{ "ok": true, "user_hash": "…", "database": { "id": "…", "name": "notes",
  "kind": "shareable", "access": "owner", "state": "ready",
  "sharingMode": "whole_database" } }
```

| `action` | Owner route | Other fields | Success |
|---|---|---|---|
| `list` | `GET /api/databases` | none | 200 `{ok, user_hash, databases}` |
| `create` | `POST /api/databases` | `name`, `kind`, `sharingMode` | 200 `{ok, user_hash, database}` |
| `accept` | `POST /api/databases/accept` | `invitation`, `publisher`, `name`, `database_id`, or `link` and `name` | 200 `{ok, user_hash, database}` |
| `schemas` | `GET /api/databases/{id}/schemas` | `database`, `include_full`, `include_definition_ids` | 200 schema list |
| `declare_schema` | `POST /api/databases/{id}/apps/declare-schema` | `database`, `schema`, `intent`, `retention`, `app_id` | 200 declare envelope |
| `share_config` | `GET /api/databases/{id}/share/config` | `database` | 200 `{ok, user_hash, databaseId, revision, read, write, memberWrites, members}` |
| `set_share_config` | `PUT /api/databases/{id}/share/config` | `database`, `config` | 200 `{ok, user_hash, databaseId, requestId, revision, committed}` |
| `invite` | `POST /api/databases/{id}/invitations` | `database`, `contactCode`, `expectedNodeKey`, `expiresInHours` | 200 `{ok, user_hash, databaseId, invitation}` |
| `share_link` | `POST /api/databases/{id}/share/link` | `database` | 200 `{ok, user_hash, databaseId, link}` |
| `submit` | `POST /api/databases/{id}/submissions` | `database`, `mutation`, `idempotency_key` | 202 `{ok, status, mutation_id, submission_sequence}` |
| `submission_status` | `POST /api/databases/{id}/submissions/status` | `database`, `mutationId` | 200 `{ok, mutation_id, status}` |

- `database` is the id or the name. Omit it, or send JSON null, and the call uses the personal database. A bad value is `invalid_request` and the message is `invalid database identifier`. `list`, `create`, and `accept` reject the field as `unknown field database`. `accept` names its target `database_id`.
- `accept` takes an invitation or a link. The invitation fields are `invitation`, `publisher`, optional `name`, and optional `database_id`. The link fields are `link` and optional `name`. A body with both is `invalid_request` and the message is `unknown field invitation` when `link` is present, because that shape allows only `link` and `name`. A malformed link is `invalid link`.
- `share_link` sends an empty body to `POST /api/databases/{id}/share/link`. The reply field `link` is one opaque token: `lastdb-read-v1.` plus base64url of canonical JSON. It is not a URL, so the content key does not land in a query string, a browser history, or a proxy log. The token carries the environment, the publisher account scope, the database id, the publisher key, and the content key. It carries no member identity and it is not sealed to a recipient. The node does not read the share settings when it makes the token. The token is useful when the service setting is `read: anyone`. A cloud failure uses the three answers in Errors.
- `kind` is `shareable` or `personal`. Omit it for `shareable`. `sharingMode` is `whole_database` for a shareable shared database. A filtered or app catalog `spec` on create is refused.
- A present `Idempotency-Key` header on `create` is forwarded. It is a 64-character lowercase hex database id, only when `sharingMode` is `whole_database`. Any other present key is `invalid_request` and the message is `whole-database creation uses a 64-hex database id idempotency key`. An absent header is allowed.
- `include_full` and `include_definition_ids` are booleans. `true` is sent as that query flag. `false` and JSON null are omitted. Another type is `invalid_request`.
- `declare_schema` is allowed by the `databases` extension. An app that declared the extension may place a schema. The read list and the write list are not consulted for `declare_schema`. Those lists govern `/v1/read` and `/v1/write`.
- On the app socket `declare_schema` takes an optional `app_id`. A present string is passed to the owner handler as that `app_id`. Omit the field, or send JSON null, and `app_id` is the calling app's id. A value that is not a string is `app_id must be a string`. The owner handler requires that name to be `{app_id}/{local}`, or a local name with no slash, which it prefixes with the app id. An app places a schema in another namespace by setting `app_id` to that namespace. Without that field, the owner handler refuses the name: `invalid_request` and the message is `schema.name namespace must match request.app_id`. A missing or empty name is the owner handler's refusal. On the owner socket `app_id` is required. A missing or empty value is `app_id is required`.
- `intent` is `catalog_sync`. Omit it for that value. `retention` sits beside `schema`, not inside it. It has `ttl_days` or `ttl_seconds`, and optional `confirm_expire_existing`.
- `config` is the owner PUT body: `expectedRevision`, `requestId`, `members`, and optional `read` (`members` or `anyone`), `write` (`proposals` or `direct`), and `memberWrites`. `memberWrites` is an object from a schema name to `owner_only`, `own_records`, or `shared`. Omit `read`, `write`, or `memberWrites` to keep the stored value. An empty `memberWrites` object clears the map. A schema name that is absent is `owner_only`. No stored statement is `members`, `proposals`, and an empty map. The same three send no settings statement. `mutation` is the owner submission document. Its tag is `type` with the value `mutation`. `idempotency_key` becomes the `Idempotency-Key` header. It is a non-nil lowercase UUID.
- `submit` answers 202. It appends the proposal to the share's proposal log. It does not write a record. `submission_status` reads that proposal. The status text comes from the share service. The service's authorization refusal is final: status 403, code `permission_denied`, message `this member cannot submit this shared mutation`, and no `Retry-After`. Do not retry. That refusal is the service code `unauthorized`, or HTTP 401 or 403, other than an unpaid account (`unauthorized` with field `payment.subscription`). An append the node cannot confirm is status 503, code `busy`, message `shared mutation was not confirmed`, and the reply carries `Retry-After`. Retry after the delay. An unpaid account on this append stays that 503.
- An app that did not declare `databases` is `permission_denied`. The message is `the app {id} did not declare the extension databases`. The node checks this before it reads the body. An unknown `action` is `invalid_request` and the message is `unknown action {action}`. An unknown field is `unknown field {name}`. A missing or empty required string is `{name} is required`. A value of another type is `{name} must be a string`, or `must be an object`, or `must be a boolean`.
- A success reply is the owner route's JSON, including the status. A refusal uses the app API error object. Status 402 becomes 403 `permission_denied` and keeps the payment message. A share error uses its `message`, or else its `reason`, or else a sentence: `share_revision_conflict` is 409 `conflict` and the message is `the share configuration is at another revision`; `no_cloud_account` is 409 `no_cloud_account` and the message is `this node has no cloud account`; `database_enrolling` is 503 `enrolling` and the message is `the database is still enrolling with the cloud; retry`; `share_configuration_unconfirmed` is 503 `busy` and the message is `share configuration is not confirmed`; `invalid_share_configuration` keeps the `reason` text as 400 `invalid_request`. A settings refusal whose `error` is not one of those codes has the message `{code}: {field}`, for example `invalid_input: share.settings.unchanged`.
- `share_config`, `set_share_config`, `invite`, and `share_link` ask the node for this database's cloud state before they call the cloud. The owner member route does the same. No cloud account is final: the code is `no_cloud_account`, do not retry. Still enrolling is the code `enrolling`: it means the node already waited up to 30 seconds: retry after the `Retry-After` header. Enrolled and unconfirmed is `busy`: the service answer could not be confirmed, or the saved credentials could not be read or verified: the node is connected, and waiting does not enroll the database. An unpaid account is the existing payment answer, not still enrolling.
- A received database in `list` includes `selfAccess.read`, `selfAccess.write`, and `selfAccess.memberWrites`. `read` is `members` or `anyone`. `write` is `proposals` or `direct`. `memberWrites` is an object from a schema name to `owner_only`, `own_records`, or `shared`. It is `{}` when the map is empty, and a name that is absent is `owner_only`. No stored statement is `members`, `proposals`, and an empty map. `selfAccess.memberWrites` shows the map and does not decide a write. A direct `/v1/write` reads `write` and the member role from this view, and it does not read the map. The list reads names and `sharingMode` from the local registration, and `selfAccess` from the stored file `log-sync/{id}/self-access.json`. It does not call the cloud and it does not open a database store. This node has no cloud call that lists every database the caller can access. `POST /api/cloud/backups/v2/mutation-log/access` reads one database. The row field `access` stays `reader`. The row field `state` is `ready` when `log-sync/{id}/prefix.json` is durable, and `restoring` otherwise. Membership role and membership state are `selfAccess.role` and `selfAccess.state`.
- A received database in `list`, and the `database` object from `accept`, includes `publish`. An owned database omits it. `pending` is how many of this member's accepted local writes are still on the spool: the spool head minus the prefix the service has confirmed. It is null when this process has not seen the spool, including after a restart, until a path that already has the store open records the count. It is 0 when the registration is not a signed log. A number is the count last recorded. The list does not open a store to read it. `lastAttempt` is null until this process tries to publish, and null again after a restart. It is `{at, outcome, status}`. `at` is RFC 3339 UTC with second precision. `outcome` is `confirmed`, `refused`, or `unconfirmed`. `status` is the HTTP status when the service answered, or a string when the turn stopped before that answer: `access_unknown`, `no_credentials`, `gap`, `spool_position_mismatch`, `empty_page`, `writer_tip`, `upload_identity`, `upload_position`, `reader_stopped`, `writer_stopped`, `confirm_failed`, `prepare_failed`, `queue_unreadable`, `spool_position`, `batch_invalid`, `entry_too_large`, `undo_failed`, `policy_unknown`, `local_store_unavailable`, `capture_disabled`, `payment_required`, `writer_sequence_unknown`, or `upload_unconfirmed`. `access_unknown` means the self-access view is unknown or stale. `no_credentials` means the node has no usable cloud credentials for the share. The other strings name the stop. `policy_unknown`, `local_store_unavailable`, and `capture_disabled` mean this node stopped for a local reason before it asked the cloud, so they are not an unreachable cloud. `payment_required` means the account is unpaid. `writer_sequence_unknown` means this node lost its saved upload position and the cloud no longer keeps the delivered entries that show this device's last writer sequence, so the node does not guess one and stays stopped. `status` is null when the attempt failed before a status and the node had usable credentials. A turn that finds pending writes records one of these. `lastAttempt` stays null only when this process has not tried, or after a restart. `confirmed` means the service accepted the append or reported a duplicate that advanced the writer sequence. `refused` is HTTP 401, HTTP 403 other than an unpaid account, or the service code `unauthorized`: the node restores the preimage and drops the spool item. An unpaid account (`unauthorized` with field `payment.subscription`) stays `unconfirmed` and the spool item remains. Any other unconfirmed append stays on the spool and is retried.
- Record reads and writes in a named database stay on `/v1/read` and `/v1/write` under the app's schema grants. This extension adds none.
- The call does not serve backup, cloud, status, registration, members, or spec binding.
- The app socket does not serve `POST /api/databases/{id}/apps/declare-schema`. An app places a schema with `declare_schema`.

## Errors

A refusal is an HTTP status with one body shape:

```
{ "error": { "code": "invalid_request", "message": "…" } }
```

A refusal that concerns one mutation also sends `index`, the zero-based position in the list. A write whose condition failed also sends `reason` (`exists`, `absent`, or `revision`):

```
{ "error": { "code": "conflict", "message": "mutation 1: the record is at another revision", "index": 1, "reason": "revision" } }
```

| Status | Code |
|---|---|
| 400 | `invalid_request` |
| 403 | `permission_denied` |
| 404 | `not_found` |
| 409 | `conflict`, or `no_cloud_account` on a share call with no cloud account |
| 413 | `too_large` |
| 503 | `busy`, or `enrolling` on a share call for a database that is still enrolling |
| 500, 507 | `internal` |

The code is for the program. The message is for people and can change.

A cloud share call has three answers. The node owns the wait. An app does not guess which failure it saw.

- No cloud account. The owner status is 409, the owner code is `no_cloud_account`, and the message is `this node has no cloud account`. There is no `Retry-After` header. The app API status is 409 and the code is `no_cloud_account`, with that message. Do not retry. The code is its own value so that an app does not read it as `conflict`, which on a share call is the share revision conflict.
- Still enrolling. The node has a cloud account and this database's cloud log is not confirmed. The call waits up to 30 seconds. If enrollment finishes, the call continues. If it does not, the owner status is 503, the owner code is `database_enrolling`, and the message is `the database is still enrolling with the cloud; retry`. The reply carries `Retry-After`. The app API status is 503 and the code is `enrolling`, with that message and that header. Retry after the delay. The code is its own value so that an app does not read it as `busy`.
- Enrolled, and the service answer could not be confirmed. The owner status is 503, the owner code is `share_configuration_unconfirmed`, and the message is `share configuration is not confirmed`. The app API status is 503 and the code is `busy`, with that message. This is an unreachable service, a refusal, or a bad signature. The same answer covers saved cloud credentials that could not be read or verified: that is not proof of a missing account, so the call may be retried. It is not a missing account and it is not a database that is still enrolling.

An unpaid cloud account stays HTTP 402. The owner code is `payment_required`. The app API turns 402 into 403 `permission_denied` and keeps the payment message. The node does not call that still enrolling.

## Access declaration

An app published to the registry states the LastDB access it asks for. The
developer writes it in the manifest:

```
"access": { "read": ["notes/Note", "notes/Tag"], "write": ["notes/Note"], "extensions": ["changes"] }
```

An app that works on schemas it cannot name in advance writes `"*"` in place of
the list, for `read`, for `write`, or for both:

```
"access": { "read": "*", "write": ["notes/Note"], "extensions": ["changes"] }
```

`lastdb app publish` looks each named schema up on the running node and replaces
the name with a pair: the name and the definition ID of the version the node holds.
That is the version the developer built against. `"*"` stays `"*"`. The result
goes into the listing, under the publisher signature:

```
"access": { "read": { "notes/Note": "9f2c…", "notes/Tag": "41ab…" },
            "write": { "notes/Note": "9f2c…" },
            "extensions": ["changes"] }
```

```
"access": { "read": "*", "write": { "notes/Note": "9f2c…" }, "extensions": ["changes"] }
```

- Access is per schema, not per field. A read returns the whole record.
- `read` and `write` are independent. A schema on `write` alone cannot be read.
  `"*"` on one side does not grant the other side.
- An extension is a plain name with no version. `"*"` is not an extension value.
- `"*"` in `read` or `write` is allowed at every app API version. Publish copies
  it into the listing as `"*"`.
- Declaring is the permission. Nobody approves the declaration: install does
  not show it for consent, and a listing that changes it does not ask the
  owner again. The owner still approves the install steps.
- Install refuses a listing that names an extension this build does not have.
- An app that is not published declares nothing.

Install places each schema named in `read` or `write` at the signed definition.
`"*"` places nothing by itself. Names on the other side are placed as usual.
When either side is `"*"`, install also places every schema in the manifest
`schemas` array that no side names. The definition is the manifest entry at
the installed commit. The listing signs that commit, so the entry is the
publisher's statement. The installer prints
`placed schema <name> from the app's manifest`, the same line it prints when
Schema Service does not supply a named schema and the manifest entry matches
the signed id. When neither side is `"*"`, a manifest schema that no side names
is not placed. Publish's Schema Service push is the schemas the manifest
lists, plus the names on a side that is a map. A `"*"` side adds none. An
app with `"*"` on both sides still pushes every schema the manifest lists.

The installer reads the node's schemas once. A name already at the definition
being placed stays, and its retention stays. For a name in the access block,
the definition is taken from Schema Service
(`GET /v1/schemas/<definition id>`, no credentials) when the returned
definition hashes to the signed id. A miss, a timeout, or a hash that does
not match falls through to the manifest entry of that name, with no warning.
The manifest entry is used only when it hashes to the signed id, and it keeps
its retention. A schema the access block does not name is not placed, unless
a side is `"*"` and the manifest lists it. A listing with no access block
installs as before. If none of the three supplies the signed definition,
install fails and the previous version stays active.

A manifest schema placed because a side is `"*"` has no signed definition id
in the access block. The node already holding that name at the manifest's
definition id is left as it is. The node holding a different definition is
posted that manifest declaration, which is what install already does for a
named schema once the manifest entry is the definition. The node accepts a
compatible change. It refuses a declaration that changes an existing field,
the key, or the alternate keys. That refusal fails the install, and the
previous version stays active.

The declaration is recorded with the install. The installer registers it when
that version is activated, and again when a failed activation or a rollback
restores the previous version. A person who runs `lastdb app install <app>` or
`lastdb app upgrade <app>` also registers when that version is already active,
so a lost grant can be restored. The registry poll does not. The poll is
`lastdb app upgrade --all` with `LASTDB_APP_REGISTRY_POLL=1`, and an app that
is already at the listed release is left as it is. The launcher does not
register. The app socket serves the app under the grant the node holds.

## Listing version

A registry listing is filed under the app API version. The app API is version 1.

`APP_API_VERSION` and `MIN_SUPPORTED_APP_API_VERSION` live in
`lastdb_node/src/version.rs`. They change only when `/v1/read`, `/v1/write`, or
the extension mechanism changes in a way that breaks an installed app. Adding
an extension does not change them. An owner-side change does not change them.

The listing's signed `api_version` member, the list member `api_version`, and
the list path `<channel>/api/<n>.json` keep those names. The value is the app
API version. `lastdb app publish` sends the running node's `app_api_version`.
A node reads the lists from `min_supported_app_api_version` through
`app_api_version` and merges them. Today that range is the single list
`api/1.json`.

`GET /api/version` reports `app_api_version` and `min_supported_app_api_version`
beside `api_version` and `min_supported_api_version`. Those two existing members
stay the socket version, for the CLI and the SDK.

Listings already filed under a socket number stay unread by this build until
the app is republished under app API version 1.

## Schemas at publish

`lastdb app publish` sends schema definitions to Schema Service after the
registry lists the app. The set is every schema the manifest lists, plus every
schema in `access.read` and `access.write`. Each definition is the wire form
the running node holds, read in one request.

Schema Service accepts the push only for an app the registry lists with the
same commit and the same publisher key. It keeps each definition under the
definition id computed from that wire form. A private app is not pushed.
`--prepare` sends nothing.

A failed push prints a warning. The app stays listed. With `--json`, the
registry JSON gains a `schema_service` member (`registered` or `error`) and
the command prints no extra line on stdout.

## The app socket

A node serves two sockets in its data directory.

| Socket | File | Serves |
|---|---|---|
| Owner socket | `folddb.sock` | The app API, plus databases, sharing, status, request counts, the schema catalog, backup, and admin |
| App socket | `apps.sock` | Only `/v1/read`, `/v1/write` and `/v1/x/<name>`, to a registered app |

- The owner registers one app: `POST /api/apps/register` on the owner socket
  with `{ "app": "notes", "access": { "read": [...], "write": [...], "extensions": [...] }, "data": "<apps root>/notes/data" }`.
  `read` and `write` are each a list of schema names or `"*"`. `data` is
  optional. When present it is the absolute path of that app's data
  directory, a real directory and not a symlink, and outside the LastDB home.
  The reply holds `app`, set to the same app id the request sent. The node
  does not substitute a different id. The reply does not hold
  `expires_in_seconds`. The node keeps one grant per app id: the `read` and
  `write` grants, the `extensions` list, and the data folder when `data` was
  sent. The same app registered again replaces that grant when the read side,
  the write side, the extensions, or the data folder differ. A side is the same
  when both are the same set of schema names, or both are `"*"`. The same grant
  answers success and does not rewrite the file. A grant has no lifetime.
- The owner drops one app's grant with `POST /api/apps/unregister` on the owner
  socket and `{ "app": "<id>" }`. The route is `unregister` because it undoes
  `register` and nothing else. The node does not delete the app's files, its
  schemas, or its records. No stored grant answers success and does not rewrite
  the file. The reply is the owner envelope (`ok`, `user_hash`) plus `app`, the
  id the request sent, and `removed` (`true` when a grant was dropped, `false`
  when the node held none). A caller that is not the owner gets 403 `the owner
  unregisters an app`. An invalid app id gets 400 `invalid app id`. A body the
  node cannot parse gets 400 `invalid app unregister request: ...`. A body over
  256 KiB gets 413 `app unregister request is too large`. A store failure is
  the same 500 as register, `could not store the app grant`, which the owner
  socket answers as `Internal Server Error`.
- The grants are one file, `app-grants.json`, in the data directory beside the
  sockets. The file is mode 0600. The node writes a temporary file and renames
  it into place when a grant changes, and loads the file at start. A missing
  or unreadable file means no grants, and that is not an error. The file is
  not a database file. Backup and cloud sync do not copy it. It holds
  `{ "version": 1, "apps": { "<app id>": { "read": [...], "write": "*", "extensions": [...], "data": "<path>" } } }`.
  `read` and `write` are a list of names or the string `"*"`. The file version
  stays 1. An older node deserializes those fields as name lists, so `"*"`
  fails the file instead of looking like an empty list. A higher version would
  make that node reject every file, including grants that only name schemas.
  `data` is omitted when the grant has no data folder. The node keeps at most
  256 apps. Each name list holds at most 256 names. A schema name is at most
  128 bytes and an extension name at most 40. The file is at most 1 MiB.
- A request on the app socket carries the app id in the header
  `X-LastDB-App`. An app id the node has no grant for is refused with
  `permission_denied` and the message `the app socket needs a registered app`.
  While the app is still installed, `lastdb app install <app>`, `lastdb app
  upgrade <app>`, or a rollback registers the grant again. The registry poll
  does not. `lastdb app remove <app>` drops the grant on purpose. An app id is
  a plain app name, checked with the same rule the launcher and the installer
  use.
- Any route other than the app API answers `not_found` on the app socket.
- A read names only a schema the app's `read` grant allows, a write only one
  its `write` grant allows, and an extension only one in its `extensions` list.
  `"*"` allows every schema on that side. Anything else is `permission_denied`.
  A read refusal is per item; a write refusal refuses the list.
- The name must be the exact name of a schema on the node. A loose spelling
  that the owner socket would resolve is `not_found` here. An undeclared name
  is still `permission_denied`. `"*"` does not add a schema directory. The app
  socket still serves only `/v1/read`, `/v1/write`, and `/v1/x/<name>`.
- The app socket has its own four workers and its own request budget. Load on
  it can delay only other requests on the app socket, never the owner socket.
- A request body on the app socket is at most 8 MiB. A larger one answers
  `too_large`.
- `changes` returns entries for schemas the `read` grant allows. Without a
  `schemas` filter, a named grant returns all of those names, and `"*"` returns
  entries for every schema. A `schemas` filter still limits the entries. An
  app whose read grant names nothing gets `permission_denied`.
- The owner socket also serves databases, sharing, status, request counts
  (`GET /api/ops`), the schema list and schema get,
  `POST /api/apps/declare-schema` (the installer), schema seed and drop,
  backup, and admin. It does not serve a separate record query, mutation,
  key list, file-byte upload, or `GET /api/changes`. Those calls are unknown
  paths. Record reads and writes use `/v1/read` and `/v1/write`. A receiver's
  proposal is `POST /api/databases/{id}/submissions`. That call is not a record write.
- The installer places one schema on the personal database with
  `POST /api/apps/declare-schema`. The same body placed in one database this
  node owns is `POST /api/databases/{id}/apps/declare-schema`. The app socket
  does not serve that path. An app places a schema with `declare_schema` on
  `POST /v1/x/databases`.

## Place a schema in an owned database

`POST /api/apps/declare-schema` writes the schema into the personal database.
To place that same schema in a named database this node owns, send the same
body to the database path. The reply is the same object.

```
POST /api/databases/{id}/apps/declare-schema

{ "app_id": "social", "schema": { "name": "social/Comment", "schema_type": "Hash",
  "record_keys": { "primary": { "kind": "hash" }, "alternates": {} },
  "fields": ["id", "text"], "field_types": { "id": "String", "text": "String" },
  "field_descriptions": { "id": "comment id", "text": "comment text" },
  "field_versions": { "id": 1, "text": 1 } } }
```

`intent` defaults to `catalog_sync`. `retention`, when present, sits beside
`schema`, not inside it. A name that already contains a namespace is declared
under that namespace, and the namespace has to match `app_id`. A bare name is
declared under `app_id`.

```
200
{ "ok": true, "user_hash": "<owner>", "app_id": "social", "namespace": "social",
  "schema": "social/Comment", "schema_name": "social/Comment",
  "descriptive_name": "Comment", "canonical": "social/Comment",
  "identity_hash": "<hash>", "definition_id": "<portable id>",
  "resolution": "register", "bind_eligible": true }
```

`descriptive_name` is the local name when the request omits it. `resolution`
is `reuse` when that definition is already installed. `retention` is present
only when the request declared one and the retention step finished.

- `{id}` is the id or the name of a database this node owns. The personal
  database is one of those. The schema is stored in that database, and that
  database's accepted log records the schema with kind `schema`. A share
  publishes that entry when the schema namespace is one the database spec admits.
- A received database answers 404 and the body is `Not Found`. An id this node
  does not know answers 404 and the body is `database is not registered on this node`.
- A registered app that sends this path to the app socket gets `not_found`
  and the message `the app socket serves only the app API`.
- `POST /api/schemas/declare` and `POST /api/databases/{id}/schemas/declare`
  are not routes.

### Request counts

`GET /api/ops` on the owner socket returns the requests completed since the
node started. The app socket answers `not_found` for this route. The counts
are in memory only. They start at zero when the node starts. They are not
written to disk, and a backup does not include them.

`lastdb ops` prints the same counts as aligned text, sorted by total time.
`--by-count` sorts by request count. `--json` prints the route's JSON on
stdout and nothing else.

One call row is one caller and one route. On the app socket the caller is
the app id from `X-LastDB-App`. On the owner socket the caller is `owner`,
and the row includes `X-LastDB-Client` when the request sent one. A served
route is its path. A database id in that path is `{database}` and a schema
name is `{schema}`. A path the node does not serve is kept as sent, cut at
64 bytes on a character boundary. The row holds the request count, total
time, slowest request, item count, and refused requests by error code. A
read counts the reads in the list, a write counts the mutations, and any
other request counts as one item.

The schema table counts `/v1/read` and `/v1/write` only, by caller, schema,
and read or write. Each schema named in the request receives that request's
whole time, once. The item count is the number of list entries for that schema.

The call table holds at most 256 rows. The next new key counts on caller
`{overflow}`, an empty client, and route `other`. The schema table holds at
most 256 rows. The next new key counts on caller `{overflow}`, an empty
client, schema `{overflow}`, and op `read`. One request contributes at most
8 schema names; further names in that request count on schema `{overflow}`
for that caller and that read or write. At most 32 distinct unknown paths
are kept. A further unknown path keeps its caller and uses route `other`.
A schema name longer than 128 bytes is omitted. The item still counts on
the call. `{overflow}` and `other` are ordinary keys, so a real caller,
schema, or path with that name shares the row.

## Sharing

A receiver of a whole-database share submits one encrypted proposal on the
owner socket. The app socket does not serve this path.

```
POST /api/databases/{id}/submissions
Idempotency-Key: <uuid>

{ "type": "mutation", "schema": "social/Comment", "record": { … },
  "key_value": { "hash": "photo-7", "range": "<uuid>" }, "alternates": { },
  "mutation_type": "create", "expected": { "type": "absent" },
  "file": null }
```

- The body is one proposal document. It is the mutation document a received database accepted. It is not a record API and it is not a `/v1/write` item. It does not carry a source file name or a metadata map. The database sets `created_by` only when the write creates the record, and sets `updated_by` from the authenticated writer. A replace keeps the previous `created_by`, including when it is absent. The publisher checks the database member-write map. There is no separate rule for `social/Comment`. Under `own_records`, the creator is the live record's `created_by`, and a record with no `created_by` is the publisher's alone. A member may delete a record they created.
- The proposal id is the `Idempotency-Key` header. It is one non-nil UUID. The body has no separate id field. The same id and the same body are idempotent.
- The route answers only for a received whole-database share. It never writes a record on this node. It seals the proposal and appends it to the proposal log.
- The node replaces `definition_id` with the installed schema definition before it seals the proposal. `convergence` and `durability` are optional. A proposal that carries a file is refused.
- `202` means the proposal log accepted the append: `{ "ok": true, "status": "submitted", "mutation_id": "<uuid>", "submission_sequence": 1 }`. The publisher still checks the proposal.
- An owned database, including the owner's own database, answers `404` with the text `this database has no received submissions`. An unknown id answers `404` with the text `database is not registered on this node`. A share that is not a whole database answers `403` with the text `this share is read-only`.
- A missing or invalid `Idempotency-Key` answers `400` with the text `a shared write requires a UUID Idempotency-Key`. A document the node cannot parse answers `400` with the text `invalid shared mutation or schema definition`. A receiver that is not active answers `403` with the text `this member cannot submit this shared mutation`.
- The service's authorization refusal is the same 403 and the same text, with no `Retry-After`. Do not retry. That refusal is the service code `unauthorized`, or HTTP 401 or 403, other than an unpaid account (`unauthorized` with field `payment.subscription`). The node does not decide that this member may not write. It repeats the service's refusal.
- A snapshot that is not durable answers `503` with the text `database snapshot is not durable`. An append the node cannot confirm answers `503` with the text `shared mutation was not confirmed` and `Retry-After`. That is an unreachable service or an answer that is not a confirmation. Retry after the delay. An unpaid account on this append is that 503.
- `GET` on this path is not served. The status call beside it is `POST /api/databases/{id}/submissions/status` with `{ "mutationId": "<uuid>" }`. Its answer is `submitted`, `published`, `discarded`, or `unknown`.
- While `write` is `proposals`, `/v1/write` to a received database stays refused with `a received database is read only`. A proposal still uses this submissions route. While `write` is `direct` and this node is an active Submit member, `/v1/write` commits to the local copy and the node appends that change to the mutation log. The write's HTTP status is that local commit. A later service refusal (HTTP 401 or 403, or the code `unauthorized`) restores the preimage and drops the spool item, so it is not retried. An append the node cannot confirm stays on the spool and is retried. That path does not answer 403 or 503 for the service's decision. A reader with no member entry never writes. A prefix that is not durable answers 503 with `database snapshot is not durable`.

## The launcher

An installed app that declares LastDB access is started by the launcher, not
directly.

- The installer points each of the app's command links at the `lastdb` command.
  Called under the app's command name, `lastdb` finds the app that owns the
  name and starts it. `lastdb app run <app> <command> [args]` does the same by name.
- The launcher does not call the node. It starts the app's program with
  `LASTDB_SOCKET_PATH` set to the app socket and `LASTDB_APP` set to the app
  id in the installed receipt. `POST /api/apps/register` answers with that
  same id: the node returns the `app` it was sent, and install sends the
  receipt's app id. Install steps do not receive `LASTDB_APP`. The launcher
  starts the program whether the node is up, down, or slow.
- On activation, and on rollback, the installer registers that version's
  access, so a process that is already running is served under the new lists
  at once. If activation fails and the previous version is restored, the
  previous access is registered again. When a person runs `lastdb app install
  <app>` or `lastdb app upgrade <app>` and that version is already active, the
  installer registers that access again. The registry poll does not.
- A node that holds no grant for the app refuses its calls with `the app
  socket needs a registered app`. While the app is still installed, `lastdb app
  install <app>`, `lastdb app upgrade <app>`, or a rollback registers the grant
  again. The registry poll does not. The launcher does not register.
- `lastdb app remove <app>` removes an installed app. It asks the node to drop
  that app's grant (`POST /api/apps/unregister`), then removes the command
  links this app installed, every version directory, the `current` and
  `previous` links, and the install receipt. It also removes `rejected-source`,
  `last-checked`, and a leftover staging directory under the app root. A
  command link is removed only when it is a symlink that points at the `lastdb`
  launcher and this app lists that command, or when the symlink points inside
  this app's directory. A regular file is left in place. A symlink that points
  somewhere else is left in place.
- Schemas and records are not deleted. They stay readable by the owner and by
  any app whose access lists those schemas. There is no flag that deletes
  schemas or records. The data folder `<apps root>/<app>/data` stays unless
  `--delete-data` is given. That flag removes only that folder.
- An app that is not installed is an error. When the files are already gone
  and the node still holds the grant, running the command again drops that
  grant and succeeds. `--delete-data` on that later run does not remove the
  data folder. If the node is not running, the files are still removed and the
  command says the grant was not. Run `lastdb app remove <app>` again when the
  node is up. No stored grant is success.
- `lastdb app upgrade --all` walks installed apps only. Installed means
  `<apps root>/<app>/current/lastdb-app-install.json` is a file. Remove deletes
  that link and the receipt, so the registry poll does not install the app
  again. Pruning a version directory that is neither current nor previous still
  does not drop the grant. Remove does.
- The install manifest has no uninstall step. `post_install` is the step that
  runs after activation. The receipt does not record what those steps started.
  Remove refuses an app when a retained version that was not installed with
  `--inactive` has `post_install` steps. The error names each such version and
  the step's argv, and the app stays installed. An `--inactive` install did
  not run `post_install`, so those steps do not block removal. `prepare` runs
  inside the source tree and is not treated as a service step.
- The declaration exists to record which app uses which schema (Tom,
  2026-11-17). The node serves an app only the schemas it declared, so the
  record stays true. Confining an installed app on the owner's machine is not a
  goal: the launcher applies no sandbox, on any platform. An earlier macOS
  sandbox profile was removed. It could not nest inside a caller's own
  profile, and it kept apps that act on the machine from declaring at all.
- The launcher creates the app's own data folder, `<apps root>/<app>/data`,
  and its `tmp` directory. It names that folder in `LASTDB_APP_DATA`. `TMPDIR`
  points at `tmp`. The grant's `data` is this same folder, stored when the
  installer registers the app. The node stores its canonical path. The folder
  sits beside the version directories, so an upgrade or a later launch does
  not move it. An app keeps its config and state there by convention.
- An app that ran restricted stays restricted. Install refuses a later version
  whose listing declares no LastDB access.
- An app that declares no LastDB access is linked and started as before.
- A command name belongs to one installed app. Install refuses a name that
  another installed app lists, and refuses to replace a path in `~/.local/bin`
  that this app did not install. A command name is a plain file name of at most
  64 bytes. When two restricted apps list one name, the launcher starts neither.
- A program the owner starts by hand is not restricted. Only apps from the
  registry are.

## Not built yet

- A path for a file that is only in the cloud. Nothing fetches one file from the cloud on a read yet. A file LastDB holds as bytes on this device gets a path on the app socket when the grant names the app's data folder, and on the owner socket when the read names `file_dir`.
- A limit on which databases an app may name.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
