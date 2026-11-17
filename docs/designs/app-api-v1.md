# The app API: one read, one write

Status: the interface is decided (Tom, 2026-11-16). This document describes
what `lastdbd` serves today and names what is not built yet.

The app API is the interface for apps installed through the app registry. It
has two routes. Each takes a list; one item is a list of one. Everything else
on the socket is the user API, for the owner and the owner's tools.

## Read

```
POST /v1/read
{ "reads": [
    { "schema": "notes/Note", "key": { "hash": "n1" } },
    { "schema": "notes/Tag",  "range": { "hash": "work" }, "limit": 100, "keys_only": true },
    { "schema": "notes/Note", "via": "by_author", "range": { "hash": "tom" } },
    { "database": "work", "schema": "notes/Note", "key": { "hash": "n1" } }
] }

200
{ "results": [
    { "records": [ { "key": { "hash": "n1", "range": null }, "revision": "9c2e…", "record": { … } } ], "next_cursor": null },
    { "keys": [ … ], "next_cursor": "b3f1…" },
    { "records": [ … ], "next_cursor": null },
    { "error": { "code": "not_found", "message": "…" } }
] }
```

- A read names a schema and exactly one of `key` or `range`. A scan does not exist.
- `key` is one record: `hash`, and `range` when the schema has one.
- `range` is the records under one `hash`: all of them, those with a `prefix`,
  or those from `start` up to `end`.
- `via` reads through a named alternate key. The reply holds the primary keys.
- `limit` bounds a page. `next_cursor` is an opaque string; send it back as `cursor`.
- `keys_only` returns keys without records.
- `database` selects a database. Without it the read uses the personal database.
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
- A mutation whose condition does not hold refuses the whole list with `409 conflict`. The caller reads again and decides.
- A schema that declares alternate keys takes their addresses in `alternates` on a `create` and a `replace`.
- The list is all or nothing. Every mutation in one list names the same database.
- The reply comes after the write is on disk. `revisions` holds the new revision of each record, in order.

A read returns each record's `revision` beside its key.

## Files

A file is a path in and a path out. No file bytes pass through the app API.

```
{ "schema": "notes/Note", "op": "create", "key": { "hash": "n5" }, "record": { … },
  "file": { "path": "/Users/me/Pictures/whiteboard.jpg", "name": "whiteboard.jpg", "media_type": "image/jpeg" } }
```

- A `create` or a `replace` can carry a `file` with the path of a file the caller can read. `name` and `media_type` are optional.
- LastDB keeps a reference to the file where it is and makes no local copy. The bytes go up at cloud upload.
- A path inside the LastDB home is refused.
- A `replace` without a `file` keeps the record's file.
- Every record in a read reply has `file`: null, or its `name`, `media_type` and `size`.
- A read item with `"file": true` also gets `path`. When the original is gone or has another size, that read item answers `not_found`.
- `path` is null for a file that LastDB holds as bytes in its home and not as a reference.


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

- `after` is the last number the app handled. Without it the read starts at the oldest kept entry.
- `limit` is at most 500. Zero reads `head` only.
- `schemas` is optional. Without it the read returns every schema.
- An entry holds no record values. The app reads the changed records through `/v1/read`.
- The app saves `through` as its new position. When `id` differs from the one it
  saved, or `gap` is true, it rebuilds from key pages.
- `GET /api/changes` is the same list for the owner, with query parameters.

## Errors

A refusal is an HTTP status with one body shape:

```
{ "error": { "code": "invalid_request", "message": "…" } }
```

| Status | Code |
|---|---|
| 400 | `invalid_request` |
| 403 | `permission_denied` |
| 404 | `not_found` |
| 409 | `conflict` |
| 413 | `too_large` |
| 503 | `busy` |
| 500, 507 | `internal` |

The code is for the program. The message is for people and can change.

## Access declaration

An app published to the registry states the LastDB access it asks for. The
developer writes it in the manifest:

```
"access": { "read": ["notes/Note", "notes/Tag"], "write": ["notes/Note"], "extensions": ["changes"] }
```

`lastdb app publish` looks each schema up on the running node and replaces the
name with a pair: the name and the definition ID of the version the node holds.
That is the version the developer built against. The result goes into the
listing, under the publisher signature:

```
"access": { "read": { "notes/Note": "9f2c…", "notes/Tag": "41ab…" },
            "write": { "notes/Note": "9f2c…" },
            "extensions": ["changes"] }
```

- Access is per schema, not per field. A read returns the whole record.
- `read` and `write` are independent. A schema on `write` alone cannot be read.
- An extension is a plain name with no version.
- Declaring is the permission. Nobody approves the declaration: install does
  not show it for consent, and a listing that changes it does not ask the
  owner again. The owner still approves the install steps.
- Install refuses a listing that names an extension this build does not have.
- An app that is not published declares nothing.

The declaration is recorded with the install. It is not enforced until the app
socket exists.

## Not built yet

- A path for a file that is only in the cloud or that LastDB holds as bytes. Nothing fetches one file from the cloud on a read yet.
- Removing a record's file while keeping the record.
- The check of a file path against an app's sandbox profile.
- A received (shared) database as a `database` value.
- An app naming its extensions in its manifest, the installer checking them, and
  the limit of `changes` to the schemas an app declared.
- The app socket, the sandbox profile and the per-app schema check. Until they
  exist, these routes serve every caller of the owner socket as the owner.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
- The older routes under `/api` stay until the apps move to these two.
