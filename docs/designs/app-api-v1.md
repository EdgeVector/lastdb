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
    { "records": [ { "key": { "hash": "n1", "range": null }, "record": { … } } ], "next_cursor": null },
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
    { "schema": "notes/Note", "op": "put",    "key": { "hash": "n4" }, "record": { … } },
    { "schema": "notes/Note", "op": "put",    "key": { "hash": "n5" }, "record": { … },
      "alternates": { "by_author": { "hash": "tom", "range": "n5" } } },
    { "schema": "notes/Tag",  "op": "delete", "key": { "hash": "work", "range": "2026-11-16" } }
] }

200
{ "written": 3 }
```

- A mutation is `put` or `delete`. It names no condition and is carried out as
  sent. The new value wins.
- A `put` carries the complete record. A `put` of the same value again is written again.
- A `delete` of a record that does not exist is not an error.
- A schema that declares alternate keys takes their addresses in `alternates` on a `put`.
- The list is all or nothing. When one list names a key twice, the last mutation for it wins.
- Every mutation in one list names the same database.
- The reply comes after the write is on disk.

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
| 413 | `too_large` |
| 503 | `busy` |
| 500, 507 | `internal` |

The code is for the program. The message is for people and can change.

## How a write with no condition is carried out today

The engine commits each record against its revision. The route reads the
current revision of every key in the list, commits against those revisions, and
starts again when another writer changed a record in between. After eight
attempts it answers `busy`. A caller never sees a revision or a conflict.

## Not built yet

- A `file` path on a `put`, and a file path on a read.
- A received (shared) database as a `database` value.
- The app socket, the sandbox profile and the per-app schema check. Until they
  exist, these routes serve every caller of the owner socket as the owner.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
- The older routes under `/api` stay until the apps move to these two.
