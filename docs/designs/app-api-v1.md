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
- Install shows the declaration with the install steps and asks the owner
  again when a later listing changes either.
- Install refuses a listing that names an extension this build does not have.
- An app that is not published declares nothing.

The declaration is recorded and shown. It is not enforced until the app socket
exists.

## Not built yet

- A `file` path on a `create` or a `replace`, and a file path on a read.
- A received (shared) database as a `database` value.
- The app socket, the sandbox profile and the per-app schema check. Until they
  exist, these routes serve every caller of the owner socket as the owner.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
- The older routes under `/api` stay until the apps move to these two.
