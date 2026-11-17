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
- A path inside the LastDB home is refused, through any symlink. That covers the
  key file and every database in the home. Every file that cannot be linked gets
  the same answer.
- A `replace` without a `file` keeps the record's file.
- Every record in a read reply has `file`: null, or its `name`, `media_type` and `size`.
- A read item with `"file": true` also gets `path`. When the original is gone or has another size, that read item answers `not_found`.
- `path` is null for a file that LastDB holds as bytes in its home and not as a reference.


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

## The app socket

A node serves two sockets in its data directory.

| Socket | File | Serves |
|---|---|---|
| Owner socket | `folddb.sock` | Everything, to the owner and the owner's tools |
| App socket | `apps.sock` | Only `/v1/read`, `/v1/write` and `/v1/x/<name>`, to a caller with an app session |

- The owner issues a session for one app: `POST /api/apps/session` on the owner
  socket with `{ "app": "notes", "access": { "read": [...], "write": [...], "extensions": [...] } }`.
  The reply holds `session` and `expires_in_seconds` (24 hours). Sessions live in
  memory and end when the node stops.
- A request on the app socket carries the session in the header `X-LastDB-App-Session`.
  Without a valid one it is refused with `permission_denied`.
- Any route other than the app API answers `not_found` on the app socket.
- A read names only a schema in the session's `read` list, a write only one in
  its `write` list, and an extension only one in its `extensions` list. Anything
  else is `permission_denied`. A read refusal is per item; a write refusal
  refuses the list.
- The name must be the declared name exactly.
- `changes` returns only entries for schemas in the `read` list. Without a
  `schemas` filter it returns all of those.
- The owner socket is not limited by any of this.

## The launcher

An installed app that declares LastDB access is started by the launcher, not
directly.

- The installer points each of the app's command links at the `lastdb` command.
  Called under the app's command name, `lastdb` finds the app that owns the
  name and starts it. `lastdb app run <app> <command> [args]` does the same by name.
- The launcher asks the node for an app session with the lists from the app's
  signed listing, and starts the app's program with `LASTDB_SOCKET_PATH` set to
  the app socket and `LASTDB_APP_SESSION` set to the session.
- On macOS the program runs inside a sandbox profile. The profile denies the
  LastDB home, every other installed app's files, every socket in the home
  other than the app socket, reading other processes, and signalling them.
  The app's own install tree stays readable.
- The profile denies every write. An app writes only inside its own data
  folder, `<apps root>/<app>/data`, which the launcher creates and names in
  `LASTDB_APP_DATA`. `TMPDIR` points inside it. An app cannot write a config
  file, a command, or anything else of the owner's.
- An app that ran restricted stays restricted. Install refuses a later version
  whose listing declares no LastDB access.
- On other platforms no sandbox tool is chosen yet. The app still gets only the
  app socket and a session, which holds an app that does not try to get around it.
- An app that declares no LastDB access is linked and started as before.
- A program the owner starts by hand is not restricted. Only apps from the
  registry are.

## Not built yet

- A path for a file that is only in the cloud or that LastDB holds as bytes. Nothing fetches one file from the cloud on a read yet.
- Removing a record's file while keeping the record.
- The check of a file path against an app's sandbox profile.
- A received (shared) database as a `database` value.
- A sandbox tool for platforms other than macOS.
- Install steps and the install probe run outside the profile.
- The profile does not limit the network, or what an app reads among the
  owner's other files.
- A limit on which databases an app may name.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
- The older routes under `/api` stay until the apps move to these two.
