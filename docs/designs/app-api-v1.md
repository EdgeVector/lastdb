# The app API: one read, one write

The explicit definitions are in `lastdb_node/app-api.json`.
When this document and that file differ, the file wins.

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
    { "database": "work", "schema": "notes/Note", "key": { "hash": "n1" } },
    { "schema": "notes/Note", "keys_only": true, "limit": 100 }
] }

200
{ "results": [
    { "records": [ { "key": { "hash": "n1", "range": null }, "revision": "9c2e…", "record": { … } } ], "next_cursor": null },
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
  them. It takes no `via` and no `file`. An app that keeps something derived
  from a schema uses it to build and to rebuild.
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
- A mutation whose condition does not hold refuses the whole list with `409 conflict`. The reply names that mutation and why. The caller reads again and decides.
- `index` is the zero-based position of the first mutation whose condition failed. `reason` is `exists` when a `create` finds a record at the key, `absent` when a `replace` or a `delete` finds no record, or `revision` when the record is at another revision.
- `{ "error": { "code": "conflict", "message": "mutation 1: the record is at another revision", "index": 1, "reason": "revision" } }`
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
- Every record in a read reply has `file`: null, or `name`, `media_type`, `size` and `local`.
- `file: null` means the record has no file.
- `local` is the path on this device when this device holds the file as a reference and a regular file of the recorded size is still there. It is null when that original is gone or has another size, and when the file is not on this device.
- A read item with `"file": true` also gets `path`. For a reference, `path` is that same local path. When the original is gone or has another size, that read item answers `not_found`.
- For a file LastDB holds as its own bytes, `local` is null until a copy sits in the calling app's data folder. `path` is null on the owner socket, and on an app session issued without `data`. On the app socket, when the session names that folder, a read with `"file": true` writes the plain bytes to `<data>/files/<content hash>` and returns that path. The folder is mode 0700 and the file is mode 0600. The node writes a temporary name in that folder and renames it into place, and it does not follow a symlink at the destination. A file of that name and size is reused. That reply sets `local` to the same path. A read that does not set `file` to true does not write; it reports `local` only when the file is already there.
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

A refusal that concerns one mutation also sends `index`, the zero-based position in the list. A write whose condition failed also sends `reason` (`exists`, `absent`, or `revision`):

```
{ "error": { "code": "conflict", "message": "mutation 1: the record is at another revision", "index": 1, "reason": "revision" } }
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

Install places each schema named in `read` or `write` at the signed definition.
The installer reads the node's schemas once. A name already at that definition
stays, and its retention stays. Otherwise the definition is taken from Schema
Service (`GET /v1/schemas/<definition id>`, no credentials) when the returned
definition hashes to the signed id. A miss, a timeout, or a hash that does not
match falls through to the manifest entry of that name, with no warning. The
manifest entry is used only when it hashes to the signed id, and it keeps its
retention. A schema the access block does not name is not placed. A listing
with no access block installs as before. If none of the three supplies the
signed definition, install fails and the previous version stays active.

The declaration is recorded with the install. The installer registers it when
that version is activated, and again when a failed activation or a rollback
restores the previous version. The launcher registers it at each start.
Registering the same app again replaces its grant. The app socket serves the
app under the grant the node holds.

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
| Owner socket | `folddb.sock` | Everything, to the owner and the owner's tools |
| App socket | `apps.sock` | Only `/v1/read`, `/v1/write` and `/v1/x/<name>`, to a registered app |

- The owner registers one app: `POST /api/apps/session` on the owner socket
  with `{ "app": "notes", "access": { "read": [...], "write": [...], "extensions": [...] }, "data": "<apps root>/notes/data" }`.
  `data` is optional. When present it is the absolute path of that app's data
  directory, a real directory and not a symlink, and outside the LastDB home.
  The reply holds `session`, set to the app id. It does not hold
  `expires_in_seconds`. The node keeps one grant per app id: the `read`,
  `write` and `extensions` lists, and the data folder when `data` was sent.
  The same app registered again replaces that grant. A grant has no lifetime.
- The grants are one file, `app-grants.json`, in the data directory beside the
  sockets. The file is mode 0600. The node writes a temporary file and renames
  it into place when a grant changes, and loads the file at start. A missing
  or unreadable file means no grants, and that is not an error. The file is
  not a database file. Backup and cloud sync do not copy it. It holds
  `{ "version": 1, "apps": { "<app id>": { "read": [...], "write": [...], "extensions": [...], "data": "<path>" } } }`.
  `data` is omitted when the grant has no data folder. The node keeps at most
  256 apps. Each list holds at most 256 names. A schema name is at most 128
  bytes and an extension name at most 40. The file is at most 1 MiB.
- A request on the app socket carries the app id in the header
  `X-LastDB-App-Session`. An app id the node has no grant for is refused with
  `permission_denied`. An app id is a plain app name, checked with the same
  rule the launcher and the installer use.
- Any route other than the app API answers `not_found` on the app socket.
- A read names only a schema in the app's `read` list, a write only one in
  its `write` list, and an extension only one in its `extensions` list. Anything
  else is `permission_denied`. A read refusal is per item; a write refusal
  refuses the list.
- The name must be the declared name exactly, and it must be the exact name of a
  schema on the node. A loose spelling that the owner socket would resolve is
  `not_found` here. An undeclared name is still `permission_denied`.
- The app socket has its own four workers and its own request budget. Load on
  it can delay only other requests on the app socket, never the owner socket.
- A request body on the app socket is at most 8 MiB. A larger one answers
  `too_large`.
- `changes` returns only entries for schemas in the `read` list. Without a
  `schemas` filter it returns all of those.
- The owner socket is not limited by any of this.

## The launcher

An installed app that declares LastDB access is started by the launcher, not
directly.

- The installer points each of the app's command links at the `lastdb` command.
  Called under the app's command name, `lastdb` finds the app that owns the
  name and starts it. `lastdb app run <app> <command> [args]` does the same by name.
- The launcher registers the app with the lists from the app's signed listing,
  and starts the app's program with `LASTDB_SOCKET_PATH` set to the app socket
  and `LASTDB_APP_SESSION` set to the app id. The header and the variable keep
  those names. The launcher still needs the node: if it cannot reach the owner
  socket, it does not start the app.
- On activation, and on rollback, the installer registers that version's
  access, so a process that is already running is served under the new lists
  at once. If activation fails and the previous version is restored, the
  previous access is registered again.
- The declaration exists to record which app uses which schema (Tom,
  2026-11-17). The node serves an app only the schemas it declared, so the
  record stays true. Confining an installed app on the owner's machine is not a
  goal: the launcher applies no sandbox, on any platform. An earlier macOS
  sandbox profile was removed. It could not nest inside a caller's own
  profile, and it kept apps that act on the machine from declaring at all.
- The launcher creates the app's own data folder, `<apps root>/<app>/data`,
  before it registers the app, and passes that folder as `data`. It also
  names the folder in `LASTDB_APP_DATA`. `TMPDIR` points inside it. An app keeps
  its config and state there by convention.
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

- A path for a file that is only in the cloud. Nothing fetches one file from the cloud on a read yet. A file LastDB holds as bytes on this device gets a path on the app socket when the session names the app's data folder.
- Removing a record's file while keeping the record.
- A received (shared) database as a `database` value.
- A limit on which databases an app may name.
- `keys_only` still loads the records on the node; it only leaves them out of the reply.
- The older routes under `/api` stay until the apps move to these two.
