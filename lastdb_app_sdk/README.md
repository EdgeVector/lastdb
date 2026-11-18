# @lastdb/app-sdk

The runtime SDK for LastDB apps. A dependency-free TypeScript client over a
node's Unix socket.

Record calls are the app API: `read`, `write`, and `changes`. Those shapes
are defined in `lastdb_node/app-api.json`. The TypeScript types are generated
from that file.

The owner socket still serves version, identity, the schema list, named
databases, short-lived database sessions, and the status of a share
submission. `LASTDB_SDK_API_VERSION` and `LASTDB_MIN_SUPPORTED_API_VERSION`
are generated from the node source.

## Install a build of the SDK

```bash
npm install /path/to/lastdb-app-sdk-0.18.0.tgz
```

Build a tarball from this directory with `npm ci`, `npm run build`, and
`npm pack --ignore-scripts`.

## Quickstart

```ts
import { connect } from '@lastdb/app-sdk';

// Uses explicit socketPath when passed, else discovers LASTDB_SOCKET_PATH ->
// FOLDDB_SOCKET_PATH -> <LASTDB_HOME or ~/.lastdb>/data/folddb.sock.
const db = await connect({ appId: 'my-app' });

await db.write([
  {
    schema: 'my-app/Concept',
    op: 'create',
    key: { hash: 'c1' },
    record: { id: 'c1', title: 'hello' },
  },
]);

const [page] = await db.read([
  { schema: 'my-app/Concept', key: { hash: 'c1' } },
]);
if (page !== undefined && 'records' in page) {
  console.log(page.records[0]);
}
```

For synchronous setup when the caller has already resolved `socketPath`, use
`ownerClient`:

```ts
import { ownerClient, LASTDB_SDK_API_VERSION } from '@lastdb/app-sdk';

const db = ownerClient({
  appId: 'my-app',
  socketPath: '/Users/me/.lastdb/data/folddb.sock',
});
await db.requireApiVersion(LASTDB_SDK_API_VERSION);
```

## Named databases and sessions

`listDatabases` reads `GET /api/databases`. The personal database and received
shares stay in that list. `createDatabase` calls `POST /api/databases` to create
an owned database, and `bindDatabaseSpec` calls `POST /api/databases/spec` to
bind or clear an owned database's spec. `select` returns a new client bound to
one database ID or alias without mutating the existing client.

```ts
const root = await connect({ appId: 'photos' });
const created = await root.createDatabase({ name: 'photo-drafts', kind: 'personal' });
const drafts = root.select(created.id);
const same = await connect({ appId: 'photos', databaseId: created.name ?? created.id });
```

To mint an owner-approved short-lived database session (`POST /api/databases/session`)
and attach `X-LastDB-Database-Session` to every scoped request, call
`issueDatabaseSession` or `openDatabaseSession`:

```ts
const { session, client: draftReader } = await root.openDatabaseSession(created.id, 'read');
// Omitting `database` addresses the session's database.
const [album] = await draftReader.read([
  { schema: 'photos/Album', key: { hash: 'vacation' } },
]);

const delegated = await connect({
  appId: 'photos-viewer',
  databaseId: session.databaseId,
  databaseSession: session.session,
});
```

Keep `session.session` tokens out of logs. Schema list and schema get on a
selected database use `/api/databases/<selector>/...`. `read` and `write` do
not. On a session client, omit `database` to address that session's database.
An item that names a different database is `permission_denied` and its message
is `a database session addresses only its own database`. With no session, omit
`database` to address the personal database. A session cannot call `changes`.

## Set up sharing with Social

Sharing setup is done with Social's own client, `@lastdb/social-client`. It
ships with the Social app at
[EdgeVector/social `client/`](https://github.com/EdgeVector/social/tree/main/client)
and calls the installed `social` command. This package is the data client.
Point both at the same LastDB home. The Social client returns the stable
database ID.

A received database is read with `read` and `database` set to that ID. A
`write` to a received database is refused: the node answers
`a received database is read only`.

`submitProposal(schemaName, op, options?)` queues one encrypted proposal on
a received whole-database share. Select that database first. The call posts
the proposal document to `POST /api/databases/{id}/submissions` on the owner
socket, with the proposal id in the `Idempotency-Key` header. The document
is the mutation document a received database used to accept. It is not a
`/v1/write` item, and the call does not write a record on this node. A
`202` receipt means the publisher's pending log accepted the append. The
publisher still checks the proposal. Reuse the same id and the same body
when the outcome is uncertain: `submitProposal` throws
`ShareSubmissionUncertainError` with `.mutationId`.

```ts
import { randomUUID } from 'node:crypto';

const { client: share } = await recipient.openDatabaseSession(albumDatabaseId, 'write');
const mutationId = randomUUID();
const proposal = await share.submitProposal('social/Comment', {
  mutationType: 'create',
  expected: { type: 'absent' },
  alternates: {},
  key: { hash: 'photo-7', range: mutationId },
  record: {
    target: { database: albumDatabaseId, schema: 'social/Photo', key: 'photo-7' },
    social_comment_text: 'Great photo',
    created_at: new Date().toISOString(),
  },
}, { mutationId });
console.log(`Queued ${proposal.mutationId}`);
```

A read session cannot submit. Pass a write session, or call with no session
on the owner socket. `getShareSubmissionStatus(mutationId)` on a client
selected to that database reports `submitted`, `published`, `discarded`, or
`unknown` for a submission this node already sent.

## Owner operations

- `version()`, `requireApiVersion(required)` — `GET /api/version`.
- `autoIdentity()` — `GET /api/system/auto-identity`.
- `listSchemas()`, `getSchema(schemaName)`, `resolveSchema(descriptor)` —
  schema catalog.
- `listDatabases()`, `createDatabase(req)`, `bindDatabaseSpec(id, spec)`,
  `issueDatabaseSession(id, intent)`, `openDatabaseSession(id, intent)`,
  `select(id, { databaseSession? })` — named databases and sessions.
- `submitProposal(schemaName, op, options?)` — queue one proposal on a received database.
- `getShareSubmissionStatus(mutationId)` — status of one share submission.

## App API

An app started by the LastDB launcher runs on the app socket. The launcher sets
`LASTDB_SOCKET_PATH` to that socket and `LASTDB_APP` to the app id.
`read`, `write`, and `changes` send that value as `X-LastDB-App` with every
request. The app socket serves only these three methods.

Pass `database` on a read, a mutation, or `changes` to name a database. These
methods do not copy the database chosen by `select`. A database-session header
already on the client is still sent. On that client, an omitted `database`
addresses the session's database, and an item that names a different database
is `permission_denied` with the message `a database session addresses only its own database`.
With no session, an omitted `database` addresses the personal database. A
session cannot call `changes`.

- `read(reads)` posts `{ reads }` to `POST /v1/read` and returns one result per
  read, in order. A result is `{ records, next_cursor }`, `{ keys, next_cursor }`
  when the read sets `keys_only: true`, or `{ error: { code, message, index?, reason? } }`.
  A per-item error is returned. It is not thrown. Each read names `schema` and
  exactly one of `key` or `range`, and may set `via`, `limit`, `cursor`,
  `file: true`, and `keys_only: true`. A read with `keys_only: true` and
  neither `key` nor `range` lists the primary keys of the schema; follow
  `next_cursor` for the next page. `range` is one of three forms: hash alone,
  hash plus `prefix`, or hash plus `start` and `end`. On the owner socket,
  `file: true` with `file_dir` set to an existing directory asks the node to
  name the file. `file_dir` is refused on the app socket.
- `write(mutations)` posts `{ mutations }` to `POST /v1/write` and returns
  `{ written, revisions }` after the write is on disk. `create` carries
  `record` and optional `alternates` and `file`, and no `revision`. `replace`
  adds `revision`. `delete` carries `revision` and no `record` and no `file`.
  `file` is `{ path, name?, media_type? }`, a path on disk. On `replace`,
  JSON `null` clears the record's file. Omitting `file` keeps it. Every
  mutation in one list names the same database.
- `changes({ after?, limit?, schemas?, database? })` posts that object to
  `POST /v1/x/changes` and returns `{ id, head, through, gap, changes }`.
  Omit `after` and the read starts at 0.

Version 0.18.0 takes the app API types from `lastdb_node/app-api.json`.
Regenerate them with `python3 scripts/lints/app_api_types.py --write`.
CI runs the same script with `--check` and fails when the committed file differs.
Use the schema names: `ReadItem`, `WriteItem`, `ChangesRequest`, `ChangesReply`,
`ChangeEntry`, `ReadResult`, `WriteResult`, `Record`, `FileInfo`, `FileLink`,
`Range`, `ErrorEnvelope`, `WriteCreate`, `WriteReplace`, and `WriteDelete`.
`AppApiError` is the class for a non-200 reply.

A read list holds at most 64 items. A read page defaults to 100 and is capped
at 1000. A changes page defaults to 500 and is capped at 500. The node keeps
the newest 250000 change entries. The app socket accepts a body of at most
8388608 bytes. A manifest access list holds at most 256 names. The constants
are `APP_API_READS_PER_REQUEST`, `APP_API_READ_PAGE_DEFAULT`,
`APP_API_READ_PAGE_CAP`, `APP_API_CHANGES_PAGE_DEFAULT`,
`APP_API_CHANGES_PAGE_CAP`, `APP_API_CHANGES_KEPT`,
`APP_API_MANIFEST_ACCESS_ENTRIES`, and `APP_API_MAX_BODY_BYTES`.

A non-200 reply throws `AppApiError` with `status`, `code`, and `message`
from `{ "error": { "code", "message" } }`. A refusal that names one mutation
also sets `index`. A write whose condition failed also sets `reason`:
`exists`, `absent`, or `revision`.

```ts
const [page] = await db.read([
  { schema: 'notes/Note', key: { hash: 'n1', range: null } },
]);
if (page !== undefined && 'records' in page) {
  const current = page.records[0];
  if (current !== undefined) {
    const saved = await db.write([
      {
        schema: 'notes/Note',
        op: 'replace',
        key: current.key,
        revision: current.revision,
        record: { ...current.record, title: 'updated' },
      },
    ]);
    console.log(saved.written, saved.revisions);
  }
}
const feed = await db.changes({ after: 1200, schemas: ['notes/Note'] });
console.log(feed.head, feed.gap, feed.changes);
```

## Typed errors

- `NodeTooOldError` — `GET /api/version` reported a lower `api_version` than
  required.
- `CasConflictError` — `409 {error: "cas_conflict"}` with `.schema`, `.key`,
  `.expected`, `.actual`, and `.body`.
- `RequestRejectedError` — `400` / `404` / non-CAS `409` / `413` request or
  target rejection with `.kind`, `.status`, and `.body`.
- `PermissionDeniedError` — `403` posture, namespace, or session denial with
  `.category`.
- `AuthenticationRequiredError` — `401` credential failure.
- `AppApiError` — non-200 reply from `read`, `write`, or `changes`, with
  `.status`, `.code`, and `.message`. A refusal that names one mutation also
  sets `.index`. A failed write condition also sets `.reason` (`exists`,
  `absent`, or `revision`). A per-item read error is a result, not this class.
- `TransportError` / `UnexpectedResponseError` — socket or timeout failure, or
  an HTTP status this client does not model.

## Development

```bash
npm ci
npm run typecheck
npm run lint
npm run build
```
