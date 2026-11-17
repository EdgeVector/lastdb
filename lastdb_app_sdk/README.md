# @lastdb/app-sdk

The runtime SDK for LastDB apps. A dependency-free TypeScript client over a
node's Unix socket. The `/api/*` methods connect, verify protocol `API_VERSION`
compatibility (`LASTDB_SDK_API_VERSION` and `LASTDB_MIN_SUPPORTED_API_VERSION`,
generated from the node source), and run keyed reads, single and batch
conditional mutations, record file/blob operations, membership lists, named
databases, and short-lived scoped database sessions. `read`, `write`, and
`changes` call the app API. The older methods still work.

## Install a build of the SDK

```bash
npm install /path/to/lastdb-app-sdk-0.15.0.tgz
```

Build a tarball from this directory with `npm ci`, `npm run build`, and
`npm pack --ignore-scripts`.

## Quickstart

```ts
import { connect } from '@lastdb/app-sdk';

// Uses explicit socketPath when passed, else discovers LASTDB_SOCKET_PATH ->
// FOLDDB_SOCKET_PATH -> <LASTDB_HOME or ~/.lastdb>/data/folddb.sock.
const db = await connect({ appId: 'my-app' });

await db.mutate('my-app/Concept', {
  mutationType: 'create',
  alternates: {},
  expected: { type: 'absent' },
  record: { id: 'c1', title: 'hello' },
  key: { hash: 'c1', range: null },
});

const single = await db.get('my-app/Concept', 'c1');
console.log(single?.key, single?.fields, single?.metadata);

const { rows } = await db.query('my-app/Concept', {
  filter: { HashKey: 'c1' },
});
for (const row of rows) {
  console.log(row.key, row.fields, row.metadata);
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

## Named Databases & Scoped Sessions

`listDatabases` reads `GET /api/databases`. The personal database and received
shares stay in that list. `createDatabase` calls `POST /api/databases` to create
an owned shareable or personal database, and `bindDatabaseSpec` calls
`POST /api/databases/spec` to bind or clear an owned database's spec. `select`
returns a new client bound to one database ID or alias without mutating the
existing client.

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
const rows = await draftReader.query('photos/Album', { filter: { HashKey: 'vacation' } });

// Or hand `session.session` to an isolated client process:
const delegated = await connect({
  appId: 'photos-viewer',
  databaseId: session.databaseId,
  databaseSession: session.session,
});
```

Keep `session.session` tokens out of logs. Every scoped client method (`get`,
`list`, `query`, `queryBatch`, `queryAll`, `mutate`, `mutateBatch`, `putBlob`,
`fetchBlob`, `deleteBlob`, `listSchemas`, `getSchema`, `declareSchema`) routes
uniformly through `/api/databases/<selector>/...`.

## Record Files & Blobs

Attach, read, or clear a record's external `blob` or `inline` file over the
Unix-socket binary fast path:

```ts
const uploaded = await drafts.putBlob('photos/Photo', {
  op: {
    mutationType: 'create',
    expected: { type: 'absent' },
    alternates: {},
    key: { hash: 'photo-7', range: null },
    record: { caption: 'Sunset' },
  },
  bytes: jpegBytes,
  name: 'sunset.jpg',
  mediaType: 'image/jpeg',
  thumbnail: { bytes: thumbBytes, name: 'sunset-thumb.jpg', mediaType: 'image/jpeg' },
});

const blob = await drafts.fetchBlob('photos/Photo', 'photo-7');
const thumb = await drafts.fetchBlob('photos/Photo', 'photo-7', { placement: 'inline' });

await drafts.deleteBlob('photos/Photo', {
  mutationType: 'replace',
  expected: { type: 'revision', revision: uploaded.recordRevision },
  alternates: {},
  key: { hash: 'photo-7', range: null },
  record: { caption: 'Sunset' },
});
```

## Atomic Batch Mutations

Commit multiple complete conditional record mutations atomically via
`POST /api/mutations/batch`:

```ts
const batch = await db.mutateBatch([
  {
    schemaName: 'my-app/Concept',
    op: {
      mutationType: 'create',
      expected: { type: 'absent' },
      alternates: {},
      key: { hash: 'c2', range: null },
      record: { id: 'c2', title: 'first' },
    },
  },
  {
    schemaName: 'my-app/Concept',
    op: {
      mutationType: 'create',
      expected: { type: 'absent' },
      alternates: {},
      key: { hash: 'c3', range: null },
      record: { id: 'c3', title: 'second' },
    },
  },
]);
console.log(batch.count, batch.operations, batch.recordRevisions);
```

## Set up sharing with Social

Sharing setup is done with Social's own client, `@lastdb/social-client`. It
ships with the Social app at
[EdgeVector/social `client/`](https://github.com/EdgeVector/social/tree/main/client)
and calls the installed `social` command: create a named share or a
`shared-albums` setup, list circles, propose a sharing change for the owner
to approve, seal and accept invitations, and revoke access. Before 0.14.0
this package exported those helpers as `socialClient`.

This package is the data client. Point both at the same LastDB home. The
Social client returns the stable database ID. Select that ID here for
schemas, records, and files.

```ts
import { join } from 'node:path';
import { connect } from '@lastdb/app-sdk';
import { socialClient } from '@lastdb/social-client';

const social = socialClient({ dataDir: publisherHome });
const root = await connect({
  appId: 'photo-journal',
  socketPath: join(publisherHome, 'data', 'folddb.sock'),
});
const database = await social.createSharedDatabase('family-trips');
const trips = root.select(database.id);
await trips.declareSchema(tripSchema);
```

On a recipient's device, accept the sealed invitation with the Social client,
save the returned database ID, and open it with this package.

```ts
const recipient = await connect({
  appId: 'photo-journal',
  socketPath: join(recipientHome, 'data', 'folddb.sock'),
});

// Wait for this ID to be ready in listDatabases() before reading its content.
const { client: sharedTrips } = await recipient.openDatabaseSession(receivedDatabaseId, 'read');
const trip = await sharedTrips.get('photo-journal/Trip', tripKey);
```

Use the same ID when another app opens this share on that device.
`listDatabases` exposes the receiving node's `selfAccess` when available. An
initial response can be unknown while access is confirmed. An active grant
with `stale: false` can guide which write actions the app displays. The
publisher still checks each proposal.

## Registered Shares

Every active receiver can read the whole database. Social supplies the
current schema grants for the app to show available edit actions. A receiver
with a `social/Comment` grant can create comments and edit comments they
originally created. The publisher handles moderation and deletion.

Any active receiver can submit an encrypted mutation. A `submitted` receipt
confirms delivery to the publisher's private pending log. The publisher's
LastDB checks the current schema grant and record rules, then publishes
accepted changes under the publisher's key. Use the normal query API to
read the result after sync. The receiver follows the separate published log.
[Sharing and write approval](../docs/designs/shared-database-mutation-logs.md)

After accepting and syncing a shared-albums database, verify the setup on the
receiver's node with the Social client's `setupDatabase`. Use the returned
definition and a write session for the selected album database.

```ts
import { randomUUID } from 'node:crypto';

// recipientSocial is socialClient({ dataDir: recipientHome }) from @lastdb/social-client.
const verified = await recipientSocial.setupDatabase({
  setupName: 'shared-albums', requiredVersion: 1, databaseId: albumDatabaseId,
});
const { client: share } = await recipient.openDatabaseSession(albumDatabaseId, 'write');
share.rememberDefinition('social/Comment', verified.definitions['social/Comment']!);
const mutationId = randomUUID();
const proposal = await share.mutate('social/Comment', {
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
if ('status' in proposal && proposal.status === 'submitted') {
  console.log(`Queued ${proposal.mutationId}`);
}
```

The publisher derives `author_id` from the verified sender. To edit a comment,
read its current version, carry forward every field you are leaving as is,
and send the full record with its primary key and revision.

When a Share submission outcome is uncertain, `mutate` throws
`ShareSubmissionUncertainError` with `.mutationId` so the caller can retry with
the same body and UUID.
`submissionSequence` is the writer sequence within this submission. Use
`mutationId` to correlate it with an app action.

Call `share.getShareSubmissionStatus(mutationId)` on the receiving database.
It reports `submitted`, `published`, `discarded`, or `unknown`. Published
means the publisher has uploaded the change. The receiver may still need to
sync before a query returns it. Unknown also covers older settlements without
a recorded outcome. Keep using normal record queries to render shared data.

## Access Model & Operations

LastDB is a keyed store: every `query`, `queryAll`, and `queryBatch` item names
a primary or alternate key (`HashKey`, `HashRangeKey`, `HashRangePrefix`,
`HashRangeBetween`, `HashRangeKeys`, etc.). Unfiltered scans and server-side
`where` field filters are not supported.

- `get(schemaName, key)` — single-record point lookup by `HashKey` or `HashRangeKey`.
- `list(schemaName, { keyName?, hash?, limit?, cursor? })` — keys-only cursor
  pages over `GET /api/list`.
- `query(schemaName, { keyName?, filter?, limit?, offset?, cursor? })` — one
  keyed page over `POST /api/query`.
- `queryAll(schemaName, filter?, { pageSize?, maxRows? })` — drains a keyed
  query across pages up to `maxRows` (default 100,000).
- `queryBatch(items)` — up to 64 independent keyed queries per request over
  `POST /api/queries/batch`.
- `mutate(schemaName, op, options?)` — complete conditional record `create` /
  `replace` / `delete` / `purge` over `POST /api/mutation`.
- `mutateBatch(items, options?)` — atomic batch of conditional mutations over
  `POST /api/mutations/batch`.
- `putBlob(schemaName, request)`, `fetchBlob(schemaName, key, options?)`,
  `deleteBlob(schemaName, op, options?)` — record file and blob operations over
  `POST /api/db/{put-blob,fetch-blob,delete-blob}`.
- `declareSchema(request)` / `rememberDefinition(schemaName, definitionId)` —
  declare a schema on `POST /api/schemas/declare` and attach `definition_id` on
  subsequent mutations.
- `listSchemas()`, `getSchema(schemaName)`, `resolveSchema(descriptor)`,
  `autoIdentity()` — schema lookup and owner identity helpers.
- `listDatabases()`, `createDatabase(req)`, `bindDatabaseSpec(id, spec)`,
  `issueDatabaseSession(id, intent)`, `openDatabaseSession(id, intent)`,
  `select(id, { databaseSession? })` — named database and scoped session helpers.

## App API

`read`, `write`, and `changes` call the app API. The older methods still work.

An app started by the LastDB launcher runs on the app socket. The launcher sets
`LASTDB_SOCKET_PATH` to that socket and `LASTDB_APP_SESSION` to the app's
session, and these three methods send the session with every request. The app
socket serves only these three methods, so a restricted app uses no older one.

Pass `database` on a read, a mutation, or `changes` to name a database. These
methods do not copy the database chosen by `select`. A database-session header
already on the client is still sent; the app API selects its database from
the JSON body.

- `read(reads)` posts `{ reads }` to `POST /v1/read` and returns one result per
  read, in order. A result is `{ records, next_cursor }`, `{ keys, next_cursor }`
  when the read sets `keys_only: true`, or `{ error: { code, message } }`. A
  per-item error is returned. It is not thrown. Each read names `schema` and
  exactly one of `key` or `range`, and may set `via`, `limit`, `cursor`,
  `file: true`, and `keys_only: true`.
- `write(mutations)` posts `{ mutations }` to `POST /v1/write` and returns
  `{ written, revisions }` after the write is on disk. `create` carries
  `record` and optional `alternates` and `file`. `replace` adds `revision`.
  `delete` carries `revision` only. `file` is `{ path, name?, media_type? }`,
  a path on disk, not bytes. Every mutation in one list names the same database.
- `changes({ after?, limit?, schemas?, database? })` posts that object to
  `POST /v1/x/changes` and returns `{ id, head, through, gap, changes }`.

A non-200 reply throws `AppApiError` with `status`, `code`, and `message`
from `{ "error": { "code", "message" } }`.

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

## Typed Errors

- `NodeTooOldError` — `GET /api/version` reported a lower `api_version` than
  required, or a route answered `400 {kind: "unknown_key"}` (subclasses
  `RequestRejectedError`).
- `CasConflictError` — `409 {error: "cas_conflict"}` with `.schema`, `.key`,
  `.expected`, `.actual`, and `.body`.
- `RequestRejectedError` — `400` / `404` / non-CAS `409` / `413` request or
  target rejection with `.kind`, `.status`, and `.body`.
- `PermissionDeniedError` — `403` posture/namespace/session denial with
  `.category` (`'namespace_denied' | 'unverified_identity' | 'write_denied' | 'session_denied' | 'unknown'`).
- `AuthenticationRequiredError` — `401` credential failure.
- `QueryPaginationError` — `queryAll` detected stalled pagination or a count
  mismatch.
- `ShareSubmissionUncertainError` — uncertain Share submission upload outcome.
- `AppApiError` — non-200 reply from `read`, `write`, or `changes`, with
  `.status`, `.code`, and `.message`. A per-item read error is a result, not
  this class.
- `TransportError` / `UnexpectedResponseError` — socket/timeout failure or
  unmodeled HTTP status.

## Development

```bash
npm ci
npm run typecheck
npm run lint
npm run build
```
