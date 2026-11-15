# @lastdb/app-sdk

The runtime SDK for LastDB apps. A dependency-free TypeScript client over a
node's Unix-socket `/api/*` surface: connect, verify protocol `API_VERSION`
compatibility (`LASTDB_SDK_API_VERSION = 34`, minimum supported node protocol
`LASTDB_MIN_SUPPORTED_API_VERSION = 29`), and run keyed reads, single and batch
conditional mutations, record file/blob operations, membership lists, named
databases, and short-lived scoped database sessions.

## Install a build of the SDK

```bash
npm install /path/to/lastdb-app-sdk-0.10.0.tgz
```

Use the tarball from the Social sharing DEV operation, or build one from
this directory with `npm ci`, `npm run build`, and `npm pack --ignore-scripts`.

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

To mint an owner-approved short-lived capability session (`POST /api/databases/session`)
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

Import `socialClient` from the same SDK as `connect`. It invokes the installed
Social app. Social manages verified contacts, reusable circles, and invitations.
`connect` and `select` use the LastDB data API for schemas, records, and files.
Use Social 0.3.0 or later for the whole-database helpers and point both clients
at the same LastDB home. The publisher needs a connected ExeMem account.

### Share an app's own database

Create an empty named whole-database share when your app owns the schemas.
Save the stable database ID and declare the app's schemas with the selected
LastDB client. For standard album and comment schemas, use the setup below.

```ts
import { join } from 'node:path';
import { connect, socialClient } from '@lastdb/app-sdk';

const social = socialClient({ dataDir: publisherHome });
const root = await connect({
  appId: 'photo-journal',
  socketPath: join(publisherHome, 'data', 'folddb.sock'),
});
const database = await social.createSharedDatabase('family-trips');
const trips = root.select(database.id);
await trips.declareSchema(tripSchema);
```

Open Social on the publisher's device to add verified contacts and link a
circle to the database. Choose read-only access or the schemas that circle
can write, then preview and apply the change. Use `social --data-dir HOME open --database ID`
to go directly to the database. After Social confirms the grant, the app can
get a sealed receipt with `await social.sealInvitation(database.id, anaVerifiedKey)`.
The caller delivers the receipt to Ana. Keep sealed receipts out of logs.

On Ana's device, use a Social client pointed at her home. Pass the publisher
key verified independently of the receipt, and save the returned database ID.

```ts
const recipientSocial = socialClient({ dataDir: recipientHome });
const received = await recipientSocial.acceptInvitation(receipt, verifiedPublisherKey, 'family-trips');
const recipient = await connect({
  appId: 'photo-journal',
  socketPath: join(recipientHome, 'data', 'folddb.sock'),
});

// Wait for this ID to be ready in listDatabases() before reading its content.
const { client: sharedTrips } = await recipient.openDatabaseSession(received.id, 'read');
const trip = await sharedTrips.get('photo-journal/Trip', tripKey);
```

Use the same ID when another app opens this share on Ana's device. The owner
can manage access in Social or revoke a recipient with
`await social.revokeAccess(database.id, anaVerifiedKey)`.

`acceptInvitation` and `listDatabases` expose the receiving node's
`selfAccess` when available. An initial response can be unknown while
access is confirmed. An active grant with `stale: false` can guide which
write actions the app displays. The publisher still checks each proposal.

### Use a named database setup

Social can provision **shared-albums** version 1 with the standard
`social/Album`, `social/AlbumItem`, `social/Photo`, and `social/Comment`
schemas. The owner chooses a local name for provisioning. Other apps open
the selected database by its stable ID and get the verified definitions
before using the ordinary record APIs.

```ts
const created = await social.setupDatabase({
  setupName: 'shared-albums', requiredVersion: 1, localName: 'family',
});
const verified = await social.setupDatabase({
  setupName: 'shared-albums', requiredVersion: 1, databaseId: created.databaseId,
});
const albums = root.select(verified.databaseId);
albums.rememberDefinition('social/Album', verified.definitions['social/Album']!);
```

Verification by ID also works on a received share once it has synced its
schemas. Setup version 1 requires the catalog's schema shapes and leaves
schema upgrades to an explicit owner operation. Use the same circle and
invitation flow as for an app-owned share.

Social 0.4.0 and a LastDB build with retryable whole-database creation
resume interrupted provisioning by the original local name. An owner can
choose an existing incomplete setup by its ID and pass `finishExisting: true`
to install missing schemas after checking compatibility.

### Existing filtered shares

`social.createDatabase(name)` uses the original filtered Social format.
`social.inviteReader(database, { name, nodeKey, contactPath })` grants reader
access and returns a sealed invitation. Social reuses verified contacts, so
later reader invitations can omit `contactPath`. Use `sealInvitation` when
the recipient already has a circle grant on a whole-database share.

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
receiver's node. Use the returned definition and a write session for the
selected album database.

```ts
import { randomUUID } from 'node:crypto';

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
- `TransportError` / `UnexpectedResponseError` — socket/timeout failure or
  unmodeled HTTP status.

## Development

```bash
npm ci
npm run typecheck
npm run lint
npm run build
```
