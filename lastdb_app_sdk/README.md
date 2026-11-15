# @lastdb/app-sdk

The runtime SDK for LastDB apps. A dependency-free TypeScript client over a
node's Unix-socket `/api/*` surface: connect, verify protocol `API_VERSION`
compatibility (`LASTDB_SDK_API_VERSION = 34`, minimum supported node protocol
`LASTDB_MIN_SUPPORTED_API_VERSION = 29`), and run keyed reads, single and batch
conditional mutations, record file/blob operations, membership lists, named
databases, and short-lived scoped database sessions.

## Install a build of the SDK

```bash
npm install /path/to/lastdb-app-sdk-0.8.0.tgz
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
const created = await root.createDatabase({
  name: 'social-family',
  spec: { name: 'social', version: 1 },
});
const family = root.select(created.id);
const same = await connect({ appId: 'photos', databaseId: created.name ?? created.id });
```

To mint an owner-approved short-lived capability session (`POST /api/databases/session`)
and attach `X-LastDB-Database-Session` to every scoped request, call
`issueDatabaseSession` or `openDatabaseSession`:

```ts
const { session, client: familyReader } = await root.openDatabaseSession(created.id, 'read');
const rows = await familyReader.query('photos/Album', { filter: { HashKey: 'vacation' } });

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
const uploaded = await family.putBlob('photos/Photo', {
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

const blob = await family.fetchBlob('photos/Photo', 'photo-7');
const thumb = await family.fetchBlob('photos/Photo', 'photo-7', { placement: 'inline' });

await family.deleteBlob('photos/Photo', {
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

Use `socialClient` with Social 0.2.0 or later and the same LastDB home
as your data client. Use a database name to create, invite, or revoke.
The create helper uses LastDB's existing Social format.

```ts
import { connect, socialClient } from '@lastdb/app-sdk';

const social = socialClient({ dataDir });
const root = await connect({ appId: 'photo-journal', socketPath });
const database = await social.createDatabase('family');
const family = root.select(database.id);

const receipt = await social.inviteReader('family', {
  name: 'Ana',
  nodeKey: anaVerifiedKey,
  contactPath: anaContactFile,
});
```

On Ana's device, call `social.acceptInvitation(receipt, verifiedPublisherKey)`.
The returned database includes its verified spec. Use `root.select(database.id)`
on that device to open it, and save the ID to reopen it later. The publisher
can call `social.revokeAccess('family', anaVerifiedKey)`. Social remembers
verified contacts across databases, so later invitations can omit `contactPath`.
Keep sealed receipts out of logs.

## Registered Shares

For whole-database shares, use the receiver's current schema permissions to
show available edit actions. Any active receiver can submit an encrypted
mutation. A `submitted` receipt confirms delivery to the publisher's pending
mutation log. The publisher's LastDB checks the current sharing settings and
record rules, then publishes accepted changes under the publisher's key.
Use the normal query API to read the result after sync. The receiver's LastDB
follows the published log. Access to the pending log belongs to the publisher.
[Sharing and write approval](../docs/designs/shared-database-mutation-logs.md)

Pass the database ID or alias from `lastdb db list` to `connect` or `select`
on the recipient's socket:

```ts
import { randomUUID } from 'node:crypto';
import { connect } from '@lastdb/app-sdk';

const share = await connect({ appId: 'photos', databaseId: 'family-photos' });
const comments = await share.query('photos/Comment', { filter: { HashKey: 'photo-7' } });

share.rememberDefinition('photos/Comment', commentDefinitionId);
const mutationId = randomUUID();
const receipt = await share.mutate('photos/Comment', {
  mutationType: 'create',
  expected: { type: 'absent' },
  alternates: {},
  key: { hash: 'photo-7', range: mutationId },
  record: { text: 'Great photo' },
}, { mutationId });
if ('status' in receipt && receipt.status === 'submitted') {
  console.log(`Queued ${receipt.mutationId}`);
}
```

When a Share submission outcome is uncertain, `mutate` throws
`ShareSubmissionUncertainError` with `.mutationId` so the caller can retry with
the same body and UUID.

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
