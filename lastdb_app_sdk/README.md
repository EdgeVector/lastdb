# @lastdb/app-sdk

The runtime SDK for LastDB apps. A dependency-free TypeScript client over a
node's Unix-socket `/api/*` surface: connect, verify API compatibility, and run
keyed queries, conditional record mutations, membership lists, and change-feed
reads.

## Install

```bash
npm install @lastdb/app-sdk
```

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

## Registered Shares

Pass the database ID or alias from `lastdb db list` to `connect` on the
recipient's socket. `list`, `query`, `queryAll`, `queryBatch`, `listSchemas`,
and `mutate` target `/api/databases/<id>/...`:

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
  console.log(`Submitted ${receipt.mutationId} at ${receipt.submissionSequence}`);
}
```

When a Share submission outcome is uncertain, `mutate` throws
`ShareSubmissionUncertainError` with `.mutationId` so the caller can retry with
the same body and UUID.

## Interop Rule

Schemas use runtime names `{app_id}/{local}`. Read another app's schema by its
full name and write only schemas in your own namespace. Each schema owns its
record values independently.

## Access Model & Queries

LastDB is a keyed store: every `query`, `queryAll`, and `queryBatch` item names
a primary or alternate key (`HashKey`, `HashRangeKey`, `HashRangePrefix`,
`HashRangeBetween`, `HashRangeKeys`, etc.). Unfiltered scans and server-side
`where` field filters are not supported.

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
- `declareSchema(request)` / `rememberDefinition(schemaName, definitionId)` —
  declare a schema on `POST /api/schemas/declare` and attach `definition_id` on
  subsequent mutations.
- `autoIdentity()`, `listSchemas()`, `resolveSchema(descriptor)` — owner/host
  helpers over `GET /api/system/auto-identity` and `GET /api/schemas`.

## Typed Errors

- `NodeTooOldError` — `GET /api/version` reported a lower `api_version` than
  required, or a route answered `400 {kind: "unknown_key"}` (subclasses
  `RequestRejectedError`).
- `CasConflictError` — `409 {error: "cas_conflict"}` with `.schema`, `.key`,
  `.expected`, `.actual`, and `.body`.
- `RequestRejectedError` — `400` request/schema rejection with `.kind` and `.body`.
- `PermissionDeniedError` — `403` posture/namespace denial with `.category`.
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
