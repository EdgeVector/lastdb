# Shared database mutation logs

Each shared database has one publisher, one stable database ID, and two logs in ExeMem. The publisher's log carries the accepted changes used to sync and restore the database. A private inbox carries encrypted write submissions from all contributing receivers. Each log has its own sequence and progress.

## Sharing configuration

Social owns contacts and circles on the publisher's LastDB. For a database, it resolves the circles into a sharing specification keyed by recipient node identity. Each active entry grants whole-database read access and can also grant permission to write selected schema definitions or all schemas. Social reads or updates the roster through `GET` or `PUT /api/databases/{id}/share/config`. The update includes the expected revision and the effective entries with their node keys, states, roles, and write scopes. LastDB signs and sends the effective roster under the database ID. ExeMem stores the current revision, binds it to the publisher's key, and uses it to authorize the database's transport. The publisher's LastDB evaluates individual writes against the effective schema grants.

| Identity on this database | Read publisher log | Append to publisher log | Append to inbox | Read and settle inbox |
| --- | --- | --- | --- | --- |
| Publisher | Yes | Yes | | Yes |
| Receiver with read access | Yes | | | |
| Receiver with write access | Yes | | Yes | |
| Outside the active roster | | | | |

The inbox rules are fixed. Every receiver is unable to read any raw receiver entry, including their own. The sharing specification has no setting that changes this rule. Receivers see their own grant and the published data. The publisher sees the roster.

## From invitation to shared data

1. The publisher sets sharing in Social. Once LastDB confirms the roster in ExeMem, it returns a sealed invitation for the caller to deliver. The recipient accepts it with the verified publisher key. The recipient's LastDB obtains the database identity and keys, confirms current access, and syncs published records and schema definitions.
2. A publisher write commits to its LastDB and is captured for upload into the publisher's log. Every active receiver can sync the resulting data across all schemas.
3. A contributor's LastDB sends a signed, encrypted write through the existing append API. ExeMem checks the signed node identity and current database grant, and routes it to the private inbox. HTTP 202 means queued. The receiver's published download cursor stays at the same position.
4. The publisher's LastDB reads the inbox, decrypts each submission, and checks its sharing revision and the sender's current write access to the target schema. It durably commits valid changes using the submission identity for retries. It discards refused changes. Accepted changes go through normal publication under the publisher's key. Every active receiver can sync them, including the original contributor.
5. The publisher advances the inbox position after the page has been decided and the durable accepted writes through that point have been published. A restart can repeat an unsettled page. The same submitter and submission ID identify the same mutation across retries. Receivers confirm acceptance by reading the resulting record.

Whole-database revocation stops new reads and submissions. Narrowing a write grant leaves whole-database reads available. The publisher refuses an uncommitted submission from a superseded sharing revision. A contributor with current permission can submit the write again. Previously published records and data already downloaded remain available under the normal database rules.

## Transport and rollout

`POST /api/cloud/backups/v2/mutation-log/append` keeps its existing signed envelope with the database ID, writer device, and encrypted entries. ExeMem uses the caller's database role to select the log. `head`, `list`, and snapshot delivery continue to serve the published database. The publisher's LastDB uses signed `inbox/list` with a database ID and `inbox/settle` with a database ID and inclusive inbox position. Each request binds the signing node, database, path, and current sharing revision.

ExeMem stores the inbox in a separate partition for the original database ID. It keeps the inbox delivery position, settlement position, and idempotency records independent of the published log. It treats the mutation payloads as ciphertext. LastDB owns schema validation, record and attachment semantics, and decisions about safe retirement of data.

The first upgraded publisher's inbox read activates the split for its whole-database share. An older publisher continues to drain the existing queue until it upgrades. Racing appends either complete before activation in the old queue, or retry against the new inbox after activation. The upgraded publisher continues to drain historical entries through the existing publisher-only path. The published view served to receivers filters out every historical entry written by a receiver. The initial inbox keeps settled entries and retry identities for recovery. Deletion requires a LastDB retention decision.
