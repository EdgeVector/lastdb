# Shared database mutation logs

The publisher sets the sharing rules in Social. Those rules say who can read the whole database and which schemas each person can change. Apps use the rules to show what people can do. The publisher's LastDB checks each write independently, so the rules still hold when a receiver's app offers the wrong action or sends a write outside that person's permissions.

Each whole-database share has two logs in ExeMem. Any active receiver can submit a proposed change to the **pending mutation log**. The receiver's LastDB encrypts the change for the publisher before sending it. ExeMem checks that the sender is an active receiver and stores the encrypted request. Only the publisher can read the pending log. Receivers read the **published log**, and every receiver sees the same published changes.

The publisher's LastDB downloads and decrypts the pending changes. It checks each sender against the current sharing settings on the publisher's machine and checks that the change is valid for the database. It discards requests that fail either check. It saves the accepted changes locally and uploads them to the published log under the publisher's key. Everyone with active access, including the person who suggested the change, gets the result when they sync.

## Requirements to preserve

These requirements apply as Social adds features and as LastDB and ExeMem change their storage and transport.

- Keep both stored logs under the same database ID. The published log and pending mutation log have independent positions. Do not replace the split with a mixed log and a read filter. Keep older mixed history private and recoverable through migrations.
- Every active receiver can read the whole published database and send an encrypted proposal, even with an empty write grant. Receivers follow only the published log. Only the publisher can read or settle the pending log, including entries submitted by a receiver who asks for their own.
- Keep the current publisher-signed configuration for each database, including its revision, receiver identities, active or revoked state, and schema write grants. ExeMem checks active membership. The publisher's LastDB uses the confirmed configuration on its machine to check every downloaded and decrypted proposal. Apps display the grant.
- Keep approval, local commit, publication, and settlement in that order. Denied or invalid proposals produce no published change. For accepted proposals, the publisher uploads the committed change under its own key before marking the pending entry processed. LastDB decides when stored entries and retry records are safe to retire.

Tom's explicit direction is required to change these requirements. [Agent instructions](../../AGENTS.md#preserve-whole-database-sharing-tom-2026-11-14)

## Sharing configuration

Social owns contacts and circles on the publisher's LastDB. For a database, it resolves the circles into a sharing specification keyed by recipient node identity. Each active entry grants whole-database read access and can also grant permission to write selected schema names or all schemas. Social reads or updates the roster through `GET` or `PUT /api/databases/{id}/share/config`. The update includes the expected revision and the effective entries with their node keys, states, roles, and write scopes. LastDB signs and sends the effective roster under the database ID. ExeMem stores the current revision, binds it to the publisher's key, and uses it to authorize the database's transport. The publisher's LastDB evaluates individual writes against the effective schema grants.

A named scope such as `"writeSchemas": {"names": ["social/Comment"]}` matches the exact canonical name in that database. A compatible schema update leaves the grant in force. The submitted operation still has to match the installed definition and pass the normal record checks. A different name needs its own grant. Existing signed arrays of exact definition IDs retain their original meaning. An empty array grants reads only, and `"all"` covers every schema in that database. ExeMem carries the signed scope and leaves schema checks to the publisher.

| Identity on this database | Read published log | Append to published log | Append to pending log | Read and mark pending entries processed |
| --- | --- | --- | --- | --- |
| Publisher | Yes | Yes | | Yes |
| Active receiver | Yes | | Yes | |
| Outside the active roster | | | | |

The pending log has fixed access rules, including for a receiver's own entries. Receivers see their own grant and the published data. The publisher sees the roster.

## From invitation to shared data

1. The publisher sets sharing in Social. Once LastDB confirms the roster in ExeMem, it returns a sealed invitation for the caller to deliver. The recipient accepts it with the verified publisher key. The recipient's LastDB obtains the database identity and keys, confirms current access, and syncs published records and schema definitions.
2. A publisher write commits to its LastDB and is captured for upload into the publisher's log. Every active receiver can sync the resulting data across all schemas.
3. Any active receiver's LastDB can send a signed, encrypted mutation through the existing append API. ExeMem checks that the signed node identity is on the active roster and routes it to the private inbox. HTTP 202 means queued. The receiver's published download cursor stays at the same position.
4. The publisher's LastDB reads the inbox, decrypts each submission, and checks its sharing revision and the sender's current write access to the target schema. A receiver with an empty write scope can submit, and the publisher discards their mutations. It also discards mutations that fail the schema or record rules. It durably commits valid changes using the submission identity for retries. Accepted changes go through normal publication under the publisher's key. Every active receiver can sync them, including the original sender.
5. The publisher advances the inbox position after the page has been decided and the durable accepted writes through that point have been published. A restart can repeat an unsettled page. The same submitter and submission ID identify the same mutation across retries. Receivers confirm acceptance by reading the resulting record.

Whole-database revocation stops new reads and submissions. Narrowing a write grant leaves whole-database reads available. The publisher refuses an uncommitted submission from a superseded sharing revision. A contributor with current permission can submit the write again. Previously published records and data already downloaded remain available under the normal database rules.

## Transport and rollout

`POST /api/cloud/backups/v2/mutation-log/append` keeps its existing signed envelope with the database ID, writer device, and encrypted entries. ExeMem uses the caller's database role to select the log. `head`, `list`, and snapshot delivery continue to serve the published database. The publisher's LastDB uses signed `inbox/list` with a database ID and `inbox/settle` with a database ID and inclusive inbox position. Each request binds the signing node, database, path, and current sharing revision.

ExeMem stores the inbox in a separate partition for the original database ID. It keeps the inbox delivery position, settlement position, and idempotency records independent of the published log. It treats the mutation payloads as ciphertext. LastDB owns schema validation, record and attachment semantics, and decisions about safe retirement of data.

The first upgraded publisher's inbox read activates the split for its whole-database share. An older publisher continues to drain the existing queue until it upgrades. Racing appends either complete before activation in the old queue, or retry against the new inbox after activation. The upgraded publisher continues to drain historical entries through the existing publisher-only path. The published view served to receivers filters out every historical entry written by a receiver. The initial inbox keeps settled entries and retry identities for recovery. Deletion requires a LastDB retention decision.

## Publisher device authority

A whole-database share uses one active publisher device. Before deciding proposals in the private inbox, the publisher checks its durable download position against the current published log. If an owned device sees a published write from another device with the same publisher key, it reports that it cannot continue shared sync. Pending proposals stay available. The signed publisher key authenticates the publisher, and the writer device ID identifies entries. Two machines that share the publisher key can still open the inbox at the same time before either publishes.

Supporting authorized replacement or replicas requires an explicit owner policy. ExeMem would bind one database's publishing authority to an independently authenticated device key and revision. Its transactions would verify that revision for publisher appends and both settlement paths. Replicas would get the published view and would replay accepted contribution receipts along with the records. An owner-approved handoff would confirm the old publisher's accepted writes reached the cloud before changing authority, and older clients would need a rollout decision before a share enrolls.
