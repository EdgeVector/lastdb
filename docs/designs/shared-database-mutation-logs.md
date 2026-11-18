# Shared databases: the mutation log, the proposal log, and the share settings

## Decided state

Tom decided this on 2026-11-18, and corrected it the same day. Later the same day he decided that the mutation log is the database: a reader is served every entry in it, and ExeMem is the single place that decides who may append and what that reader is served.

The **mutation log** is the log every database has. A personal database already has one under that name. A shared database has the same log. The name is not "the publisher's log" or "the published log", because those names assume only the publisher writes it. While `write` is `direct`, an active member with the Submit role can write it too. Every entry in the mutation log is part of the database.

The **proposal log** is the other log of a shared database. An active member with the Submit role appends to it while `write` is `proposals`. A member's proposal goes only to that log. Entries already there stay until the publisher settles them. The publisher can list and settle that log under any setting. Changing `read` or `write` does not delete the log, refuse the change, or rewrite an entry already stored.

Tom, 2026-11-18: "The different database types of sharing should just be Exemem configurations. It shouldn't care what the underlying is."

Tom, 2026-11-18: "from Exemem's point of view: it shouldn't know whether it's share, org, or deliver. It just has configurations that it reads for who can read, who can write."

Tom, 2026-11-18: "Let's go with mutation log and proposal log."

Tom, 2026-11-18: ExeMem is the one that serves a member only the logs that member should have. A member's node receives every entry ExeMem serves and does not reject one because of who wrote it. ExeMem is the single point of trust and the single configuration.

One sharing mechanism. Two settings per database, plus the per-schema member-write map below. ExeMem stores the statement and enforces `read` and `write`. ExeMem does not interpret the map. The underlying database does not change with the settings.

`read` is `members` or `anyone`. It says who may read the mutation log, the snapshot, and file ranges. `members` means an active member. `anyone` means a caller with no grant reads what a member reads and cannot append. A caller who may read is served every entry in the mutation log. The writer's identity does not hide an entry. The current settings do not hide an entry that is already stored.

`write` is `proposals` or `direct`. It says where an append by a member with the Submit role goes from now on. `proposals` means the proposal log. `direct` means the mutation log. The publisher may always append to the mutation log. An active Submit holder may append to the mutation log only while `write` is `direct`. While `write` is `proposals`, the log model refuses a non-publisher append to the mutation log. There is no older path that appends a member's proposal to the mutation log when the proposal log has no head yet. A publisher may change either setting at any time, in any direction, any number of times.

No rule depends on a combination of the two settings, or on what either log already holds. `read: anyone` together with `write: direct` is valid.

In every case only the publisher publishes a snapshot. A receiver (a member, or an open-link reader) fetches the snapshot and replays the mutation log on top, in the same way that multi-device works. The member's node applies every entry it is served.

<a id="requirements-to-preserve"></a>

## What each setting means

ExeMem stores one publisher-signed settings statement per shared database. The statement has `read`, `write`, and `member_writes`. The stored spellings are `members` or `anyone`, and `proposals` or `direct`. The publisher signs the statement. ExeMem uses `read` and `write` for transport and stores the map without interpreting it. The publisher's LastDB uses the confirmed configuration on the publisher's machine when it checks a proposal. A change of `read`, `write`, or the map is the next sequence number. Repeating all three is refused. The logs' contents are not an input to that check.

### `members` and `proposals`

This pair is the default. A database with no settings follows it. The requirements below hold for this pair.

- A personal database has one log, the mutation log. A shared database has exactly two logs under one database ID: the mutation log and the proposal log. Their positions are independent. Do not delete either log, combine them, or keep a mixed-log path. There is no compatibility with a retired log format.
- Every active member can read every entry in the mutation log, and can append an encrypted proposal, even with an empty write grant. Members follow only the mutation log. Only the publisher can read or settle the proposal log, including an entry from a member who asks for their own. A member cannot read that entry.
- Keep the publisher-signed configuration for each database, including its revision, member identities, active or revoked state, and schema write grants. ExeMem checks active membership for transport. The publisher's LastDB checks every downloaded and decrypted proposal against the confirmed configuration on its machine. An app's displayed permissions grant no write authority.
- Keep check, local commit, append to the mutation log, and settlement in that order. A denied or invalid proposal produces no mutation-log entry. For an accepted proposal, the publisher appends the committed change to the mutation log under the publisher's key before marking the proposal-log entry processed. LastDB decides when stored entries and retry records are safe to retire. A queue receipt confirms delivery to the proposal log.
- The publisher's authenticated settlement authorizes ExeMem to remove the original encrypted proposal from the proposal log. Retain the identity needed for an exact append retry and any outcome the publisher supplied. Settlement preserves both logs, their positions, and the publisher's accepted record history.

On this pair, ExeMem admits a member read of the mutation log and a member append to the proposal log. Only the publisher appends to the mutation log. Only the publisher lists and settles the proposal log.

The publisher's node downloads the proposal log, decrypts each entry, checks the sender's current schema grant and the record rules, commits an accepted change locally, and appends that change to the mutation log. A member's node reads the mutation log and appends proposals. It applies every mutation-log entry it is served. It does not read the proposal log.

### `read: anyone`

There is no third write spelling. `anyone` lets a caller with no member entry read what a member reads, and that caller cannot append. The reader holds the read key from a link. While `write` is `proposals`, a Submit member still appends to the proposal log. The database may have no such member. Entries already in the proposal log stay until the publisher settles them.

The reader's node fetches the publisher's snapshot and replays the mutation log. It applies every entry it is served. It does not append.

### `write: direct`

An active member with the Submit role appends to the mutation log. ExeMem checks only that the member may write. It does not check the mutation the way the publisher checks a proposal. Order is the order ExeMem assigns at append. Entries already in the proposal log stay until the publisher settles them.

A member node with Submit appends straight to the mutation log while `write` is `direct`. A reader who may read is served that entry with every other entry in the mutation log. The publisher still publishes the snapshot.

### Examples an app might choose

The names below are examples of configurations an app might choose. No rule depends on a name.

| Name | read | write | What the name refers to |
| --- | --- | --- | --- |
| Share (the default; also when no settings exist) | `members` | `proposals` | Members read every mutation-log entry. A Submit member appends to the proposal log. The publisher writes the mutation log. |
| Deliver | `anyone` | `proposals` | A caller with no member entry reads every mutation-log entry, and holds the read key from a link. A Submit member still appends to the proposal log while `write` is `proposals`. There may be no such member. |
| Org | `members` | `direct` | Members read every mutation-log entry. A Submit member appends to the mutation log. ExeMem checks only that the member may write. Order is the order ExeMem assigns at append. |

## Member write behaviours

What a member may do to the records of each schema is a setting of the database. The schema's author does not set it, and it is not set per member. The settings statement carries it as `member_writes`: a map from a canonical schema name, such as `social/Comment`, to one behaviour.

- `owner_only`. A member cannot write records of this schema. A schema that is not in the map has this behaviour. An empty map means every schema is `owner_only`.
- `own_records`. A member with write access may create a record, and may replace or delete a record only when that member created it.
- `shared`. A member with write access may create, replace, and delete any record.

The map lives in the one settings statement ExeMem already stores for the database. ExeMem stores the statement and does not interpret the map. It cannot read the entries. The publisher's LastDB enforces the map when it checks a proposal.

While `write` is `direct` there is no proposal check. No third party enforces the map on that path. The map is a statement of intent for apps.

An empty map is valid, and it is what an absent `member_writes` field means. The signed digest omits an empty map, so a statement stored before this field still verifies. A successor is unchanged only when `read`, `write`, and the map are all the same. A change of only the map is a real change.

The map holds at most 256 names. A name is non-empty and at most 128 bytes. `ShareWriteScope::Named` does not cap the byte length. It rejects an empty name, leading or trailing whitespace, and a control character. This map uses that check and adds the 128-byte cap. The largest settings set request those bounds accept is 70,635 bytes. The control JSON limit is 256 KiB (262,144 bytes).

## Sharing configuration

The publisher sets the sharing rules in Social. Those rules say who can read the whole database and which schemas each person can change. Apps use the rules to show what people can do. The publisher's LastDB checks each write independently, so the rules still hold when a member's app offers the wrong action or sends a write outside that person's permissions.

Social owns contacts and circles on the publisher's LastDB. For a database, it resolves the circles into a sharing specification keyed by recipient node identity. Each active entry grants whole-database read access and can also grant permission to write selected schema names or all schemas. Social reads or updates the roster through `GET` or `PUT /api/databases/{id}/share/config`. The update includes the expected revision and the effective entries with their node keys, states, roles, and write scopes. LastDB signs and sends the effective roster under the database ID. ExeMem stores the current revision, binds it to the publisher's key, and uses it to authorize the database's transport. The publisher's LastDB evaluates individual writes against the effective schema grants.

A named scope such as `"writeSchemas": {"names": ["social/Comment"]}` matches the exact canonical name in that database. A compatible schema update leaves the grant in force. The submitted operation still has to match the installed definition and pass the normal record checks. A different name needs its own grant. Existing signed arrays of exact definition IDs retain their original meaning. An empty array grants reads only, and `"all"` covers every schema in that database. ExeMem carries the signed scope and leaves schema checks to the publisher.

For a conditional replacement or delete, the member passes the identity of the record version in the encrypted proposal. The share layer stores that identity beside the download journal for the record revision it wrote or downloaded. `created_by` and `updated_by` are on the record. The publisher checks the identity against its current record and uses its own record revision when committing. An intervening write causes the proposal to be discarded. A record without this identity needs an updated publication before a member can edit it this way.

The database sets `created_by` and `updated_by`. A caller cannot write them. `created_by` is set only when no live record is at the key: a create, or a create on a deleted key. On a replace, `created_by` is whatever the previous live record has, including when it is absent. `updated_by` is the writer. The same rule applies to a local write, an accepted proposal, replay on an own device, a baseline, and replay on a receiver. An existing meta block stays on a member replace. New records have no meta block. A proposal's source file name and metadata map are not stored.

The standard `social/Comment` schema has a fixed write rule on whole-database shares. A member with a current write grant can create comments and edit their own comments. The publisher can edit or delete any comment. A member's delete is discarded. The creator is the live record's `created_by` at the checked revision. The node does not set `author_id`, and it keeps no creator marker. A comment with no `created_by` stays readable and only the publisher can edit it, before and after the publisher edits it. A publisher edit leaves `created_by` absent. Social and apps read `created_by`. LastDB enforces the rule for every grant, including all-schema grants. Other schemas keep their existing write behavior. That fixed rule is what the node does today. The decided configuration is `member_writes` (`own_records` for a schema the publisher lists that way). The node does not read the map yet.

This table is `members` and `proposals`. Members append to the proposal log only while `write` is `proposals`. Entries already there stay until the publisher settles them. While `write` is `direct`, an active Submit member appends to the mutation log instead, and a reader is served that entry with every other mutation-log entry.

| Identity on this database | Read mutation log | Append to mutation log | Append to proposal log | Read and settle proposal log |
| --- | --- | --- | --- | --- |
| Publisher | Yes | Yes | | Yes |
| Active member | Yes | | Yes | |
| Outside the active roster | | | | |

On this pair the proposal log has fixed access rules, including for a member's own entries. Members see their own grant and every entry in the mutation log. The publisher sees the roster.

## From invitation to shared data

Apps can use Social's client, [`@lastdb/social-client`](https://github.com/EdgeVector/social/tree/main/client),
for setup and invitations ([how it fits with the app SDK](../../lastdb_app_sdk/README.md#set-up-sharing-with-social)).
The client calls the Social app for the selected LastDB home. After accepting an invitation, the app
opens the returned stable database ID in the ordinary LastDB data client.

1. The publisher sets sharing in Social. Once LastDB confirms the roster in ExeMem, it returns a sealed invitation for the caller to deliver. The recipient accepts it with the verified publisher key. The recipient's LastDB obtains the database identity and keys, confirms current access, and syncs records and schema definitions from the mutation log.
2. A publisher write commits to its LastDB and is captured for upload into the mutation log. Every active member can sync the resulting data across all schemas.
3. Any active member's LastDB can send a signed, encrypted mutation through the existing append API. ExeMem checks that the signed node identity is on the active roster and routes it to the proposal log. HTTP 202 means queued. The member's mutation-log download cursor stays at the same position.
4. The publisher's LastDB reads the proposal log, decrypts each submission, and checks its sharing revision and the sender's current write access to the target schema. A member with an empty write scope can submit, and the publisher discards their mutations. It also discards mutations that fail the schema or record rules. It durably commits valid changes using the submission identity for retries. Accepted changes go through normal publication under the publisher's key, onto the mutation log. Every active member can sync them, including the original sender.
5. The publisher advances the proposal-log position after the page has been decided and the durable accepted writes through that point are on the mutation log. An unsettled page remains available after a restart. ExeMem replaces newly settled ciphertext with a receipt when it advances the position. ExeMem recognizes the same submitter, submission ID, and encrypted entry as an exact append retry and adds no new proposal-log entry.

The submitting member can request the outcome for its mutation UUID.
The publisher supplies the outcome in its authenticated settlement after
the mutation-log append. ExeMem retains it with a digest of the original encrypted entry and
returns only the caller's own result through the signed access route.
The result carries the UUID and status alone. A settlement response that does
not carry that result is an error.

Whole-database revocation stops new reads and submissions. Narrowing a write grant leaves whole-database reads available. The publisher refuses an uncommitted submission from a superseded sharing revision. A contributor with current permission can submit the write again. Records already on the mutation log, and data already downloaded, remain available under the normal database rules.

## Transport

`POST /api/cloud/backups/v2/mutation-log/append` keeps its signed envelope with the database ID, writer device, and encrypted entries. ExeMem uses the caller's database role and the share settings to select the log. While `write` is `proposals`, only the publisher appends to the mutation log, and a member append goes to the proposal log. `head`, `list`, and snapshot delivery serve every entry in the mutation log to a caller who may read. The publisher's LastDB uses signed `inbox/list` with a database ID and `inbox/settle` with a database ID, inclusive proposal-log position, and the decision for each entry. Those path spellings stay. Each request binds the signing node, database, path, and current sharing revision.

ExeMem stores the proposal log in a separate partition for the same database ID. It keeps that log's delivery position, settlement position, and idempotency records independent of the mutation log. It treats the mutation payloads as ciphertext. LastDB owns schema validation, record and attachment semantics, and decisions about safe retirement of data.

While `write` is `proposals`, a contribution reaches readers only after the publisher commits it and appends that committed change to the mutation log. A non-publisher append to the mutation log is refused. While `write` is `direct`, a Submit member's append is on the mutation log, and a reader who may read is served it with every other entry.

## Publisher device authority

A whole-database share uses one active publisher device. Before deciding entries on the proposal log, the publisher checks its durable download position against the mutation log. If an owned device sees a write from another device with the same publisher key, it reports that it cannot continue shared sync. Proposals stay available. The signed publisher key authenticates the publisher, and the writer device ID identifies entries. Two machines that share the publisher key can still open the proposal log at the same time before either appends to the mutation log.

Supporting authorized replacement or replicas requires an explicit owner policy. ExeMem would bind one database's publishing authority to an independently authenticated device key and revision. Its transactions would verify that revision for publisher appends to the mutation log and for proposal-log settlement. Replicas would get the mutation log and would replay accepted contribution receipts along with the records. An owner-approved handoff would confirm the old publisher's accepted writes reached the cloud before changing authority.

## Status

The settings statement and the log model treat `read` and `write` as independent. `ShareSettings::validate` accepts every pair. `ShareAdmission::new` returns the pair and does not fail. The absent value is still `members` and `proposals`, and the absent member-write map is empty. `validate_successor_of` still requires the next sequence and refuses a statement that repeats the same `read`, `write`, and `member_writes`.

The settings statement can carry `member_writes`. An empty map is left out of the digest, so a statement stored before the field still verifies. The node does not set the map and does not enforce it yet.

`read: anyone` lets a caller with no grant read the mutation log and does not let that caller append. `LogState::append_log` names the log for a writer. `LogState::append` is the mutation log: the publisher, or an active Submit holder while `write` is `direct`. While `write` is `proposals`, a non-publisher append to that log is refused. `LogState::append_proposal` is the proposal log, for the publisher or an active Submit holder. `LogState::reader_allowed` decides who may read. On the mutation log, a caller who may read is served every entry. On the proposal log, only the publisher may read. There is no per-entry writer mark and no per-entry writer signature.

The list wire is unchanged. `lastdb_node/src/log_sync/wire.rs` `EntryBody` still has `deny_unknown_fields`.

These are not built:

- ExeMem enforcement. The service still has to follow `append_log`, stop storing a direct-write attribute, and serve every mutation-log entry to a reader who may read. It must not append a member's proposal to the mutation log when the proposal log has no head. It must not refuse a change to `direct` or `anyone` because unsettled proposals exist, because the mutation log holds entries the publisher did not write, or because `read` is `anyone`. An `anyone` database is not a database without a proposal log.
- The open-link read. A reader with no member entry cannot yet read by holding the read key from a link.
- A Submit member's node does not yet send its write to the mutation log while `write` is `direct`. ExeMem does not yet assign that append's order.
- Receiver snapshot fetch. A member or an open-link reader does not yet fetch the publisher's snapshot and replay the mutation log on top.

The node does not yet pass a stored statement into the log model. A page the node applies still has to be written by the publisher. That check is `received_page` in `lastdb_node/src/log_sync/mod.rs` and `check_page` in `lastdb_node/src/database_replay/page.rs`. This change does not remove it. The member's node should apply every entry it is served. That is the next change.

## Old names

| Old name | Name |
| --- | --- |
| published log, publisher log, publisher's log | mutation log |
| pending mutation log, inbox, shadow log | proposal log |

Stored keys and wire paths keep their existing spellings, for example the ExeMem partition `{database_id}#inbox` and the route `mutation-log/inbox/list`, because renaming those moves live data.
