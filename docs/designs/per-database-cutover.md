# Per-database cutover

DEV-only operator procedure for preparing one database to move from legacy bearer reads to signed node-key reads.

The decision lives in the `database_cutover` crate. The operator command is `lastdb cutover`. The crate reads no clock, opens no file, and does not contact the cloud or Exemem. The shell passes evidence bytes.

Exemem is the only authority that commits signed-only for a database. This tool checks the evidence and writes one inactive request. That file does not set `signed_only`, does not forbid legacy resume, and does not allow retirement. A local file is not a service receipt. `service_receipt` is JSON null until Lane E and Lane I publish an authenticated receipt interface. Any other value fails to parse, and the error does not include the body.

This lane does not upload a snapshot, change grants, call Exemem, or delete legacy rows. Lane E writes the daemon receipts. Lane I writes the delivery receipts from Lane H's current grant view.

Approved source: Brain design `lastdb-named-databases-unified-log-sharing-revocation` (2026-11-11). Migrate one database at a time. Verify the node-key grants, then switch every read path for that database to signed-only together. That switch is Exemem's commit, not this file. Legacy bearer passes stop authorizing reads for that database when the service commits, including on a device that still holds a stale local file. Old server rows stay until a second device acks and keyed reads match. This tool can check that proof. It still does not retire the rows.

## What this tool refuses

- Any `environment` other than `dev`. There is no production variant.
- A path whose canonical location is inside the primary LastDB home. The shell treats `$HOME/.lastdb` and `$LASTDB_HOME` as that home, and canonicalizes a home that exists.
- `--data-dir`. The command does not open a data directory.
- `--db`. Cutover is not on the selector allow-list.
- The daemon socket, the cloud, and Exemem. There is no local signed-only commit.
- A second request for different evidence. The first inactive request stays.
- A file whose phase is `signed_only`, whose `activation` is true, whose `legacy_resume` is not `unchanged`, or whose `legacy_retirement` is not `forbidden`.
- A non-null `service_receipt`. No receipt schema is accepted yet.
- Retirement. No command deletes legacy data.
- Unknown JSON fields. A bearer token cannot be stored in these files. Parse errors name the file and do not echo the body.

`production_migration` on the request is `false`. A file that says otherwise does not parse.

## Identity

`database_id` is the stable id from the owned-database registry, not the local name. Lane A's id is a lowercase UUID. This checker accepts the database-selector alphabet: 1 to 64 bytes, ASCII letters, digits, `_`, or `-`. A lowercase UUID fits. The tool does not open the registry. Every evidence file must carry the same id the operator named.

`database_kind` is `personal` or `shareable`.

Node keys, device ids, and adapter or service revisions use that same alphabet. A git commit SHA fits. A dotted version does not.

`snapshot_ref` is an opaque pointer. It is non-empty, at most 256 bytes, and contains neither a control character nor whitespace. The tool does not open the snapshot bytes. Lane E may set `full`, `fresh`, and `covers_final_writes` only after it has seen those bytes.

## Positions

A legacy position is a canonical decimal: digits only, no leading zero, at most 20 characters, and it parses as a `u64`. The tool compares the text, not a numeric JSON value. `0120` and `120.0` do not pass.

These four strings must be the same text:

- `snapshot.json` `through_legacy_position`
- `legacy-position.json` `position`
- `final-writes.json` `fence_position`
- `final-writes.json` `position_after_drain`

Position `0` is an empty database: `byte_count` is `0`, and `writer_tips` is empty. Any other position has `byte_count` greater than `0` and at least one writer tip with `sequence` at least `1`.

## Evidence directory

Each file is at most 65536 bytes. The schema stamp is `lastdb.database_cutover/v1`. Member rows and writer tips are capped at 1024. The byte cap is the tighter limit.

| File | Writer | `kind` |
| --- | --- | --- |
| `snapshot.json` | Lane E | `fresh_full_snapshot` |
| `legacy-position.json` | Lane E | `legacy_position` |
| `final-writes.json` | Lane E | `final_writes` |
| `grants.json` | Lane I, from Lane H's current view | `node_key_grants` |
| `daemon-ready.json` | Lane E | `daemon_sync_ready` |
| `delivery-ready.json` | Lane I | `delivery_signed_only_ready` |

Every file also carries `schema`, `environment`, `database_id`, and `database_kind`.

`snapshot.json` requires `full`, `fresh`, and `covers_final_writes` all true.

`legacy-position.json` requires `quiescent` true. Writer tips have `node_key`, `device_id`, and integer `sequence`. The pair `(node_key, device_id)` is unique. A tip with sequence `0` means that device has not written. A non-empty log still needs one tip that has written.

`final-writes.json` requires `legacy_appends_after_fence` of `0` and `snapshot_covers_fence` true. The fence is the position where new legacy appends stopped. The position after the drain is that same text.

`daemon-ready.json` is Lane E's promise, not the service commit. It requires `accepts_cutover_record` true, `legacy_resume` of `refused_after_signed_only`, and `final_write_fence` of `honored`. Those values mean the adapter will honor a future Exemem commit. They do not mean the database is already signed-only. `migrating_device_id` is the device that is migrating. `adapter_revision` is a selector label the operator can compare with the deployed DEV binary.

`delivery-ready.json` is Lane I's promise, not the service commit. It requires `legacy_bearer_closed` true, `outstanding_urls` of `drained_or_invalid`, and `grant_check` of `current_node_key`. `legacy_bearer_closed` means Lane I will close legacy bearer reads when Exemem commits. `grant_revision` equals `grants.json` `revision`, and that integer is at least `1`. `service_revision` is a selector label.

### Grants

`authority_node_key` is the personal owner or the shareable publisher. `revision` is at least `1`. Node keys are unique.

Personal: every member capability is `owner`. Exactly one member is `active`, and that member's node key is the authority. Other owner rows may be `revoked`. There is no `receive` or `submit` row.

Shareable: the publisher is `authority_node_key` and is not a member row. The log has no `publish` capability. Members are `receive` or `submit`, each `active` or `revoked`. An empty member list is valid: the publisher is the authority and nobody else is granted. An `owner` row is refused. Revoked rows stay on the member list.

The capability words match the mutation log: `owner`, `receive`, and `submit`. This crate does not depend on that crate or on the cloud backup protocol.

## Evidence digest

The digest is SHA-256, lowercase hex, of these lines. Each line ends with one newline. Timestamps are not inputs. Member rows are sorted by node key, capability, then state. Writer tips are sorted by node key, device id, then sequence.

```
schema lastdb.database_cutover/v1
database <id>
kind <personal|shareable>
snapshot <ref>
position <decimal>
bytes <decimal>
authority <node key>
revision <decimal>
member <node key> <capability> <state>
tip <node key> <device> <decimal>
daemon <adapter revision>
migrating <device>
delivery <service revision>
```

`member` and `tip` lines are omitted when those lists are empty. The digest is meaningful only after verify succeeds.

## Inactive request

`prepare` writes kind `cutover_request`, phase `request_ready`, `activation` false, `authority` `exemem`, `legacy_resume` `unchanged`, `legacy_retirement` `forbidden`, and `production_migration` false. `service_receipt` is null. The shell's error text names the file `cutover-request.json`. The operator chooses the path. This tool does not write `cutover-record.json`.

The same evidence again returns the stored request and does not write. Different evidence returns `request_differs` and does not overwrite. The same digest with different bound fields (snapshot, position, authority, grant revision, migrating device, or kind) returns `record_inconsistent`.

The shell writes the request by creating `path.partial` and renaming it. It does not create the parent directory. It does not replace a file it cannot read. An old signed-only file does not parse as this request, so prepare will not overwrite it and resume-check will not treat it as authority.

## Second device

The proof is a separate file. It is not one of the six evidence files. `kind` is `second_device_proof`.

`proof` writes nothing. It accepts the proof only when all of these hold:

- The request is the inactive DEV request for that database.
- `device_id` is a different device from `migrating_device_id`.
- `acked_through_legacy_position` is the request's final legacy position, as the same text.
- `keyed_reads_match` is true.
- `compared_keys` is greater than `0`, unless the position is `0` and `empty_database` is true. Position `0` requires `compared_keys` of `0`.

Success prints `retirement_allowed` false and `activated` false. Old rows stay.

## Commands

```
lastdb cutover verify --database-id ID --evidence DIR [--json]
lastdb cutover prepare --database-id ID --evidence DIR --request FILE [--json]
lastdb cutover resume-check --database-id ID --request FILE [--json]
lastdb cutover proof --database-id ID --request FILE --proof FILE [--json]
```

Verify writes nothing. On success it prints the digest, `request_ready`, and `activated=false`. JSON is `ok`, `database_id`, `evidence_sha256`, `request_ready`, and `activated`.

Prepare writes only for a new inactive request. JSON is `result` (`request_ready` or `unchanged`) and `request`. Text prints the phase, `activation`, the service authority, the resume flag, and the retirement flag. Text does not print node keys.

Resume-check never changes access and never exits 2:

| Exit | Meaning |
| --- | --- |
| 0 | No request, or an inactive request for this database. Legacy resume stays unchanged. The database is not signed-only. |
| 1 | The file is unreadable, names another database, claims activation, allows retirement, or the path is inside the primary home. Do not treat the file as a service commit. Do not stop legacy resume because of it. |

Proof writes nothing. JSON reports `second_device_ack`, `keyed_reads_match`, `retirement_allowed` false, and `activated` false.

Reason text is `code: detail`. The detail does not include evidence values. Codes: `unreadable`, `not_dev`, `schema`, `kind`, `database`, `database_kind`, `invalid_database_id`, `snapshot`, `position`, `writer_tips`, `grants`, `daemon`, `delivery`, `request_differs`, `record_inconsistent`, `same_device`, `second_device`, `record_rejected`.

## Operator order

1. Lane E fences legacy appends for one database, drains the final writes, and takes a fresh full snapshot through that exact position. It writes `snapshot.json`, `legacy-position.json`, `final-writes.json`, and `daemon-ready.json`. The daemon file is a promise to honor a future service commit.
2. Lane I copies the current node-key grants and writes `grants.json` and `delivery-ready.json`. The delivery file is a promise to close legacy bearer reads when Exemem commits.
3. The operator runs `verify` on that directory.
4. The operator runs `prepare`. That stores an inactive request. It is not the switch. Read paths stay on their current access until Exemem commits.
5. Lane E does not treat the request file, or a missing `cutover-record.json`, as signed-only. Legacy resume stays available until the service commit. After that commit, the service rejects legacy access even if a device still has this file.
6. When Lane E and Lane I publish an authenticated receipt interface, a later change may cache a verified receipt and request the service commit. Until then, `service_receipt` stays null and this tool sends no request.
7. Legacy rows stay. A second device acks the same final position and reports matching keyed reads. The operator runs `proof`. Retirement stays forbidden.

## Lane boundary

Lane C adds create, list, and select on the same CLI enum. The `Cutover` variant has to stay. This command does not create a database and does not select one with `--db`.

Lane D owns the cloud backup wire types. This crate does not import them. Positions here are canonical decimal strings. The cloud route family is `/api/cloud/backups/v2/mutation-log/`. This tool does not call it and does not adopt a `/log/` draft.

Lane A merges first. This change does not merge before that, and it does not merge before `benchmark-complete` and the other required gates are green. Nothing here activates an unmigrated database or starts a production migration.
