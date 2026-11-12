# Per-database cutover

ExeMem retired the legacy cutover route in #780. The LastDB cutover commands were removed. The evidence format below documents the retired procedure.

`lastdb db create` registers a fresh named database. The success body is `NativeDatabase` (`state: native`). It needs no evidence files, no legacy position, and no cutover. A cutover of a stored native database is `share.cutover.native`. E, D, G, H, and I own that create. K dogfoods it through the normal client.

Approved source: Brain design `lastdb-named-databases-unified-log-sharing-revocation` (2026-11-11). Migrate one existing database at a time. Verify the node-key grants, then switch every read path for that database to signed-only together. That switch is Exemem's commit. Legacy bearer passes stop authorizing reads for that database when the service commits, including on a device that still holds stale local state. Old server rows stay until a later retirement step. This command does not check that step and does not delete rows.

## What this tool refuses

- An evidence realm other than `dev` or `prod`. Both realms use the same checks. `verify` uses `--environment`. `request` uses the saved connection and refuses evidence for the other realm.
- Evidence whose canonical path is inside `$HOME/.lastdb` or `$LASTDB_HOME`.
- A credential home whose canonical path is inside `$HOME/.lastdb`. `request` reads that home for the saved connection and the existing `identity.key`. It does not treat every `LASTDB_HOME` as the primary. It does not create an identity.
- `--data-dir` on `verify`. Verify does not open a data directory.
- `--db`. Cutover is not on the selector allow-list.
- The daemon socket and the local store. Neither command opens them.
- A node key that is not `authority_node_key`, a missing saved connection, or a connection whose endpoint is not the HTTPS origin for its `dev` or `prod` realm.
- A live receipt whose fields differ from the receipt that was sent, or a body that is not `CutoverReceipt`.
- A local request file, a resume gate, a second-device proof, and a cached receipt. Neither command writes `cutover-request.json` or `cutover-record.json`.
- Retirement. No command deletes legacy data.
- Unknown JSON fields. A bearer token cannot be stored in the evidence files. Parse errors name the file and do not echo the body. A refused HTTP body is printed only when it is a `ProtocolError` (`code` and static `field`). Otherwise the status code is the only HTTP detail.

## Identity

`database_id` is the stable id from the owned-database registry, not the local name. Lane A's id is a lowercase UUID. This checker accepts the database-selector alphabet: 1 to 64 bytes, ASCII letters, digits, `_`, or `-`. A lowercase UUID fits. The tool does not open the registry. Every evidence file must carry the same id the operator named.

`database_kind` is `personal` or `shareable`.

`environment` is `dev` or `prod`. Every evidence file must carry the same realm the operator named. The realm isolates the service. It is not a second migration procedure.

Node keys, device ids, and adapter or service revisions use the selector alphabet. A git commit SHA fits. A dotted version does not.

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

`daemon-ready.json` is an input to the digest. It is Lane E's promise, not the service commit. It requires `accepts_cutover_record` true, `legacy_resume` of `refused_after_signed_only`, and `final_write_fence` of `honored`. Those values mean the adapter will honor a future Exemem commit. They do not mean the database is already signed-only. `migrating_device_id` is the device that is migrating. `adapter_revision` is a selector label.

`delivery-ready.json` is an input to the digest. It is Lane I's promise, not the service commit. It requires `legacy_bearer_closed` true, `outstanding_urls` of `drained_or_invalid`, and `grant_check` of `current_node_key`. `legacy_bearer_closed` means Lane I will close legacy bearer reads when Exemem commits. `grant_revision` equals `grants.json` `revision`, and that integer is at least `1`. `service_revision` is a selector label.

### Grants

`authority_node_key` is the personal owner or the shareable publisher. `revision` is at least `1`. Node keys are unique.

Personal: every member capability is `owner`. Exactly one member is `active`, and that member's node key is the authority. Other owner rows may be `revoked`. There is no `receive` or `submit` row.

Shareable: the publisher is `authority_node_key` and is not a member row. The log has no `publish` capability. Members are `receive` or `submit`, each `active` or `revoked`. An empty member list is valid: the publisher is the authority and nobody else is granted. An `owner` row is refused. Revoked rows stay on the member list.

The evidence capability words stay `owner`, `receive`, and `submit`. D's member-list wire uses `read` and `submit`, and H maps `read` to `receive`. This digest does not translate those words. The service transition authorizes with H's `ShareAccess`. It does not read this file as a second grant document.

This crate does not import the cloud backup protocol.

## Evidence digest

The digest is SHA-256, lowercase hex, of these lines. Each line ends with one newline. Timestamps are not inputs. The realm is not an input. `CutoverReceipt` carries `environment` beside this digest. Nothing signs the receipt digest. The node signature covers the exact receipt bytes. Member rows are sorted by node key, capability, then state. Writer tips are sorted by node key, device id, then sequence.

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

On success, text prints `verified`, the database id, the environment, `evidence_sha256`, `grant_revision`, and `activated=false`. JSON is `ok`, `database_id`, `environment`, `evidence_sha256`, `grant_revision`, and `activated`. Text does not print node keys. `grant_revision` in this JSON is the evidence integer. The service object uses D's decimal string for the same value.

Reason text is `code: detail`. The detail does not include evidence values. Codes: `unreadable`, `invalid_environment`, `environment`, `schema`, `kind`, `database`, `database_kind`, `invalid_database_id`, `snapshot`, `position`, `writer_tips`, `grants`, `daemon`, `delivery`.

## Service transition

The transition is one idempotent publisher request for one existing database. D #1500 (`1f7e4a058`) defines the body and the route. The client imports `CutoverReceipt`. It does not copy the type.

```
POST /api/cloud/backups/v2/mutation-log/cutover
```

The path is the same in `dev` and in `prod`. There is no second path, no `/log/` alias, and no environment flag. The action stays outside `is_mutation_log_path`. Create, cutover, and access are outside that set.

The body is canonical JSON of `CutoverReceipt`: `state` `committed`, `version` `2`, `environment` `dev` or `prod`, `database_id`, `evidence_sha256`, and `grant_revision`. `grant_revision` is a decimal string. Zero is `share.cutover.grant_revision`. `evidence_sha256` is this tool's digest. D does not recompute the evidence lines. Domain `lastdb:share:cutover-receipt:v1`. The digest array is `version`, `environment`, `database_id`, `evidence_sha256`, `grant_revision`. `state` is not in the array. Nothing signs that digest. There is no service-signature field.

`database_id` on the wire is D's `Id`: a 36-character lowercase UUID, not all zeros. The evidence checker still accepts the wider selector alphabet. `request` refuses an id that is not an `Id`.

The node signs the exact body bytes with `lastdb_node::node_read::authorization`. The account, database, realm, and route travel as `SignedPostTarget`. Cutover uses `CanonicalPost::Cutover`, whose path is `MUTATION_LOG_CUTOVER_PATH`. The same helper also covers create (`CanonicalPost::CreateDatabase`), access (`CanonicalPost::Access`), and the publisher member-list read (`CanonicalPost::MemberRead`). E and E4 call this helper for those posts. They do not build a second `NodeReadToken` header. Data routes stay on E's signer until that signer accepts this token. The member-list update does not use this header.

The helper builds `NodeReadToken`, including `database_id`, and signs the lowercase hex of `NodeReadToken::digest(body)`. The header is `LastDBNode`, then a space, then base64url of the token JSON, a dot, and base64url of the 64-byte signature. Each call uses a new nonce. The raw body, the clock, the signing key, and the grant revision stay arguments. The realm comes from the saved connection. Dev and prod use this same call.

The account scope comes from `byte_cloud_backup::client_from_home`. That function reads the saved recovery credentials and does not enroll backup or enable automatic backup. Its context read still sends `X-API-Key`. The cutover POST does not. It sends `Authorization` only. The signer must be the evidence authority: the personal owner or the shareable publisher node key already in `identity.key`.

A repeated call sends the same receipt bytes and a new nonce. `authorize_legacy_cutover` returns `AlreadyStored` when the stored receipt is equal, and `share.cutover.conflict` when it is not. The client accepts any live `CutoverReceipt` that validates and compares equal to the receipt it sent. It does not cache that body. A saved copy is not the service write.

`authorize_request` still returns `share.cutover.required` when no signed state is stored. The first store is `authorize_legacy_cutover` plus the conditional write. It is not a data-route call.

### What is not on the service yet

The client sends the canonical request anyway. Three current trees do not complete the first store:

- E's `lastdb_node::log_sync::sign::authorization` is the data-route header. Its token has no `database_id` field, and its digest omits `database_id` and `environment`. H treats a token without `database_id` as a legacy proof. Create, cutover, and access call `node_read::authorization` with `SignedPostTarget`. E4's member-list read uses `CanonicalPost::MemberRead` on that same helper. E's data-route signer stays until G accepts `NodeReadToken`.
- G's mutation-log match does not include `cutover`. A POST to the canonical path is `http.route` / not found. `commit_cutover_receipt` is present and unused by that match. G's `TokenBody` is `deny_unknown_fields` and has no `database_id`, so it rejects this header on the data routes.
- I dispatches `mutation-log/delivery/signed-only`, not `mutation-log/cutover`. `signed-only` parses the receipt fields. `Open` returns `mutation_log.cutover` integrity mismatch and does not call `authorize_legacy_cutover` or `commit_cutover_receipt`. The first store cannot succeed there. An equal stored receipt is echoed. `Native` is `share.cutover.native`.

No second path and no invented signature fill those gaps.

## Operator order

1. A fresh named database stops at `lastdb db create`. Do not build these six files.
2. For an existing database, Lane E fences legacy appends, drains the final writes, and takes a fresh full snapshot through that exact position. It writes the four snapshot, position, fence, and daemon files. The daemon file is a digest input and a promise. It is not the switch.
3. Lane I writes `grants.json` and `delivery-ready.json` from the current grant view. The delivery file is a digest input and a promise. It is not the close.
4. The operator may run `verify`. That check only reports the digest. `request` runs it again before the POST.
5. The publisher runs `request` from a non-primary home that already has the cloud connection and `identity.key`. The live matching receipt is the commit. This session does not run that command.
6. Lane E starts signed mutation-log sync for that database only after that receipt is stored. Data routes keep E's current signer until G accepts `NodeReadToken`.
7. Legacy rows stay. Retirement is a later step. This command does not authorize it.

## Lane boundary

Lane C's `lastdb db create` stays the fresh-database operation. `Command::Cutover` stays off the `--db` allow-list. Cutover does not create a database.

Lane D owns `CutoverReceipt`, `NodeReadToken`, and `MUTATION_LOG_CUTOVER_PATH`. `database_cutover` does not import that crate. The send lives in `lastdb_node`.

The client is ready for a review PR. Merge waits until Exemem's canonical cutover accepts the first valid receipt, and until `benchmark-complete` and the other required gates are green. Nothing here activates an unmigrated database, changes the primary, or starts a production migration.
