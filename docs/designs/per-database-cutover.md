# Per-database cutover

One product operation moves an existing database from legacy bearer reads to signed node-key reads. The operation is the same in `dev` and in `prod`. Exemem commits that change. A verified service response is the only authority. This tool does not perform the commit, does not call Exemem, and does not deploy.

The local command is read-only preparation:

```
lastdb cutover verify --database-id ID --environment dev|prod --evidence DIR [--json]
```

`database_cutover` reads no clock, opens no file, and does not contact the cloud. The shell passes the six evidence files. `verify` returns the evidence digest those files bind. It writes nothing. Success prints `activated=false`. That digest is not a signed-only commit.

A fresh named database does not use this command. The normal signed create registers it and places it in the authorized signed state, with its spec and its initial grants. It does not need these evidence files, a legacy position, a legacy token, or a separate environment procedure. E, D, G, H, and I own that bootstrap. K dogfoods it through the normal client.

Approved source: Brain design `lastdb-named-databases-unified-log-sharing-revocation` (2026-11-11). Migrate one existing database at a time. Verify the node-key grants, then switch every read path for that database to signed-only together. That switch is Exemem's commit. Legacy bearer passes stop authorizing reads for that database when the service commits, including on a device that still holds stale local state. Old server rows stay until a later retirement step. This command does not check that step and does not delete rows.

## What this tool refuses

- An `environment` other than `dev` or `prod`. Both realms use these same checks. A file whose environment is not the realm the operator named is refused.
- A path whose canonical location is inside the primary LastDB home. The shell treats `$HOME/.lastdb` and `$LASTDB_HOME` as that home, and canonicalizes a home that exists.
- `--data-dir`. The command does not open a data directory.
- `--db`. Cutover is not on the selector allow-list.
- The daemon socket, the cloud, and Exemem.
- A local request file, a resume gate, and a second-device proof. Those flows do not decide access. This command does not write `cutover-request.json` or `cutover-record.json`.
- Retirement. No command deletes legacy data.
- Unknown JSON fields. A bearer token cannot be stored in these files. Parse errors name the file and do not echo the body.

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

The digest is SHA-256, lowercase hex, of these lines. Each line ends with one newline. Timestamps are not inputs. The realm is not an input: D's receipt signs `environment` beside this digest. Member rows are sorted by node key, capability, then state. Writer tips are sorted by node key, device id, then sequence.

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

The transition is one idempotent, publisher-authorized Exemem change for one existing database. It runs only after the grants are valid and the direct URL is drained. One durable service state then closes every legacy read route for that database. J does not execute it.

D's local commit `953e4cc4e` defines the response and does not define the HTTP action that returns it. The response object is `CutoverReceipt` and only that object. J does not copy the type. Domain `lastdb:share:cutover-receipt:v1`. The signed array is `version` (JSON integer 2), `environment`, `database_id`, `evidence_sha256`, `grant_revision`. The signature is not in the array. The signer is the service key, not the publisher key and not the node key. A zero `grant_revision` is `share.cutover.grant_revision`. `evidence_sha256` is this tool's digest. D does not recompute the evidence lines.

`is_mutation_log_path` is only the seven data paths: append, head, list, ack, snapshot, snapshot bytes, and trim. `authorize_request` returns `share.cutover.required` when no receipt is stored. The transition cannot be one of those seven paths. A call on them would require the receipt it is trying to obtain.

G's dispatch arms today are `create-database`, `append`, `head`, `list`, `ack`, `revoke`, `snapshot`, and `trim`. None of them is this transition. `create-database` is the fresh-database bootstrap. H's published routes are the member-list update and the member-list read. I's delivery routes are for after a receipt exists. E's `/api/cloud/backups/v2/log/` draft is not this family.

### Proposed action

D, G, H, and I accept one new POST before any client calls it. J does not hard-code the path and does not send it.

```
POST /api/cloud/backups/v2/mutation-log/cutover
```

The lambda path omits `/api`, matching the other actions. The path is the same in `dev` and in `prod`. There is no `/log/` alias, no environment prefix, and no feature flag. The action stays outside `is_mutation_log_path`. D adds the constant beside the other route constants when this path is accepted. Until that constant exists, E does not call a cutover URL.

The request binds the four values the receipt will sign, other than version and the service signature: `database_id`, `environment`, `evidence_sha256`, and `grant_revision`. `grant_revision` is D's decimal string, the same integer `verify` printed. A retry with those same four values returns the stored receipt and does not mint a second state. A different digest or revision for a database that already has a receipt is refused and does not replace the stored receipt.

Authorization uses paths that already exist:

- Shareable: the publisher account key that already signs `MemberListUpdate`. That key is not a node key and not a new credential. The member-list update is the existing publisher-authorized idempotent mutation: publisher signature, no `LastDBNode` nonce, same signed bytes ack again.
- Personal: the node-read verifier G and H already use for personal logs. Personal evidence has no member list. The one active owner node key is `authority_node_key`.
- The cloud client still presents `X-API-Key` to the configured Exemem base URL. That header is the existing account credential. It is not a second cutover login.

The success body is `CutoverReceipt`. H verifies `signature` over `CutoverReceipt::digest` with the service key before the receipt is stored or passed to `authorize_request`. E and J consume that authenticated response. They do not switch because a local file exists, is missing, or is unreadable.

This tree's cloud client trusts HTTPS and `X-API-Key`. It does not pin a service Ed25519 key, and share-invitation checks use the publisher key. J therefore does not cache a receipt and does not treat a written file, or a TLS response, as activation. No new key-distribution system is added here. When the response path H already uses names that public key, a later change may cache a receipt only after the signature check succeeds and `database_id`, `environment`, `evidence_sha256`, and `grant_revision` match this check.

## Operator order

1. A fresh named database stops here. Create and sync it with the normal client. Do not build these six files.
2. For an existing database, Lane E fences legacy appends, drains the final writes, and takes a fresh full snapshot through that exact position. It writes `snapshot.json`, `legacy-position.json`, `final-writes.json`, and `daemon-ready.json`. The daemon file is a digest input and a promise. It is not the switch.
3. Lane I copies the current node-key grants and writes `grants.json` and `delivery-ready.json`. The delivery file is a digest input and a promise. It is not the close.
4. The operator runs `verify` for that database and realm. The command writes nothing.
5. After D, G, H, and I accept the action above, the publisher requests that one transition. The verified `CutoverReceipt` is the commit. Until the action exists, nobody sends it and nobody treats a local file as the commit.
6. Lane E starts signed mutation-log sync for that database only after that verified receipt. Before it, E does not send `NodeReadProof::digest` on the mutation-log paths and does not hard-code the realm as `dev`.
7. Legacy rows stay. Retirement is a later step. This command does not authorize it.

## Lane boundary

Lane C adds create, list, and select on the same CLI enum. The `Cutover` variant has to stay. This command does not create a database and does not select one with `--db`.

Lane D owns `CutoverReceipt` and the mutation-log route family. This crate does not import that crate and does not vendor the receipt. Positions here are canonical decimal strings. The route family is `/api/cloud/backups/v2/mutation-log/`. This tool does not call it.

Lane A merges first. This change does not publish before the lead coordinates, and it does not merge before `benchmark-complete` and the other required gates are green. Nothing here activates an unmigrated database, changes the primary, or starts a production migration.
