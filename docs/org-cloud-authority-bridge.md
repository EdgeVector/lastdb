# Org cloud authority through the owner socket

The Org app signs the complete epoch chain with the Org root. LastDB sends those exact bytes through the signed cloud account.
The cloud service checks the current account member role before it accepts an update.
Before publication, LastDB waits for accepted local work and write lanes, then flushes the local store.
The signed local successor therefore reaches disk before the cloud authority advances.
The bridge does not start cloud sync or change a paused cloud configuration to active.

## Status

Send `POST /api/org/authority/status` on the owner socket:

```json
{"account":{"kind":"org","id":"<SHA256 of the root SPKI>"}}
```

The result contains `ok`, `configured`, `account`, `absent`, `epoch_hash`, `epoch_no`, and `limits`.
Only the absence of both cloud configuration files returns `configured:false`.
A configured home reads the actual cloud tip. A credential, role, or network error remains an error.
The flat response includes the standard top-level `user_hash` envelope field.
For `configured:true`, `user_hash` is the exact fresh authenticated node principal used for the cloud request.

## Publish

Send `POST /api/org/authority/publish` on the owner socket:

```json
{
  "account":{"kind":"org","id":"<SHA256 of the root SPKI>"},
  "expected_epoch_hash":"<the current cloud tip hash>",
  "org_authority":{
    "org_root_spki_base64":"<Ed25519 root SPKI>",
    "epochs":[{"payload_jcs":"<exact canonical JSON>","signature_base64url":"<root signature>"}],
    "owner_enrollment":{"payload_jcs":"<exact original-owner proof>","signature_base64url":"<member signature>"}
  }
}
```

The optional account must match the root. The chain starts at epoch zero and includes every epoch through the tip.
The body cap is 1 MiB. The chain cap is 1,000 epochs.
The exact stored terminal JSON cap is 350 KiB.
Its fields are `account`, `epoch_no`, `epoch_hash`, `org_root_spki_base64`, and the parsed terminal `epoch`.
Status returns these fixed caps as `body_bytes`, `epochs`, and `authority_json_bytes` under `limits`.
Unknown request fields cause an error. `sig` is an accepted input alias for `signature_base64url`.
The bridge verifies each root signature, canonical payload, account, epoch number, and predecessor hash.
The cloud service verifies the current member role and the optional original-owner proof.

Omit `expected_epoch_hash` to use the exact cloud tip that the bridge reads.
Use `null` to require first enrollment. Use a hash to require that exact current tip.
A changed tip causes a conflict. The bridge does not choose a different expected tip for an update.
An exact terminal replay can retain the previous expected hash after a lost reply.
The cloud service still checks current membership for that replay.
If tracked Org work blocks the exact cloud update, the bridge returns status 503 and code `cloud_org_authority_busy_or_changed`.
Read the cloud tip and retry the same signed chain after that work ends.

The success result contains only `ok`, `account`, `epoch_hash`, and `epoch_no`, plus the standard envelope field.
No credential or private key enters the request or result.
The app socket cannot reach either route. A selected source write pause also holds these routes.

## Cloud Off

The bridge uses the existing source-only engine over the open local store.
It refreshes the session through the signed node identity and keeps the paused configuration paused.
The engine endpoint must match the current configuration before the new credential enters the cache.
It does not attach a normal sync engine, register an ordinary data writer, or start a cloud worker.
The existing owner-only member grant and revoke routes use the same signed source-only path while Cloud Off stays off.
Those storage mutations keep the normal generation, hold, adoption, and current cloud role checks.
After an Org purge, select the actual configured Org home and owner socket for member grant or revoke.
That home must hold its valid Org adoption receipt. A foreign Org cannot replace a personal node through these commands.
The actual owned account proof follows the compatible cloud deployment.
