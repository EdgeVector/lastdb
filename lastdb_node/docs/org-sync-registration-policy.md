# Org cloud sync registration policy

The owner route `POST /api/org/sync/register` is closed by default on every
LastDB home. It cannot add a target or claim a cloud head until that home has an
explicit policy file. This protects the primary home while org sync is proven
on DEV. The personal backup still uses `cloud_sync.json` and is independent of
this policy.

To enable registration on a DEV home, write
`<DEV-home>/org_sync_registration.json` with this exact JSON shape:

```json
{"allow_registration":true}
```

The node reads the policy for each registration request. A missing file,
`false`, an unreadable file, or invalid JSON denies the request. To stop new
registrations, set `allow_registration` to `false` or remove the file. A policy
change does not alter existing target rows or stop their sync. Review those
rows separately with `GET /api/org/sync/targets` before any deactivation.

This file controls registration only. It does not enable personal cloud sync,
org sync for existing rows, or cloud credentials. A successful registration
still needs an org locator, a valid org key, and a cloud owner claim.
