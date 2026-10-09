# Cloud storage database registration (`db_hash`)

Cloud object keys root at `{db_hash}/…`, where `db_hash =
sha256("laststore-db:" + store_uuid)`. `user_hash` / `org_hash` remain the
identity and billing principals; they are no longer blob roots.

The storage service authorizes every `db_hash`-scoped request against a
principal → database registry (DynamoDB, `DB_REGISTRY_TABLE`). A home whose
database has never been claimed is refused:

```
HTTP 403 {"ok":false,"error":"principal is not registered for this db_hash",
          "code":"FORBIDDEN","statusCode":403}
```

## What the node does about it

Nothing else clears that 403. The credential is valid, so the 401
refresh-and-retry path is not involved, and every later sync cycle fails
identically — which is why `lastdb cloud on` used to appear to succeed and then
never sync.

`AuthClient::post` now recovers: the first scoped request that comes back
unregistered triggers one `register_db` owner claim for the client's own
`db_hash`, then retries the original request. Properties worth knowing when
reading logs:

- **Idempotent.** An already-registered principal gets its existing membership
  back and the retry proceeds.
- **Never steals a claim.** If another principal already owns the database, the
  service refuses and the original 403 surfaces.
- **At most one claim per process.** The outcome — granted *or* refused — is
  remembered, so a genuinely foreign database produces one `register_db` call,
  not one per sync cycle.
- **Only for this client's own scope.** A request that explicitly addressed a
  different database is left alone; claiming ours would be a silent scope
  change, not a recovery.

Success logs at INFO (`claimed cloud storage database root for this
principal`); a refusal logs at WARN and names the service's reason.

## Operator fallback

The automatic claim covers first-enable. Two cases still need hands:

**The database is owned by another principal.** Expected when a store was
restored or copied between accounts. The claim cannot resolve this by design —
sort out which account should own the root, or roll the store UUID so the home
derives a fresh `db_hash`.

**A client too old to self-claim** (pre-recovery build, or a scripted
integration driving the API directly). Register by hand against the same
endpoint the node uses:

```bash
curl -sS -X POST "$EXEMEM_STORAGE_URL/api/sync/presign" \
  -H "X-API-Key: $EXEMEM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"action":"register_db","db_hash":"<64 hex>"}'
```

A successful response echoes `db_hash`, `principal_hash`, and `role: "owner"`.
Derive `db_hash` from the store UUID with the formula above — it is also the
prefix already visible in the node's cloud object keys.

## Cross-service coupling

The recovery branches on the 403's **message text**, pinned as
`DB_HASH_NOT_REGISTERED` in
`fold_db/crates/core/src/sync/auth/ops/register_db.rs`. It is the message and
not `code` because every denial in the storage service shares the generic
`FORBIDDEN` code and `ApiError` bodies carry no machine-readable `reason`.
`exemem_service/lambdas/storage_service/src/db_registry.rs` carries a canary
test on the literal, so changing it fails there rather than silently disarming
the recovery on every client already in the field.
