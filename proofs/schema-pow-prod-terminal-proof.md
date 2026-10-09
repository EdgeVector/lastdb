BLOCKED

# Schema PoW production terminal proof

The terminal proof has not run and this file intentionally does not claim
production PASS. The remaining prerequisite is the human-executed guarded
production cutover tracked by `schema-pow-prod-enforcement` in
`EdgeVector/schema-infra`. Tom approved that cutover conditionally on 2026-08-08,
but explicitly kept execution human-owned.

After that cutover records a redacted deployment attestation, run:

```sh
schema_service/scripts/schema_pow_prod_terminal_proof.sh \
  --allow-prod \
  --attestation /path/to/redacted-schema-pow-prod-attestation.json
```

The command resolves the production endpoint from
`folddb_profile/environments.json`, uses the real production client with an
ephemeral signing identity, verifies challenge/grind/signed retry/idempotent
repost plus missing/invalid/expired rejection, and refuses to replace this file
with `PASS` unless the redacted attestation also proves enforcement, production
scope, quota/alarm evidence, canary promotion, and rollback readiness.

No secret locator is currently required by the Fold-side probe. Any credentials
needed to produce the deployment attestation remain owned by the supervised
`schema-infra` cutover and must be resolved through LastSecrets only at point of
use.
