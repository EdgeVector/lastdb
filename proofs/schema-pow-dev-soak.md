# Schema PoW dev soak proof

Status: PASS
Environment: dev
Service URL: https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com
Fold revision under test: 242c7570331e5b31b22df99da8aa03f0cce16693
Evidence run: 2026-08-04T00-33-04-635Z
Validated at: 2026-08-04T00:43:10Z
Investigation: `hga-invest-schema-pow-prod-enforcement`
Evidence: `~/.routines/runs/last-stack-fkanban-validate/2026-08-04T00-33-04-635Z/schema-pow-hga-dev-proof.jsonl`

## Decision

The dev Schema registration Proof-of-Work soak passed. The bounded investigation
captured successful valid registration and stable repost identity, plus the
expected rejection reasons for missing and invalid proof.

## Proven behavior

- Valid registration was accepted.
- Reposting the same schema preserved its identity.
- A registration without proof was rejected as `node_key_required`.
- A registration with invalid proof was rejected as `proof_of_work_invalid`.
- Every captured record reports `private_key_persisted: false`.

## Scope

This proof records the already-completed dev investigation. The investigation
did not deploy or mutate production, and this artifact does not authorize or
perform the separate production enforcement step.
