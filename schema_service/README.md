# schema_service

Canonical schema service for FoldDB's global schema catalog - the control plane
for direct schema resolution, intentionally published contracts, cross-app
discovery, deduplication, semantic similarity matching, view registration, and
the global transform registry. App and agent writes resolve to catalog
identities before durable storage; local field names are bridged by edge
adapters, not stored as a second on-disk language.

Canonical field names use full concept and property names. New fields cannot
use the audited app abbreviations such as `bge_*` and `rle_*`. The direct
checks are in `crates/core/src/field_name_policy.rs`.

**Distribution:** shared apps add publish/attach metadata and distribution
verification on top of catalog identities.

## Status

✅ **Extraction complete** (2026-04-21). All phases of
`projects/extract-schema-service-repo` have landed. The registry brain,
HTTP handlers, Lambda handler, typed client, and OpenAPI spec all live
in this repo; `fold_db` is back to being a pure DB library. Deployed to
`schema.folddb.com` via the
[EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra)
submodule consumer.

## Cloud-only constraint

This service is **not** a user-installable binary. Production = the
schema-infra Lambda → `schema.folddb.com`. The actix binary in
`crates/server_http/` is for local development only. No brew formula,
no GitHub release tarballs, no `cargo install` end-user path.

## Workspace layout

| Crate                                | Purpose                                                                                                                              |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `crates/core`                        | Registry brain — schema/view/transform state, similarity, canonical-field seeding. ~7400 LOC moved from `fold_db` in Phase 2.        |
| `crates/server_shared`               | Framework-agnostic HTTP handlers. Shared verbatim by the actix binary and the Lambda.                                                |
| `crates/server_http`                 | Actix wrapper + `schema_service` dev binary. Mounts the shared handlers under `/v1/*`. Dev-only.                                     |
| `crates/server_lambda`               | AWS Lambda handler — wraps the shared handlers for API Gateway invocation. Built by `schema-infra/build.sh`.                         |
| `crates/client`                      | Typed HTTP client (`schema_service_client`). Consumed by `fold_db_node`.                                                             |

The `core` crate has **no** `fastembed`/`ONNX` dependencies — callers
inject an `Arc<dyn Embedder>` into `SchemaServiceState::new_in_memory()` or
`SchemaServiceState::new_with_external()`, so a
Lambda deployment can swap embedder implementations without touching core.
Default local/dev binaries inject a disabled embedder. Exact name matching
works without initializing or downloading a model. Real FastEmbed is an
explicit build feature for semantic matching.

## Run locally

From the monorepo root, run the dev binary. The registry lives only in
process memory: a restart starts again from the built-in seeds.

```bash
cargo run -p schema_service_server_http --bin schema_service \
  -- --port 9102
```

To start with production-like data, pass `--hydrate-from <url>` with
`EXEMEM_API_KEY` set. The binary imports that snapshot into memory at
startup.

Enable the local FastEmbed adapter only when you need to validate the
live semantic path:

```bash
cargo run -p schema_service_server_http --features fastembed --bin schema_service \
  -- --port 9102
```

Smoke check:

```bash
curl -s http://127.0.0.1:9102/v1/health
# {"status":"healthy"}
```

## Operations

Mini calls the live schema service. It does not load an offline resolver pack.

The shared-only snapshot stays available for operators as
`GET /v1/snapshot/shared-only` on the production schema service URL from
`folddb_profile/environments.json` (`environments.prod.schema_service`).
The route is gated by `X-API-Key`. The key locator is
`lastsecrets://schema-service-prod-api-key`. Provision it with
`lastsecrets put schema-service-prod-api-key --value-stdin`.
Read the key from LastSecrets at point of use. Do not print it, commit it,
or pass it as a CLI argument.

## Canonicalization — dual-signal flag

`add_schema` consults two signals when deciding whether a proposal
merges into an existing canonical:

- **Structural** — descriptive_name + schema_name embedding similarity
  (τ_struct = 0.80 in
  `state_matching::DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD`).
- **Purpose** — embedding of `"{descriptive_name} — {purpose_statement}"`,
  thresholded at τ_purpose = 0.88 in
  `state_matching::PURPOSE_SIMILARITY_THRESHOLD`. Tighter because purpose
  statements are short and high-signal.

A proposal merges only when BOTH signals agree above their thresholds;
otherwise it registers as a new canonical. **Default on** as of the
Phase E cutover (PR #303 / #306 / #309 / #311 / this PR). Gated by env
var:

| `SCHEMA_DUAL_SIGNAL_CANONICALIZATION` | Behavior |
| ------------------------------------- | -------- |
| unset (default), `1`, `true`, `TRUE`, `yes`, or anything else | Dual-signal: structural AND purpose must agree. |
| `0` / `false` / `FALSE` / `no`         | Single-signal: legacy structural-only canonicalization. Operator escape hatch for rollback. |

### Rollback

The env-var escape hatch lets operators pin back to single-signal
without a code change:

**Dev (local actix binary).** Export the var before starting the dev
binary:
```bash
export SCHEMA_DUAL_SIGNAL_CANONICALIZATION=false
cargo run -p schema_service_server_http --bin schema_service -- \
  --port 9102
```

**Prod (Lambda).** Set `SCHEMA_DUAL_SIGNAL_CANONICALIZATION=false` in
the Lambda function's environment and redeploy. The CDK that owns the
function lives in [EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra)
— this PR cannot touch it from here. See schema-infra's `README.md`
for the env-var stanza.

To re-enable dual-signal: remove the env var (or set it to anything
that isn't an off value) and redeploy.

See fbrain `dual-signal-schema-canonicalization` for the design.

## HTTP surface (`/v1/*`)

| Method | Path                                | Description                             |
| ------ | ----------------------------------- | --------------------------------------- |
| GET    | `/v1/health`                        | Liveness probe                          |
| GET    | `/v1/schemas`                       | List schema names                       |
| POST   | `/v1/schemas`                       | Add a schema (with mutation mappers; accepts an optional `purpose_statement`) |
| POST   | `/v1/schemas/batch-check-reuse`     | Batch reuse-check for proposed schemas  |
| POST   | `/v1/schemas/reload`                | Reload schemas from storage             |
| GET    | `/v1/schemas/available`             | Full schema definitions                 |
| GET    | `/v1/schemas/similar/{name}`        | Find similar schemas (Jaccard)          |
| GET    | `/v1/schema/{name}`                 | Get one schema                          |
| GET    | `/v1/views`                         | List view names                         |
| POST   | `/v1/views`                         | Register a view                         |
| GET    | `/v1/views/available`               | Full view definitions                   |
| GET    | `/v1/view/{name}`                   | Get one view                            |
| POST   | `/v1/system/reset`                  | Clear schemas on the in-memory dev server |

## OpenAPI spec

A machine-readable OpenAPI 3.0 spec lives at repo root as
[`openapi.yaml`](./openapi.yaml). It documents every `/v1/*` endpoint
served by both the dev binary and the production Lambda — they mount
the same handler set, so the wire contract is identical.

Third-party integrators can generate a client directly from the spec:

```bash
# Rust
openapi-generator-cli generate -i openapi.yaml -g rust -o ./out/client-rust

# TypeScript (fetch)
openapi-generator-cli generate -i openapi.yaml -g typescript-fetch -o ./out/client-ts

# Python
openapi-generator-cli generate -i openapi.yaml -g python -o ./out/client-py
```

Or against the live service:

```bash
curl -fsSL https://raw.githubusercontent.com/EdgeVector/fold/main/schema_service/openapi.yaml \
  | openapi-generator-cli generate -i /dev/stdin -g rust -o ./out/client-rust
```

A reference Rust client also ships as a workspace crate at
[`crates/client/`](./crates/client) (published as
`schema_service_client`). Use it when you want retry semantics and
strongly-typed FoldDB wire structs without running codegen.

For an operator-safe end-to-end check of mutation proof-of-work, run the
client's live proof harness against the intended dev/test environment:

```bash
schema_service/scripts/schema_pow_dev_proof_harness.sh \
  --url "$(jq -r '.environments.dev.schema_service' folddb_profile/environments.json)" \
  --evidence schema_service/target/schema-pow-dev-proof.jsonl \
  --report schema_service/target/schema-pow-dev-proof.report.json
```

The harness creates ephemeral node identities, exercises the real client's
challenge/solve/repost path, proves missing and invalid PoW submissions are
rejected, and emits a JSONL evidence stream plus a redacted report that records
explicit `PASS` results for challenge retrieval, PoW grinding, signed retry,
and identity-preserving idempotent repost. The harness exits nonzero if any
required stage or negative control is absent. The retired
`https://schema-dev.folddb.com` alias is resolved through
`folddb_profile/environments.json` for compatibility with older proof commands.
Production is guarded in the underlying Rust probe by `--allow-prod` and uses a
fixed schema identity so reruns remain idempotent.

After the human-owned guarded production cutover, run the fail-closed terminal
proof harness with its redacted deployment attestation:

```bash
schema_service/scripts/schema_pow_prod_terminal_proof.sh \
  --allow-prod \
  --attestation /path/to/redacted-schema-pow-prod-attestation.json
```

The harness targets only the canonical production URL from
`folddb_profile/environments.json`. It runs the real client positive path and
missing, invalid, and expired negative controls, then writes a `PASS` report
to `schema_service/target/` (outside Git) only when the attestation also
proves enforcement, production scope, quota/alarm evidence, canary promotion,
and rollback readiness.

The canary promotion gate consumes those evidence files without copying their
payloads into its output:

```bash
schema_service/scripts/schema_pow_canary_alarm_gate.sh \
  --stage canary \
  --evidence schema_service/target/schema-pow-dev-proof-01.jsonl \
  --evidence schema_service/target/schema-pow-dev-proof-02.jsonl \
  > schema_service/target/schema-pow-canary-decision.json
```

The executable policy lives in
[`config/schema_pow_canary_alarm_gate.json`](./config/schema_pow_canary_alarm_gate.json).
Promotion requires 24 completed hourly runs with all three required probes and
zero active alarms. One challenge, grind, retry, missing-probe, evidence-safety,
or unclassified failure reaches its named alarm threshold and produces
`decision: "hold"`. After promotion, evaluate the same evidence with
`--stage promoted`; any failed criterion produces `decision: "rollback"` and
`rollback_signal: true`. Repeat `--evidence` once per hourly artifact in the
24-run window. The generated decision contains only counts, stable
reason categories, alarm names, and evidence basenames. Credential fields,
headers, request/response bodies, raw payloads, and persisted private-key
markers activate the evidence-safety alarm and their values are never emitted.

### Spec / handler sync

When you add, remove, or rename a `/v1/*` endpoint, update **both**
`configure_routes` and `openapi.yaml` in the same commit. No automated
drift check exists; the lint below checks only the spec itself.

### Lint the spec

```bash
npx -y @redocly/cli@latest lint openapi.yaml
```

Remaining warnings (e.g. missing `4XX` responses on read-only or verify
endpoints) are intentional: those handlers can only fail at the `5XX`
transport layer, which `#/components/responses/InternalError` already
covers.

## Pre-PR checklist

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --all-targets
```
