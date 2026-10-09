# schema_service

Canonical schema service for FoldDB's global schema catalog - the control plane
for direct schema resolution, intentionally published contracts, cross-app
discovery, deduplication, semantic similarity matching, view registration, and
the global transform registry. App and agent writes resolve to catalog
identities before durable storage; local field names are bridged by edge
adapters, not stored as a second on-disk language.

See [direct_schema_registration.md](docs/direct_schema_registration.md) for the
one-language storage model, and
[shared_surface_schema_service.md](docs/shared_surface_schema_service.md) for
publish/attach governance.

**Distribution:** shared apps add publish/attach metadata and distribution
verification on top of catalog identities - see
[schema_registration_distribution_gate.md](docs/schema_registration_distribution_gate.md).

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
inject an `Arc<dyn Embedder>` into `SchemaServiceState::new()`, so a
Lambda deployment can swap embedder implementations without touching core.
Default local/dev binaries inject a disabled embedder: exact matching,
resolver packs, imported embedding artifacts, and test-injected embedders
work without initializing or downloading a model. Real FastEmbed is an
explicit build feature for semantic/full-product validation.

## Run locally

From the monorepo root, run the dev binary against a local Sled registry:

```bash
cargo run -p schema_service_server_http --bin schema_service \
  -- --port 9102 --db-path schema_registry
```

Enable the local FastEmbed adapter only when you need to validate the
live semantic path:

```bash
cargo run -p schema_service_server_http --features fastembed --bin schema_service \
  -- --port 9102 --db-path schema_registry
```

Smoke check:

```bash
curl -s http://127.0.0.1:9102/v1/health
# {"status":"healthy"}
```

## Operations — backfill `purpose_statement`

Phase A of the dual-signal schema canonicalization (fbrain
`dual-signal-schema-canonicalization`) added a `purpose_statement`
field to the canonical schema definition and wired `POST /v1/schemas`
to default it to `descriptive_name` for every new registration.
Schemas registered **before** Phase A landed still carry
`purpose_statement = None` in dev's Sled registry and prod's S3
`schemas.json` blob. Run the Phase D backfill binary once per
environment to fill them in.

The binary is idempotent — a second run reports `updated = 0`. It
supports `--dry-run` (no writes) and an optional `--mapping-file`
JSON with curated per-`descriptive_name` purpose statements;
unmatched records fall back to `descriptive_name` itself, matching
the Phase A default.

**Dev (local Sled).** Always dry-run first:

```bash
cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
  --backend sled --db-path ~/.folddb/schema_registry --dry-run

cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
  --backend sled --db-path ~/.folddb/schema_registry
```

**Prod/R2 (S3-compatible).** Export schema-store-specific credentials and
region first. Do not set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to R2
values in Lambda: those names are reserved for the normal AWS SDK credential
chain used by DynamoDB, KMS, Secrets Manager, CloudWatch, and other AWS
clients. For Cloudflare R2, set `SCHEMA_STORE_REGION=auto` and point
`SCHEMA_STORE_ENDPOINT_URL` (or the legacy alias `SCHEMA_STORE_R2_ENDPOINT`) at
the account endpoint; when the endpoint is set the client uses path-style bucket
addressing. `SCHEMA_STORE_BUCKET`, `SCHEMA_EMBEDDINGS_TABLE`, and
`SCHEMA_STORE_ENDPOINT_URL` can be set via env or CLI flag:

```bash
export SCHEMA_STORE_BUCKET=<schema-registry-r2-bucket>
export SCHEMA_EMBEDDINGS_TABLE=<prod-embeddings-table>
export SCHEMA_STORE_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
export SCHEMA_STORE_ACCESS_KEY_ID=<r2-access-key-id>
export SCHEMA_STORE_SECRET_ACCESS_KEY=<r2-secret-access-key>
export SCHEMA_STORE_REGION=auto

cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
  --backend s3 --dry-run

cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
  --backend s3
```

Rollback: redeploy the Lambda without `SCHEMA_STORE_ENDPOINT_URL` /
`SCHEMA_STORE_R2_ENDPOINT` and without the `SCHEMA_STORE_ACCESS_KEY_ID` /
`SCHEMA_STORE_SECRET_ACCESS_KEY` pair, then restore the previous S3
`SCHEMA_STORE_BUCKET`; the persisted blob layout is unchanged (`schemas.json`,
`canonical_fields.json`, `apps.json`, `near_misses.json`), so the cutover is a
storage endpoint swap rather than a schema migration.

**Custom curated mapping.** Override the built-in fbrain six-kinds
placeholders (`Concept` / `Preference` / `Reference` / `Agent` /
`Project` / `Spike`) with environment-specific copy:

```bash
cat > mapping.json <<'EOF'
{
  "Photo Collection": "Photos a user has taken or saved.",
  "Recipe Collection": "Recipes the user wants to cook."
}
EOF
cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
  --backend sled --db-path ~/.folddb/schema_registry \
  --mapping-file mapping.json --dry-run
```

## Operations — publish Schema Resolver Packs

> Packs are **format_version 2** (signed `resolver_config.json` + snapshot +
> embeddings; no WASM). Wire snapshot exports strip embeddings — use
> `build --recompute-embeddings` (feature `recompute-embeddings`) or feed a
> snapshot that already has embeddings. Local dogfood:
> `publish --local-out-dir ./serve --dry-run` then `python3 -m http.server`.
> Mini install config: [docs/mini_schema_resolver_config.md](docs/mini_schema_resolver_config.md).

`schema_resolver_pack_publish` is the local-machine release publisher for
Schema Resolver Packs. It runs on Tom's provisioned machine, not Forgejo CI,
so the Ed25519 signing key and R2 write credentials stay local. It signs the
manifest with purpose `schema_resolver_pack`, uploads content-addressed
artifacts first, then promotes the latest manifest pointer.

Secret locators are references, not raw values. Supported signing-key
locators:

- `lastsecrets://schema-resolver-pack-signing-dev`
- `keychain://<service>/<account>`
- `envelope-file:/path/to/ciphertext-envelope.json`
- `file:/path/to/seed.b64` for local tests
- `env:VAR_NAME` for local tests only

Print the public trusted-key tuple for a signing locator (safe to paste into
docs or install config; it does not print the private seed):

```bash
cargo run -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  trusted-key \
  --signing-key-locator lastsecrets://schema-resolver-pack-signing-dev
```

```bash
TRUSTED_KEY="$(cargo run -q -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  trusted-key \
  --signing-key-locator lastsecrets://schema-resolver-pack-signing-dev)"
```

Brain-backed storage must contain only ciphertext or `lastsecrets://`
references. Raw private keys, PEM blocks, Ed25519 seeds, R2 tokens, and
decryption roots must not be stored in Brain, Git, Forgejo, R2, shell args,
or logs. For `envelope-file:...`, the JSON envelope stores AES-GCM ciphertext
plus a macOS Keychain service/account for the decrypt root; a new machine
must provision that Keychain root once before it can sign.

The shared-surface native-resolver capstone uses the production shared-only
snapshot endpoint from `folddb_profile/environments.json`
(`environments.prod.schema_service` plus `/v1/snapshot/shared-only`), which is
gated by `X-API-Key`. Scheduled agents must use the documented locator
`lastsecrets://schema-service-prod-api-key`; provision it with
`lastsecrets put schema-service-prod-api-key --value-stdin`, then run:

```bash
schema_service/scripts/capstone-shared-surface-native-resolver/fetch_shared_only_snapshot.sh \
  --out /tmp/schema-service-shared-only.json
```

The helper resolves the key at point of use with `lastsecrets get`, feeds the
header to `curl` through stdin config, and prints only the locator and sanitized
HTTP status. Raw API-key material must not be committed, stored in Brain or
Kanban, pasted into PRs, or passed as a CLI argument.

Dry-run a dev pack (format_version 2 — declarative `resolver_config.json`, no WASM):

```bash
cargo run -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  build \
  --snapshot ./schema_service_snapshot.json \
  --out-dir ./resolver-pack-dev

# Optional: pass --resolver-config ./my_config.json; default is embedding-beam shadow defaults.

cargo run -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  publish \
  --env dev \
  --resolver-config ./resolver-pack-dev/resolver_config.json \
  --schema-snapshot ./resolver-pack-dev/schema_snapshot.json \
  --embedding-artifact ./resolver-pack-dev/embedding_artifact.json \
  --signing-key-locator lastsecrets://schema-resolver-pack-signing-dev \
  --trusted-key "$TRUSTED_KEY" \
  --dry-run \
  --audit-log ./resolver-pack-dev-audit.json
```

Publish after the dry-run manifest and target keys look right:

```bash
export AWS_REGION=auto
export SCHEMA_RESOLVER_PACK_BUCKET=schema-resolver-packs
export SCHEMA_RESOLVER_PACK_R2_ENDPOINT=https://3e872c20f10d065dc3ac1687c17cec60.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID="$R2_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_APP_KEY"

cargo run -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  publish \
  --env dev \
  --resolver-config ./resolver-pack-dev/resolver_config.json \
  --schema-snapshot ./resolver-pack-dev/schema_snapshot.json \
  --embedding-artifact ./resolver-pack-dev/embedding_artifact.json \
  --signing-key-locator lastsecrets://schema-resolver-pack-signing-dev \
  --trusted-key "$TRUSTED_KEY" \
  --audit-log ./resolver-pack-dev-audit.json
```

The public dev read origin is documented in
[`docs/mini_schema_resolver_config.md`](docs/mini_schema_resolver_config.md).

Promote a previous signed manifest without rebuilding:

```bash
cargo run -p schema_resolver_pack_publisher --bin schema_resolver_pack_publish -- \
  rollback \
  --env prod \
  --manifest ./previous-manifest.json \
  --trusted-key fe6e3d682497da4e05d1a87b2116765140714e11773dc1868cf8e629e0845202=7ldjSUcjTa0DSEeHpCMbt3Bo4BW/Jmmq1DFu9Lm+sYE= \
  --audit-log ./resolver-pack-prod-rollback.json
```

For rollback, pass the three artifact files (`--resolver-config`,
`--schema-snapshot`, `--embedding-artifact`) when they are available; the
binary then performs full hash/signature/config verification before promoting.
Without artifacts, it still fails closed on wrong env, wrong purpose,
key-id mismatch, or untrusted signing key before writing the latest pointer.
v1 (WASM) manifests are rejected — only format_version 2 packs can be promoted.

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
  --port 9102 --db-path schema_registry
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
| POST   | `/v1/system/reset`                  | Reset Sled-backed local state           |

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
missing, invalid, and expired negative controls, then replaces
`proofs/schema-pow-prod-terminal-proof.md` with a `PASS` report only when the
attestation also proves enforcement, production scope, quota/alarm evidence,
canary promotion, and rollback readiness. The checked-in report remains
`BLOCKED` until that live proof genuinely succeeds.

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

`crates/server_http/tests/openapi_spec.rs` drift-checks the spec against
the route table in `configure_routes`. When you add, remove, or rename
a `/v1/*` endpoint, update **both** `configure_routes` and
`openapi.yaml` in the same commit — `cargo test -p schema_service_server_http`
will fail otherwise.

### Lint the spec

```bash
npx -y @redocly/cli@latest lint openapi.yaml
```

Remaining warnings (e.g. missing `4XX` responses on read-only or verify
endpoints) are intentional: those handlers can only fail at the `5XX`
transport layer, which `#/components/responses/InternalError` already
covers.

## Design docs

In-repo design notes live under [`docs/`](./docs). Notable in-flight designs:

- [`docs/registry_api_split_design.md`](./docs/registry_api_split_design.md) —
  the `index` / `resolve` / `publish` API split (North Star
  `schema-service-local-first-schema-sync`): replicate registry *knowledge*
  for local dedup, protect registry *mutation* behind DevCert / node-key+PoW.
  The contract the endpoint-implementation cards target.

## Pre-PR checklist

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```
