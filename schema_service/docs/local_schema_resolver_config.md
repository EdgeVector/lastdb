# Native cached catalog resolver configuration

Status: proposed

Engineering plan: [Native local schema resolver engineering plan](local_schema_resolver_engineering_plan.md)

Empirical basis: [[local-schema-field-embedding-component-cover-2026-07-06]]
(`embedding-beam`: **99% field cover / 1% residue** on the 64-item corpus).

## Decision

Node-local catalog resolution will not load or execute resolver WASM. The resolver
algorithm is native Rust code shipped with LastDB. A signed resolver release
contains only declarative configuration and data:

- the native algorithm identifier and compatible version;
- a schema snapshot;
- service-computed registry embeddings;
- bounded matching policy and fallback rules.

This removes `wasmtime` and its compilation/runtime closure from the local
schema-resolution path while preserving remotely updateable thresholds,
registry data, rollout controls, and emergency rollback.

### Algorithm basis: embedding-beam component cover

`native_component_cover@1` is the production port of the 2026-07-06
**embedding-beam** component-cover eval (not a new invention):

1. Shortlist candidate schemas by intent / descriptive-name similarity.
2. Match proposal fields to schema fields primarily via **field-context
   embeddings** (cosine), with exact/synonym boosts only as secondary signals.
3. Prefer a single high-coverage schema when it clears score + coverage gates.
4. Otherwise run a **bounded beam search** that covers the proposal with a
   small set of existing schemas (one-to-many component cover).
5. Residue / ambiguity / low confidence → `needs_live_schema_service`.
6. **Never** mint a new canonical schema locally.

Eval headline (64 records × ~934 Schema.org-derived candidates + inheritance
overlay): `embedding-beam` → full-record 94%, field cover **99%**, residue
**1%**, no-cover 0%. Production must keep the eval's guardrail: require
schema intent/name agreement **and** field embedding agreement (the eval
observed a note→Trip false positive when field embeddings were used alone).

Policy direction (Tom, reaffirmed 2026-07-21): every Mini declaration resolves
through Schema Service (match / decompose / deliberate create). An offline
queue may preserve a proposal, but it does not create a usable schema identity:
registration must succeed before durable writes. The node-local resolver is
the **hot path** that reuses the service-authored catalog without a live call
when confidence is high — never an alternative schema authority.

The live `schema_service` remains authoritative for new canonical schemas.
Local resolution may only reuse existing schemas or components. Any
unsupported, missing, stale, ambiguous, or invalid configuration falls back
to the live service (or the offline queue when the node cannot reach the
service).

## Trust and fetch model

There are two configuration layers with different trust boundaries.

### Local bootstrap configuration

The installed application owns a small bootstrap configuration. It is not
downloaded from the resolver channel.

```toml
[schema_resolver]
enabled = true
channel = "dev"
max_mode = "shadow"
base_url = "https://resolver.example.invalid/schema-resolver-configs"
refresh_interval_seconds = 3600
request_timeout_seconds = 10
max_download_bytes = 134217728
max_config_age_seconds = 604800
cache_dir = "schema-resolver"
expected_embedder_id = "all-MiniLM-L6-v2@1"
trusted_key_ids = ["resolver-release-2026-01"]
```

The bootstrap configuration controls where bytes may be fetched from and
which signing keys are trusted. Downloaded content cannot redirect the client
to another origin. Production builds should pin HTTPS and must not accept a
runtime downgrade to an insecure scheme. The corresponding public keys are
pinned in the binary or an installation-owned trust store; a downloaded key id
never adds a new trust root.

### Signed resolver release

The latest pointer is selected by environment, native resolver contract
version, algorithm, and embedder:

```text
{base_url}/{env}/latest/
  contract-v1/native-component-cover-v1/
  embedder-sha256-{sha256(embedder_id)}/manifest.json
```

The pointer is a signed manifest, not a mutable JSON document that is trusted
by location. Artifacts remain content-addressed:

```text
{base_url}/{env}/artifacts/sha256/{hash}/resolver_config.json
{base_url}/{env}/artifacts/sha256/{hash}/schema_snapshot.json
{base_url}/{env}/artifacts/sha256/{hash}/embedding_artifact.json
```

The manifest binds every artifact hash and compatibility claim:

```json
{
  "format_version": 2,
  "resolver_contract_version": 1,
  "algorithm": {
    "id": "native_component_cover",
    "version": 1
  },
  "env": "dev",
  "embedder_id": "all-MiniLM-L6-v2@1",
  "generated_at": "2026-07-13T00:00:00Z",
  "expires_at": "2026-07-20T00:00:00Z",
  "resolver_config_hash": "<sha256>",
  "schema_snapshot_hash": "<sha256>",
  "embedding_artifact_hash": "<sha256>",
  "counts": {},
  "artifact_sizes": {},
  "signing_key_id": "resolver-release-2026-01",
  "signature": {}
}
```

`resolver_contract_version` versions the input/output data contract between
the consumer and native evaluator. `algorithm.version` selects behavior that
is compiled into the client. Neither field is an executable ABI.

## Resolver configuration

`resolver_config.json` replaces both `resolver.wasm` and the current separate
`policy.json`. Keeping the algorithm selector and its policy in one signed,
content-addressed document prevents unsupported combinations.

The initial contract is captured in two reviewable artifacts:

- [resolver_config_v1.schema.json](resolver_config_v1.schema.json) —
  machine-readable JSON Schema;
- [resolver_config_v1.example.json](resolver_config_v1.example.json) —
  conservative shadow-mode configuration.

The example values are engineering defaults for fixtures and shadow-mode
instrumentation, not production-calibrated thresholds. Enforcement values must
come from the disagreement/evaluation gates in the engineering plan.

The top-level groups are:

- `format_version` and `resolver_contract_version` for compatibility;
- `algorithm` for selection among native implementations compiled into the
  client;
- `policy_version` for release receipts, status, and telemetry;
- `rollout` for the signed publisher's requested mode;
- `candidate_generation` for bounded shortlist and component search breadth;
- `scoring` for thresholds, weights, coverage, and ambiguity margins;
- `permissions` for locally allowable reuse decisions;
- `limits` for publisher-requested ceilings below compiled hard limits.

All numeric ranges and collection sizes are validated by native code before
activation. Unknown fields are rejected. The publisher cannot select an
unknown algorithm, exceed compiled hard limits, enable canonical creation, or
weaken non-configurable safety checks.

### Configurable versus compiled behavior

The v1 configuration may change:

- requested rollout mode;
- candidate counts and maximum components;
- similarity, score, coverage, and ambiguity thresholds;
- the relative weight of descriptive-name and field-match evidence;
- whether existing-schema or component reuse is permitted;
- resource ceilings that are no greater than native hard limits.

The following remain compiled invariants and are not remotely configurable:

- cosine similarity and input normalization;
- field-type compatibility for every local mapping;
- exclusion of deprecated schemas;
- deterministic ordering and stable-id tie-breaking;
- the meaning of each native algorithm version;
- the fallback-reason taxonomy;
- rejection of unknown required-field metadata in enforcement mode;
- prohibition of local expansion and canonical creation;
- artifact origin, trusted keys, signature rules, and native hard limits.

The schema includes some compiled invariants as `const` fields so a human can
see them in a release config and cross-artifact verification can reject a
contradictory document. Their presence does not make them tunable.

### Rollout precedence

The signed config requests one of `disabled`, `shadow`, or
`enforce_reuse_only`. The installation-owned bootstrap config independently
sets a maximum allowed mode. The effective mode is the less permissive value:

```text
effective_mode = min(bootstrap.max_mode, signed_config.rollout.requested_mode)
```

Thus a downloaded config cannot enable enforcement on a client pinned to
shadow or disabled. The example requests `shadow` and disables component reuse
until disagreement validation clears it.

### Native component-cover v1 scoring

For each proposal and candidate schema:

1. Compute descriptive-name cosine similarity. Candidates below
   `descriptive_name_min_similarity` do not enter field matching.
2. For each proposal field, consider only type-compatible schema fields. A
   match is safe only when its context similarity clears
   `context_min_similarity` and its lead over the runner-up clears the field
   ambiguity margin. Unmatched or ambiguous fields contribute zero.
3. Compute `field_match_score` as the sum of each proposal field's safe best
   similarity divided by the total proposal field count. This incorporates
   both match quality and coverage because residue contributes zero.
4. Compute:

   ```text
   schema_score =
       descriptive_name_weight * descriptive_name_similarity
       + field_match_weight * field_match_score
   ```

   Native validation requires the two weights to sum to `1.0` within a fixed
   epsilon; JSON Schema alone cannot express that cross-field constraint.
5. `use_existing` additionally requires the configured total-field and
   required-field coverage, schema ambiguity margin, score threshold, and
   permission.
6. If single-schema reuse fails, bounded component search may combine existing
   schemas. It uses the same safe field mappings and coverage gates, plus
   `components.min_score`, `component_candidates`, and
   `max_components_per_resolution`.

The native proposal contract must represent requiredness as known true, known
false, or unknown. Unknown requiredness is allowed for shadow comparison but
forces live-service fallback in enforcement mode; it must not be silently
converted to `false`.

`max_work_units` bounds algorithmic effort. One schema-name comparison or one
proposal-field-to-candidate-field comparison consumes one unit. Exhausting the
budget produces `work_limit_exceeded` and a live-service fallback, never a
partial local reuse decision.

Fallback codes are defined by native code rather than downloaded strings.
This prevents release configuration from creating unbounded telemetry labels
or changing the semantics callers use for fallback routing.

## Native evaluation pipeline

`native_component_cover` is a pure evaluator over proposal metadata,
proposal embeddings, the downloaded schema snapshot, registry embeddings,
and the validated configuration.

1. Reject incompatible embedder ids, vector dimensions, snapshot/config
   versions, oversized inputs, duplicate ids, non-finite values, and missing
   referenced records.
2. Shortlist schemas by descriptive-name/intent similarity, lifecycle, and
   compatible field types.
3. Score proposal fields against schema field-context and canonical-field
   embeddings.
4. Evaluate single-schema coverage first, then bounded one-to-many component
   cover using the configured candidate limits.
5. Apply score, coverage, required-field, and ambiguity gates.
6. Return the existing structured resolution result and evidence shape:
   `use_existing`, `use_components`, `expand_existing_if_allowed`,
   `ambiguous`, `needs_live_schema_service`, or `reject`.
7. Apply a final non-configurable host policy. In particular, local canonical
   creation is always denied even if a malformed config requests it.

The evaluator must be deterministic for the same normalized inputs and
configuration. Tie-breaking uses stable ids, never map iteration order.

Configuration is intentionally not a general expression language, rule DAG,
or bytecode. A new matching procedure requires a reviewed native algorithm
version and a client release. Routine tuning, registry refreshes, rollout
permissions, and fallback policy remain remotely updateable without a binary
release.

## Fetch, validation, and activation

The consumer refresh is staged and atomic:

1. Fetch the latest compatible signed manifest with conditional HTTP
   (`ETag`/`If-None-Match`), jittered refresh, time limits, and size limits.
2. Verify purpose, signature, environment, expiry, contract version,
   algorithm support, embedder id, and declared sizes before fetching large
   artifacts.
3. Read content-addressed artifacts from cache when present; otherwise fetch
   them from the bootstrap origin.
4. Verify hashes before parsing.
5. Parse and cross-validate config, snapshot, and embeddings, including every
   id reference and embedding dimension.
6. Build an immutable in-memory resolver state and run a small self-check
   fixture declared by the native algorithm version.
7. Atomically replace the active state and last-known-good pointer only after
   all checks pass.

Concurrent resolutions retain the previous immutable state during refresh.
A bad latest release never overwrites the last-known-good cache.

On startup, the client may use a previously verified last-known-good release
within the configured age/expiry bounds while refreshing in the background.
If no valid state exists, resolution goes directly to the live service.

## Failure behavior

Configuration distribution is an optimization, not an availability
dependency. These conditions fail closed to live `schema_service`:

- resolver disabled;
- latest pointer missing or fetch timeout;
- untrusted key, bad signature, wrong purpose, or wrong environment;
- expired or too-old release;
- unsupported contract or native algorithm version;
- embedder mismatch;
- artifact missing, oversized, malformed, or hash-mismatched;
- invalid config bounds or forbidden permission;
- inconsistent snapshot/embedding references;
- ambiguous or low-confidence resolution.

Transport failure may use a still-valid last-known-good state. Verification or
compatibility failure in a newly fetched release should not silently use that
release and should emit a distinct reason even when the old state remains
active.

## Telemetry

Keep low-cardinality counters for:

- refresh results: fetched, unchanged, cache hit, transport failure;
- validation results: signature, compatibility, config, snapshot, embedding;
- activation source: latest, last-known-good, none;
- local decisions and live fallback reason codes;
- active algorithm/config version and release age as bounded status fields.

Do not include proposal text, field names, user values, arbitrary app ids, or
raw URLs in metric labels.

## Migration from resolver-pack v1

The current WASM consumer is not wired into a production ingestion path, so
this should be a clean format break rather than a permanent dual stack.

1. Introduce the native evaluator and golden-vector tests using the current
   resolver input/output structures.
2. Change the pack manifest to format v2: replace `resolver_abi_version` and
   `resolver_wasm_hash` with `resolver_contract_version`, `algorithm`, and
   `resolver_config_hash`.
3. Merge the current policy artifact into `resolver_config.json` and update
   the publisher/build/rollback commands.
4. Update the consumer cache and latest-pointer paths to v2. Do not activate a
   v1 manifest; report `unsupported_format` and fall back live.
5. Remove `ResolverWasmHost`, `resolver-wasm`, `wasm_exec_caps`, `wasmtime`,
   `wat`, `resolver.wasm` fixtures, and WASM-specific documentation/tests.
6. Publish a dev v2 release and prove fetch, cache, rollback, bad-latest
   preservation, deterministic native decisions, and live fallback.
7. Add shadow comparison against live `schema_service` before enabling local
   reuse.

Keeping v1 readers would retain the large dependency and defeat this change.
Old content-addressed v1 objects may remain in storage for auditability, but
new latest pointers must select only v2 releases.

## Validation gates

- The default and resolver-enabled builds contain no `wasmtime` or Cranelift
  dependency (`cargo tree` assertion).
- Golden vectors pin deterministic decisions and evidence for each supported
  native algorithm version.
- Adversarial fixtures cover the known note/event/trip false-positive class,
  type mismatch, ambiguous ties, incomplete required fields, NaN/Inf vectors,
  duplicate ids, and configured size limits.
- A corrupt or unsupported latest release leaves last-known-good intact.
- Snapshot embeddings are imported and reused across restarts; normal startup
  does not recompute the registry embedding set.
- High-confidence matches avoid live calls in shadow tests; misses and policy
  uncertainty still call the live service.

## Non-goals

- Downloadable executable plugins or user-supplied resolver code.
- Local canonical schema creation.
- Arbitrary fetch URLs supplied by downloaded configuration.
- Remotely changing native safety invariants.
- Preserving resolver-pack v1 compatibility in local binaries.
