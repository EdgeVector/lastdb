# Native cached catalog resolver engineering plan

Status: ready for implementation planning

Design: [Native cached catalog resolver configuration](local_schema_resolver_config.md)

Service boundary: [Schema service boundary: shared surfaces only](shared_surface_schema_service.md)

## Outcome

**Algorithm basis (locked, Tom 2026-07-14):** port the
`embedding-beam` variant from
[[local-schema-field-embedding-component-cover-2026-07-06]] into native Rust as
`native_component_cover@1`. That eval is the success criterion the product is
chasing (99% field cover / 1% residue on the 64-item Schema.org cover corpus),
with production guardrails (intent/name + field agreement) added so the
note→Trip false positive cannot ship as silent reuse.

Restore/keep `schema_service/eval/component_cover.mjs` as the regression
oracle; native golden fixtures should not regress field-cover relative to
embedding-beam on that corpus without an explicit decision.


Replace the resolver-pack v1 WASM execution path with a native Rust resolver
that consumes signed declarative configuration, a service-authored schema
snapshot, and service-computed embeddings. Remove `wasmtime` and Cranelift from
the resolver dependency closure. Preserve content-addressed distribution,
signature verification, last-known-good rollback, shadow comparison, and live
`schema_service` fallback.

The work is complete only when every Mini declaration either reuses a
service-authored catalog identity from the verified cache or reaches Schema
Service to register/resolve it before durable use. Shared-surface
publish/attach adds governance metadata, but is not the registration boundary.

## Current state

The following is present on `fold/main`:

- `schema_service_core::resolver_pack` defines the signed v1 manifest,
  snapshot, embedding, and policy artifacts.
- `schema_service_core::schema_resolver_abi` contains useful pure
  input/output/evidence types, plus `ResolverWasmHost` and WASM-specific policy
  validation.
- `schema_service_core::resolver_pack_consumer` fetches through an injected
  object-store trait, verifies v1 packs, imports embeddings, and writes an FS
  cache.
- `schema_resolver_pack_publish` builds, signs, uploads, promotes, and rolls
  back v1 packs.
- the `resolver-wasm` Cargo feature pulls `wasmtime`; `wat` is a dev
  dependency; `wasm_exec_caps.rs` exists only for resolver execution.
- GitHub CI has a required `resolver-wasm` clippy job and includes it in the
  aggregate gate.

Important gaps in the current implementation:

- there is no native resolver algorithm;
- there is no concrete production object-store/HTTP fetch implementation for
  the consumer;
- `load_latest` retains last-known-good files but does not load them when the
  latest fetch or verification fails;
- there is no periodic refresh/atomic active-state owner;
- `SchemaServiceClient::resolve_schemas` always calls the live endpoint and
  has no non-test caller in this repository;
- no shared-surface publish/attach product surface currently invokes the
  resolver pack.

The removed `fold_db_node`, desktop app, and DMG path are not integration
targets. LastDB Mini's `/api/schemas/declare` route still has legacy
`local_mint` behavior; that is implementation drift to remove, not an approved
private-schema mode. The route must return/load only service-registered catalog
identities.

## Delivery strategy

First land the shared/private contract and inventory existing callers. Then
use four mergeable resolver PRs followed by the shared-surface activation PR.
The resolver implementation may proceed after the contract is fixed; its
registry projection must not ship until the service can select shared-only
records.

```text
PR 0 shared-surface contract + caller inventory
        |
        v
PR 1 native evaluator
        |
        v
PR 2 pack v2 cutover + WASM removal
        |
        v
PR 3 HTTP fetch + last-known-good runtime
        |
        v
PR 4 local-first client facade + shadow harness
        |
        v
PR 5 shared publish/attach adoption and controlled enforcement
```

Do not maintain v1 and v2 as long-lived runtime modes. A temporary native
module may coexist with v1 during PR 1, but PR 2 removes the v1 reader,
publisher, feature, fixtures, and CI lane together.

## PR 0 — shared-surface contract and caller inventory

Suggested card/branch: `schema-service-shared-surface-contract` /
`fkanban/schema-service-shared-surface-contract`

### Scope

1. Define the explicit shared publish/attach request, including owner,
   visibility, purpose, compatibility, provenance, and the app-facing alias of
   a registered catalog identity.
2. Route generic Mini declarations through cached catalog resolution and live
   Schema Service registration on a novel shape; fail closed when neither can
   return a registered identity.
3. Inventory current service registrations and classify each as shared,
   private legacy bootstrap, system-owned, or unknown.
4. Add observability for legacy callers before rejecting or removing anything.
5. Define the shared-only registry projection consumed by resolver packs.

### Exit gate

The API boundary and migration inventory are reviewable, existing behavior is
measured, and no code path can accidentally interpret an ordinary local
declaration as sharing intent.

## PR 1 — native resolver contract and evaluator

Suggested card/branch: `schema-resolver-native-evaluator` /
`fkanban/schema-resolver-native-evaluator`

### Scope

1. Split the reusable contract from the WASM host:
   - add `schema_resolver_contract.rs` for proposal metadata, proposal
     embeddings, registry handles, decision, resolution, and evidence types;
   - replace `abi_version` with `resolver_contract_version` in the new native
     types;
   - keep the result taxonomy: `use_existing`, `use_components`,
     `expand_existing_if_allowed`, `ambiguous`,
     `needs_live_schema_service`, and `reject`.
2. Add `native_schema_resolver.rs` with an explicit algorithm registry. The
   first and only supported entry is `native_component_cover@1`.
3. Add the v2 declarative config types and validation:
   - treat `docs/resolver_config_v1.schema.json` and
     `docs/resolver_config_v1.example.json` as the review baseline, then pin
     equivalent Rust serde types and native cross-field validation in tests;
   - `deny_unknown_fields` everywhere;
   - finite scores and weights only;
   - thresholds and coverage values in `[0, 1]`;
   - scoring weights must sum to `1.0` within a fixed epsilon;
   - configured candidate/work limits cannot exceed compiled hard limits;
   - canonical creation is rejected unconditionally;
   - unknown algorithm ids or versions are unsupported, not ignored.
4. Implement deterministic evaluation:
   - validate ids, references, dimensions, duplicate records, and non-finite
     vectors before scoring;
   - shortlist by descriptive-name/intent similarity, lifecycle, and field
     type compatibility;
   - score single-schema field coverage first;
   - run bounded one-to-many component cover only if single-schema reuse does
     not pass;
   - apply confidence, total coverage, required-field coverage, and ambiguity
     gates;
   - use stable ids as final tie-breakers and never depend on map iteration
     order;
   - run a final non-configurable permission check on every local decision.
5. Move the reusable host-side output validation out of WASM-gated code and
   apply it to native results.

### Primary files

- `schema_service/crates/core/src/schema_resolver_contract.rs` — new
- `schema_service/crates/core/src/native_schema_resolver.rs` — new
- `schema_service/crates/core/src/resolver_config.rs` — new
- `schema_service/crates/core/src/lib.rs`
- `schema_service/crates/core/src/schema_resolver_abi.rs` — retained only
  until PR 2

### Tests

- golden input/config/output fixtures for every decision;
- repeat each golden case with shuffled registry order and assert byte-stable
  normalized output;
- note/event/trip false-positive fixture;
- incompatible field types and lifecycle filtering;
- ambiguous score ties and margins;
- partial and missing required-field coverage;
- component-cover beam limits and deterministic tie-breaking;
- NaN/Inf, dimension mismatch, duplicate ids, missing references, oversized
  proposal, and excessive work-budget rejection;
- forbidden local canonical creation and forbidden decision permissions.

### Exit gate

The native evaluator can reproduce pinned safe decisions without invoking
WASM, but no production behavior has changed. Both the default build and the
temporary v1 feature build remain green until PR 2.

## PR 2 — resolver-pack v2 cutover and WASM removal

> **Status (implementation):** format_version 2 is the only supported pack format. WASM host, `resolver-wasm` feature/CI job, and v1 policy artifacts are removed. Clients reject `format_version != 2` at verify time.

Suggested card/branch: `schema-resolver-pack-v2-no-wasm` /
`fkanban/schema-resolver-pack-v2-no-wasm`

Depends on PR 1.

### Manifest and artifact changes

1. Bump the resolver pack to `format_version: 2`.
2. Replace:
   - `resolver_abi_version` with `resolver_contract_version`;
   - `resolver_wasm_hash` with `resolver_config_hash`;
   - the separate `policy_hash` with policy embedded in
     `resolver_config.json`;
   - WASM artifact size/count fields with config equivalents.
3. Add signed algorithm claims `{ id, version }` and `expires_at` to the
   manifest payload.
4. Keep schema snapshot and embedding artifacts content-addressed. Remove
   stale view/transform counts from the pack if no remaining consumer requires
   them.
5. Use a v2 latest-pointer namespace keyed by environment, resolver contract,
   algorithm version, and embedder digest. Old v1 objects may remain in R2 for
   audit, but v2 clients never activate them.

### Publisher changes

- `build` writes `resolver_config.json`, `schema_snapshot.json`, and
  `embedding_artifact.json`;
- remove `--resolver-wasm` and `--policy`; add `--resolver-config` with a
  conservative generated default;
- `publish` verifies config bounds, cross-artifact references, signature,
  hashes, declared sizes, environment, embedder, contract, and algorithm before
  upload;
- upload immutable artifacts first and the signed latest pointer last;
- `rollback` promotes a previously verified v2 manifest only;
- receipts report config/algorithm/contract versions and contain no secrets.

### Consumer changes

- load and validate `resolver_config.json` instead of WASM and policy;
- construct an immutable native resolver state from the config, snapshot, and
  embeddings;
- cross-check every schema/field/canonical-field embedding target against the
  snapshot;
- route decisions through the native evaluator and final host policy;
- reject v1 with `unsupported_format` and fall back live.

### Remove the WASM closure

- delete `ResolverWasmHost` and all WASM memory/ABI execution code;
- delete `schema_resolver_abi_test.rs` and replace relevant policy assertions
  with native evaluator tests;
- delete `wasm_exec_caps.rs` if no remaining feature uses it;
- remove `resolver-wasm`, optional `wasmtime`, and `wat` from
  `schema_service_core`;
- delete `resolver.wasm` fixtures and minimal-placeholder generation;
- remove/replace WASM commands and prose in `schema_service/README.md`;
- replace the `resolver-wasm` CI job with a lightweight native-resolver job, or
  fold its tests into the normal schema-service lane;
- update the aggregate CI `needs` list in the same commit so the required gate
  remains structurally valid.

### Primary files

- `schema_service/crates/core/src/resolver_pack.rs`
- `schema_service/crates/core/src/resolver_pack_consumer.rs`
- `schema_service/crates/core/src/schema_resolver_abi.rs` — delete after moving
  retained contract types
- `schema_service/crates/core/src/wasm_exec_caps.rs` — delete
- `schema_service/crates/core/Cargo.toml`
- `schema_service/crates/resolver_pack_publisher/src/bin/schema_resolver_pack_publish.rs`
- resolver-pack fixtures/tests
- `.github/workflows/ci.yml`
- `schema_service/README.md`

### Exit gate

```bash
cargo test -p schema_service_core --test resolver_pack_test
cargo test -p schema_service_core --test resolver_pack_consumer_test
cargo test -p schema_service_core --test native_schema_resolver_test
cargo test -p schema_resolver_pack_publisher
cargo clippy -p schema_service_core -p schema_resolver_pack_publisher --all-targets -- -D warnings
```

And negative dependency/source checks:

```bash
! cargo tree -p schema_service_core | rg 'wasmtime|cranelift'
! cargo tree -p schema_resolver_pack_publisher | rg 'wasmtime|cranelift'
! rg -n 'resolver-wasm|ResolverWasmHost|resolver_wasm|resolver\.wasm' schema_service .github/workflows/ci.yml
```

Intentional historical references in design/migration documents should be
excluded explicitly rather than weakening the source check globally.

## PR 3 — configuration fetch, cache, and active runtime

> **Status (implementation):** `ResolverBootstrapConfig`, LKG load-on-failure,
> conditional object-store fetch, `ResolverRuntime` atomic active state, and
> `schema_service_client::HttpResolverPackStore` land here. No Mini profile
> wiring and no product call-path activation (PR 4/5).

Suggested card/branch: `schema-resolver-config-fetch-runtime` /
`fkanban/schema-resolver-config-fetch-runtime`

Depends on PR 2.

### Scope

1. Add an installation-owned `ResolverBootstrapConfig`:
   - enabled/channel/environment;
   - HTTPS base URL;
   - refresh interval and jitter;
   - request timeout and maximum response/artifact bytes;
   - maximum pack age;
   - expected embedder id;
   - cache root;
   - trusted key ids referencing binary/install-owned public keys.
2. Add a concrete HTTP fetcher. Downloaded documents may supply hashes and
   compatibility claims but never a new origin or trust root.
3. Extend the fetch abstraction to support `ETag`/`If-None-Match`, not-modified
   responses, response-size limits, and typed transport/HTTP errors.
4. Implement a `ResolverRuntime` that owns `Arc<NativeResolverState>`:
   - load a still-valid last-known-good release before network refresh;
   - refresh in the background with bounded retry and jitter;
   - stage all artifacts, verify, parse, cross-check, and self-test before
     activation;
   - atomically swap active state so in-flight resolutions keep the previous
     immutable state;
   - never overwrite last-known-good on failed verification;
   - distinguish transport failure with old-state continuation from a bad
     latest release.
5. Make cache paths include format/contract/algorithm/embedder identity. Write
   content-addressed artifacts once and update a small last-known-good pointer
   atomically.
6. Add low-cardinality fetch, verification, activation, cache, and fallback
   telemetry.

### Placement

Keep pack parsing, verification, native state, and cache invariants in
`schema_service_core`. Put HTTP-specific fetching in
`schema_service_client` unless a second non-HTTP transport is identified.
Do not add resolver configuration to LastDB Mini's global profile until a real
Mini caller is approved.

### Tests

- 200, 304, timeout, truncated body, oversized body, and non-success HTTP;
- HTTPS/base-origin policy;
- cache hit avoids artifact download;
- cold start from valid last-known-good while network is unavailable;
- expired last-known-good falls back live;
- corrupt/tampered latest leaves old active state and cache untouched;
- concurrent refresh and resolution;
- atomic-write interruption recovery;
- key rotation with two pinned public keys;
- no raw URL, proposal data, or field names in metric labels.

### Exit gate

The runtime can refresh and serve a native resolver state safely, but still
does not alter a product call path.

## PR 4 — local-first resolver facade and shadow comparison

Suggested card/branch: `schema-resolver-local-first-facade` /
`fkanban/schema-resolver-local-first-facade`

Depends on PR 3.

### Scope

1. Add a `LocalFirstSchemaResolver` facade in `schema_service_client` that
   owns:
   - the native resolver runtime;
   - an injected proposal embedder;
   - the existing live `SchemaServiceClient` fallback.
2. Define an adapter between `SchemaResolveProposal`/`SchemaResolveResponse`
   and the native resolver contract. Do not leak pack-only types into callers.
3. Support two explicit modes:
   - `shadow`: resolve locally, call live for every proposal, compare, return
     live behavior unchanged;
   - `enforce_existing_only`: return high-confidence existing/component reuse
     locally and call live only for fallback proposals.
4. For mixed batches, send only fallback proposals live and merge results by a
   stable proposal id. Do not key internal correlation solely by descriptive
   name; duplicate names must be safe.
5. Record decision, disagreement class, fallback reason, pack/config version,
   and latency with bounded labels. Redact proposal content.

### Shadow validation

- live response remains byte/semantically unchanged to the caller;
- local match, local miss, and local ambiguity all still call live;
- registry embeddings are imported, not recomputed;
- proposal embedding work is bounded and cancellable;
- disagreement fixtures distinguish unsafe local reuse, missed reuse, and
  equivalent-but-different component plans;
- the 64-item component-cover corpus runs through the facade;
- note/event/trip false positives count as unsafe disagreements and block
  enforcement.

### Exit gate

The facade is ready for adoption and has measured shadow accuracy, but it is
not yet a shipped feature without a real caller.

## PR 5 — shared-surface publish/attach adoption and enforcement

Suggested card/branch: `schema-resolver-shared-surface-facade` /
`fkanban/schema-resolver-shared-surface-facade`

Depends on PR 4 and measured shadow gates.

### Product boundary

The caller includes Mini's declaration path. It accepts a schema proposal,
attempts reuse against the service-authored resolver snapshot, and calls live
Schema Service for any novel, ambiguous, stale, or unsupported proposal before
the schema can be loaded or used. Shared-surface publish/attach adds sharing
intent and governance metadata to an already registered catalog identity. The
removed desktop/node path is not a target.

### Rollout

1. Wire shared publish/attach in `shadow`, off by default outside dev.
2. Collect a bounded comparison window and publish an aggregate validation
   report with no proposal content.
3. Gate `enforce_existing_only` on:
   - zero unsafe-reuse disagreements in adversarial fixtures;
   - agreed minimum match precision and coverage;
   - bounded p95 proposal embedding and resolution latency;
   - verified live fallback for every non-local decision;
   - successful bad-latest and offline last-known-good drills.
4. Enable dev, then a small production cohort, then broader rollout.
5. Keep a bootstrap kill switch that immediately routes everything live.

### Exit gate

Shared publish/attach skips live schema-service calls for safe reuse, calls
live for every miss/ambiguity/new-canonical case, persists an auditable
attachment, and can be returned to live-only behavior through configuration.

## Proposed Kanban dependency graph

```text
schema-service-shared-surface-contract
  -> schema-service-shared-registry-projection
    -> schema-resolver-pack-v2-no-wasm

schema-service-shared-surface-contract
  -> schema-resolver-native-evaluator
    -> schema-resolver-pack-v2-no-wasm
      -> schema-resolver-config-fetch-runtime
        -> schema-resolver-shared-surface-facade
```

The existing `schema-resolver-pack-wasm-r2-umbrella` should be updated in
place to the native configuration-fetch program rather than duplicated. Done
WASM-era cards remain historical evidence; do not reopen them. New cards
should cite the stable Brain design slug
`schema-resolver-pack-wasm-r2-plan-2026-07-06`, whose content now contains the
native directive.

## Cross-cutting invariants

- No local canonical schema creation.
- Every Mini declaration produces a Schema Service-registered catalog identity
  before durable use; cached reuse may avoid a live call only for an identity
  already authored by the service.
- Resolver artifacts contain only explicitly shared contracts.
- Sharing requires explicit owner, purpose, visibility, and compatibility
  metadata; similarity alone never promotes a schema.
- No executable artifacts, expression language, bytecode, or arbitrary URLs
  in downloaded configuration.
- No raw secret material in source, logs, Brain, Kanban, receipts, or PRs;
  publishers use `lastsecrets://` locators at point of use.
- Signature, environment, purpose, expiry, compatibility, declared size, and
  artifact hash verification precede activation.
- Config cannot raise compiled hard limits or disable host safety checks.
- Stable deterministic ordering for all scores, matches, evidence, and merged
  batch results.
- Normal operation imports service-computed registry embeddings; full local
  registry recompute is not silently introduced.
- The live service is the fallback and authority, not a peer whose answer may
  be ignored on uncertainty.
- No changes to `fold_db_node`, Tauri/Desktop, or DMG paths.

## Full validation matrix

### Correctness

- all native decision golden vectors;
- single-schema reuse and one-to-many component cover;
- ambiguity, residue, field-type, lifecycle, and permission gates;
- remote adapter and mixed-batch merge;
- deterministic output under shuffled input ordering.

### Supply-chain and cache safety

- bad signature/purpose/env/key/expiry;
- unsupported format/contract/algorithm/embedder;
- hash and size mismatch;
- cross-artifact dangling/duplicate ids;
- corrupt latest preserves active and last-known-good state;
- rollback promotes only a previously verified v2 manifest.

### Resource bounds

- maximum proposal fields, schemas, embedding dimensions, artifact bytes, and
  work units;
- timeout/cancellation behavior;
- concurrent refresh/resolution;
- p50/p95/p99 import, proposal embedding, native evaluation, and live fallback
  latency.

### Dependency and CI proof

- no `wasmtime` or Cranelift in core, publisher, client, or adopting binary;
- no resolver-WASM feature or fixture references outside historical docs;
- formatter, clippy, unit/integration tests, workflow structure lint, and full
  schema-service workspace lanes green;
- Forge required check remains `Forge CI / ci-required`; do not bypass it.

## Rollback model

- Before enforcement: disable the facade and use live service only.
- After enforcement: the bootstrap kill switch routes all proposals live
  without deleting cache state.
- For a bad release: retain active last-known-good, publish/promote a previous
  signed v2 manifest, and record distinct fetch-versus-verification telemetry.
- Do not roll back to v1 WASM. v1 remains unsupported after PR 2.

## Definition of done

The program is done when all of the following are true:

1. resolver pack v2 contains config, snapshot, and embeddings but no WASM;
2. resolver core, publisher, client, and adopting binary contain no
   `wasmtime`/Cranelift dependency;
3. signed fetch, validation, cache, refresh, and rollback are proven;
4. native decisions are deterministic and adversarially tested;
5. shared publish/attach runs shadow mode with acceptable results;
6. that workflow safely skips live calls only for existing-schema/component
   reuse and falls back live for everything else;
7. generic declaration is proven unable to return or persist an unregistered
   identity, including while offline;
8. operational docs, Brain context, and Kanban work state describe the native
   configuration design rather than WASM.
