# Schema core functional shell proof

Status: PASS
Environment: main
Fold revision under test: c51f29ad150665426fcb1e61d14acc70f5065628
Validated at: 2026-10-29T22:25:34Z
Toolchain: rustc 1.96.1 (31fca3adb 2026-06-26)
Predecessor commits: #747 `c08e7aa3`, #759 `0c41682b`, #767 `075d5bc5`, #771 `5fb9f683`, #774 `c51f29ad`

## Decision

`schema_service_core` is the functional core of the schema service. It decides
registry results and takes clock, id, configuration, runtime, filesystem,
embedder, and storage values from the caller. `schema_service_server_shared`
is the imperative shell for the process adapters. The commands below were run
on `c51f29ad150665426fcb1e61d14acc70f5065628` with no schema configuration
variables set. Absolute paths in `cargo tree` are this checkout.

## Proven behavior

- The core publishes the ports below. Each process adapter in
  `schema_service_server_shared` is a single implementation.
- `rg` over the core's Rust sources prints nothing for `std::fs`, Tokio
  runtime constructors, `::now()`, `new_v4`, `static mut`, and `lazy_static`.
- The required CI workflow denies the crate-local Clippy ban on every run,
  and the statics gate covers `static mut` and `lazy_static`.
- The normal dependency edge is the list below. Direct Tokio features are
  `macros` and `sync`. Direct `uuid` features include `v4`.
- The baseline stdout hash is
  `bef2dce6f9cee89eb898de3d60a608f6db4f2b66cbd4063bb62b2492bc13e971`.

### Ports and the single shell adapter

| Port | Core definition | Shell adapter |
|---|---|---|
| `Clock` | `schema_service/crates/core/src/clock.rs` | `SystemClock` in `schema_service/crates/server_shared/src/clock.rs` |
| `IdGen` | `schema_service/crates/core/src/ids.rs` | `SystemIds` in `schema_service/crates/server_shared/src/ids.rs` |
| `BlockingRuntime` | `schema_service/crates/core/src/blocking_runtime.rs` | `DedicatedThreadRuntime` in `schema_service/crates/server_shared/src/blocking_runtime.rs` |
| `ResolverPackCache` | `schema_service/crates/core/src/resolver_pack_consumer/cache.rs` | `FsResolverPackCache` in `schema_service/crates/server_shared/src/resolver_pack_cache.rs` |
| `SchemaConfig` | `schema_service/crates/core/src/schema_config.rs` | `SchemaConfigEnv::from_env` in `schema_service/crates/server_shared/src/schema_config.rs` |
| `Embedder` | `schema_service/crates/core/src/embedder.rs` | `FoldDbFastEmbedder` in `schema_service/crates/server_shared/src/fastembed_adapter.rs`, behind `fastembed` and `fastembed-layer` |

`SchemaServiceState::new_in_memory`, `new_with_external`, and
`new_with_external_and_schema_mutation_gate_store` take the embedder,
`SchemaConfig`, `Clock`, `BlockingRuntime`, and `IdGen`.
`ResolverRuntime::new` takes `Clock` and `ResolverPackCache`.

These ports have their external adapter in another shell crate. The
in-memory implementations stay in the core.

| Port | Core definition | Adapter |
|---|---|---|
| `ExternalSchemaPersistence` | `schema_service/crates/core/src/external_persistence.rs` | `InMemorySchemaPersistence` in the core. `S3BlobPersistence` in `schema_service/crates/schema_service_s3/src/persistence_impl.rs` |
| `SchemaMutationGateQuotaStore` | `schema_service/crates/core/src/schema_mutation_gate/types.rs` | `InMemorySchemaMutationGateQuotaStore` in the core. `DynamoDbSchemaMutationGateStore` in `schema_service/crates/schema_service_s3/src/quota_store.rs` |
| `ResolverPackObjectStore` | `schema_service/crates/core/src/resolver_pack_consumer/types.rs` | `HttpResolverPackStore` in `schema_service/crates/client/src/resolver_pack_http.rs` |

`DisabledEmbeddingModel` also implements `Embedder` inside the core. It
returns an init error and does not load a model. Default builds of the dev
server and the Lambda pass that value. `FoldDbFastEmbedder` is the shell
adapter when the `fastembed` feature is on.

Implementations found by search:

```text
rg -n --glob '*.rs' -e 'impl Clock for' -e 'impl IdGen for' -e 'impl BlockingRuntime for' -e 'impl ResolverPackCache for' -e 'impl Embedder for' -e 'impl ExternalSchemaPersistence for' -e 'impl ResolverPackObjectStore for' -e 'impl SchemaMutationGateQuotaStore for' -e 'impl SchemaConfigEnv for' .
```

```text
./schema_service/crates/schema_service_s3/tests/s3/backend.rs:15:impl ExternalSchemaPersistence for DefinitionBackend {
./schema_service/crates/schema_service_s3/src/quota_store.rs:141:impl SchemaMutationGateQuotaStore for DynamoDbSchemaMutationGateStore {
./schema_service/crates/schema_service_s3/src/persistence_impl.rs:7:impl ExternalSchemaPersistence for S3BlobPersistence {
./schema_service/crates/core/tests/canonical/failure.rs:29:impl ExternalSchemaPersistence for FailedCommit {
./schema_service/crates/core/tests/canonical/concurrency.rs:187:impl Embedder for ParcelEmbedder {
./schema_service/crates/core/src/memory_persistence.rs:107:impl ExternalSchemaPersistence for InMemorySchemaPersistence {
./schema_service/crates/core/src/schema_mutation_gate/quota_store.rs:37:impl SchemaMutationGateQuotaStore for InMemorySchemaMutationGateQuotaStore {
./schema_service/crates/core/src/embedder.rs:34:impl Embedder for DisabledEmbeddingModel {
./schema_service/crates/server_shared/src/schema_config.rs:25:impl SchemaConfigEnv for SchemaConfig {
./schema_service/crates/server_shared/src/ids.rs:13:impl IdGen for SystemIds {
./schema_service/crates/server_shared/src/blocking_runtime.rs:18:impl BlockingRuntime for DedicatedThreadRuntime {
./schema_service/crates/server_shared/src/clock.rs:13:impl Clock for SystemClock {
./schema_service/crates/server_shared/src/resolver_pack_cache.rs:48:impl ResolverPackCache for FsResolverPackCache {
./schema_service/crates/server_shared/src/fastembed_adapter.rs:104:impl Embedder for FoldDbFastEmbedder {
./schema_service/crates/client/src/resolver_pack_http.rs:102:impl ResolverPackObjectStore for HttpResolverPackStore {
```

Exit status 0. `Clock`, `IdGen`, `BlockingRuntime`, `ResolverPackCache`, and
`SchemaConfigEnv` each have one implementation, and it lives in
`schema_service_server_shared`.

### Where those adapters are wired

`SystemClock`, `DedicatedThreadRuntime`, `SystemIds`, and `SchemaConfig`
are passed into the state constructors at these call sites:

- `schema_service/crates/server_http/src/bin/schema_service.rs` constructs
  `DedicatedThreadRuntime` and passes it to
  `SchemaServiceServer::new_with_builtins`.
  `schema_service/crates/server_http/src/lib.rs` `SchemaServiceServer::new`
  passes `SchemaConfig::from_env()`, `SystemClock`, that runtime, and
  `SystemIds`. The default feature set passes `DisabledEmbeddingModel`.
  Feature `fastembed` passes `FoldDbFastEmbedder::new`. Storage is
  `InMemorySchemaPersistence`.
- `schema_service/crates/server_lambda/src/state_init.rs`
  `get_or_init_state` passes `SchemaConfig::from_env()`, `SystemClock`,
  `DedicatedThreadRuntime`, and `SystemIds` into
  `new_with_external_and_schema_mutation_gate_store`. Persistence is
  `S3BlobPersistence`. `SCHEMA_MUTATION_GATE_QUOTA_TABLE` selects
  `DynamoDbSchemaMutationGateStore`; an unset table keeps the in-memory
  quota store. Feature `fastembed` (which enables
  `schema_service_server_shared/fastembed-layer`) passes
  `FoldDbFastEmbedder::from_lambda_layer`. The default feature set passes
  `DisabledEmbeddingModel`. The Lambda shell holds the state in
  `static SCHEMA_STATE: OnceCell`.
- `schema_service/crates/worker/src/bin/schema_service_backfill_purpose.rs`
  `build_state` passes `SchemaConfig::from_env()`, `SystemClock`,
  `DedicatedThreadRuntime`, `SystemIds`, and `FoldDbFastEmbedder::new`.
  The worker crate enables `schema_service_server_shared/fastembed`.
  Persistence is `S3BlobPersistence`.
- `schema_service/scripts/generate_validated_schema_org_seeds/src/main.rs`
  passes the same four shell values and `FoldDbFastEmbedder::new` into
  `new_in_memory`.
- `schema_service/crates/server_shared/examples/semantic/checks.rs`
  `observe` passes the same four shell values into `new_with_external`.
  The caller supplies `FoldDbFastEmbedder`. Storage is in-memory.
- `scripts/schema_core_baseline/src/state.rs` `open_state` passes
  `SchemaConfig::default()`, `SystemClock`, `DedicatedThreadRuntime`,
  `SystemIds`, and `DisabledEmbeddingModel`, then calls
  `configure_app_identity`.
- `schema_service/crates/core/tests/canonical/support.rs` passes
  `SchemaConfig::default()` and the same three shell adapters
  (`process_clock`, `process_runtime`, `process_ids`). The canonical
  harness drives futures with `block_on_current_thread` in the shell.
- `schema_service/crates/schema_service_s3/tests/s3/support.rs` passes
  `SchemaConfig::default()`, `SystemClock`, `DedicatedThreadRuntime`, and
  `SystemIds` with `DisabledEmbeddingModel`.

`FsResolverPackCache` is constructed by `FsResolverPackCache::new` and
`FsResolverPackCache::from_bootstrap`. `ResolverRuntime::new` is the
constructor that accepts `Arc<dyn ResolverPackCache>`. Search of the tree:

```text
rg -n --glob '*.rs' -e 'FsResolverPackCache' -e 'ResolverRuntime::' -e 'ResolverRuntime' .
```

```text
./schema_service/crates/server_shared/src/lib.rs:31:pub use resolver_pack_cache::FsResolverPackCache;
./schema_service/crates/server_shared/src/resolver_pack_cache.rs:15:pub struct FsResolverPackCache {
./schema_service/crates/server_shared/src/resolver_pack_cache.rs:19:impl FsResolverPackCache {
./schema_service/crates/server_shared/src/resolver_pack_cache.rs:48:impl ResolverPackCache for FsResolverPackCache {
./schema_service/crates/core/src/lib.rs:139:pub use resolver_runtime::{NativeResolverState, ResolverRuntime, RuntimeTelemetry};
./schema_service/crates/core/src/resolver_runtime.rs:46:pub struct ResolverRuntime<S: ResolverPackObjectStore> {
./schema_service/crates/core/src/resolver_runtime.rs:54:impl<S> ResolverRuntime<S>
```

Exit status 0. No file calls `ResolverRuntime::new`,
`FsResolverPackCache::new`, or `FsResolverPackCache::from_bootstrap`.
`HttpResolverPackStore` is likewise only defined and re-exported. See
Remaining gaps.

The only `Uuid::new_v4` call in `schema_service` is the shell adapter.
The other hit is the ban entry:

```text
rg -n 'Uuid::new_v4' schema_service
```

```text
schema_service/crates/core/clippy.toml:49:  { path = "uuid::Uuid::new_v4", reason = "use the injected IdGen" },
schema_service/crates/server_shared/src/ids.rs:15:        Uuid::new_v4()
```

Exit status 0.

### Clippy ban list

Clippy loads `schema_service/crates/core/clippy.toml` for that crate and
stops, so the list does not apply to the rest of the workspace. The crate
root denies both lints:

```text
rg -n -C 3 'disallowed_methods, clippy::disallowed_types' schema_service/crates/core/src/lib.rs
```

```text
schema_service/crates/core/src/lib.rs-45-
schema_service/crates/core/src/lib.rs-46-// Time, env, filesystem, threads, runtimes, and randomness stay in the shell.
schema_service/crates/core/src/lib.rs-47-// The ban list is `clippy.toml` in this crate, so it does not apply to the workspace.
schema_service/crates/core/src/lib.rs:48:#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
schema_service/crates/core/src/lib.rs-49-
schema_service/crates/core/src/lib.rs-50-pub mod app_identity;
schema_service/crates/core/src/lib.rs-51-pub mod app_release;
```

The ban list on this revision:

```text
cat schema_service/crates/core/clippy.toml
```

```toml
# Crate-local Clippy config. Clippy loads the file at this crate's
# Cargo.toml and stops, so these bans do not apply to the rest of the workspace.
#
# Paths that are not in this crate's dependency graph set allow-invalid so the
# config still loads. A later edit that adds the crate and calls the path is denied.

disallowed-methods = [
  { path = "std::time::SystemTime::now", reason = "use the injected Clock" },
  { path = "std::time::Instant::now", reason = "use Clock::monotonic" },
  { path = "chrono::Utc::now", reason = "use the injected Clock", allow-invalid = true },
  { path = "chrono::Local::now", reason = "use the injected Clock", allow-invalid = true },
  { path = "std::env::var", reason = "read configuration in the shell and pass SchemaConfig" },
  { path = "std::env::var_os", reason = "read configuration in the shell and pass SchemaConfig" },
  { path = "std::env::vars", reason = "read configuration in the shell and pass SchemaConfig" },
  { path = "std::fs::canonicalize", reason = "filesystem access stays in the shell" },
  { path = "std::fs::copy", reason = "filesystem access stays in the shell" },
  { path = "std::fs::create_dir", reason = "filesystem access stays in the shell" },
  { path = "std::fs::create_dir_all", reason = "filesystem access stays in the shell" },
  { path = "std::fs::exists", reason = "filesystem access stays in the shell" },
  { path = "std::fs::hard_link", reason = "filesystem access stays in the shell" },
  { path = "std::fs::metadata", reason = "filesystem access stays in the shell" },
  { path = "std::fs::read", reason = "filesystem access stays in the shell" },
  { path = "std::fs::read_dir", reason = "filesystem access stays in the shell" },
  { path = "std::fs::read_link", reason = "filesystem access stays in the shell" },
  { path = "std::fs::read_to_string", reason = "filesystem access stays in the shell" },
  { path = "std::fs::remove_dir", reason = "filesystem access stays in the shell" },
  { path = "std::fs::remove_dir_all", reason = "filesystem access stays in the shell" },
  { path = "std::fs::remove_file", reason = "filesystem access stays in the shell" },
  { path = "std::fs::rename", reason = "filesystem access stays in the shell" },
  { path = "std::fs::set_permissions", reason = "filesystem access stays in the shell" },
  { path = "std::fs::soft_link", reason = "filesystem access stays in the shell" },
  { path = "std::fs::symlink_metadata", reason = "filesystem access stays in the shell" },
  { path = "std::fs::write", reason = "filesystem access stays in the shell" },
  { path = "std::path::Path::exists", reason = "filesystem access stays in the shell" },
  { path = "std::path::Path::try_exists", reason = "filesystem access stays in the shell" },
  { path = "std::fs::File::open", reason = "filesystem access stays in the shell" },
  { path = "std::fs::File::create", reason = "filesystem access stays in the shell" },
  { path = "std::fs::File::create_new", reason = "filesystem access stays in the shell" },
  { path = "std::fs::OpenOptions::open", reason = "filesystem access stays in the shell" },
  { path = "std::fs::DirBuilder::create", reason = "filesystem access stays in the shell" },
  { path = "std::thread::spawn", reason = "use the injected BlockingRuntime" },
  { path = "std::thread::Builder::spawn", reason = "use the injected BlockingRuntime" },
  { path = "tokio::spawn", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "tokio::task::spawn", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "tokio::spawn_blocking", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "tokio::task::spawn_blocking", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "tokio::runtime::Builder::new_current_thread", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "tokio::runtime::Builder::new_multi_thread", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "uuid::Uuid::new_v4", reason = "use the injected IdGen" },
  { path = "rand::thread_rng", reason = "randomness stays in the shell", allow-invalid = true },
  { path = "rand::rng", reason = "randomness stays in the shell", allow-invalid = true },
]

disallowed-types = [
  { path = "std::sync::OnceLock", reason = "no process-global lazy state in the functional core" },
  { path = "std::sync::LazyLock", reason = "no process-global lazy state in the functional core" },
  { path = "once_cell::sync::OnceCell", reason = "no process-global lazy state in the functional core", allow-invalid = true },
  { path = "once_cell::sync::Lazy", reason = "no process-global lazy state in the functional core", allow-invalid = true },
  { path = "once_cell::unsync::OnceCell", reason = "no process-global lazy state in the functional core", allow-invalid = true },
  { path = "once_cell::unsync::Lazy", reason = "no process-global lazy state in the functional core", allow-invalid = true },
  { path = "once_cell::race::OnceBox", reason = "no process-global lazy state in the functional core", allow-invalid = true },
  { path = "std::fs::File", reason = "filesystem access stays in the shell" },
  { path = "tokio::runtime::Runtime", reason = "the shell owns the runtime", allow-invalid = true },
  { path = "std::net::TcpStream", reason = "network access stays in the shell" },
  { path = "reqwest::Client", reason = "network access stays in the shell", allow-invalid = true },
  { path = "reqwest::blocking::Client", reason = "network access stays in the shell", allow-invalid = true },
]
```

`allow-invalid = true` keeps the file loading for paths this crate cannot
name yet. Adding the dependency and calling the path is still denied.

### CI denies the ban on every run

`.github/workflows/ci-required.yml` runs on `push` to `main`, on every
`pull_request`, and on `workflow_dispatch`. It has no path filter. Job
`build_test` has no `if`. These two steps also have no `if`, so they run
on every workflow run. Job `ci-required` requires `build_test` to succeed.

```text
rg -n -C 3 'cargo clippy \(schema_service_core functional core\)|Schema core statics gate' .github/workflows/ci-required.yml
```

```text
.github/workflows/ci-required.yml-93-          CLIPPY_FAMILY: ${{ steps.required-clippy-plan.outputs.family }}
.github/workflows/ci-required.yml-94-          CLIPPY_MODE: ${{ steps.required-clippy-plan.outputs.mode }}
.github/workflows/ci-required.yml-95-        run: bash scripts/ci/mini-gate-step.sh "cargo clippy (${CLIPPY_MODE}:${CLIPPY_FAMILY})" -- ruby scripts/ci/required-clippy-plan.rb --run "$CLIPPY_FAMILY"
.github/workflows/ci-required.yml:96:      - name: cargo clippy (schema_service_core functional core)
.github/workflows/ci-required.yml-97-        env:
.github/workflows/ci-required.yml-98-          CARGO_BUILD_JOBS: "2"
.github/workflows/ci-required.yml-99-        run: bash scripts/ci/mini-gate-step.sh "cargo clippy (schema_service_core)" -- cargo clippy --locked -p schema_service_core --lib --features schema-org-seed-data -- -D warnings -D clippy::disallowed_methods -D clippy::disallowed_types
.github/workflows/ci-required.yml:100:      - name: Schema core statics gate
.github/workflows/ci-required.yml:101:        run: bash scripts/ci/mini-gate-step.sh "Schema core statics gate" -- bash scripts/lints/lint-schema-core-statics.sh
.github/workflows/ci-required.yml-102-      - name: Assert fastembed is NOT compiled in the Mini gate lane
.github/workflows/ci-required.yml-103-        run: |
.github/workflows/ci-required.yml-104-          bash scripts/ci/mini-gate-step.sh "Assert fastembed is NOT compiled in the Mini gate lane" -- bash -euo pipefail -c '
```

The path-selected clippy step above those lines carries an `if`. The
functional-core step has no `if`. The same deny attribute is on the crate root,
so the heavy workspace clippy step also lints this crate under that deny.
That step is `cargo clippy --workspace --lib --bins --locked` with
`-D warnings` in job `full_workspace_check`, which `ci-required` also
requires, and that job's `if` matches every trigger of this workflow.

The statics gate is `scripts/lints/lint-schema-core-statics.sh`. It fails
the run when `static mut` or `lazy_static` appears under
`schema_service/crates/core`.

### Core source search

Rust sources only. `clippy.toml` names the banned paths, so a search of
the directory without `--glob '*.rs'` matches the ban list.

```text
rg -n --glob '*.rs' -e 'std::fs' -e 'tokio::runtime' -e 'Runtime::new' -e 'new_current_thread' -e 'new_multi_thread' -e '::now()' -e 'new_v4' -e 'static mut' -e 'lazy_static' schema_service/crates/core
```

No output. Exit status 1. The scope includes `schema_service/crates/core/tests`.

```text
bash scripts/lints/lint-schema-core-statics.sh
```

```text
OK: schema_service/crates/core has no static mut or lazy_static
```

Exit status 0.

### Dependency edge

```text
cargo tree -p schema_service_core -e normal --depth 1
```

```text
schema_service_core v0.1.0 (/workspace/schema_service/crates/core)
├── app_identity_crypto v0.1.0 (/workspace/app_identity_crypto)
├── async-trait v0.1.89 (proc-macro)
├── base64 v0.22.1
├── blake3 v1.8.5
├── ed25519-dalek v2.2.0
├── hmac v0.12.1
├── schema_core_resolver v0.1.0 (/workspace/schema_service/crates/schema_core_resolver)
├── schema_types v0.1.0 (/workspace/schema_service/crates/schema_types)
├── semver v1.0.28
├── serde v1.0.228
├── serde_json v1.0.149
├── sha2 v0.10.9
├── thiserror v1.0.69
├── tokio v1.51.1
├── tracing v0.1.44
└── uuid v1.23.0
```

Exit status 0. `chrono`, `rand`, and `reqwest` are absent from this edge.
The shell crate is absent from the normal edge.

```text
cargo tree -p schema_service_core -e features --depth 1
```

```text
schema_service_core v0.1.0 (/workspace/schema_service/crates/core)
├── app_identity_crypto feature "default"
├── base64 feature "default"
├── serde feature "default"
├── serde feature "derive"
├── ed25519-dalek feature "default"
├── ed25519-dalek feature "std"
├── semver feature "default"
├── sha2 feature "default"
├── serde_json feature "default"
├── serde_json feature "raw_value"
├── hmac feature "default"
├── thiserror feature "default"
├── async-trait feature "default"
├── blake3 feature "default"
├── schema_core_resolver feature "default"
├── schema_types feature "default"
├── tokio feature "macros"
├── tokio feature "sync"
├── tracing feature "default"
├── uuid feature "default"
└── uuid feature "v4"
[dev-dependencies]
└── schema_service_server_shared feature "default"
```

Exit status 0. The direct Tokio features are `macros` and `sync`.
`uuid` feature `v4` stays so `IdGen` can return `Uuid`. The canonical
tests depend on the shell; the library's normal graph does not.

`chrono` is still reachable transitively. The core crate does not name it.

```text
cargo tree -p schema_service_core -e normal -i chrono
```

```text
chrono v0.4.44
├── app_identity_crypto v0.1.0 (/workspace/app_identity_crypto)
│   ├── schema_service_core v0.1.0 (/workspace/schema_service/crates/core)
│   └── schema_types v0.1.0 (/workspace/schema_service/crates/schema_types)
│       └── schema_service_core v0.1.0 (/workspace/schema_service/crates/core)
└── schema_types v0.1.0 (/workspace/schema_service/crates/schema_types) (*)
```

Exit status 0. `schema_types` depends on `chrono` with the `clock` feature.
`app_identity_crypto` uses `chrono::DateTime`.

### Baseline

No schema configuration variable from the baseline's ambient list was set.

```text
cargo run --locked -p schema_core_baseline --bin schema-core-baseline | sha256sum
```

```text
bef2dce6f9cee89eb898de3d60a608f6db4f2b66cbd4063bb62b2492bc13e971  -
```

The pipeline exited 0. Cargo's stderr on this run ended with:

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.26s
     Running `target/debug/schema-core-baseline`
```

The `Finished` duration is this run. A later run of the same command on
this revision prints the same hash. That hash is
`bef2dce6f9cee89eb898de3d60a608f6db4f2b66cbd4063bb62b2492bc13e971`.

## Remaining gaps

The core Rust sources are clear of the searched calls. The surrounding
crates still read the operating-system clock, and two shell adapters have
no production caller.

`schema_types::clock` still reads the clock. `SystemClock` delegates to
these functions. The Clippy file is crate-local, so this crate is outside
the ban:

```text
schema_service/crates/schema_types/src/clock.rs:13:    SystemTime::now()
schema_service/crates/schema_types/src/clock.rs:21:    Utc::now()
schema_service/crates/schema_types/src/clock.rs:26:    Instant::now()
```

Other direct clock calls under `schema_service`, from
`rg -n --glob '*.rs' -e 'SystemTime::now' -e 'Instant::now' -e 'Utc::now' schema_service`:

```text
schema_service/crates/resolver_pack_publisher/src/bin/schema_resolver_pack_publish/embeddings.rs:96:        generated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
schema_service/crates/resolver_pack_publisher/src/bin/schema_resolver_pack_publish/commands.rs:84:    let issued_at = Utc::now();
schema_service/crates/resolver_pack_publisher/src/bin/schema_resolver_pack_publish/commands.rs:154:        timestamp: Utc::now(),
schema_service/crates/resolver_pack_publisher/src/bin/schema_resolver_pack_publish/commands.rs:200:        timestamp: Utc::now(),
schema_service/scripts/generate_validated_schema_org_seeds/src/snapshot.rs:36:    let now = Utc::now().to_rfc3339();
schema_service/crates/schema_service_s3/tests/s3/support.rs:86:        let number = SystemTime::now()
schema_service/scripts/generate_schema_org_classifications/src/main.rs:119:        generated_at: Utc::now().to_rfc3339(),
schema_service/crates/client/examples/schema_pow_live_probe/phases.rs:168:    let first_started = Instant::now();
schema_service/crates/client/examples/schema_pow_live_probe/phases.rs:190:    let repost_started = Instant::now();
schema_service/crates/client/examples/schema_pow_live_probe/negative.rs:26:        issued_at: Utc::now(),
schema_service/crates/client/src/publish_auth.rs:89:        issued_at: Utc::now(),
```

`schema_service_s3` `put_embedding` and the schema client
(`client_pow`, `live_resolver`, and the PoW probe model) call
`schema_types::clock::unix_secs`, which is `SystemTime::now`.

`FsResolverPackCache` and `HttpResolverPackStore` have no caller that
constructs them. `ResolverRuntime::new` is the injection point for the
cache and the object store, and nothing calls it.

The core still owns the registry maps, `InMemorySchemaPersistence`, and
`InMemorySchemaMutationGateQuotaStore`. Those are in-process values. The
shell swaps in S3 and DynamoDB at the Lambda, the backfill binary, and
the v2 registry proof.

`uuid` feature `v4` remains a direct feature so the port can return
`Uuid`. The call `Uuid::new_v4` is denied in the core and present only
in `SystemIds`. `chrono` remains a transitive dependency through
`schema_types` and `app_identity_crypto`.

## Scope

This file records the shell split on the revision above. It changes no
registry behavior and adds no test suite. It does not claim that
`schema_types`, the schema client, the S3 crate, or the pack publisher
are free of ambient clock reads. Re-run the commands on a later revision
before treating the transcripts as current.
